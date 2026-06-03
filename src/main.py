"""
fb-execution-futures: Executa ordens de Futures na Binance.

Fluxo:
  trade.order.futures → para cada ordem:
    → configura margem ISOLATED
    → configura alavancagem dinâmica
    → market BUY/LONG
    → publica trade.executed.futures
  trade.close.futures → para cada encerramento:
    → market SELL/SHORT (reduceOnly=True)
"""
import asyncio, logging, os, json, ccxt, nats, base64
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-execution-futures")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
FUTURES_MAX_POSITIONS = int(os.getenv("FUTURES_MAX_POSITIONS", "5"))


class FuturesExecutionEngine:
    def __init__(self):
        self.nc = None
        self.js = None
        self.kv = None
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future"
            }
        })

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        self.kv = await self.js.key_value("active_positions")
        logger.info(f"NATS conectado: {NATS_URL}")

    async def _kv_key(self, symbol):
        return base64.b64encode(symbol.encode()).decode()

    async def count_futures_positions(self):
        """Conta posições ativas de Futures no KV store."""
        try:
            keys = await self.kv.keys()
            count = 0
            for key in keys:
                entry = await self.kv.get(key)
                if entry:
                    val = json.loads(entry.value.decode())
                    if val.get("is_futures"):
                        count += 1
            return count
        except Exception:
            return 0

    async def execute_futures_order(self, order):
        symbol = order["symbol"]
        quantity = order["quantity"]
        sl_price = order.get("sl_price", 0.0)
        tp_price = order.get("tp_price", 0.0)
        entry_price = order["entry_price"]
        leverage = int(order.get("leverage", 2))
        direction = order.get("direction", "LONG")
        is_short = direction == "SHORT"
        side = "sell" if is_short else "buy"

        # Verifica se já tem posição aberta no KV store
        key = await self._kv_key(symbol)
        try:
            await self.kv.get(key)
            logger.info(f"  {symbol}: já possui posição aberta no KV store → ignorando")
            return None
        except Exception:
            pass

        # Verifica máximo de posições de Futures
        active_futures = await self.count_futures_positions()
        if active_futures >= FUTURES_MAX_POSITIONS:
            logger.info(f"  {symbol}: máximo de posições Futures ({FUTURES_MAX_POSITIONS}) atingido → ignorando")
            return None

        if DRY_RUN:
            logger.info(f"  [DRY RUN FUTURES] {symbol}: {side.upper()} {quantity} @ ~{entry_price} (Alavancagem {leverage}x) SL={sl_price} TP={tp_price}")
            pos_data = {
                "symbol": symbol,
                "quantity": quantity,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "entry_time": __import__('time').time(),
                "is_futures": True,
                "leverage": leverage,
                "direction": direction,
            }
            await self.kv.put(key, json.dumps(pos_data).encode())
            return {
                "symbol": symbol,
                "status": "dry_run",
                "quantity": quantity,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "is_futures": True,
                "leverage": leverage,
                "direction": direction,
                "tier": order.get("tier"),
                "strategy": order.get("strategy"),
                "score": order.get("score"),
                "rsi": order.get("rsi"),
            }

        try:
            logger.info(f"  {symbol}: Configurando margem ISOLATED...")
            try:
                self.exchange.set_margin_mode("ISOLATED", symbol)
            except Exception as e:
                logger.info(f"  {symbol}: Margem isolada já configurada ou erro: {e}")

            logger.info(f"  {symbol}: Configurando alavancagem para {leverage}x...")
            try:
                self.exchange.set_leverage(leverage, symbol)
            except Exception as e:
                logger.error(f"  {symbol}: Falha ao configurar alavancagem: {e}")

            logger.info(f"  {symbol}: Executando market {side.upper()} {quantity}...")
            order_result = self.exchange.create_order(symbol, "market", side, quantity)
            filled_price = float(order_result.get("average", order_result.get("price", entry_price)))
            filled_qty = float(order_result.get("filled", quantity))
            logger.info(f"  {symbol}: {side.upper()} FUTURES executado {filled_qty} @ {filled_price}")

            import time as _time
            pos_data = {
                "symbol": symbol,
                "quantity": filled_qty,
                "entry_price": filled_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "entry_time": _time.time(),
                "is_futures": True,
                "leverage": leverage,
                "direction": direction,
            }
            await self.kv.put(key, json.dumps(pos_data).encode())

            return {
                "symbol": symbol,
                "status": "executed",
                "quantity": filled_qty,
                "entry_price": filled_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "is_futures": True,
                "leverage": leverage,
                "direction": direction,
                "tier": order.get("tier"),
                "strategy": order.get("strategy"),
                "score": order.get("score"),
                "rsi": order.get("rsi"),
                "order_id": order_result.get("id")
            }

        except ccxt.InsufficientFunds as e:
            logger.warning(f"  [REACTIVE FALLBACK] Saldo insuficiente no Futures para {symbol}. Republicando para SPOT: {e}")
            spot_order = {
                "symbol": symbol,
                "tier": order.get("tier", ""),
                "strategy": order.get("strategy", ""),
                "direction": "LONG",
                "score": order.get("score", 0.0),
                "rsi": order.get("rsi", 0),
                "entry_price": entry_price,
                "quantity": quantity,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "is_futures": False,
                "leverage": 1
            }
            try:
                await self.js.publish("trade.order", json.dumps([spot_order]).encode())
                logger.info(f"  [REACTIVE FALLBACK] Ordem de Spot para {symbol} publicada no NATS.")
            except Exception as publish_err:
                logger.error(f"  [REACTIVE FALLBACK] Falha ao publicar fallback de Spot: {publish_err}")
            return None

        except Exception as e:
            logger.error(f"  {symbol}: Erro ao executar ordem de Futures: {e}")
            return None

    async def process_futures_orders(self, msg):
        try:
            orders = json.loads(msg.data.decode())
            logger.info(f"Processando {len(orders)} ordens de Futures (dry_run={DRY_RUN})")
            results = []

            for order in orders:
                result = await self.execute_futures_order(order)
                if result:
                    results.append(result)

            if results:
                payload = json.dumps(results).encode()
                await self.js.publish("trade.executed.futures", payload)
                logger.info(f"Publicadas {len(results)} execuções em trade.executed.futures")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar ordens: {e}")

    async def close_futures_position(self, msg):
        try:
            data = json.loads(msg.data.decode())
            symbol = data["symbol"]
            key = await self._kv_key(symbol)

            try:
                entry = await self.kv.get(key)
                pos = json.loads(entry.value.decode())
            except Exception:
                logger.warning(f"  {symbol}: Tentativa de fechar posição inexistente no KV store.")
                await msg.ack()
                return

            if not pos.get("is_futures"):
                logger.warning(f"  {symbol}: Posição no KV não é de Futures. Ignorando.")
                await msg.ack()
                return

            qty = pos["quantity"]

            if DRY_RUN:
                logger.info(f"  [DRY RUN FUTURES] Fechando posição {symbol} ({qty}) com Reduce-Only")
                await self.kv.delete(key)
                await msg.ack()
                return

            logger.info(f"  {symbol}: Fechando posição de Futures de {qty}...")
            # Ordem de venda de fechamento com reduceOnly=True
            sell_order = self.exchange.create_order(symbol, "market", "sell", qty, params={"reduceOnly": True})
            logger.info(f"  {symbol}: Posição fechada com sucesso. Order ID: {sell_order.get('id')}")

            await self.kv.delete(key)
            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao fechar posição de Futures: {e}")

    async def run(self):
        await self.connect_nats()
        # Subscreve para abertura de ordens
        await self.js.subscribe("trade.order.futures", durable="EXECUTION_FUTURES_WORKER",
                                 cb=self.process_futures_orders, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        # Subscreve para fechamento de ordens
        await self.js.subscribe("trade.close.futures", durable="EXECUTION_FUTURES_CLOSE_WORKER",
                                 cb=self.close_futures_position, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))

        mode = "DRY RUN" if DRY_RUN else "PRODUÇÃO REAL"
        logger.info(f"fb-execution-futures online [{mode}] (max_positions={FUTURES_MAX_POSITIONS})")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    engine = FuturesExecutionEngine()
    asyncio.run(engine.run())
