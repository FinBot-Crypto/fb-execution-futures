# fb-execution-futures

Execução de ordens de Futures na Binance (isolated margin, dynamic leverage, reduceOnly close).

## Fluxo

```
trade.order.futures (fb-trade-decision)
  → fb-execution-futures
    → configura margem ISOLATED
    → configura alavancagem dinâmica (2x, 3x, 5x)
    → envia market BUY/LONG
    → persiste posição no KV store (active_positions)
    → trade.executed.futures

trade.close.futures (fb-position-management)
  → fb-execution-futures
    → envia market SELL/SHORT com reduceOnly: True
    → remove posição do KV store
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `NATS_URL` | `nats://crypto-nats:4222` | Servidor NATS |
| `DRY_RUN` | `true` | `true` = simulação, `false` = ordens reais |
| `FUTURES_MAX_POSITIONS` | `5` | Máximo de posições simultâneas de Futures |
| `BINANCE_API_KEY` | | Chave API Binance |
| `BINANCE_API_SECRET` | | Secret API Binance |
