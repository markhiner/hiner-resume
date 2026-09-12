# BTC Lab — lumibot + OpenBB + Kalshi

Python companion to the Node ticker (`../server.js`). The ticker stays the
single source of live truth; these tools consume its API and add three
things it can't do alone: historical data, simulated/real execution, and a
prediction-market reality check.

```
ticker server (Node, :3001)
  ├─ /api/trend ──────────► ticker_signal.py ──► strategy_trend.py (lumibot)
  │                                    │
  │                                    └──────► kalshi_edge.py ──► Kalshi
  └─ /api/kalshi ◄── Kalshi public API          (paper ledger / live orders)

openbb_data.py ──► historical candles / context (research + backtests)
```

## Setup (on the iMac)

```bash
cd bitcoin/lab
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Heavy install (~a few hundred MB — OpenBB pulls a lot). Only `requests`
is needed for `kalshi_edge.py` paper mode, so `pip install requests
cryptography` gets you the Kalshi loop alone if you want to start light.

## The pieces

### `kalshi_edge.py` — is the model actually any good?

Kalshi's `KXBTCD` series ("Bitcoin price above/below at {hour} ET")
settles at the top of the hour — the *exact* question the ticker's Next
Hour Outlook answers. Each strike's price is the market's probability, so
model and market are directly comparable, and the UI now shows both.

The monitor polls the ticker, and whenever the model disagrees with the
market by ≥5 points *and* there's positive expected value after Kalshi's
quadratic fee, it logs a quarter-Kelly-sized trade intent to a SQLite
ledger. Paper by default — no keys, no orders:

```bash
python kalshi_edge.py run       # watch + log paper intents
python kalshi_edge.py score     # after hours settle: hit rate, P&L, Brier
```

`score` is the point of the whole exercise: it compares the model's Brier
score against the market's. **Run paper mode for at least a few days
first.** If the market's Brier score is lower (it usually is — hourly BTC
markets are competitive), the model has no edge and live trading would
just donate fees.

Live mode (`run --live`) places small capped limit orders and requires a
Kalshi API key: kalshi.com → Settings → API keys, then

```bash
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_PATH="$HOME/.kalshi/kalshi_private_key"
```

Guardrails baked in (patterned on ryanfrigo/kalshi-ai-trading-bot, MIT):
quarter-Kelly sizing off a small notional bankroll, 10-contract cap per
trade, one open position per market, minimum-edge and minimum-EV gates,
and a 10-minute minimum model warm-up.

### `strategy_trend.py` — the trend signal as a lumibot strategy

Turns the outlook into a rule: long BTC when P(up) ≥ 62%, cash when ≤ 45%.
The same class backtests and paper-trades (lumibot's whole value):

```bash
python strategy_trend.py --backtest --years 2   # Yahoo daily data, no keys
python strategy_trend.py                        # live paper via Alpaca
```

Live mode reads `/api/trend` from the running ticker server and needs
free Alpaca paper keys (`ALPACA_API_KEY` / `ALPACA_API_SECRET`); it is
pinned to `PAPER: True`. Backtests recompute the same drift/vol math from
historical bars (`bar_signal`) since the live server can't see the past.

### `openbb_data.py` — long memory

The ticker keeps 24h; OpenBB fetches years.

```bash
python openbb_data.py candles --days 365 --out btc_daily.csv
python openbb_data.py candles --days 30 --interval 1h --out btc_hourly.csv
python openbb_data.py context     # BTC/ETH/SPY/GLD 30d snapshot
```

Note OpenBB is AGPLv3 — fine for personal use; relevant only if this repo
ever ships as part of a commercial product.

### `kalshi_client.py` / `ticker_signal.py`

Shared plumbing: a minimal Kalshi REST client (public data unauthenticated;
RSA-PSS request signing for trading, same scheme as the frigo bot) and the
reader for the ticker server's JSON API.

## Honest status

- Kalshi public-data path and the ticker integration were exercised live
  (real KXBTCD quotes) when this was built.
- `strategy_trend.py` and `openbb_data.py` are syntax-checked but their
  lumibot/OpenBB code paths need the real pip install to run — expect the
  usual first-run friction (Yahoo rate limits, provider quirks).
- Live order placement (`--live`) was written against Kalshi's documented
  API but has NOT been fired with real credentials. First live run: watch
  it place one order, small.
- Nothing here is financial advice. Paper first; `score` decides.
