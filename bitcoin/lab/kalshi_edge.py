"""Kalshi edge monitor for the hourly BTC market (KXBTCD).

Compares the ticker's model probability against the Kalshi market's
implied probability every poll, and when the model sees positive expected
value after fees, records a trade. PAPER BY DEFAULT: intents go to a local
SQLite ledger only. `--live` additionally places a real (small, capped)
limit order and requires Kalshi API credentials.

After each hour settles, `score` fetches results and reports hit rate,
P&L, and Brier scores for model vs market — run it for a while in paper
mode first: if the model's Brier score isn't beating the market's, there
is no edge and live mode is pointless.

Usage:
  python kalshi_edge.py run              # paper monitor (default)
  python kalshi_edge.py run --live       # real orders, needs credentials
  python kalshi_edge.py score            # settle + calibration report

Risk guardrails (borrowed from ryanfrigo/kalshi-ai-trading-bot):
quarter-Kelly sizing, per-trade contract cap, minimum edge threshold,
one open intent per market.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sqlite3
import time
from pathlib import Path

from kalshi_client import KalshiClient, estimated_fee
from ticker_signal import fetch_signal

DB_PATH = Path(__file__).parent / "edge_ledger.sqlite"

MIN_EDGE = 0.05          # model vs market gap before we act (prob points)
MIN_EV = 0.02            # required EV per contract after fees, in dollars
MAX_CONTRACTS = 10       # hard cap per trade
KELLY_FRACTION = 0.25    # quarter-Kelly
PAPER_BANKROLL = 100.0   # notional bankroll for paper sizing, dollars
MIN_SAMPLE_MINUTES = 10  # don't trust the model on a thin sample
POLL_SECONDS = 30
FEE_MULTIPLIER = 0.07


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS intents (
            id INTEGER PRIMARY KEY,
            created_at TEXT NOT NULL,
            market_ticker TEXT NOT NULL,
            close_time TEXT,
            side TEXT NOT NULL,             -- yes|no
            price REAL NOT NULL,            -- limit price paid, dollars
            contracts INTEGER NOT NULL,
            model_prob REAL NOT NULL,       -- P(side wins) per model
            market_prob REAL NOT NULL,      -- P(side wins) per market mid
            ev_per_contract REAL NOT NULL,
            live INTEGER NOT NULL DEFAULT 0,
            order_id TEXT,
            result TEXT,                    -- yes|no once settled
            pnl REAL
        )"""
    )
    return conn


def kelly_contracts(p: float, price: float, bankroll: float) -> int:
    """Fractional Kelly for a binary contract bought at `price`."""
    if price <= 0 or price >= 1:
        return 0
    b = (1 - price) / price  # net odds
    f = (p * (b + 1) - 1) / b
    if f <= 0:
        return 0
    stake = bankroll * f * KELLY_FRACTION
    return max(0, min(MAX_CONTRACTS, int(stake / price)))


def evaluate(sig) -> dict | None:
    """Return a trade intent when there's EV after fees, else None."""
    if sig is None or sig.kalshi_prob is None or sig.kalshi_ticker is None:
        return None
    if sig.sample_minutes < MIN_SAMPLE_MINUTES:
        return None
    edge = sig.prob_up - sig.kalshi_prob
    if abs(edge) < MIN_EDGE:
        return None

    # model more bullish -> buy YES at ~market prob (approximate fill at mid
    # + a cent of slippage); more bearish -> buy NO.
    if edge > 0:
        side, price, p_win = "yes", min(sig.kalshi_prob + 0.01, 0.99), sig.prob_up
    else:
        side, price, p_win = "no", min(1 - sig.kalshi_prob + 0.01, 0.99), 1 - sig.prob_up

    contracts = kelly_contracts(p_win, price, PAPER_BANKROLL)
    if contracts == 0:
        return None
    fee = estimated_fee(price, contracts, FEE_MULTIPLIER) / contracts
    ev = p_win - price - fee
    if ev < MIN_EV:
        return None

    return {
        "market_ticker": sig.kalshi_ticker,
        "side": side,
        "price": round(price, 2),
        "contracts": contracts,
        "model_prob": round(p_win, 4),
        "market_prob": round(sig.kalshi_prob if side == "yes" else 1 - sig.kalshi_prob, 4),
        "ev_per_contract": round(ev, 4),
        "close_time": dt.datetime.fromtimestamp(sig.kalshi_close_ms / 1000, dt.timezone.utc).isoformat()
        if sig.kalshi_close_ms
        else None,
    }


def run(live: bool):
    conn = db()
    client = KalshiClient()
    if live and not client.can_trade:
        raise SystemExit("--live requires KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH")
    mode = "LIVE" if live else "paper"
    print(f"Edge monitor running ({mode}); ledger: {DB_PATH}")

    while True:
        try:
            sig = fetch_signal()
            intent = evaluate(sig)
            if sig:
                k = f"{sig.kalshi_prob:.0%}" if sig.kalshi_prob is not None else "n/a"
                print(
                    f"{dt.datetime.now():%H:%M:%S}  price {sig.price:,.0f}  "
                    f"model {sig.prob_up:.0%}  kalshi {k}  "
                    f"{'-> ' + intent['side'].upper() + ' x' + str(intent['contracts']) if intent else ''}"
                )
            if intent:
                already = conn.execute(
                    "SELECT 1 FROM intents WHERE market_ticker=? AND result IS NULL",
                    (intent["market_ticker"],),
                ).fetchone()
                if not already:
                    order_id = None
                    if live:
                        resp = client.place_limit_order(
                            intent["market_ticker"],
                            intent["side"],
                            "buy",
                            intent["contracts"],
                            int(round(intent["price"] * 100)),
                        )
                        order_id = (resp.get("order") or {}).get("order_id")
                        print(f"  placed order {order_id}")
                    conn.execute(
                        """INSERT INTO intents (created_at, market_ticker, close_time, side, price,
                           contracts, model_prob, market_prob, ev_per_contract, live, order_id)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                        (
                            dt.datetime.now(dt.timezone.utc).isoformat(),
                            intent["market_ticker"],
                            intent["close_time"],
                            intent["side"],
                            intent["price"],
                            intent["contracts"],
                            intent["model_prob"],
                            intent["market_prob"],
                            intent["ev_per_contract"],
                            1 if live else 0,
                            order_id,
                        ),
                    )
                    conn.commit()
        except KeyboardInterrupt:
            print("\nstopped")
            return
        except Exception as e:
            print(f"error: {e}")
        time.sleep(POLL_SECONDS)


def score():
    conn = db()
    client = KalshiClient()
    rows = conn.execute(
        "SELECT id, market_ticker, side, price, contracts, model_prob, market_prob "
        "FROM intents WHERE result IS NULL"
    ).fetchall()
    for row_id, ticker, side, price, contracts, _mp, _kp in rows:
        m = client.market(ticker)
        result = m.get("result")
        if result not in ("yes", "no"):
            continue  # not settled yet
        won = result == side
        fee = estimated_fee(price, contracts, FEE_MULTIPLIER)
        pnl = contracts * ((1 - price) if won else -price) - fee
        conn.execute("UPDATE intents SET result=?, pnl=? WHERE id=?", (result, pnl, row_id))
    conn.commit()

    settled = conn.execute(
        "SELECT side, price, contracts, model_prob, market_prob, result, pnl, live FROM intents "
        "WHERE result IS NOT NULL"
    ).fetchall()
    if not settled:
        print("Nothing settled yet.")
        return
    wins = sum(1 for s in settled if s[5] == s[0])
    pnl = sum(s[6] or 0 for s in settled)
    brier_model = sum((s[3] - (1 if s[5] == s[0] else 0)) ** 2 for s in settled) / len(settled)
    brier_market = sum((s[4] - (1 if s[5] == s[0] else 0)) ** 2 for s in settled) / len(settled)
    print(f"settled: {len(settled)}   wins: {wins} ({wins / len(settled):.0%})   pnl: ${pnl:+.2f}")
    print(f"Brier score  model {brier_model:.4f}  vs market {brier_market:.4f} "
          f"({'model better' if brier_model < brier_market else 'MARKET better — no edge'})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["run", "score"])
    ap.add_argument("--live", action="store_true", help="place real Kalshi orders (needs credentials)")
    args = ap.parse_args()
    if args.command == "run":
        run(live=args.live)
    else:
        score()
