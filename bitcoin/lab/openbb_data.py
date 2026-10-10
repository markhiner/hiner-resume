"""OpenBB data helpers for the BTC lab.

The live ticker only keeps 24h of history in memory; OpenBB is the
long-memory side — historical candles for backtesting the trend strategy,
and a quick cross-asset context snapshot.

  python openbb_data.py candles --days 365 --out btc_daily.csv
  python openbb_data.py candles --days 30 --interval 1h --out btc_hourly.csv
  python openbb_data.py context
"""

from __future__ import annotations

import argparse
import datetime as dt


def get_candles(days: int, interval: str = "1d"):
    from openbb import obb

    start = (dt.date.today() - dt.timedelta(days=days)).isoformat()
    out = obb.crypto.price.historical(
        symbol="BTCUSD", start_date=start, interval=interval, provider="yfinance"
    )
    return out.to_dataframe()


def cmd_candles(args):
    df = get_candles(args.days, args.interval)
    df.to_csv(args.out)
    print(f"{len(df)} rows -> {args.out}")
    print(df.tail(3))


def cmd_context(_args):
    from openbb import obb

    start = (dt.date.today() - dt.timedelta(days=35)).isoformat()
    print(f"{'asset':<10} {'last':>12} {'30d':>8}")
    for label, fetch in [
        ("BTC", lambda: obb.crypto.price.historical(symbol="BTCUSD", start_date=start, provider="yfinance")),
        ("ETH", lambda: obb.crypto.price.historical(symbol="ETHUSD", start_date=start, provider="yfinance")),
        ("S&P 500", lambda: obb.equity.price.historical(symbol="SPY", start_date=start, provider="yfinance")),
        ("Gold", lambda: obb.equity.price.historical(symbol="GLD", start_date=start, provider="yfinance")),
    ]:
        try:
            df = fetch().to_dataframe()
            last, first = df["close"].iloc[-1], df["close"].iloc[0]
            print(f"{label:<10} {last:>12,.2f} {100 * (last / first - 1):>+7.1f}%")
        except Exception as e:
            print(f"{label:<10} unavailable ({e})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    c = sub.add_parser("candles", help="export historical BTC candles to CSV")
    c.add_argument("--days", type=int, default=365)
    c.add_argument("--interval", default="1d", help="1d, 1h, 1m (provider-dependent)")
    c.add_argument("--out", default="btc_history.csv")
    c.set_defaults(func=cmd_candles)

    x = sub.add_parser("context", help="quick cross-asset 30d snapshot")
    x.set_defaults(func=cmd_context)

    args = ap.parse_args()
    args.func(args)
