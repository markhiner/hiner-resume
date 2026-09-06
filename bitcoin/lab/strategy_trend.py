"""Lumibot strategy driven by the ticker's trend signal.

The idea, mechanized: when the drift/volatility model says BTC is likely
rising, hold BTC; when it says falling, hold cash; in between, stay put.
Same class runs a historical backtest and live paper trading — that
backtest/live parity is the reason lumibot is here.

  python strategy_trend.py --backtest            # Yahoo daily data, no keys
  python strategy_trend.py                       # paper trade on Alpaca

Live mode reads the signal from the running ticker server (TICKER_URL,
default localhost:3001) and needs Alpaca paper keys in the environment
(ALPACA_API_KEY / ALPACA_API_SECRET — free account, paper=True is forced
here). Backtests can't see the live server, so the same drift/vol math is
recomputed from historical bars via `bar_signal`.
"""

from __future__ import annotations

import math
import os

ENTER_ABOVE = 0.62   # go long when P(up) clears this
EXIT_BELOW = 0.45    # back to cash when it drops under this
LOOKBACK_BARS = 20


def bar_signal(closes: list[float]) -> float:
    """P(next bar up) from drift/vol regression over closes — the same
    random-walk projection server.js runs on second ticks, applied to bars."""
    n = len(closes)
    if n < 5:
        return 0.5
    xs = range(n)
    sx, sy = sum(xs), sum(closes)
    sxy = sum(x * y for x, y in zip(xs, closes))
    sxx = sum(x * x for x in xs)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.5
    slope = (n * sxy - sx * sy) / denom
    rets = [closes[i] - closes[i - 1] for i in range(1, n)]
    mean = sum(rets) / len(rets)
    vol = math.sqrt(sum((r - mean) ** 2 for r in rets) / len(rets)) or 1.0
    z = slope / vol
    prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
    return min(max(prob, 0.03), 0.97)


try:
    from lumibot.entities import Asset
    from lumibot.strategies import Strategy
except ImportError:  # allow importing bar_signal without lumibot installed
    Strategy = object
    Asset = None


class TickerTrendStrategy(Strategy):
    parameters = {"use_live_signal": True}

    def initialize(self):
        self.sleeptime = "10M" if not self.is_backtesting else "1D"
        self.set_market("24/7")
        self.base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        self.quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    def _prob_up(self) -> float:
        if not self.is_backtesting and self.parameters["use_live_signal"]:
            try:
                from ticker_signal import fetch_signal

                sig = fetch_signal()
                if sig is not None:
                    self.log_message(
                        f"live signal: price {sig.price:,.0f}, P(up) {sig.prob_up:.0%}, "
                        f"kalshi {sig.kalshi_prob if sig.kalshi_prob is None else f'{sig.kalshi_prob:.0%}'}"
                    )
                    return sig.prob_up
            except Exception as e:
                self.log_message(f"ticker server unreachable ({e}); falling back to bars")
        bars = self.get_historical_prices(self.base, LOOKBACK_BARS, quote=self.quote)
        if bars is None or bars.df.empty:
            return 0.5
        return bar_signal(list(bars.df["close"]))

    def on_trading_iteration(self):
        prob = self._prob_up()
        position = self.get_position(self.base)
        holding = position is not None and float(position.quantity) > 0

        if prob >= ENTER_ABOVE and not holding:
            price = self.get_last_price(self.base, quote=self.quote)
            if price:
                qty = round((self.get_cash() * 0.95) / price, 6)
                if qty > 0:
                    self.submit_order(
                        self.create_order(self.base, qty, "buy", quote=self.quote)
                    )
                    self.log_message(f"BUY {qty} BTC @ ~{price:,.0f} (P(up)={prob:.0%})")
        elif prob <= EXIT_BELOW and holding:
            self.submit_order(
                self.create_order(self.base, position.quantity, "sell", quote=self.quote)
            )
            self.log_message(f"SELL all (P(up)={prob:.0%})")


if __name__ == "__main__":
    import argparse
    import datetime as dt

    ap = argparse.ArgumentParser()
    ap.add_argument("--backtest", action="store_true")
    ap.add_argument("--years", type=int, default=2)
    args = ap.parse_args()

    if args.backtest:
        from lumibot.backtesting import YahooDataBacktesting

        end = dt.datetime.now()
        start = end - dt.timedelta(days=365 * args.years)
        TickerTrendStrategy.backtest(
            YahooDataBacktesting,
            start,
            end,
            benchmark_asset="BTC-USD",
            quote_asset=Asset("USD", asset_type=Asset.AssetType.FOREX),
        )
    else:
        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        creds = {
            "API_KEY": os.environ["ALPACA_API_KEY"],
            "API_SECRET": os.environ["ALPACA_API_SECRET"],
            "PAPER": True,  # paper only by default — flip deliberately, not here
        }
        trader = Trader()
        trader.add_strategy(TickerTrendStrategy(broker=Alpaca(creds)))
        trader.run_all()
