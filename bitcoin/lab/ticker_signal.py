"""Client for the local BTC ticker server (bitcoin/server.js).

The Node server does the heavy lifting — live Coinbase+Bitstamp blend,
drift/volatility regression, Kalshi polling. This module just reads its
JSON API so the Python side never duplicates that logic.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

import requests

TICKER_URL = os.environ.get("TICKER_URL", "http://localhost:3001")

# Match the UI's clamp: a thin sample saturates the CDF and 100% is never honest.
PROB_CLAMP = (0.03, 0.97)


def _normal_cdf(z: float) -> float:
    return 0.5 * (1 + math.erf(z / math.sqrt(2)))


@dataclass
class Signal:
    price: float
    prob_up: float          # P(price above current at top of next hour), clamped
    minutes_remaining: float
    drift_per_min: float
    vol_per_min: float
    sample_minutes: float
    rsi: float | None
    kalshi_prob: float | None      # market-implied P(above current price)
    kalshi_ticker: str | None      # nearest strike market
    kalshi_close_ms: int | None

    @property
    def edge(self) -> float | None:
        """Model minus market, in probability points. Positive = model more bullish."""
        if self.kalshi_prob is None:
            return None
        return self.prob_up - self.kalshi_prob


def minutes_to_top_of_hour() -> float:
    import datetime as dt

    now = dt.datetime.now()
    m = 60 - now.minute - now.second / 60
    return m if m > 0 else 60.0


def fetch_signal(base_url: str = TICKER_URL, timeout: float = 5.0) -> Signal | None:
    """None while the ticker is still warming up (needs ~60s of data)."""
    trend = requests.get(f"{base_url}/api/trend", timeout=timeout).json()
    if trend.get("insufficient"):
        return None

    mins = minutes_to_top_of_hour()
    drift = trend["driftPerMin"] * mins
    std = trend["volPerMin"] * math.sqrt(max(mins, 0.01))
    prob = _normal_cdf(drift / std) if std > 0 else 0.5
    prob = min(max(prob, PROB_CLAMP[0]), PROB_CLAMP[1])

    kalshi = trend.get("kalshi") or {}
    available = kalshi.get("available")
    nearest = kalshi.get("nearestStrike") or {}

    return Signal(
        price=trend["currentPrice"],
        prob_up=prob,
        minutes_remaining=mins,
        drift_per_min=trend["driftPerMin"],
        vol_per_min=trend["volPerMin"],
        sample_minutes=trend["sampleMinutes"],
        rsi=trend.get("rsi"),
        kalshi_prob=kalshi.get("impliedProbAbove") if available else None,
        kalshi_ticker=nearest.get("ticker") if available else None,
        kalshi_close_ms=kalshi.get("closeTime") if available else None,
    )


def fetch_price(base_url: str = TICKER_URL, timeout: float = 5.0) -> float | None:
    snap = requests.get(f"{base_url}/api/latest", timeout=timeout).json()
    return snap.get("average")
