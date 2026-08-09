"""Client for the local BTC ticker server (bitcoin/server.js).

The Node server is the single source of truth: it computes the forecast
probability itself (exponentially-weighted drift with momentum decay,
multi-horizon volatility blend, Student-t tails, Kalshi-anchored settle
time) and publishes it at /api/trend under `model`. This module just
reads it so the Python side never duplicates that logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import requests

TICKER_URL = os.environ.get("TICKER_URL", "http://localhost:3001")


@dataclass
class Signal:
    price: float
    prob_up: float          # P(price above the benchmark at settlement), server-computed
    minutes_remaining: float
    drift_per_min: float
    vol_per_min: float
    sample_minutes: float
    rsi: float | None
    kalshi_prob: float | None      # market-implied P(above) for the same strike
    kalshi_ticker: str | None      # nearest strike market
    kalshi_close_ms: int | None

    @property
    def edge(self) -> float | None:
        """Model minus market, in probability points. Positive = model more bullish."""
        if self.kalshi_prob is None:
            return None
        return self.prob_up - self.kalshi_prob


def fetch_signal(base_url: str = TICKER_URL, timeout: float = 5.0) -> Signal | None:
    """None while the ticker is still warming up (needs ~60s of data)."""
    trend = requests.get(f"{base_url}/api/trend", timeout=timeout).json()
    model = trend.get("model")
    if trend.get("insufficient") or not model:
        return None

    kalshi = trend.get("kalshi") or {}
    available = kalshi.get("available")
    nearest = kalshi.get("nearestStrike") or {}

    # market prob for the SAME strike the model targets (bid/ask mid)
    market_prob = None
    if available and nearest.get("bid") is not None and nearest.get("ask") is not None:
        market_prob = (nearest["bid"] + nearest["ask"]) / 2

    return Signal(
        price=trend["currentPrice"],
        prob_up=model["probAbove"],
        minutes_remaining=model["minutesRemaining"],
        drift_per_min=trend["driftPerMin"],
        vol_per_min=trend["volPerMin"],
        sample_minutes=trend["sampleMinutes"],
        rsi=trend.get("rsi"),
        kalshi_prob=market_prob,
        kalshi_ticker=nearest.get("ticker") if available else None,
        kalshi_close_ms=kalshi.get("closeTime") if available else None,
    )


def fetch_price(base_url: str = TICKER_URL, timeout: float = 5.0) -> float | None:
    snap = requests.get(f"{base_url}/api/latest", timeout=timeout).json()
    return snap.get("average")
