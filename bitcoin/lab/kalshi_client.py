"""Minimal Kalshi API client.

Public market data needs no credentials. Trading requires an API key
created at kalshi.com -> Settings -> API keys: set KALSHI_API_KEY_ID and
KALSHI_PRIVATE_KEY_PATH (the downloaded RSA private key) in the
environment. Request signing follows Kalshi's scheme: RSA-PSS/SHA256 over
"{timestamp_ms}{METHOD}{path}" in the KALSHI-ACCESS-* headers.

Patterned on ryanfrigo/kalshi-ai-trading-bot's client (MIT), stripped to
what the edge monitor needs.
"""

from __future__ import annotations

import base64
import datetime as dt
import os
import time

import requests

BASE = "https://api.elections.kalshi.com"
API_PREFIX = "/trade-api/v2"
BTC_HOURLY_SERIES = "KXBTCD"


class KalshiClient:
    def __init__(self, key_id: str | None = None, private_key_path: str | None = None):
        self.key_id = key_id or os.environ.get("KALSHI_API_KEY_ID")
        self.private_key_path = private_key_path or os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        self._private_key = None
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"

    # ── auth ──

    @property
    def can_trade(self) -> bool:
        return bool(self.key_id and self.private_key_path and os.path.exists(self.private_key_path))

    def _load_key(self):
        if self._private_key is None:
            from cryptography.hazmat.primitives import serialization

            with open(self.private_key_path, "rb") as f:
                self._private_key = serialization.load_pem_private_key(f.read(), password=None)
        return self._private_key

    def _sign_headers(self, method: str, path: str) -> dict:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding

        ts = str(int(time.time() * 1000))
        msg = f"{ts}{method}{path}".encode()
        sig = self._load_key().sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    def _request(self, method: str, path: str, *, params=None, json=None, auth=False):
        url = BASE + API_PREFIX + path
        headers = {}
        if auth:
            if not self.can_trade:
                raise RuntimeError(
                    "Kalshi credentials missing: set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH"
                )
            headers = self._sign_headers(method, API_PREFIX + path)
        for attempt in range(3):
            resp = self.session.request(method, url, params=params, json=json, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(1 + attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    # ── public market data ──

    def open_events(self, series_ticker: str = BTC_HOURLY_SERIES) -> list[dict]:
        return self._request("GET", "/events", params={"series_ticker": series_ticker, "status": "open", "limit": 20}).get("events", [])

    def event_markets(self, event_ticker: str, status: str | None = "open") -> list[dict]:
        params = {"event_ticker": event_ticker, "limit": 200}
        if status:
            params["status"] = status
        return self._request("GET", "/markets", params=params).get("markets", [])

    def market(self, ticker: str) -> dict:
        return self._request("GET", f"/markets/{ticker}").get("market", {})

    def next_hourly_event(self, series_ticker: str = BTC_HOURLY_SERIES) -> tuple[str, dt.datetime] | None:
        """The open event with the soonest future close (the live hourly)."""
        best = None
        now = dt.datetime.now(dt.timezone.utc)
        for ev in self.open_events(series_ticker):
            markets = self._request(
                "GET", "/markets", params={"event_ticker": ev["event_ticker"], "limit": 1}
            ).get("markets", [])
            if not markets:
                continue
            close = dt.datetime.fromisoformat(markets[0]["close_time"].replace("Z", "+00:00"))
            if close > now and (best is None or close < best[1]):
                best = (ev["event_ticker"], close)
        return best

    @staticmethod
    def ladder(markets: list[dict]) -> list[dict]:
        """Quoted 'greater' strikes sorted ascending, with mid = P(BTC >= strike)."""
        rows = []
        for m in markets:
            if m.get("strike_type") != "greater" or m.get("floor_strike") is None:
                continue
            try:
                bid = float(m["yes_bid_dollars"])
                ask = float(m["yes_ask_dollars"])
            except (KeyError, TypeError, ValueError):
                continue
            if ask <= 0 or ask - bid > 0.2:
                continue
            rows.append(
                {
                    "ticker": m["ticker"],
                    "strike": m["floor_strike"],
                    "bid": bid,
                    "ask": ask,
                    "mid": (bid + ask) / 2,
                    "volume": float(m.get("volume_fp") or 0),
                }
            )
        return sorted(rows, key=lambda r: r["strike"])

    @staticmethod
    def implied_prob_above(ladder: list[dict], price: float) -> float | None:
        """Interpolate the ladder at `price` -> market-implied P(BTC >= price)."""
        below = above = None
        for r in ladder:
            if r["strike"] <= price:
                below = r
            else:
                above = r
                break
        if below and above:
            frac = (price - below["strike"]) / (above["strike"] - below["strike"])
            return below["mid"] + (above["mid"] - below["mid"]) * frac
        if below:
            return below["mid"]
        if above:
            return above["mid"]
        return None

    # ── authenticated ──

    def balance(self) -> dict:
        return self._request("GET", "/portfolio/balance", auth=True)

    def positions(self) -> dict:
        return self._request("GET", "/portfolio/positions", auth=True)

    def place_limit_order(self, ticker: str, side: str, action: str, count: int, price_cents: int) -> dict:
        """side: yes|no, action: buy|sell, price in cents (1-99)."""
        assert side in ("yes", "no") and action in ("buy", "sell")
        assert 1 <= price_cents <= 99 and count > 0
        order = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": "limit",
            ("yes_price" if side == "yes" else "no_price"): price_cents,
            "client_order_id": f"btc-lab-{int(time.time() * 1000)}",
        }
        return self._request("POST", "/portfolio/orders", json=order, auth=True)


def estimated_fee(price: float, contracts: int, multiplier: float = 0.07) -> float:
    """Kalshi's quadratic taker fee: ceil-style 0.07 * C * p * (1-p), in dollars."""
    import math

    return math.ceil(multiplier * contracts * price * (1 - price) * 100) / 100
