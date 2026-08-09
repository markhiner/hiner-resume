// Bitcoin live price ticker — Coinbase + Bitstamp, blended in real time.
// Streams both exchanges over WebSocket, keeps a rolling 24h history in
// memory (persisted to disk so a restart doesn't lose the chart), and
// serves a self-contained mobile UI + JSON API + live WebSocket feed.
//
// Run with Node 18+. Requires the `ws` package: npm install, then
// node server.js (see README.md for Cloudflare Tunnel + launchd setup).

const http = require("http");
const fs = require("fs");
const path = require("path");
const WebSocket = require("ws");

const PORT = process.env.PORT || 3001;
const DATA_FILE = path.join(__dirname, "history.json");
const COINBASE_WS = "wss://ws-feed.exchange.coinbase.com";
const BITSTAMP_WS = "wss://ws.bitstamp.net";

const MAX_SECOND_TICKS = 3600; // 1 hour @ 1/sec — full resolution window
const MAX_MINUTE_BARS = 1440; // 24 hours @ 1/min — long-range window
const MAX_TRADES = 40; // rapid-fire trade ticker buffer
const SAVE_INTERVAL_MS = 60_000;
const STALE_MS = 20_000; // force-reconnect a feed that's gone quiet

// ---------- state ----------

const ex = {
  coinbase: { price: null, prevPrice: null, bid: null, ask: null, high24: null, low24: null, ts: null, connected: false },
  bitstamp: { price: null, prevPrice: null, ts: null, connected: false },
};

let secondTicks = []; // { t, coinbase, bitstamp, avg, vol }
let minuteBars = []; // { t, open, high, low, close, avg, vol }
let currentBar = null;
let recentTrades = []; // { t, ex, price, size, side, dir }
let volSinceLastTick = 0;

function currentAverage() {
  const c = ex.coinbase.price, b = ex.bitstamp.price;
  if (c != null && b != null) return (c + b) / 2;
  if (c != null) return c;
  if (b != null) return b;
  return null;
}

// ---------- persistence ----------

function loadHistory() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    if (Array.isArray(parsed.minuteBars)) minuteBars = parsed.minuteBars.slice(-MAX_MINUTE_BARS);
    if (Array.isArray(parsed.secondTicks)) secondTicks = parsed.secondTicks.slice(-MAX_SECOND_TICKS);
    console.log(`Loaded ${minuteBars.length} minute bars, ${secondTicks.length} second ticks from disk`);
  } catch {
    console.log("No prior history on disk, starting fresh");
  }
}

function saveHistory() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ minuteBars, secondTicks }));
  } catch (e) {
    console.error("Failed to save history:", e.message);
  }
}

// ---------- ingest ----------

function recordTrade(exchange, price, size, side) {
  const prev = ex[exchange].prevPrice;
  const dir = prev == null ? "flat" : price > prev ? "up" : price < prev ? "down" : "flat";
  volSinceLastTick += size || 0;
  const trade = { t: Date.now(), ex: exchange, price, size, side, dir };
  recentTrades.push(trade);
  if (recentTrades.length > MAX_TRADES) recentTrades.shift();
  broadcast({ type: "trade", ...trade });
}

function ingestTick() {
  const avg = currentAverage();
  if (avg == null) { volSinceLastTick = 0; return; }

  const t = Date.now();
  const vol = volSinceLastTick;
  volSinceLastTick = 0;

  secondTicks.push({ t, coinbase: ex.coinbase.price, bitstamp: ex.bitstamp.price, avg, vol });
  if (secondTicks.length > MAX_SECOND_TICKS) secondTicks.shift();

  const minuteEpoch = Math.floor(t / 60000) * 60000;
  if (!currentBar || currentBar.t !== minuteEpoch) {
    if (currentBar) {
      minuteBars.push(currentBar);
      if (minuteBars.length > MAX_MINUTE_BARS) minuteBars.shift();
    }
    currentBar = { t: minuteEpoch, open: avg, high: avg, low: avg, close: avg, avg, vol };
  } else {
    currentBar.high = Math.max(currentBar.high, avg);
    currentBar.low = Math.min(currentBar.low, avg);
    currentBar.close = avg;
    currentBar.avg = avg;
    currentBar.vol += vol;
  }

  broadcast({ type: "snapshot", ...snapshot() });
}
setInterval(ingestTick, 1000);

function snapshot() {
  const avg = currentAverage();
  const prevAvg = secondTicks.length > 1 ? secondTicks[secondTicks.length - 2].avg : avg;
  return {
    ts: Date.now(),
    coinbase: ex.coinbase.price,
    coinbaseConnected: ex.coinbase.connected,
    bid: ex.coinbase.bid,
    ask: ex.coinbase.ask,
    high24: ex.coinbase.high24,
    low24: ex.coinbase.low24,
    bitstamp: ex.bitstamp.price,
    bitstampConnected: ex.bitstamp.connected,
    average: avg,
    averagePrev: prevAvg,
  };
}

// ---------- exchange connectors ----------

let cbSocket = null, cbReconnectDelay = 1000;
function connectCoinbase() {
  cbSocket = new WebSocket(COINBASE_WS);
  cbSocket.on("open", () => {
    cbReconnectDelay = 1000;
    ex.coinbase.connected = true;
    cbSocket.send(JSON.stringify({ type: "subscribe", product_ids: ["BTC-USD"], channels: ["ticker"] }));
    console.log("Coinbase connected");
  });
  cbSocket.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.type !== "ticker" || msg.product_id !== "BTC-USD") return;
    const price = parseFloat(msg.price);
    if (!Number.isFinite(price)) return;
    ex.coinbase.prevPrice = ex.coinbase.price;
    ex.coinbase.price = price;
    if (msg.best_bid) ex.coinbase.bid = parseFloat(msg.best_bid);
    if (msg.best_ask) ex.coinbase.ask = parseFloat(msg.best_ask);
    if (msg.high_24h) ex.coinbase.high24 = parseFloat(msg.high_24h);
    if (msg.low_24h) ex.coinbase.low24 = parseFloat(msg.low_24h);
    ex.coinbase.ts = Date.now();
    recordTrade("coinbase", price, parseFloat(msg.last_size) || 0, msg.side || "");
  });
  cbSocket.on("close", scheduleCoinbaseReconnect);
  cbSocket.on("error", (err) => { console.error("Coinbase WS error:", err.message); try { cbSocket.terminate(); } catch {} });
}
function scheduleCoinbaseReconnect() {
  ex.coinbase.connected = false;
  console.log(`Coinbase disconnected, reconnecting in ${cbReconnectDelay}ms`);
  setTimeout(connectCoinbase, cbReconnectDelay);
  cbReconnectDelay = Math.min(cbReconnectDelay * 2, 30000);
}

let bsSocket = null, bsReconnectDelay = 1000;
function connectBitstamp() {
  bsSocket = new WebSocket(BITSTAMP_WS);
  bsSocket.on("open", () => {
    bsReconnectDelay = 1000;
    ex.bitstamp.connected = true;
    bsSocket.send(JSON.stringify({ event: "bts:subscribe", data: { channel: "live_trades_btcusd" } }));
    console.log("Bitstamp connected");
  });
  bsSocket.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (msg.event !== "trade") return;
    let data = msg.data;
    if (typeof data === "string") { try { data = JSON.parse(data); } catch { return; } }
    if (!data) return;
    const price = parseFloat(data.price);
    if (!Number.isFinite(price)) return;
    ex.bitstamp.prevPrice = ex.bitstamp.price;
    ex.bitstamp.price = price;
    ex.bitstamp.ts = Date.now();
    recordTrade("bitstamp", price, parseFloat(data.amount) || 0, data.type === 0 ? "buy" : "sell");
  });
  bsSocket.on("close", scheduleBitstampReconnect);
  bsSocket.on("error", (err) => { console.error("Bitstamp WS error:", err.message); try { bsSocket.terminate(); } catch {} });
}
function scheduleBitstampReconnect() {
  ex.bitstamp.connected = false;
  console.log(`Bitstamp disconnected, reconnecting in ${bsReconnectDelay}ms`);
  setTimeout(connectBitstamp, bsReconnectDelay);
  bsReconnectDelay = Math.min(bsReconnectDelay * 2, 30000);
}

// watchdog: some dead sockets never fire "close" — force-kill if stale
setInterval(() => {
  const now = Date.now();
  if (ex.coinbase.connected && ex.coinbase.ts && now - ex.coinbase.ts > STALE_MS) {
    console.log("Coinbase feed stale, forcing reconnect");
    try { cbSocket.terminate(); } catch {}
  }
  if (ex.bitstamp.connected && ex.bitstamp.ts && now - ex.bitstamp.ts > STALE_MS) {
    console.log("Bitstamp feed stale, forcing reconnect");
    try { bsSocket.terminate(); } catch {}
  }
}, STALE_MS);

// ---------- trend / momentum analytics ----------
// Fun, best-effort statistical estimate — not financial advice. Modeled as
// a random walk: projected drift (linear regression on recent price) plus
// noise scaled by sqrt(time), turned into a probability via the normal CDF.
// Client applies its own local wall-clock "minutes to the top of the hour"
// so the projection always matches the viewer's timezone, not the server's.

function linearRegression(points) {
  const n = points.length;
  if (n < 2) return null;
  let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
  for (const p of points) { sumX += p.x; sumY += p.y; sumXY += p.x * p.y; sumXX += p.x * p.x; }
  const denom = n * sumXX - sumX * sumX;
  if (denom === 0) return null;
  const slope = (n * sumXY - sumX * sumY) / denom;
  const intercept = (sumY - slope * sumX) / n;
  let ssRes = 0;
  for (const p of points) { const pred = slope * p.x + intercept; ssRes += (p.y - pred) ** 2; }
  return { slope, intercept, residualStd: Math.sqrt(ssRes / n) };
}

function computeRSI(closes, period) {
  if (closes.length < period + 1) return null;
  const recent = closes.slice(-(period + 1));
  let gainSum = 0, lossSum = 0;
  for (let i = 1; i < recent.length; i++) {
    const diff = recent[i] - recent[i - 1];
    if (diff > 0) gainSum += diff; else lossSum += -diff;
  }
  const avgGain = gainSum / period, avgLoss = lossSum / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function computeStreak(closes) {
  if (closes.length < 2) return { count: 0, direction: "flat" };
  let dir = null, count = 0;
  for (let i = closes.length - 1; i > 0; i--) {
    const d = closes[i] > closes[i - 1] ? "up" : closes[i] < closes[i - 1] ? "down" : "flat";
    if (d === "flat") break;
    if (dir === null) dir = d;
    if (d !== dir) break;
    count++;
  }
  return { count, direction: dir || "flat" };
}

function computeTrend() {
  const now = Date.now();
  const lookbackMs = 20 * 60 * 1000;
  const relevant = secondTicks.filter((p) => now - p.t <= lookbackMs);
  if (relevant.length < 60) {
    return { insufficient: true, sampleSeconds: relevant.length, warmupSeconds: 60 };
  }

  const t0 = relevant[0].t;
  const points = relevant.map((p) => ({ x: (p.t - t0) / 60000, y: p.avg }));
  const reg = linearRegression(points);
  if (!reg) return { insufficient: true, sampleSeconds: relevant.length };

  // Realized volatility from every second-over-second move in the window,
  // scaled to a per-minute figure (variance scales with time for a random
  // walk, so stdev scales with sqrt(time)). Deliberately NOT sampled once a
  // minute — a snapshot every 60s can miss a spike-and-reversion that
  // happens entirely between two snapshots, understating how choppy the
  // price actually was and making the projection overconfident.
  const secReturns = [];
  for (let i = 1; i < relevant.length; i++) secReturns.push(relevant[i].avg - relevant[i - 1].avg);
  let volPerMin = reg.residualStd || 1;
  if (secReturns.length >= 30) {
    const meanR = secReturns.reduce((a, b) => a + b, 0) / secReturns.length;
    const varR = secReturns.reduce((a, b) => a + (b - meanR) ** 2, 0) / secReturns.length;
    volPerMin = Math.sqrt(varR) * Math.sqrt(60) || volPerMin;
  }

  const allCloses = minuteBars.map((b) => b.close).concat(currentBar ? [currentBar.close] : []);
  const rsi = computeRSI(allCloses, 14);
  const streak = computeStreak(allCloses.slice(-30));

  return {
    insufficient: false,
    driftPerMin: reg.slope,
    volPerMin,
    sampleMinutes: Math.round((relevant.length / 60) * 10) / 10,
    currentPrice: relevant[relevant.length - 1].avg,
    rsi: rsi == null ? null : Math.round(rsi * 10) / 10,
    streakCount: streak.count,
    streakDirection: streak.direction,
  };
}

// ---------- Kalshi hourly market (public data, no auth) ----------
// KXBTCD is Kalshi's "Bitcoin price above/below at {hour} ET" series — it
// settles at the top of the hour, the exact question the Next Hour Outlook
// answers. Each strike market's price IS the market's probability that
// BTC ≥ strike at close, so interpolating the ladder at our blended price
// gives a market-implied probability directly comparable to the model's.

const KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2";
const KALSHI_SERIES = "KXBTCD";
const KALSHI_POLL_MS = 20_000;

let kalshiState = { available: false, error: null, updatedAt: 0 };
const kalshiCloseCache = new Map(); // event_ticker -> close_time ms

async function kalshiGET(pathname) {
  const res = await fetch(KALSHI_API + pathname, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`kalshi ${pathname} → ${res.status}`);
  return res.json();
}

function ladderMid(m) {
  const bid = parseFloat(m.yes_bid_dollars), ask = parseFloat(m.yes_ask_dollars);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || ask <= 0) return null;
  if (ask - bid > 0.2) return null; // spread too wide to mean anything
  return (bid + ask) / 2;
}

async function pollKalshi() {
  try {
    const price = currentAverage();
    const { events = [] } = await kalshiGET(`/events?series_ticker=${KALSHI_SERIES}&status=open&limit=10`);
    if (!events.length) throw new Error("no open events");

    const now = Date.now();
    for (const e of events) {
      if (!kalshiCloseCache.has(e.event_ticker)) {
        const { markets = [] } = await kalshiGET(`/markets?event_ticker=${e.event_ticker}&limit=1`);
        if (markets.length) kalshiCloseCache.set(e.event_ticker, Date.parse(markets[0].close_time));
      }
    }
    const candidates = events
      .map((e) => ({ ticker: e.event_ticker, close: kalshiCloseCache.get(e.event_ticker) }))
      .filter((e) => e.close && e.close > now)
      .sort((a, b) => a.close - b.close);
    if (!candidates.length) throw new Error("no upcoming close");
    const ev = candidates[0];

    const { markets = [] } = await kalshiGET(`/markets?event_ticker=${ev.ticker}&status=open&limit=200`);
    const ladder = markets
      .filter((m) => m.strike_type === "greater" && m.floor_strike != null)
      .map((m) => ({
        strike: m.floor_strike,
        mid: ladderMid(m),
        bid: parseFloat(m.yes_bid_dollars),
        ask: parseFloat(m.yes_ask_dollars),
        vol: parseFloat(m.volume_fp) || 0,
        subtitle: m.yes_sub_title || m.subtitle,
        ticker: m.ticker,
      }))
      .sort((a, b) => a.strike - b.strike);

    let implied = null, below = null, above = null;
    if (price != null) {
      const quoted = ladder.filter((r) => r.mid != null);
      for (const r of quoted) {
        if (r.strike <= price) below = r;
        else { above = r; break; }
      }
      if (below && above) {
        const frac = (price - below.strike) / (above.strike - below.strike);
        implied = below.mid + (above.mid - below.mid) * frac;
      } else if (below) implied = below.mid;
      else if (above) implied = above.mid;
    }

    const nearest = below && above
      ? (price - below.strike <= above.strike - price ? below : above)
      : below || above || null;

    kalshiState = {
      available: implied != null,
      error: null,
      updatedAt: now,
      eventTicker: ev.ticker,
      closeTime: ev.close,
      impliedProbAbove: implied != null ? Math.round(implied * 1000) / 1000 : null,
      refPrice: price,
      nearestStrike: nearest
        ? { strike: nearest.strike, subtitle: nearest.subtitle, bid: nearest.bid, ask: nearest.ask, vol: nearest.vol, ticker: nearest.ticker }
        : null,
    };
  } catch (e) {
    kalshiState = { ...kalshiState, available: false, error: e.message, updatedAt: Date.now() };
  }
}
setInterval(pollKalshi, KALSHI_POLL_MS);
pollKalshi();

// ---------- history bucketing for chart ranges ----------

function bucketize(data, bucketCount) {
  const clean = data.filter(Boolean);
  if (!clean.length) return [];
  if (clean.length <= bucketCount) {
    return clean.map((p) => ({ t: p.t, avg: p.avg, high: p.high ?? p.avg, low: p.low ?? p.avg, vol: p.vol || 0 }));
  }
  const bucketSize = clean.length / bucketCount;
  const out = [];
  for (let i = 0; i < bucketCount; i++) {
    const start = Math.floor(i * bucketSize);
    const end = Math.max(Math.floor((i + 1) * bucketSize), start + 1);
    const slice = clean.slice(start, end);
    if (!slice.length) continue;
    let high = -Infinity, low = Infinity, sum = 0, vol = 0;
    for (const p of slice) {
      const h = p.high ?? p.avg, l = p.low ?? p.avg;
      if (h > high) high = h;
      if (l < low) low = l;
      sum += p.avg;
      vol += p.vol || 0;
    }
    out.push({ t: slice[slice.length - 1].t, avg: sum / slice.length, high, low, vol });
  }
  return out;
}

function getHistory(range) {
  const now = Date.now();
  if (range === "1h") {
    const cutoff = now - 60 * 60 * 1000;
    return bucketize(secondTicks.filter((p) => p.t >= cutoff), 300);
  }
  if (range === "3h") {
    const cutoff = now - 3 * 60 * 60 * 1000;
    const bars = minuteBars.concat(currentBar ? [currentBar] : []).filter((b) => b.t >= cutoff);
    return bucketize(bars, 300);
  }
  const cutoff = now - 24 * 60 * 60 * 1000;
  const bars = minuteBars.concat(currentBar ? [currentBar] : []).filter((b) => b.t >= cutoff);
  return bucketize(bars, 360);
}

// ---------- HTTP + WebSocket server ----------

const clients = new Set();
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(data);
}
setInterval(() => broadcast({ type: "trend", ...computeTrend(), kalshi: kalshiState }), 10000);

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  res.setHeader("Access-Control-Allow-Origin", "*");

  if (url.pathname === "/favicon.ico") { res.writeHead(204); res.end(); return; }

  if (url.pathname === "/api/latest") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(snapshot()));
    return;
  }
  if (url.pathname === "/api/history") {
    const range = url.searchParams.get("range") || "1h";
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(getHistory(range)));
    return;
  }
  if (url.pathname === "/api/trend") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ...computeTrend(), kalshi: kalshiState }));
    return;
  }
  if (url.pathname === "/api/kalshi") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(kalshiState));
    return;
  }
  if (url.pathname === "/api/trades") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(recentTrades));
    return;
  }

  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(htmlPage);
});

const wss = new WebSocket.Server({ noServer: true });
server.on("upgrade", (req, socket, head) => {
  const { pathname } = new URL(req.url, `http://${req.headers.host}`);
  if (pathname !== "/stream") { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, (ws) => {
    clients.add(ws);
    ws.send(JSON.stringify({ type: "bootstrap", snapshot: snapshot(), trades: recentTrades, trend: { ...computeTrend(), kalshi: kalshiState } }));
    ws.on("close", () => clients.delete(ws));
    ws.on("error", () => clients.delete(ws));
  });
});

setInterval(saveHistory, SAVE_INTERVAL_MS);
function shutdown() {
  console.log("Saving history before exit…");
  saveHistory();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ---------- frontend ----------

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black">
<meta name="theme-color" content="#000000">
<title>BTC/USD — Live</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg: #000000;
  --panel: #0b0b0d;
  --panel2: #131317;
  --border: #232329;
  --text1: #ffffff;
  --text2: #9a9aa2;
  --text3: #5c5c66;
  --green: #22c55e;
  --green-dim: rgba(34,197,94,0.14);
  --red: #ef4444;
  --red-dim: rgba(239,68,68,0.14);
  --yellow: #f5c518;
  --yellow-dim: rgba(245,197,24,0.14);
  --orange: #f97316;
}

html, body {
  background: var(--bg);
  color: var(--text1);
  height: 100%;
}
body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", sans-serif;
  -webkit-font-smoothing: antialiased;
  padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left);
  min-height: 100%;
}

#app {
  max-width: 480px;
  margin: 0 auto;
  padding: 10px 10px 22px;
  display: flex;
  flex-direction: column;
  gap: 10px;
}

/* ── brand row ── */
.brand-row { display: flex; align-items: center; justify-content: center; gap: 7px; padding: 0 2px; }
.brand-text { font-size: 10.5px; letter-spacing: 2px; color: var(--text3); font-weight: 800; text-transform: uppercase; }
.brand-sep { color: var(--text3); font-size: 11px; }
.status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--text3); flex-shrink: 0; }
.status-dot.live { background: var(--red); box-shadow: 0 0 0 0 rgba(239,68,68,0.5); animation: pulseRed 1.4s infinite; }
.status-dot.degraded { background: var(--yellow); }
.status-dot.down { background: var(--red); }
@keyframes pulse {
  0%   { box-shadow: 0 0 0 0 rgba(34,197,94,0.45); }
  70%  { box-shadow: 0 0 0 6px rgba(34,197,94,0); }
  100% { box-shadow: 0 0 0 0 rgba(34,197,94,0); }
}
@keyframes pulseRed {
  0%   { box-shadow: 0 0 0 0 rgba(239,68,68,0.5); }
  70%  { box-shadow: 0 0 0 7px rgba(239,68,68,0); }
  100% { box-shadow: 0 0 0 0 rgba(239,68,68,0); }
}

/* ── big price ── */
.price-flash-wrap { width: 100vw; padding: 8px 0; margin: -8px calc(50% - 50vw) 0; background-color: transparent; }
.price-big { font-size: clamp(46px, 15.5vw, 64px); font-weight: 800; letter-spacing: -1.5px; line-height: 1; text-align: center; color: var(--text1); font-variant-numeric: tabular-nums; }

/* ── hero row: delta + countdown ── */
.hero-row { display: flex; align-items: center; justify-content: space-between; padding: 4px 4px 0; }
.price-delta { display: inline-flex; align-items: center; gap: 4px; padding: 4px 10px; border-radius: 20px; font-size: 13px; font-weight: 700; }
.price-delta.up   { background: var(--green-dim); color: var(--green); }
.price-delta.down { background: var(--red-dim); color: var(--red); }
.price-delta.flat { background: rgba(255,255,255,0.06); color: var(--text2); }
.countdown { font-size: 30px; font-weight: 800; font-variant-numeric: tabular-nums; letter-spacing: -0.5px; transition: color 0.9s ease-out; }
.countdown.flashing { animation: countdownFlash 1s step-start infinite; }
@keyframes countdownFlash { 50% { opacity: 0.15; } }

/* ── range buttons ── */
.range-row-sm { display: flex; gap: 6px; justify-content: center; }
.range-btn-sm {
  background: var(--panel2);
  border: 1px solid var(--border);
  color: var(--text2);
  font-size: 11px;
  font-weight: 800;
  padding: 4px 15px;
  border-radius: 20px;
  min-width: 32px;
}
.range-btn-sm.active { background: var(--text1); color: #000; border-color: var(--text1); }

/* ── chart ── */
.chart-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 10px 8px; }
.chart-hdr { display: flex; justify-content: space-between; align-items: baseline; padding: 0 4px 6px; font-size: 11px; color: var(--text3); }
.chart-hdr .hi { color: var(--green); font-weight: 700; }
.chart-hdr .lo { color: var(--red); font-weight: 700; }
canvas#chart { width: 100%; height: 230px; display: block; }
.chart-axis { display: flex; justify-content: space-between; padding: 4px 4px 0; font-size: 10px; color: var(--text3); }

/* ── exchange rows ── */
.rows-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; display: flex; overflow: hidden; }
.ex-row { flex: 1; display: flex; flex-direction: column; gap: 3px; padding: 10px 12px; }
.ex-row + .ex-row { border-left: 1px solid var(--border); }
.ex-name { font-size: 10.5px; font-weight: 800; color: var(--text3); letter-spacing: 0.5px; text-transform: uppercase; }
.ex-price-wrap { display: flex; align-items: baseline; gap: 7px; }
.ex-price { font-size: 15px; font-weight: 800; padding: 2px 7px; border-radius: 6px; font-variant-numeric: tabular-nums; transition: background 0.25s, color 0.25s; }
.ex-price.up   { background: var(--green-dim); color: var(--green); }
.ex-price.down { background: var(--red-dim); color: var(--red); }
.ex-price.flat { background: rgba(255,255,255,0.05); color: var(--text1); }
.ex-price.off  { background: transparent; color: var(--text3); font-weight: 600; font-size: 12px; }
.ex-pct { font-size: 11px; font-weight: 700; }
.ex-pct.up { color: var(--green); }
.ex-pct.down { color: var(--red); }
.ex-pct.flat { color: var(--text3); }

/* ── outlook (chance) card ── */
.outlook-stack { display: grid; }
.outlook-stack > * { grid-area: 1 / 1; align-self: start; }
.outlook-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 14px; opacity: 1; transition: opacity 0.35s ease; }
.outlook-card.hc-hidden { opacity: 0; pointer-events: none; }

/* ── hour-close average banner ── */
.hourclose-wrap {
  width: 100vw;
  margin: 0 calc(50% - 50vw);
  padding: 14px 0 12px;
  text-align: center;
  opacity: 0;
  pointer-events: none;
  background-color: transparent;
  transition: opacity 0.35s ease, background-color 0.4s ease;
}
.hourclose-wrap.hc-visible { opacity: 1; pointer-events: auto; }
.hourclose-label { font-size: 10.5px; letter-spacing: 2px; color: rgba(255,255,255,0.8); font-weight: 800; text-transform: uppercase; margin-bottom: 4px; }
.hourclose-price { font-size: clamp(46px, 15.5vw, 64px); font-weight: 800; letter-spacing: -1.5px; line-height: 1; color: var(--text1); font-variant-numeric: tabular-nums; }
.trend-headline { font-size: 14px; font-weight: 700; line-height: 1.35; color: var(--text1); }
.trend-headline b.up { color: var(--green); }
.trend-headline b.down { color: var(--red); }
.gauge { position: relative; height: 8px; border-radius: 4px; margin: 10px 0 6px; background: linear-gradient(90deg, var(--red) 0%, var(--yellow) 50%, var(--green) 100%); }
.gauge-pointer { position: absolute; top: -4px; width: 3px; height: 16px; background: #fff; border-radius: 2px; box-shadow: 0 0 4px rgba(0,0,0,0.6); transform: translateX(-50%); animation: gaugePulse 1.4s ease-in-out infinite; }
@keyframes gaugePulse { 0%, 100% { opacity: 1; transform: translateX(-50%) scaleY(1); } 50% { opacity: 0.5; transform: translateX(-50%) scaleY(1.3); } }
.gauge-labels { display: flex; justify-content: space-between; font-size: 10px; color: var(--text3); }
.stat-chips { display: flex; gap: 8px; margin-top: 10px; flex-wrap: wrap; }
.chip { font-size: 11px; font-weight: 700; padding: 5px 10px; border-radius: 20px; background: rgba(255,255,255,0.05); color: var(--text2); }
.chip.green { color: var(--green); background: var(--green-dim); }
.chip.red { color: var(--red); background: var(--red-dim); }
.chip.yellow { color: var(--yellow); background: var(--yellow-dim); }
.trend-warmup { font-size: 13px; color: var(--text2); padding: 4px 0; }

/* ── kalshi vs model ── */
.kalshi-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 14px; }
.kalshi-hdr { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 8px; }
.kalshi-title { font-size: 11px; letter-spacing: 1.5px; color: var(--text3); font-weight: 800; text-transform: uppercase; }
.kalshi-close { font-size: 10px; color: var(--text3); }
.kalshi-compare { display: flex; align-items: stretch; gap: 8px; }
.kalshi-cell { flex: 1; background: var(--panel2); border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; text-align: center; }
.kalshi-cell .lbl { font-size: 9.5px; letter-spacing: 1px; color: var(--text3); font-weight: 700; text-transform: uppercase; }
.kalshi-cell .val { font-size: 20px; font-weight: 800; margin-top: 2px; font-variant-numeric: tabular-nums; color: var(--text1); }
.kalshi-cell.edge .val.up { color: var(--green); }
.kalshi-cell.edge .val.down { color: var(--red); }
.kalshi-cell.edge .val.flat { color: var(--yellow); }
.kalshi-detail { font-size: 11px; color: var(--text2); margin-top: 8px; text-align: center; }
.kalshi-detail b { color: var(--text1); }
.kalshi-off { font-size: 11.5px; color: var(--text3); font-style: italic; margin-top: 2px; }

/* ── live trades ── */
.trades-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 14px; }
.trades-hdr { display: flex; align-items: center; gap: 6px; font-size: 11px; letter-spacing: 1.5px; color: var(--text3); font-weight: 800; text-transform: uppercase; margin-bottom: 8px; }
.trades-hdr .live-chip { width: 6px; height: 6px; border-radius: 50%; background: var(--green); animation: pulse 1.4s infinite; }
.trades-list { display: flex; flex-direction: column-reverse; height: 168px; overflow: hidden; font-variant-numeric: tabular-nums; }
.trade-row { display: flex; align-items: center; gap: 8px; padding: 3.5px 2px; font-size: 12.5px; border-bottom: 1px solid rgba(255,255,255,0.03); animation: slideIn 0.25s ease-out; }
@keyframes slideIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: translateY(0); } }
.trade-time { color: var(--text3); width: 60px; flex-shrink: 0; }
.trade-ex { width: 62px; flex-shrink: 0; font-size: 10px; font-weight: 800; letter-spacing: 0.5px; color: var(--text2); text-transform: uppercase; }
.trade-size { color: var(--text2); flex: 1; text-align: left; font-size: 11.5px; }
.trade-price { font-weight: 700; }
.trade-price.up { color: var(--green); }
.trade-price.down { color: var(--red); }
.trade-price.flat { color: var(--text1); }

/* ── footer ── */
.footer { display: flex; justify-content: space-between; align-items: center; padding: 4px 4px 0; font-size: 10.5px; color: var(--text3); }
.footer a { color: var(--text3); text-decoration: none; }
.dot-list { display: flex; gap: 10px; }
.dot-item { display: flex; align-items: center; gap: 4px; }
.dot-item .d { width: 6px; height: 6px; border-radius: 50%; }
.dot-item .d.on { background: var(--green); }
.dot-item .d.off { background: var(--red); }

</style>
</head>
<body>
<div id="app">

  <div class="brand-row">
    <span class="brand-text">Hiner BTC Benchmark</span>
    <span class="brand-sep">|</span>
    <span class="status-dot" id="statusDot"></span>
  </div>

  <div class="price-flash-wrap" id="priceFlashWrap">
    <div class="price-big" id="bigPrice">—</div>
  </div>

  <div class="hero-row">
    <div class="price-delta flat" id="priceDelta">— %</div>
    <div class="countdown" id="countdown">60:00</div>
  </div>

  <div class="outlook-stack">
    <div class="outlook-card" id="outlookCard">
      <div id="outlookBody"><div class="trend-warmup">Gathering data for a projection…</div></div>
    </div>
    <div class="hourclose-wrap" id="hourCloseWrap">
      <div class="hourclose-label">60s Hour-Close Avg</div>
      <div class="hourclose-price" id="hourClosePrice">—</div>
    </div>
  </div>

  <div class="range-row-sm">
    <button class="range-btn-sm active" data-range="1h">1</button>
    <button class="range-btn-sm" data-range="3h">3</button>
    <button class="range-btn-sm" data-range="24h">24</button>
  </div>

  <div class="chart-card">
    <div class="chart-hdr">
      <span>High <b class="hi" id="rangeHigh">—</b></span>
      <span>Low <b class="lo" id="rangeLow">—</b></span>
    </div>
    <canvas id="chart"></canvas>
    <div class="chart-axis"><span id="axisStart">—</span><span id="axisEnd">now</span></div>
  </div>

  <div class="rows-card" id="rowsCard">
    <div class="ex-row">
      <div class="ex-name">Coinbase</div>
      <div class="ex-price-wrap">
        <span class="ex-price flat" id="cbPrice">—</span>
        <span class="ex-pct flat" id="cbPct">—</span>
      </div>
    </div>
    <div class="ex-row">
      <div class="ex-name">Bitstamp</div>
      <div class="ex-price-wrap">
        <span class="ex-price flat" id="bsPrice">—</span>
        <span class="ex-pct flat" id="bsPct">—</span>
      </div>
    </div>
  </div>

  <div class="kalshi-card" id="kalshiCard"></div>

  <div class="trades-card">
    <div class="trades-hdr"><span class="live-chip"></span>Live Trade Feed</div>
    <div class="trades-list" id="tradesList"></div>
  </div>

  <div class="footer">
    <div class="dot-list">
      <span class="dot-item"><span class="d off" id="footCbDot"></span>Coinbase</span>
      <span class="dot-item"><span class="d off" id="footBsDot"></span>Bitstamp</span>
    </div>
    <span id="lastUpdated">—</span>
  </div>

</div>

<script>
(function () {
  "use strict";

  var state = {
    range: "1h",
    history: [],
    live: { coinbase: null, bitstamp: null },
    prevRow: { coinbase: null, bitstamp: null },
    rangeStartAvg: null,
    connCoinbase: false,
    connBitstamp: false,
    high24: null,
    low24: null,
    lastMsgAt: 0,
    lastSnapshotAvg: null,
  };

  var BIG_MOVE_THRESHOLD = 10; // dollars of blended-price change within one snapshot tick (~1s)
  var flashTimeout = null;
  function triggerBigMoveFlash(direction) {
    var wrap = document.getElementById("priceFlashWrap");
    if (flashTimeout) clearTimeout(flashTimeout);
    wrap.style.transition = "background-color 0.15s ease-in";
    wrap.style.backgroundColor = direction === "up" ? "rgba(0,200,0,0.6)" : "rgba(255,0,0,0.6)";
    flashTimeout = setTimeout(function () {
      wrap.style.transition = "background-color 0.6s ease-out";
      wrap.style.backgroundColor = "transparent";
      flashTimeout = null;
    }, 10000);
  }
  function checkBigMove(avg) {
    if (avg == null) return;
    if (state.lastSnapshotAvg != null) {
      var delta = avg - state.lastSnapshotAvg;
      if (Math.abs(delta) >= BIG_MOVE_THRESHOLD) triggerBigMoveFlash(delta > 0 ? "up" : "down");
    }
    state.lastSnapshotAvg = avg;
  }

  var fmtUSD = function (n) {
    if (n == null || !isFinite(n)) return "—";
    return "$" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  var fmtUSDShort = function (n) {
    if (n == null || !isFinite(n)) return "—";
    return n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  var fmtPct = function (n) {
    if (n == null || !isFinite(n)) return "—";
    var sign = n > 0 ? "+" : "";
    return sign + n.toFixed(2) + "%";
  };
  var dirClass = function (cur, prev) {
    if (cur == null || prev == null) return "flat";
    if (cur > prev) return "up";
    if (cur < prev) return "down";
    return "flat";
  };

  // ---------- header price + rows ----------

  function recomputeAverage() {
    var c = state.live.coinbase, b = state.live.bitstamp;
    if (c != null && b != null) return (c + b) / 2;
    if (c != null) return c;
    if (b != null) return b;
    return null;
  }

  function updateRow(id, price, prevPrice, connected) {
    var priceEl = document.getElementById(id + "Price");
    var pctEl = document.getElementById(id + "Pct");
    if (!connected && price == null) {
      priceEl.textContent = "reconnecting…";
      priceEl.className = "ex-price off";
      pctEl.textContent = "—";
      pctEl.className = "ex-pct flat";
      return;
    }
    var cls = dirClass(price, prevPrice);
    priceEl.textContent = fmtUSDShort(price);
    priceEl.className = "ex-price " + cls;

    var base = state.rangeStartAvg;
    if (base && price != null) {
      var pct = ((price - base) / base) * 100;
      pctEl.textContent = fmtPct(pct);
      pctEl.className = "ex-pct " + (pct > 0 ? "up" : pct < 0 ? "down" : "flat");
    }
  }

  function refreshHeaderAndRows() {
    var avg = recomputeAverage();

    if (avg != null) {
      document.getElementById("bigPrice").textContent = fmtUSD(avg);
    }
    if (state.rangeStartAvg && avg != null) {
      var pct = ((avg - state.rangeStartAvg) / state.rangeStartAvg) * 100;
      var deltaEl = document.getElementById("priceDelta");
      var cls = pct > 0 ? "up" : pct < 0 ? "down" : "flat";
      var arrow = pct > 0 ? "\\u25B2" : pct < 0 ? "\\u25BC" : "\\u2013";
      deltaEl.className = "price-delta " + cls;
      deltaEl.textContent = arrow + " " + fmtPct(pct) + " (" + state.range.toUpperCase() + ")";
    }

    updateRow("cb", state.live.coinbase, state.prevRow.coinbase, state.connCoinbase);
    updateRow("bs", state.live.bitstamp, state.prevRow.bitstamp, state.connBitstamp);

    state.prevRow.coinbase = state.live.coinbase;
    state.prevRow.bitstamp = state.live.bitstamp;
  }

  function updateStatus() {
    var dot = document.getElementById("statusDot");
    var cb = state.connCoinbase, bs = state.connBitstamp;
    document.getElementById("footCbDot").className = "d " + (cb ? "on" : "off");
    document.getElementById("footBsDot").className = "d " + (bs ? "on" : "off");
    if (cb && bs) { dot.className = "status-dot live"; }
    else if (cb || bs) { dot.className = "status-dot degraded"; }
    else { dot.className = "status-dot down"; }
  }

  function tickLastUpdated() {
    if (!state.lastMsgAt) return;
    var secs = Math.round((Date.now() - state.lastMsgAt) / 1000);
    document.getElementById("lastUpdated").textContent = secs <= 1 ? "updated just now" : "updated " + secs + "s ago";
  }
  setInterval(tickLastUpdated, 1000);

  // ---------- countdown to top of hour ----------

  function countdownColor(remainingSec) {
    var WHITE = [255, 255, 255], YELLOW = [245, 197, 24], ORANGE = [249, 115, 22], RED = [239, 68, 68];
    function lerp(a, b, t) { return [0, 1, 2].map(function (i) { return Math.round(a[i] + (b[i] - a[i]) * t); }); }
    function rgb(c) { return "rgb(" + c[0] + "," + c[1] + "," + c[2] + ")"; }
    if (remainingSec >= 1800) return rgb(WHITE);
    if (remainingSec >= 900) return rgb(lerp(YELLOW, ORANGE, (1800 - remainingSec) / 900));
    if (remainingSec >= 300) return rgb(lerp(ORANGE, RED, (900 - remainingSec) / 600));
    return rgb(RED);
  }

  function updateCountdown() {
    var now = new Date();
    var secsPastHour = now.getMinutes() * 60 + now.getSeconds();
    var remaining = 3600 - secsPastHour;
    if (remaining <= 0) remaining += 3600;
    var m = Math.floor(remaining / 60), s = remaining % 60;
    var el = document.getElementById("countdown");
    el.textContent = m + ":" + String(s).padStart(2, "0");
    el.style.color = countdownColor(remaining);
    el.classList.toggle("flashing", remaining < 300);
  }
  setInterval(updateCountdown, 1000);
  updateCountdown();

  // ---------- hour-close average banner ----------
  // During HH:59:00-HH:59:59 sample the live blended price once a second and
  // show its running average in place of the outlook card; lock that average
  // (value + up/down color vs. the price at this hour's HH:00:00) the instant
  // the new hour starts, hold it for 10s, then hand the card back.

  var hourClose = {
    openPrice: null,
    openHourBucket: null,
    referencePrice: null,
    samples: [],
    runningSum: 0,
    sampleBucket: null,
    finalAvg: null,
    finalColor: null,
    phase: "idle", // idle | collecting | holding
  };

  function hourCloseSetColor(dir) {
    document.getElementById("hourCloseWrap").style.backgroundColor =
      dir === "up" ? "rgba(0,200,0,0.6)" : "rgba(255,0,0,0.6)";
  }

  function hourCloseShow(visible, fast) {
    var outlook = document.getElementById("outlookCard");
    var wrap = document.getElementById("hourCloseWrap");
    var dur = fast ? "0.2s" : "0.35s";
    outlook.style.transition = "opacity " + dur + " ease";
    wrap.style.transition = "opacity " + dur + " ease, background-color 0.4s ease";
    outlook.classList.toggle("hc-hidden", visible);
    wrap.classList.toggle("hc-visible", visible);
  }

  function updateHourClose() {
    var now = new Date();
    var minute = now.getMinutes();
    var second = now.getSeconds();
    var epochSec = Math.floor(now.getTime() / 1000);
    var curAvg = recomputeAverage();

    if (minute === 0 && second === 0) {
      var hourBucket = Math.floor(now.getTime() / 3600000);
      if (hourClose.openHourBucket !== hourBucket && curAvg != null) {
        hourClose.openPrice = curAvg;
        hourClose.openHourBucket = hourBucket;
      }
    }

    if (minute === 59) {
      if (hourClose.phase !== "collecting") {
        hourClose.phase = "collecting";
        hourClose.samples = [];
        hourClose.runningSum = 0;
        hourClose.sampleBucket = null;
        hourClose.referencePrice = hourClose.openPrice != null ? hourClose.openPrice : curAvg;
        document.getElementById("hourClosePrice").textContent = fmtUSD(curAvg);
        hourCloseShow(true, false);
      }
      if (curAvg != null && hourClose.sampleBucket !== epochSec) {
        hourClose.samples.push(curAvg);
        hourClose.runningSum += curAvg;
        hourClose.sampleBucket = epochSec;
        var runningAvg = hourClose.runningSum / hourClose.samples.length;
        var liveDir = (hourClose.referencePrice != null && runningAvg < hourClose.referencePrice) ? "down" : "up";
        document.getElementById("hourClosePrice").textContent = fmtUSD(runningAvg);
        hourCloseSetColor(liveDir);
      }
    } else if (hourClose.phase === "collecting") {
      // just crossed HH:59:59 -> HH:00:00 — lock the final average in place
      var finalAvg = hourClose.samples.length ? hourClose.runningSum / hourClose.samples.length : curAvg;
      hourClose.finalAvg = finalAvg;
      hourClose.finalColor = (hourClose.referencePrice != null && finalAvg < hourClose.referencePrice) ? "down" : "up";
      hourClose.phase = "holding";
      document.getElementById("hourClosePrice").textContent = fmtUSD(finalAvg);
      hourCloseSetColor(hourClose.finalColor);
    } else if (hourClose.phase === "holding") {
      if (!(minute === 0 && second < 10)) {
        hourClose.phase = "idle";
        hourCloseShow(false, true);
      }
    }
  }
  setInterval(updateHourClose, 1000);
  updateHourClose();

  // ---------- chart ----------

  var canvas = document.getElementById("chart");
  var ctx = canvas.getContext("2d");

  function resizeCanvas() {
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.getBoundingClientRect();
    canvas.width = Math.round(rect.width * dpr);
    canvas.height = Math.round(rect.height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  // Y-axis gridlines only ever land on $50 or $100 increments — never a
  // fraction of the visible range, never any other denomination. Thin them
  // out (keeping the same $50/$100 grid, just skipping some) rather than
  // switching denomination when a wide range would otherwise pack in dozens.
  function niceTicks(min, max) {
    var range = max - min;
    if (!isFinite(range) || range <= 0) return [Math.round(min)];
    var step = range / 50 > 6 ? 100 : 50;
    var start = Math.ceil(min / step) * step;
    var ticks = [];
    for (var v = start; v <= max + step * 1e-6; v += step) ticks.push(Math.round(v));
    if (!ticks.length) ticks.push(Math.round((min + max) / 2 / step) * step);
    var maxLabels = 6;
    if (ticks.length > maxLabels) {
      var stride = Math.ceil(ticks.length / maxLabels);
      var thinned = [];
      for (var i = 0; i < ticks.length; i += stride) thinned.push(ticks[i]);
      ticks = thinned;
    }
    return ticks;
  }

  function drawChart() {
    var data = state.history;
    var rect = canvas.getBoundingClientRect();
    var w = rect.width, h = rect.height;
    ctx.clearRect(0, 0, w, h);
    if (!data.length) return;

    var highs = data.map(function (p) { return p.high; });
    var lows = data.map(function (p) { return p.low; });
    var vols = data.map(function (p) { return p.vol || 0; });
    var maxV = Math.max.apply(null, vols) || 1;
    var maxP = Math.max.apply(null, highs);
    var minP = Math.min.apply(null, lows);
    if (maxP === minP) { maxP += 1; minP -= 1; }
    var pad = (maxP - minP) * 0.08;
    maxP += pad; minP -= pad;

    document.getElementById("rangeHigh").textContent = fmtUSDShort(Math.max.apply(null, highs));
    document.getElementById("rangeLow").textContent = fmtUSDShort(Math.min.apply(null, lows));

    var volH = h * 0.18;
    var chartH = h - volH - 4;
    var n = data.length;
    var leftPad = 44;
    var plotW = Math.max(w - leftPad, 10);
    var x = function (i) { return leftPad + (n <= 1 ? 0 : (i / (n - 1)) * plotW); };
    var y = function (v) { return chartH - ((v - minP) / (maxP - minP)) * chartH; };

    // volume bars
    ctx.fillStyle = "rgba(255,255,255,0.10)";
    var barW = Math.max(plotW / n - 1, 1);
    for (var i = 0; i < n; i++) {
      var bh = (vols[i] / maxV) * volH;
      ctx.fillRect(x(i) - barW / 2, h - bh, barW, bh);
    }

    // gridlines + left-side price scale — rounded to nice values, not raw fractions
    ctx.strokeStyle = "rgba(255,255,255,0.06)";
    ctx.lineWidth = 1;
    ctx.font = "9px -apple-system, sans-serif";
    ctx.fillStyle = "rgba(255,255,255,0.4)";
    ctx.textBaseline = "middle";
    var priceTicks = niceTicks(minP, maxP);
    for (var t = 0; t < priceTicks.length; t++) {
      var gy = Math.max(0, Math.min(chartH, y(priceTicks[t])));
      ctx.beginPath(); ctx.moveTo(leftPad, gy); ctx.lineTo(w, gy); ctx.stroke();
      ctx.fillText(priceTicks[t].toLocaleString(), 2, Math.min(Math.max(gy, 7), chartH - 4));
    }

    // filled area under avg line
    var grad = ctx.createLinearGradient(0, 0, 0, chartH);
    grad.addColorStop(0, "rgba(245,197,24,0.22)");
    grad.addColorStop(1, "rgba(245,197,24,0.0)");
    ctx.beginPath();
    ctx.moveTo(x(0), y(data[0].avg));
    for (var j = 1; j < n; j++) ctx.lineTo(x(j), y(data[j].avg));
    ctx.lineTo(x(n - 1), chartH);
    ctx.lineTo(x(0), chartH);
    ctx.closePath();
    ctx.fillStyle = grad;
    ctx.fill();

    // avg line
    ctx.beginPath();
    ctx.moveTo(x(0), y(data[0].avg));
    for (var k = 1; k < n; k++) ctx.lineTo(x(k), y(data[k].avg));
    ctx.strokeStyle = "#f5c518";
    ctx.lineWidth = 1.75;
    ctx.lineJoin = "round";
    ctx.stroke();

    // last point marker
    var lastX = x(n - 1), lastY = y(data[n - 1].avg);
    ctx.beginPath();
    ctx.arc(lastX, lastY, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = "#fff";
    ctx.fill();

    var axisFmt = function (t) {
      var d = new Date(t);
      return state.range === "24h"
        ? d.toLocaleTimeString([], { hour: "numeric" })
        : d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    };
    document.getElementById("axisStart").textContent = axisFmt(data[0].t);
    document.getElementById("axisEnd").textContent = axisFmt(data[n - 1].t);
  }

  window.addEventListener("resize", function () { resizeCanvas(); drawChart(); });

  function setHistory(data) {
    state.history = data;
    if (data.length) state.rangeStartAvg = data[0].avg;
    resizeCanvas();
    drawChart();
    refreshHeaderAndRows();
  }

  function fetchHistory() {
    fetch("/api/history?range=" + state.range)
      .then(function (r) { return r.json(); })
      .then(setHistory)
      .catch(function () {});
  }

  var autoRefreshTimer = null;
  function armAutoRefresh() {
    if (autoRefreshTimer) clearInterval(autoRefreshTimer);
    if (state.range !== "1h") autoRefreshTimer = setInterval(fetchHistory, 30000);
  }

  document.querySelectorAll(".range-btn-sm").forEach(function (btn) {
    btn.addEventListener("click", function () {
      document.querySelectorAll(".range-btn-sm").forEach(function (b) { b.classList.remove("active"); });
      btn.classList.add("active");
      state.range = btn.getAttribute("data-range");
      fetchHistory();
      armAutoRefresh();
    });
  });

  function appendLivePoint(avg, vol) {
    if (state.range !== "1h") return;
    var now = Date.now();
    state.history.push({ t: now, avg: avg, high: avg, low: avg, vol: vol || 0 });
    var cutoff = now - 60 * 60 * 1000;
    while (state.history.length && state.history[0].t < cutoff) state.history.shift();
    if (state.history.length && state.rangeStartAvg == null) state.rangeStartAvg = state.history[0].avg;
    drawChart();
  }

  // ---------- trend widget ----------

  function erf(x) {
    var sign = x < 0 ? -1 : 1;
    x = Math.abs(x);
    var a1 = 0.254829592, a2 = -0.284496736, a3 = 1.421413741, a4 = -1.453152027, a5 = 1.061405429, p = 0.3275911;
    var t = 1 / (1 + p * x);
    var y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
    return sign * y;
  }
  function normalCDF(z) { return 0.5 * (1 + erf(z / Math.SQRT2)); }

  function renderTrend(trend) {
    var body = document.getElementById("outlookBody");
    if (!trend || trend.insufficient) {
      var have = trend ? trend.sampleSeconds || 0 : 0;
      body.innerHTML = '<div class="trend-warmup">Gathering data for a projection\\u2026 (' + have + 's of the first 60s collected)</div>';
      renderKalshi(trend && trend.kalshi, null);
      return;
    }

    var now = new Date();
    var minutesRemaining = 60 - now.getMinutes() - now.getSeconds() / 60;
    if (minutesRemaining <= 0) minutesRemaining = 60;

    // Target the fixed Kalshi strike (the "benchmark level") instead of the
    // live price — the live price moves every tick, which made this card
    // restate a new question every time it redrew. A strike only changes
    // when price drifts into a different bracket or the hourly market rolls,
    // so the question being asked stays put.
    var kalshiForModel = trend.kalshi;
    var benchmark = (kalshiForModel && kalshiForModel.available && kalshiForModel.nearestStrike)
      ? kalshiForModel.nearestStrike.strike
      : null;
    var threshold = benchmark != null ? benchmark : trend.currentPrice;

    var expectedMove = trend.driftPerMin * minutesRemaining;
    var edgeToThreshold = threshold - trend.currentPrice;
    var std = trend.volPerMin * Math.sqrt(Math.max(minutesRemaining, 0.01));
    var z = std > 0 ? (expectedMove - edgeToThreshold) / std : (expectedMove >= edgeToThreshold ? 5 : -5);
    var probUp = normalCDF(z);
    // clamp: a thin sample can saturate the CDF, and 100% is never honest
    probUp = Math.min(Math.max(probUp, 0.03), 0.97);
    var probPct = Math.round(probUp * 100); // always "P(above)" — matches the gauge and the Kalshi card
    var direction = probUp >= 0.5 ? "up" : "down";
    var headlinePct = direction === "up" ? probPct : 100 - probPct; // P(the stated direction)
    var earlyRead = trend.sampleMinutes < 10;

    var targetHour = new Date(now.getTime() + minutesRemaining * 60000);
    var targetLabel = targetHour.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

    var edge = Math.abs(probUp - 0.5);
    var confidence = edge > 0.25 ? "Strong" : edge > 0.1 ? "Leaning" : "Toss-up";

    var headline =
      '<div class="trend-headline">' +
      headlinePct + "% chance BTC is <b class=\\"" + direction + "\\">" + (direction === "up" ? "above" : "below") + "</b> " +
      "$" + Math.round(threshold).toLocaleString("en-US") +
      " by <b>" + targetLabel + "</b></div>";

    var pointerPct = Math.min(Math.max(probPct, 2), 98);
    var gauge =
      '<div class="gauge"><div class="gauge-pointer" style="left:' + pointerPct + '%"></div></div>' +
      '<div class="gauge-labels"><span>Below</span><span>' + confidence + "</span><span>Above</span></div>";

    var chips = '<div class="stat-chips">';
    if (trend.rsi != null) {
      var rsiCls = trend.rsi > 70 ? "red" : trend.rsi < 30 ? "green" : "yellow";
      var rsiLabel = trend.rsi > 70 ? "Overbought" : trend.rsi < 30 ? "Oversold" : "Neutral";
      chips += '<span class="chip ' + rsiCls + '">RSI ' + trend.rsi + " \\u00b7 " + rsiLabel + "</span>";
    }
    if (trend.streakCount > 1 && trend.streakDirection !== "flat") {
      var streakCls = trend.streakDirection === "up" ? "green" : "red";
      chips += '<span class="chip ' + streakCls + '">' + trend.streakCount + " " + trend.streakDirection + "-minutes in a row</span>";
    }
    chips += '<span class="chip">' + trend.sampleMinutes + "m sample</span>";
    if (earlyRead) chips += '<span class="chip yellow">Early read \\u2014 low confidence</span>';
    chips += "</div>";

    body.innerHTML = headline + gauge + chips;
    renderKalshi(trend.kalshi, probPct);
  }

  function renderKalshi(k, modelPct) {
    var card = document.getElementById("kalshiCard");
    var ns = k && k.nearestStrike;
    if (!k || !k.available || !ns || !isFinite(ns.bid) || !isFinite(ns.ask)) {
      card.innerHTML =
        '<div class="kalshi-hdr"><span class="kalshi-title">Kalshi Market Check</span></div>' +
        '<div class="kalshi-off">Kalshi hourly BTC market unavailable right now.</div>';
      return;
    }
    // Kalshi % is this specific strike's own quote (bid/ask mid) — the same
    // fixed benchmark the model above is now targeting, so the two numbers
    // answer the identical question and "Edge" is a real apples-to-apples gap.
    var marketPct = Math.round(((ns.bid + ns.ask) / 2) * 100);
    var edgeStr = "\\u2013", edgeCls = "flat";
    if (modelPct != null) {
      var edge = modelPct - marketPct;
      edgeCls = edge > 2 ? "up" : edge < -2 ? "down" : "flat";
      edgeStr = (edge > 0 ? "+" : "") + edge;
    }
    var closeStr = k.closeTime
      ? new Date(k.closeTime).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" })
      : "";

    var quote = Math.round(ns.bid * 100) + "\\u00a2/" + Math.round(ns.ask * 100) + "\\u00a2";
    var detail = '<div class="kalshi-detail">Nearest bracket: <b>' + ns.subtitle + "</b> \\u00b7 " + quote +
      (ns.vol ? " \\u00b7 " + Math.round(ns.vol).toLocaleString() + " vol" : "") + "</div>";

    card.innerHTML =
      '<div class="kalshi-hdr"><span class="kalshi-title">Kalshi Market Check</span>' +
      (closeStr ? '<span class="kalshi-close">settles ' + closeStr + "</span>" : "") + "</div>" +
      '<div class="kalshi-compare">' +
        '<div class="kalshi-cell"><div class="lbl">Model</div><div class="val">' + (modelPct != null ? modelPct + "%" : "\\u2013") + "</div></div>" +
        '<div class="kalshi-cell"><div class="lbl">Kalshi</div><div class="val">' + marketPct + "%</div></div>" +
        '<div class="kalshi-cell edge"><div class="lbl">Edge</div><div class="val ' + edgeCls + '">' + edgeStr + "</div></div>" +
      "</div>" + detail;
  }

  // ---------- live trade ticker ----------

  var tradesList = document.getElementById("tradesList");
  var MAX_VISIBLE_TRADES = 30;

  function pushTradeRow(trade) {
    var row = document.createElement("div");
    row.className = "trade-row";
    var d = new Date(trade.t);
    var time = d.toLocaleTimeString([], { hour12: false });
    row.innerHTML =
      '<span class="trade-time">' + time + "</span>" +
      '<span class="trade-ex">' + trade.ex + "</span>" +
      '<span class="trade-size">' + (trade.size || 0).toFixed(5) + " BTC</span>" +
      '<span class="trade-price ' + trade.dir + '">' + fmtUSDShort(trade.price) + "</span>";
    tradesList.appendChild(row);
    while (tradesList.children.length > MAX_VISIBLE_TRADES) tradesList.removeChild(tradesList.firstChild);
  }

  // ---------- websocket + fallback polling ----------

  var ws = null, wsReconnectDelay = 1000, pollTimer = null;

  function startPollingFallback() {
    if (pollTimer) return;
    pollTimer = setInterval(function () {
      fetch("/api/latest").then(function (r) { return r.json(); }).then(function (snap) {
        state.live.coinbase = snap.coinbase;
        state.live.bitstamp = snap.bitstamp;
        state.connCoinbase = snap.coinbaseConnected;
        state.connBitstamp = snap.bitstampConnected;
        state.high24 = snap.high24;
        state.low24 = snap.low24;
        state.lastMsgAt = Date.now();
        checkBigMove(snap.average);
        refreshHeaderAndRows();
        updateStatus();
      }).catch(function () {});
    }, 3000);
  }
  function stopPollingFallback() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  function applySnapshot(snap) {
    state.live.coinbase = snap.coinbase;
    state.live.bitstamp = snap.bitstamp;
    state.connCoinbase = snap.coinbaseConnected;
    state.connBitstamp = snap.bitstampConnected;
    state.high24 = snap.high24;
    state.low24 = snap.low24;
    state.lastMsgAt = Date.now();
    checkBigMove(snap.average);
    refreshHeaderAndRows();
    updateStatus();
  }

  function connectWS() {
    var proto = location.protocol === "https:" ? "wss://" : "ws://";
    ws = new WebSocket(proto + location.host + "/stream");

    ws.onopen = function () {
      wsReconnectDelay = 1000;
      stopPollingFallback();
    };

    ws.onmessage = function (evt) {
      var msg;
      try { msg = JSON.parse(evt.data); } catch (e) { return; }

      if (msg.type === "bootstrap") {
        applySnapshot(msg.snapshot);
        (msg.trades || []).forEach(pushTradeRow);
        renderTrend(msg.trend);
        fetchHistory();
        armAutoRefresh();
        return;
      }
      if (msg.type === "snapshot") { applySnapshot(msg); return; }
      if (msg.type === "trade") {
        var exKey = msg.ex === "coinbase" ? "coinbase" : "bitstamp";
        state.live[exKey] = msg.price;
        state.lastMsgAt = Date.now();
        refreshHeaderAndRows();
        appendLivePoint(recomputeAverage(), msg.size);
        pushTradeRow(msg);
        return;
      }
      if (msg.type === "trend") { renderTrend(msg); return; }
    };

    ws.onclose = function () {
      startPollingFallback();
      setTimeout(connectWS, wsReconnectDelay);
      wsReconnectDelay = Math.min(wsReconnectDelay * 2, 15000);
    };
    ws.onerror = function () { try { ws.close(); } catch (e) {} };
  }

  function pollTrend() {
    fetch("/api/trend").then(function (r) { return r.json(); }).then(renderTrend).catch(function () {});
  }
  setInterval(pollTrend, 15000);

  connectWS();
  fetchHistory();
  armAutoRefresh();
  pollTrend();
})();
</script>
</body>
</html>`;

// ---------- boot ----------

loadHistory();
connectCoinbase();
connectBitstamp();
server.listen(PORT, () => {
  console.log(`\nBitcoin ticker running at http://localhost:${PORT}\n`);
});
