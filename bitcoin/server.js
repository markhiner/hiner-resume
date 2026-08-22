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
const crypto = require("crypto");
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

// Staleness weighting. During fast moves one exchange (usually Bitstamp)
// can go quiet for seconds at a time while the other keeps printing. A
// flat 50/50 blend then drags the benchmark toward a price that no longer
// exists. Each feed keeps full weight while it is fresh, then decays
// exponentially, so a stale quote fades out of the average smoothly
// instead of either counting fully or vanishing at a cliff edge.
const FEED_FRESH_MS = 5_000; // full weight up to here
const FEED_TAU_MS = 10_000; // e-folding time after that
const FEED_MIN_WEIGHT = 0.02; // below this, drop the feed entirely

function feedWeight(feed, now) {
  if (feed.price == null || feed.ts == null) return 0;
  const age = now - feed.ts;
  if (age <= FEED_FRESH_MS) return 1;
  const w = Math.exp(-(age - FEED_FRESH_MS) / FEED_TAU_MS);
  return w < FEED_MIN_WEIGHT ? 0 : w;
}

function currentAverage() {
  const now = Date.now();
  const c = ex.coinbase, b = ex.bitstamp;
  const wc = feedWeight(c, now), wb = feedWeight(b, now);
  const total = wc + wb;
  if (total > 0) return ((c.price || 0) * wc + (b.price || 0) * wb) / total;
  // both stale — better a known old price than nothing; take the newer one
  if (c.price != null && b.price != null) return (c.ts || 0) >= (b.ts || 0) ? c.price : b.price;
  if (c.price != null) return c.price;
  if (b.price != null) return b.price;
  return null;
}

// The benchmark. BRTI is the index Kalshi settles on, so it is the price
// everything downstream runs on: the chart, the forecast model, the big-move
// flash and the strike the market check is interpolated at.
//
// It deliberately never falls back to the exchange blend once BRTI has been
// the series. The two differ by tens of dollars, so substituting one for the
// other mid-series would put a step in the history that reads as a real move
// — a false crash flash, and a volatility estimate poisoned for the next
// twenty minutes. Better to stop appending and leave an honest gap; the
// health light in the header is already saying DELAY or OFFLINE.
const BRTI_HOLD_MS = 90_000;

function benchmarkPrice() {
  // no credentials at all — the blend is the benchmark, and says so
  if (!brtiState.enabled) return currentAverage();
  // configured but not printing yet: WAIT. Opening the series on a blend tick
  // and stepping to BRTI a second later is the same splice this rule exists to
  // prevent, just at startup — it fires the big-move flash and hands the model
  // a jump it reads as real volatility for the next twenty minutes.
  if (brtiState.value == null) return null;
  if (Date.now() - (brtiState.ts || 0) > BRTI_HOLD_MS) return null;
  return brtiState.value;
}

// ---------- persistence ----------

// Which price the stored history was recorded against. Replaying blend-priced
// bars into a BRTI series (or the reverse) would splice two different numbers
// into one line and show a $50 cliff at the restart point, so a mismatch
// discards the file instead.
function historySeries() {
  return brtiState.enabled ? "brti" : "blend";
}

function loadHistory() {
  try {
    const parsed = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    const want = historySeries();
    if (parsed.series !== want) {
      console.log(`History on disk is a "${parsed.series || "blend"}" series, this run is "${want}" — starting fresh`);
      return;
    }
    // Drop anything already outside the window it feeds before it takes up a
    // slot. secondTicks is capped by COUNT, so ticks from yesterday sit in the
    // buffer occupying room that fresh ones need — they are never read (the
    // only consumer is the 1h range) and they push the buffer's real coverage
    // well under an hour until they finally age out.
    const now = Date.now();
    if (Array.isArray(parsed.minuteBars)) {
      minuteBars = parsed.minuteBars.filter((b) => b && now - b.t < 24 * 60 * 60 * 1000).slice(-MAX_MINUTE_BARS);
    }
    if (Array.isArray(parsed.secondTicks)) {
      secondTicks = parsed.secondTicks.filter((p) => p && now - p.t < 60 * 60 * 1000).slice(-MAX_SECOND_TICKS);
    }
    console.log(`Loaded ${minuteBars.length} minute bars, ${secondTicks.length} second ticks from disk`);
  } catch {
    console.log("No prior history on disk, starting fresh");
  }
}

function saveHistory() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ series: historySeries(), minuteBars, secondTicks }));
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
  const avg = benchmarkPrice();
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

// How long the price has been going the way it is going. Walks back through
// the minute bars while the sign of each step holds; a flat minute neither
// breaks the run nor counts toward it. Rides the snapshot so the discreet
// readout in stealth mode needs no extra request.
function priceTrend() {
  const live = benchmarkPrice();
  const closes = minuteBars.map((b) => b.close).filter((n) => Number.isFinite(n));
  if (live != null) closes.push(live);
  let dir = 0, minutes = 0;
  for (let i = closes.length - 1; i > 0; i--) {
    const step = Math.sign(closes[i] - closes[i - 1]);
    if (step === 0) continue;
    if (dir === 0) dir = step;
    else if (step !== dir) break;
    minutes++;
  }
  return { dir, minutes };
}

function snapshot() {
  const avg = benchmarkPrice();
  const prevAvg = secondTicks.length > 1 ? secondTicks[secondTicks.length - 2].avg : avg;
  const now = Date.now();
  return {
    ts: now,
    coinbaseWeight: Math.round(feedWeight(ex.coinbase, now) * 100) / 100,
    bitstampWeight: Math.round(feedWeight(ex.bitstamp, now) * 100) / 100,
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
    // BRTI is the headline price now, so it rides the 1/sec heartbeat rather
    // than only the Kalshi market poll (which returns early when there are no
    // markets to quote, and would leave the price stranded).
    brti: brtiPublic(),
    trend: priceTrend(),
  };
}

// `lastMsg` is a diagnostic for /api/brti; the page never needs it, and it is
// the largest field in a message sent once a second to every client.
function brtiPublic() {
  const { lastMsg, ...rest } = brtiState;
  return rest;
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
// Statistical estimate — not financial advice. The probability is computed
// HERE on the server (single source of truth; clients just display it):
//   drift      exponentially-weighted regression over the last 20 min, so
//              an old spike stops driving "current" momentum once price
//              goes flat (recent minutes dominate, tau = 5 min)
//   momentum   drift is NOT extrapolated linearly to settlement — minute-
//              scale momentum decays fast, so its contribution saturates
//              at ~MOMENTUM_TAU_MIN minutes' worth (OU-style persistence)
//   exhaustion an extreme RSI damps drift in its own direction (a move
//              that already looks overbought gets less extrapolation)
//   volatility blend of three horizons (EW per-second, 1-min bars over the
//              last hour, 5-min bars over 3h) with an absolute floor, so
//              neither microstructure noise nor a quiet stretch dominates
//   drift SE   the regression slope's own standard error inflates the
//              projection variance — a noisy trend fit means less certainty
//   fat tails  Student-t (nu=4) CDF instead of Gaussian, softening the
//              extremes BTC actually violates
//   anchor     settlement time comes from the live Kalshi market's close
//              (top of the hour), not the viewer's wall clock
//   smoothing  output is EWMA-smoothed across updates unless the target
//              strike changes, so the card doesn't twitch

const DRIFT_TAU_SEC = 300;      // e-folding age for regression weights
const MOMENTUM_TAU_MIN = 6;     // how long current momentum is trusted to persist
const VOL_FLOOR_FRAC = 0.00008; // per-minute vol floor as a fraction of price (~0.8bp)

function weightedRegression(points, tauSec, tNow) {
  // points: { t (ms), y }, weights decay exponentially with age
  if (points.length < 2) return null;
  const t0 = points[0].t;
  let sw = 0, swx = 0, swy = 0, swxx = 0, swxy = 0;
  for (const p of points) {
    const w = Math.exp(-(tNow - p.t) / 1000 / tauSec);
    const x = (p.t - t0) / 60000;
    sw += w; swx += w * x; swy += w * p.y; swxx += w * x * x; swxy += w * x * p.y;
  }
  const denom = sw * swxx - swx * swx;
  if (denom === 0 || sw === 0) return null;
  const slope = (sw * swxy - swx * swy) / denom;
  const intercept = (swy - slope * swx) / sw;
  let ssr = 0;
  for (const p of points) {
    const w = Math.exp(-(tNow - p.t) / 1000 / tauSec);
    const x = (p.t - t0) / 60000;
    ssr += w * (p.y - (slope * x + intercept)) ** 2;
  }
  const residVar = ssr / sw;
  const sxx = swxx - (swx * swx) / sw;
  const slopeSE = sxx > 0 ? Math.sqrt(residVar / sxx) : 0;
  return { slope, intercept, residualStd: Math.sqrt(residVar), slopeSE };
}

// Student-t CDF, nu=4 (closed form; fatter tails than Gaussian)
function tCDF4(x) {
  const q = 1 + (x * x) / 4;
  return 0.5 + (3 / 8) * (x / Math.sqrt(q)) * (1 - (x * x) / (12 * q));
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

function barCloseVol(closes, barMinutes) {
  // stdev of close-to-close moves, normalized to a per-minute figure
  if (closes.length < 5) return null;
  const rets = [];
  for (let i = 1; i < closes.length; i++) rets.push(closes[i] - closes[i - 1]);
  const m = rets.reduce((a, b) => a + b, 0) / rets.length;
  const v = rets.reduce((a, b) => a + (b - m) ** 2, 0) / rets.length;
  const s = Math.sqrt(v) / Math.sqrt(barMinutes);
  return isFinite(s) && s > 0 ? s : null;
}

function computeTrend() {
  const now = Date.now();
  const lookbackMs = 20 * 60 * 1000;
  const relevant = secondTicks.filter((p) => now - p.t <= lookbackMs);
  if (relevant.length < 60) {
    return { insufficient: true, sampleSeconds: relevant.length, warmupSeconds: 60 };
  }

  const reg = weightedRegression(relevant.map((p) => ({ t: p.t, y: p.avg })), DRIFT_TAU_SEC, now);
  if (!reg) return { insufficient: true, sampleSeconds: relevant.length };

  const price = relevant[relevant.length - 1].avg;

  // Volatility component 1: exponentially-weighted per-second realized vol,
  // scaled to per-minute (recent choppiness counts most, but nothing that
  // happened inside the window is invisible the way once-a-minute sampling was)
  let sw = 0, swr2 = 0;
  for (let i = 1; i < relevant.length; i++) {
    const r = relevant[i].avg - relevant[i - 1].avg;
    const w = Math.exp(-(now - relevant[i].t) / 1000 / DRIFT_TAU_SEC);
    sw += w; swr2 += w * r * r;
  }
  const vSec = sw > 0 ? Math.sqrt(swr2 / sw) * Math.sqrt(60) : null;

  // Component 2: 1-minute bar closes over the last hour
  const allCloses = minuteBars.map((b) => b.close).concat(currentBar ? [currentBar.close] : []);
  const vMin = barCloseVol(allCloses.slice(-60), 1);

  // Component 3: 5-minute closes over the last 3 hours (regime context)
  const fiveMinCloses = [];
  const recent3h = minuteBars.slice(-36 * 5);
  for (let i = 4; i < recent3h.length; i += 5) fiveMinCloses.push(recent3h[i].close);
  const v5Min = barCloseVol(fiveMinCloses, 5);

  // Blend whatever components exist (weights renormalized), then floor
  const comps = [[vSec, 0.5], [vMin, 0.3], [v5Min, 0.2]].filter((c) => c[0] != null);
  let volPerMin = reg.residualStd || 1;
  if (comps.length) {
    const wSum = comps.reduce((a, c) => a + c[1], 0);
    volPerMin = Math.sqrt(comps.reduce((a, c) => a + (c[1] / wSum) * c[0] * c[0], 0));
  }
  volPerMin = Math.max(volPerMin, VOL_FLOOR_FRAC * price);

  const rsi = computeRSI(allCloses, 14);
  const streak = computeStreak(allCloses.slice(-30));

  // Short-horizon drift (last 3 min, fast decay) — used to detect when the
  // most recent action has turned AGAINST the medium trend, e.g. a rally
  // that just got sold. The medium EW average can't flip sign for several
  // minutes after a reversal; this can.
  const shortTicks = relevant.filter((p) => now - p.t <= 3 * 60 * 1000);
  const regShort = shortTicks.length >= 60
    ? weightedRegression(shortTicks.map((p) => ({ t: p.t, y: p.avg })), 90, now)
    : null;

  return {
    insufficient: false,
    driftPerMin: reg.slope,
    driftShortPerMin: regShort ? regShort.slope : null,
    driftSE: reg.slopeSE,
    volPerMin,
    sampleMinutes: Math.round((relevant.length / 60) * 10) / 10,
    currentPrice: price,
    rsi: rsi == null ? null : Math.round(rsi * 10) / 10,
    streakCount: streak.count,
    streakDirection: streak.direction,
  };
}

// ---------- forecast: probability of settling above the benchmark ----------

let forecastSmooth = { prob: null, threshold: null };

function computeForecast() {
  const trend = computeTrend();
  if (trend.insufficient) return { ...trend, kalshi: kalshiState };

  const price = trend.currentPrice;
  const k = kalshiState;
  const hasK = !!(k && k.available && k.nearestStrike);
  const threshold = hasK ? k.nearestStrike.strike : price;

  // settle at the Kalshi market close when known; else next top of the UTC hour
  const now = Date.now();
  let settleTs = hasK && k.closeTime ? k.closeTime : null;
  if (!settleTs || settleTs <= now) {
    const d = new Date(now);
    d.setUTCMinutes(60, 0, 0);
    settleTs = d.getTime();
  }
  const T = Math.min(Math.max((settleTs - now) / 60000, 0.25), 90);

  // momentum persists ~MOMENTUM_TAU_MIN minutes, not the whole horizon
  const persist = MOMENTUM_TAU_MIN * (1 - Math.exp(-T / MOMENTUM_TAU_MIN));

  // momentum agreement: when the last ~3 minutes have turned against the
  // medium trend (opposite signs), the trend is contested — don't
  // extrapolate either side; collapse to a heavily-shrunk average so the
  // gap and volatility dominate the forecast instead
  let drift = trend.driftPerMin;
  let contested = false;
  if (trend.driftShortPerMin != null && drift * trend.driftShortPerMin < 0) {
    contested = true;
    drift = 0.25 * (drift + trend.driftShortPerMin);
  }

  // exhaustion: extreme RSI damps drift in its own direction
  if (trend.rsi != null) {
    if (trend.rsi > 70 && drift > 0) drift *= Math.max(0.3, 1 - (trend.rsi - 70) / 40);
    else if (trend.rsi < 30 && drift < 0) drift *= Math.max(0.3, 1 - (30 - trend.rsi) / 40);
  }

  const expectedMove = drift * persist;
  const gap = threshold - price;
  // diffusion variance plus the drift estimate's own uncertainty
  const varMove = trend.volPerMin ** 2 * T + ((trend.driftSE || 0) * persist) ** 2;
  const z = varMove > 0 ? (expectedMove - gap) / Math.sqrt(varMove) : (expectedMove >= gap ? 5 : -5);

  let p = tCDF4(z);
  p = Math.min(Math.max(p, 0.03), 0.97);
  if (forecastSmooth.prob != null && forecastSmooth.threshold === threshold) {
    p = 0.6 * p + 0.4 * forecastSmooth.prob;
  }
  forecastSmooth = { prob: p, threshold };

  return {
    ...trend,
    kalshi: k,
    model: {
      probAbove: Math.round(p * 1000) / 1000,
      threshold,
      benchmark: hasK,
      minutesRemaining: Math.round(T * 10) / 10,
      settleTs,
      z: Math.round(z * 100) / 100,
      expectedMove: Math.round(expectedMove * 100) / 100,
      gap: Math.round(gap * 100) / 100,
      contested,
    },
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
// Structure (which hourly event is live, the full strike ladder, your
// position list) changes slowly; quotes move constantly. Poll them at
// different rates and batch every fast request into ONE call via the
// markets?tickers= endpoint, so per-second refresh costs ~1 request/sec
// rather than dozens.
const KALSHI_STRUCTURE_MS = Number(process.env.KALSHI_STRUCTURE_MS) || 20_000;
// 400ms, not 1s. Measured end to end against a stubbed Kalshi at 300ms
// round-trip: 1s delivered 1.10 P&L updates/sec, 400ms delivers 2.60/sec with
// zero skipped ticks. Going below that is wasted work — at 250ms the interval
// drops under the round-trip and the loop just churns (47 skips in 20s, and
// no more updates than 400ms). One batched request per tick regardless of how
// many positions are held, so this is ~2.5 reads/sec.
const KALSHI_QUOTE_MS = Number(process.env.KALSHI_QUOTE_MS) || 400;

let kalshiState = { available: false, error: null, updatedAt: 0 };
let kalshiLadder = []; // full strike ladder for the live event, quotes patched in place
let kalshiEvent = null; // { ticker, close }
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

// Slow: find the live hourly event and pull its full strike ladder.
async function pollKalshiStructure() {
  try {
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
    kalshiEvent = candidates[0];

    const { markets = [] } = await kalshiGET(`/markets?event_ticker=${kalshiEvent.ticker}&status=open&limit=200`);
    kalshiLadder = markets
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

    recomputeKalshiState();
  } catch (e) {
    kalshiState = { ...kalshiState, available: false, error: e.message, updatedAt: Date.now() };
  }
}

// Derive the displayed state from whatever quotes the ladder currently holds.
function recomputeKalshiState() {
  const price = benchmarkPrice();
  let implied = null, below = null, above = null;
  if (price != null) {
    for (const r of kalshiLadder) {
      if (r.mid == null) continue;
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
    updatedAt: Date.now(),
    eventTicker: kalshiEvent ? kalshiEvent.ticker : null,
    closeTime: kalshiEvent ? kalshiEvent.close : null,
    impliedProbAbove: implied != null ? Math.round(implied * 1000) / 1000 : null,
    refPrice: price,
    nearestStrike: nearest
      ? { strike: nearest.strike, subtitle: nearest.subtitle, bid: nearest.bid, ask: nearest.ask, vol: nearest.vol, ticker: nearest.ticker }
      : null,
  };
  return { below, above };
}

// Fast: one batched request refreshing only the quotes that are on screen —
// the two strikes bracketing the current price, plus every held position.
// setInterval fires on a timer, not on completion. Without a guard a request
// slower than the interval stacks up behind the next one, and two in flight
// can land out of order — an older quote overwriting a newer one, which shows
// up as P&L that jumps backwards or sits still. Skip the tick instead, and
// keep the counters so the real cadence is measurable rather than assumed.
let quoteInFlight = false;
let quoteStats = { lastOkAt: null, lastMs: null, skipped: 0, errors: 0, ticks: 0 };

async function refreshQuotes() {
  if (quoteInFlight) { quoteStats.skipped++; return; }
  quoteInFlight = true;
  quoteStats.ticks++;
  const startedAt = Date.now();
  try {
    const { below, above } = recomputeKalshiState();
    const wanted = new Set();
    if (below) wanted.add(below.ticker);
    if (above) wanted.add(above.ticker);
    for (const r of portfolioState.positions || []) if (r.ticker) wanted.add(r.ticker);
    if (!wanted.size) return; // finally{} still clears quoteInFlight

    const { markets = [] } = await kalshiGET(
      `/markets?tickers=${encodeURIComponent([...wanted].join(","))}&limit=${wanted.size}`
    );
    if (!markets.length) return;

    const byTicker = new Map(markets.map((m) => [m.ticker, m]));
    for (const row of kalshiLadder) {
      const m = byTicker.get(row.ticker);
      if (!m) continue;
      row.bid = parseFloat(m.yes_bid_dollars);
      row.ask = parseFloat(m.yes_ask_dollars);
      row.mid = ladderMid(m);
      row.vol = parseFloat(m.volume_fp) || row.vol;
    }
    recomputeKalshiState();
    repricePortfolio(byTicker);
    quoteStats.lastMs = Date.now() - startedAt;
    quoteStats.lastOkAt = Date.now();
    broadcast({ type: "kalshi", kalshi: kalshiState, portfolio: portfolioState, brti: brtiPublic(), quote: quoteSnapshot() });
  } catch (e) {
    quoteStats.errors++;
    kalshiState = { ...kalshiState, error: e.message };
  } finally {
    quoteInFlight = false;
  }
}

// What the quote loop is actually achieving, as opposed to what the interval
// asks for: if Kalshi round-trips slower than KALSHI_QUOTE_MS from wherever
// this runs, `skipped` climbs and the true refresh rate is lastMs, not 1s.
function quoteSnapshot() {
  return {
    intervalMs: KALSHI_QUOTE_MS,
    portfolioPollMs: PORTFOLIO_POLL_MS,
    lastRoundTripMs: quoteStats.lastMs,
    ageMs: quoteStats.lastOkAt ? Date.now() - quoteStats.lastOkAt : null,
    ticks: quoteStats.ticks,
    skipped: quoteStats.skipped,
    errors: quoteStats.errors,
  };
}

setInterval(pollKalshiStructure, KALSHI_STRUCTURE_MS);
setInterval(refreshQuotes, KALSHI_QUOTE_MS);
pollKalshiStructure();

// ---------- Kalshi portfolio (optional — needs API credentials) ----------
// Set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH (same env vars the
// Python lab uses) and the ticker shows your open contracts marked to the
// live bid, plus cash. Without credentials this whole module is inert.
// Request signing: RSA-PSS/SHA256 over "{timestamp_ms}{METHOD}{path}",
// query string excluded from the signed path — same scheme as
// lab/kalshi_client.py.

const KALSHI_KEY_ID = process.env.KALSHI_API_KEY_ID || null;
const KALSHI_KEY_PATH = process.env.KALSHI_PRIVATE_KEY_PATH || null;
// Selling places REAL orders against a REAL account, and this page is
// reachable by anyone who knows the hostname. It is therefore off unless
// SELL_PIN is set, and every sell must carry that PIN — which lives only
// in the server's environment, never in the page source.
const SELL_PIN = process.env.SELL_PIN || null;
// Position *structure* (which contracts you hold, cost basis, cash) only
// changes when you trade or a market settles; the fast quote tick handles
// marking those positions to market every second.
const PORTFOLIO_POLL_MS = Number(process.env.KALSHI_PORTFOLIO_MS) || 15_000;

let kalshiPrivateKey = null;
if (KALSHI_KEY_ID && KALSHI_KEY_PATH) {
  try {
    kalshiPrivateKey = crypto.createPrivateKey(fs.readFileSync(KALSHI_KEY_PATH));
    console.log("Kalshi portfolio: credentials loaded");
  } catch (e) {
    console.error("Kalshi portfolio: could not load private key:", e.message);
  }
}

let portfolioState = {
  enabled: !!(KALSHI_KEY_ID && kalshiPrivateKey),
  sellEnabled: !!(KALSHI_KEY_ID && kalshiPrivateKey && SELL_PIN),
  updatedAt: 0,
  error: null,
  cash: null,
  positions: null,
  positionsValue: null,
  totalValue: null,
};
if (portfolioState.sellEnabled) console.log("Kalshi selling: ENABLED (PIN required per order)");

function kalshiSignHeaders(method, signPath) {
  const ts = String(Date.now());
  const sig = crypto.sign("sha256", Buffer.from(ts + method + signPath), {
    key: kalshiPrivateKey,
    padding: crypto.constants.RSA_PKCS1_PSS_PADDING,
    saltLength: crypto.constants.RSA_PSS_SALTLEN_DIGEST,
  });
  return {
    "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
    "KALSHI-ACCESS-SIGNATURE": sig.toString("base64"),
    "KALSHI-ACCESS-TIMESTAMP": ts,
  };
}

async function kalshiAuthGET(pathname) {
  const signPath = "/trade-api/v2" + pathname.split("?")[0];
  const res = await fetch(KALSHI_API + pathname, {
    headers: { Accept: "application/json", ...kalshiSignHeaders("GET", signPath) },
  });
  if (!res.ok) throw new Error(`kalshi auth ${pathname.split("?")[0]} → ${res.status}`);
  return res.json();
}

// Kalshi errors come back as {error:{code,message,details}} — flatten that
// to something readable instead of "[object Object]".
function kalshiErrText(json, text) {
  const e = json && json.error;
  if (e && typeof e === "object") {
    return [e.code, e.message].filter(Boolean).join(": ") || JSON.stringify(e);
  }
  if (typeof e === "string") return e;
  if (json && json.message) return json.message;
  return (text || "").slice(0, 300);
}

async function kalshiAuthPOST(base, pathname, body) {
  const signPath = "/trade-api/v2" + pathname.split("?")[0];
  const res = await fetch(base + pathname, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...kalshiSignHeaders("POST", signPath),
    },
    body: JSON.stringify(body),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch {}
  if (!res.ok) {
    console.error("Kalshi order response:", res.status, text.slice(0, 500));
    throw new Error(`kalshi ${res.status}: ${kalshiErrText(json, text)}`);
  }
  return json;
}

// Close an entire open position at the going price.
//
// Uses Kalshi's V2 order API. The old /portfolio/orders endpoint now
// returns 410 deprecated_v1_order_endpoint. V2 differs in three ways that
// matter here:
//   * different host (external-api.kalshi.com) and path
//   * everything is expressed on the YES leg: side "ask" = sell YES,
//     side "bid" = buy YES. Holding NO is short YES, so closing a NO
//     position means BUYING yes.
//   * count and price are fixed-point strings, price in DOLLARS not cents
//
// Priced to cross the spread so it fills right away — hit the yes bid when
// selling yes, lift the yes ask when buying back yes — with
// immediate_or_cancel so nothing is left resting, and reduce_only so a
// mistake here can only ever shrink a position, never open a new one.
const KALSHI_TRADE_API = "https://external-api.kalshi.com/trade-api/v2";
const BUY_COUNT = Number(process.env.BUY_COUNT || 10);

// action "sell" closes the whole position; "buy" adds BUY_COUNT more of
// the side already held. Everything is quoted on the YES leg, and each
// order is priced to cross so it fills immediately:
//
//   hold YES, sell -> ask @ yes_bid      hold YES, buy -> bid @ yes_ask
//   hold NO,  sell -> bid @ yes_ask      hold NO,  buy -> ask @ yes_bid
//
// (buying NO *is* selling YES, so the NO rows are the mirror image; the
// prices reconcile exactly against the no leg, e.g. 1 - yes_bid = no_ask)
async function tradePosition(ticker, action) {
  const row = (portfolioState.positions || []).find((r) => r.ticker === ticker);
  if (!row) throw new Error("no such open position");
  if (action === "sell" && (!Number.isFinite(row.count) || row.count <= 0)) {
    throw new Error("position quantity unknown");
  }

  const { market } = await kalshiGET(`/markets/${encodeURIComponent(ticker)}`);
  if (!market) throw new Error("market not found");
  if (market.status && market.status !== "active") throw new Error(`market is ${market.status}`);

  const holdingYes = row.side === "yes";
  const selling = action === "sell";
  const orderSide = selling ? (holdingYes ? "ask" : "bid") : (holdingYes ? "bid" : "ask");
  const priceKey = selling ? (holdingYes ? "yes_bid" : "yes_ask") : (holdingYes ? "yes_ask" : "yes_bid");
  const price = money(market, priceKey);
  if (price == null || price <= 0 || price >= 1) throw new Error("no usable quote to trade against");

  // The order is priced on the YES leg because that is how V2 quotes
  // everything, but the number shown back to the user has to be the price
  // on the side they actually hold. For a NO position those are opposites:
  // buying NO is selling YES, so a $0.40 yes-leg order means paying $0.60
  // per NO contract. Reporting the raw order price made a 60c fill read as
  // "Bought 10 @ 40c".
  const shownPrice = holdingYes ? price : 1 - price;

  const count = selling ? row.count : BUY_COUNT;
  const order = {
    ticker,
    side: orderSide,
    count: String(count),
    price: price.toFixed(2),
    time_in_force: "immediate_or_cancel",
    self_trade_prevention_type: "taker_at_cross",
    client_order_id: crypto.randomUUID(),
  };
  // reduce_only guarantees a sell can never accidentally open a position.
  // A buy is meant to increase one, so it cannot use that guard — its
  // protection is the fixed lot size: BUY_COUNT contracts cap out at $1
  // each, so the most a single tap can ever commit is BUY_COUNT dollars.
  if (selling) order.reduce_only = true;

  console.log(
    `${action.toUpperCase()} ${row.side.toUpperCase()} x${count} ${ticker} -> ` +
    `${orderSide} @ $${order.price} yes-leg (= ${Math.round(shownPrice * 100)}c on the ${row.side} leg)`
  );
  const resp = await kalshiAuthPOST(KALSHI_TRADE_API, "/portfolio/events/orders", order);
  const filled = resp && (resp.fill_count != null ? resp.fill_count : null);
  console.log("  order ack:", JSON.stringify(resp));
  await pollPortfolio(); // refresh immediately so the card reflects the trade
  return {
    ok: true,
    action,
    ticker,
    side: row.side,
    count,
    priceCents: Math.round(shownPrice * 100),      // on the side actually held
    orderPriceCents: Math.round(price * 100),      // the yes-leg price sent to Kalshi
    filled,
    order: resp,
  };
}

function dollars(obj, dollarsKey, centsKey) {
  const d = parseFloat(obj?.[dollarsKey]);
  if (Number.isFinite(d)) return d;
  const c = obj?.[centsKey];
  return typeof c === "number" ? c / 100 : null;
}

// This API version reports quantities under _fp-suffixed keys
// (volume_fp, open_interest_fp, ...) and prices under _dollars, but the
// exact key varies by endpoint and version. Take the first key that
// actually parses to a finite number rather than betting on one name —
// reading a key that doesn't exist yields undefined, and Math.abs(undefined)
// is NaN, which JSON.stringify turns into null ("NO xnull" on the card).
function firstNum(obj, ...keys) {
  for (const k of keys) {
    const v = parseFloat(obj?.[k]);
    if (Number.isFinite(v)) return v;
  }
  return null;
}

// Money fields come as either "<base>_dollars" (a decimal string) or a bare
// "<base>" in integer cents, depending on endpoint and API version.
function money(obj, base) {
  const d = parseFloat(obj?.[`${base}_dollars`]);
  if (Number.isFinite(d)) return d;
  const c = parseFloat(obj?.[base]);
  if (Number.isFinite(c)) return c / 100;
  return null;
}

// ---------- settlement ----------
// A binary contract does not trade at the end — it RESOLVES, paying $1 on the
// winning side and $0 on the losing one. A closed market's bid is 0 on both
// sides, so marking a held position to the bid the way an open one is marked
// reports a winner as a total loss: value $0, P&L equal to the whole stake.
// That is what happened to a NO position on "$64,300 or above" when the
// benchmark settled at $64,281 — it won, and the card showed it wiped out.

function marketResult(m) {
  const r = String(m?.result || "").toLowerCase();
  return r === "yes" || r === "no" ? r : null;
}

function marketClosed(m, closeTime) {
  if (/closed|settled|finalized|determined/i.test(String(m?.status || ""))) return true;
  return closeTime != null && Date.now() >= closeTime;
}

// The settlement figure for an hour: the mean of the sixty benchmark prints
// before it. Same window Kalshi settles on, computed from the ticks this
// server recorded, so an outcome can be called in the gap between the market
// closing and Kalshi publishing its result.
function settlementAverage(closeMs) {
  if (!closeMs) return null;
  const vals = secondTicks.filter((p) => p.t >= closeMs - 60_000 && p.t < closeMs).map((p) => p.avg);
  if (vals.length < 30) return null; // too little of the window to call it
  return vals.reduce((a, b) => a + b, 0) / vals.length;
}

// true = this position won, false = lost, null = can't tell yet.
function provisionalOutcome(r) {
  if (r.strike == null || !r.strikeDir) return null;
  const s = settlementAverage(r.closeTime);
  if (s == null) return null;
  const yesWon = r.strikeDir === "above" ? s >= r.strike : s < r.strike;
  return r.side === "yes" ? yesWon : !yesWon;
}

// Mark every held position to the live bid on its own side and derive
// unrealized P&L against its cost basis. Called on the fast quote tick, so
// values and P&L move in step with the market rather than once a poll.
function repricePortfolio(byTicker) {
  const rows = portfolioState.positions;
  if (!rows || !rows.length) return;
  let posValue = 0, totalPnl = 0, anyPnl = false;
  for (const r of rows) {
    const m = byTicker && byTicker.get(r.ticker);
    // The fast batch returns the whole market, not just its quote, so
    // resolution state refreshes every second here instead of waiting on the
    // 15s portfolio poll — a settled market is recognised, and a win
    // announced, within a second of Kalshi publishing it.
    if (m) {
      const res = marketResult(m);
      if (res) r.result = res;
      if (m.status) r.status = m.status;
      r.closed = marketClosed(m, r.closeTime);
    } else if (!r.closed) {
      r.closed = marketClosed(null, r.closeTime); // close_time alone can decide it
    }
    if (r.result || r.closed) r.settleAvg = settlementAverage(r.closeTime);
    if (r.result) {
      // Kalshi has published the outcome — that is the price, full stop
      r.won = r.side === r.result;
      r.perContract = r.won ? 1 : 0;
      r.resolved = true;
      r.pending = false;
    } else if (r.closed) {
      // closed but not yet published: call it from our own settlement window
      // and say plainly that it is provisional
      r.resolved = false;
      r.pending = true;
      r.won = provisionalOutcome(r);
      r.perContract = r.won == null ? null : (r.won ? 1 : 0);
    } else {
      r.resolved = false;
      r.pending = false;
      r.won = null;
      if (m) {
        const per = money(m, r.side === "yes" ? "yes_bid" : "no_bid");
        if (per != null) r.perContract = per;
      }
    }
    if (r.perContract != null && Number.isFinite(r.count)) {
      const v = r.count * r.perContract;
      r.value = Math.round(v * 100) / 100;
      posValue += v;
      if (r.costBasis != null) {
        r.pnl = Math.round((v - r.costBasis) * 100) / 100;
        r.pnlPct = r.costBasis > 0 ? Math.round((r.pnl / r.costBasis) * 1000) / 10 : null;
        totalPnl += v - r.costBasis;
        anyPnl = true;
      }
    }
  }
  portfolioState = {
    ...portfolioState,
    positions: rows,
    positionsValue: Math.round(posValue * 100) / 100,
    totalValue: portfolioState.cash != null ? Math.round((portfolioState.cash + posValue) * 100) / 100 : null,
    unrealizedPnl: anyPnl ? Math.round(totalPnl * 100) / 100 : null,
    updatedAt: Date.now(),
  };
}

const warnedFractional = new Set();
const RESOLVED_HOLD_MS = Number(process.env.KALSHI_RESOLVED_HOLD_MS) || 120_000;
const resolvedMemo = new Map(); // ticker -> { row, until }

async function pollPortfolio() {
  if (!portfolioState.enabled) return;
  try {
    // This endpoint returns POSITION HISTORY, not just what is open now, so
    // ask it to filter. Without settlement_status=unsettled every market ever
    // traded comes back and — now that the quantity field parses to a non-zero
    // number for those old rows rather than undefined — every one of them
    // rendered as a settled loss. Hours of them, stacked up on the card.
    const [bal, pos] = await Promise.all([
      kalshiAuthGET("/portfolio/balance"),
      kalshiAuthGET("/portfolio/positions?limit=200&settlement_status=unsettled")
        .catch(() => kalshiAuthGET("/portfolio/positions?limit=200")),
    ]);
    const cash = dollars(bal, "balance_dollars", "balance");

    const open = (pos.market_positions || [])
      .map((p) => ({ p, qty: firstNum(p, "position", "position_fp", "quantity", "quantity_fp") }))
      .filter((r) => r.qty != null && r.qty !== 0);

    // Contract counts are whole numbers. A fractional one means the field we
    // picked is fixed-point at some scale we have not accounted for, and that
    // scale flows straight into the payout ($1 x count), so log the raw
    // payload once rather than quietly reporting the wrong money.
    for (const { p, qty } of open) {
      if (Number.isInteger(qty) || warnedFractional.has(p.ticker)) continue;
      warnedFractional.add(p.ticker);
      console.warn(
        `Kalshi position ${p.ticker}: quantity parsed as ${qty}, which is not a whole ` +
        `number of contracts. Raw fields: ` +
        JSON.stringify(Object.fromEntries(Object.entries(p).filter(([k]) => /position|quantity|exposure|traded|pnl|fp$/i.test(k))))
      );
    }

    const rows = [];
    for (const { p, qty } of open.slice(0, 20)) {
      let m = null;
      try { m = (await kalshiGET(`/markets/${p.ticker}`)).market; } catch {}
      const side = qty > 0 ? "yes" : "no";
      const count = Math.abs(qty);
      const per = m ? money(m, side === "yes" ? "yes_bid" : "no_bid") : null;
      // What the open position cost: market_exposure is the standard field;
      // fall back to total_traded net of realized P&L. Left null when it
      // can't be determined, so the UI shows no P&L rather than a wrong one.
      let costBasis = money(p, "market_exposure");
      if (costBasis == null) {
        const traded = money(p, "total_traded");
        const realized = money(p, "realized_pnl");
        if (traded != null) costBasis = traded - (realized || 0);
      }
      if (costBasis != null) costBasis = Math.abs(costBasis);
      const label = m
        ? (m.title && m.yes_sub_title && !/^yes$/i.test(m.yes_sub_title)
            ? m.yes_sub_title
            : (m.title || m.yes_sub_title || m.subtitle || p.ticker))
        : p.ticker;

      // A compact strike for the row: "$65,000+" / "$65,000-" instead of
      // "$65,000 or above", which overflows the row and gets ellipsised.
      // Taken from the market's own numeric strike rather than parsed out
      // of the label text. Range markets (both a floor and a cap) can't be
      // expressed as one strike, so those keep the full label.
      let strike = null, strikeDir = null;
      if (m) {
        const floorS = firstNum(m, "floor_strike");
        const capS = firstNum(m, "cap_strike");
        const st = String(m.strike_type || "");
        if (/^greater/.test(st) && floorS != null) { strike = floorS; strikeDir = "above"; }
        else if (/^less/.test(st) && capS != null) { strike = capS; strikeDir = "below"; }
        else if (floorS != null && capS == null) { strike = floorS; strikeDir = "above"; }
        else if (capS != null && floorS == null) { strike = capS; strikeDir = "below"; }
      }

      const closeTime = m ? Date.parse(m.close_time) || null : null;
      // Second line of defence, in case settlement_status is not honoured by
      // this API version: a market that closed longer ago than the hold window
      // has had its outcome shown already and is simply history now.
      if (closeTime != null && Date.now() - closeTime > RESOLVED_HOLD_MS) continue;
      rows.push({
        ticker: p.ticker,
        subtitle: label,
        strike,
        strikeDir,
        closeTime,
        side,
        count,
        perContract: per,
        value: per != null ? Math.round(count * per * 100) / 100 : null,
        costBasis,
        avgCost: costBasis != null && count > 0 ? Math.round((costBasis / count) * 100) / 100 : null,
        pnl: null,
        pnlPct: null,
        status: m ? m.status || null : null,
        result: marketResult(m),
        closed: marketClosed(m, closeTime),
      });
    }

    // A settled position drops out of Kalshi's list the moment it pays out,
    // which would blink the result off the screen at the exact moment it
    // becomes interesting. Hold a decided row for a couple of minutes so the
    // outcome can actually be read.
    const now = Date.now();
    for (const prev of portfolioState.positions || []) {
      if (prev.won == null) continue;
      if (rows.some((r) => r.ticker === prev.ticker)) continue;
      // Anchor the hold to the MARKET'S OWN CLOSE TIME, never to "now".
      // portfolioState.positions already contains the rows this memo added on
      // the previous poll, so deriving the expiry from the current time made
      // every poll push it forward again: the row kept itself alive forever
      // and last hour's settled position never cleared. Anchored to close
      // time the expiry is a fixed instant, so re-setting it is a no-op and
      // it agrees with the close-time filter above — everything decided is
      // visible until close + RESOLVED_HOLD_MS, then gone.
      const until = (prev.closeTime || now) + RESOLVED_HOLD_MS;
      resolvedMemo.set(prev.ticker, { row: { ...prev, settledOut: true }, until });
    }
    for (const [ticker, entry] of resolvedMemo) {
      if (entry.until <= now) { resolvedMemo.delete(ticker); continue; }
      if (!rows.some((r) => r.ticker === ticker)) rows.push(entry.row);
    }

    portfolioState = { ...portfolioState, enabled: true, error: null, cash, positions: rows };
    repricePortfolio(null); // derive values/P&L from the quotes just fetched
    broadcast({ type: "portfolio", ...portfolioState });
  } catch (e) {
    console.error("Kalshi portfolio poll:", e.message);
    portfolioState = { ...portfolioState, error: e.message, updatedAt: Date.now() };
  }
}
if (portfolioState.enabled) {
  setInterval(pollPortfolio, PORTFOLIO_POLL_MS);
  pollPortfolio();
}

// ---------- CF Benchmarks BRTI (the price Kalshi actually settles on) ----------
// Kalshi's hourly BTC markets do NOT settle on Coinbase or Bitstamp. Their
// rules read: "the simple average of the sixty seconds of CF Benchmarks'
// BRTI before {hour}". So the Coinbase/Bitstamp blend is only a proxy for
// the number that decides these markets. Kalshi republishes the BRTI feed,
// including the trailing 60-second average — i.e. the settlement figure
// itself — over an authenticated WebSocket, so the same credentials that
// read the portfolio also get us the real index.
//
// Requires credentials; without them this stays inert and the page simply
// does not show BRTI.

// The docs give the URL as .../cfbenchmarks_value, but that path 404s.
// Probing showed only /trade-api/ws/v2 exists (401 unauthenticated), so the
// value feed is a CHANNEL on Kalshi's main socket, not its own endpoint.
const BRTI_WS = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const BRTI_SIGN_PATH = "/trade-api/ws/v2";

let brtiState = {
  enabled: !!(KALSHI_KEY_ID && kalshiPrivateKey),
  connected: false, // socket handshake succeeded
  subscribed: false, // server acknowledged the channel subscription
  value: null,      // latest BRTI print
  ts: null,
  avg60: null,      // trailing 60s average — what settlement uses
  avg60Window: null,
  error: null,
  lastMsg: null,    // last frame seen, so a stuck feed can be diagnosed
};

let brtiSocket = null, brtiDelay = 1000;

function connectBRTI() {
  if (!brtiState.enabled) return;
  let headers;
  try {
    headers = kalshiSignHeaders("GET", BRTI_SIGN_PATH);
  } catch (e) {
    brtiState = { ...brtiState, error: "signing failed: " + e.message };
    return;
  }
  brtiSocket = new WebSocket(BRTI_WS, { headers });

  brtiSocket.on("open", () => {
    brtiDelay = 1000;
    brtiState = { ...brtiState, connected: true, error: null };
    console.log("BRTI: socket open, sending subscribe attempts");
    // The docs and the live endpoint have already disagreed once (the
    // documented URL 404s), so send both documented shapes. Whichever the
    // server doesn't understand comes back as an error we log; the other
    // starts the stream.
    const attempts = [
      { id: 1, cmd: "subscribe", params: { channels: ["cfbenchmarks_value"], index_ids: ["BRTI"] } },
      { type: "subscribe", channel: "cfbenchmarks_value", index_ids: ["BRTI"] },
    ];
    attempts.forEach((a, i) => {
      setTimeout(() => {
        if (brtiSocket && brtiSocket.readyState === WebSocket.OPEN && !brtiState.subscribed) {
          console.log("BRTI -> subscribe attempt " + (i + 1) + ":", JSON.stringify(a));
          try { brtiSocket.send(JSON.stringify(a)); } catch {}
        }
      }, i * 1500);
    });
  });

  brtiSocket.on("message", (raw) => {
    const text = raw.toString();
    let m;
    try { m = JSON.parse(text); } catch { return; }

    // Log every distinct frame type once so a stuck feed is diagnosable
    // from the server log rather than guessing.
    const kind = m.type || m.cmd || "?";
    if (kind !== "cfbenchmarks_value") console.log("BRTI <-", text.slice(0, 300));
    brtiState.lastMsg = text.slice(0, 200);

    if (m.type === "subscribed" || m.type === "ok" || (m.msg && m.msg.sid != null && m.type !== "cfbenchmarks_value")) {
      brtiState.subscribed = true;
      return;
    }
    if (m.type === "error" || m.error) {
      brtiState.error = text.slice(0, 200);
      return;
    }
    if (m.type !== "cfbenchmarks_value") return;

    brtiState.subscribed = true;
    // envelope has been seen as {type, msg:{...}}; tolerate a flat shape too
    const msg = m.msg || m;
    if (msg.index_id && msg.index_id !== "BRTI") return;

    // msg.data arrives as a JSON *string* — the docs call it the "raw CF
    // Benchmarks frame" and mean that literally:
    //   "data":"{\"type\":\"value\",\"id\":\"BRTI\",\"value\":\"64071.14\"}"
    // (the same double-encoding Bitstamp's feed uses above)
    let frame = msg.data;
    if (typeof frame === "string") {
      try { frame = JSON.parse(frame); } catch { frame = {}; }
    }
    if (!frame || typeof frame !== "object") frame = {};
    const v = parseFloat(frame.value != null ? frame.value : msg.value);
    if (Number.isFinite(v)) {
      brtiState.value = v;
      brtiState.ts = Date.now();
      brtiState.error = null;
    }
    const a = msg.avg_60s_data;
    if (a) {
      const av = parseFloat(a.value != null ? a.value : a.average);
      if (Number.isFinite(av)) brtiState.avg60 = av;
      if (a.window_size != null) brtiState.avg60Window = a.window_size;
    }
  });

  brtiSocket.on("close", (code) => {
    brtiState = { ...brtiState, connected: false, subscribed: false };
    console.log(`BRTI feed disconnected (${code}), retrying in ${brtiDelay}ms`);
    setTimeout(connectBRTI, brtiDelay);
    brtiDelay = Math.min(brtiDelay * 2, 30000);
  });

  brtiSocket.on("error", (err) => {
    brtiState = { ...brtiState, error: err.message };
    console.error("BRTI feed socket error:", err.message);
    try { brtiSocket.terminate(); } catch {}
  });
}
if (brtiState.enabled) connectBRTI();

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
    const ticks = secondTicks.filter((p) => p.t >= cutoff);
    // secondTicks is capped at a COUNT, not a duration: 3600 entries that,
    // across restarts and downtime, can span a whole day rather than the last
    // hour. Whatever part of the window it doesn't reach back to gets filled
    // from the 1-minute bars, which cover 24h and survive the same gaps. The
    // left of the chart then degrades to minute resolution instead of going
    // blank — the fixed-width axis makes that emptiness visible, honestly, but
    // there is no reason to show nothing when coarser data is right there.
    const from = ticks.length ? ticks[0].t : now;
    const bars = minuteBars
      .concat(currentBar ? [currentBar] : [])
      .filter((b) => b.t >= cutoff && b.t < from);
    return bucketize(bars.concat(ticks), 300);
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

// ---------- flight search (SerpApi Google Flights) ----------
// One-way only, and every search runs TWICE — once for economy, once for
// first — because SerpApi's travel_class takes a single value per request.
// The two result sets are merged into one list so the cheapest fare of either
// cabin is visible at a glance rather than buried in a separate tab.
//
// Two searches per query means two API credits, so results are cached: the
// same route/date/cabin inside FLIGHT_CACHE_MS is served from memory.

const SERPAPI_KEY = process.env.SERPAPI_KEY || "";
const FLIGHT_CACHE_MS = Number(process.env.FLIGHT_CACHE_MS) || 5 * 60 * 1000;
const SERP_CLASS = { economy: 1, first: 4 };
const flightCache = new Map(); // key -> { at, itineraries }

// Aircraft with two aisles. Listed as explicit patterns rather than a clever
// regex so a narrowbody can never sneak in: 737 and 757 are single-aisle and
// must not match, and they would under a looser pattern like 7[0-9]7.
const WIDEBODY = [
  /\b747\b/, /\b767\b/, /\b777\b/, /\b787\b/,
  /\bA300\b/i, /\bA310\b/i, /\bA330\b/i, /\bA340\b/i, /\bA350\b/i, /\bA380\b/i,
  /\bDC-?10\b/i, /\bMD-?11\b/i, /\bL-?1011\b/i, /\bIL-?96\b/i,
];
function isWidebody(name) {
  const s = String(name || "");
  return WIDEBODY.some((re) => re.test(s));
}

const LONG_LAYOVER_MIN = 90;

// "2026-08-13 07:05" -> { date: "2026-08-13", label: "7:05 AM" }. Parsed by
// hand on purpose: these are LOCAL times at each airport, and handing them to
// Date() would reinterpret them in the server's zone and shift the clock.
function splitStamp(stamp) {
  const [date, time] = String(stamp || "").split(" ");
  if (!time) return { date: date || null, label: null };
  const [hStr, m] = time.split(":");
  let h = parseInt(hStr, 10);
  const ampm = h >= 12 ? "PM" : "AM";
  h = h % 12 || 12;
  return { date, label: h + ":" + m + " " + ampm };
}

function fmtDuration(mins) {
  const n = Number(mins);
  if (!Number.isFinite(n) || n <= 0) return null;
  const h = Math.floor(n / 60), m = n % 60;
  return (h ? h + "h" : "") + (h && m ? " " : "") + (m || !h ? m + "m" : "");
}

function normalizeItinerary(it, cabin) {
  const legs = Array.isArray(it.flights) ? it.flights : [];
  if (!legs.length) return null;
  const first = legs[0], last = legs[legs.length - 1];
  const dep = splitStamp(first.departure_airport && first.departure_airport.time);
  const arr = splitStamp(last.arrival_airport && last.arrival_airport.time);
  const layovers = (it.layovers || []).map((l) => ({
    id: l.id || null,
    name: l.name || null,
    minutes: Number(l.duration) || null,
    durationLabel: fmtDuration(l.duration),
    overnight: !!l.overnight,
  }));
  const aircraft = legs.map((l) => l.airplane).filter(Boolean);
  return {
    cabin,
    price: Number(it.price) || null,
    logo: it.airline_logo || first.airline_logo || null,
    airlines: [...new Set(legs.map((l) => l.airline).filter(Boolean))],
    flightNumbers: legs.map((l) => l.flight_number).filter(Boolean),
    depAirport: (first.departure_airport && first.departure_airport.id) || null,
    depTime: dep.label,
    arrAirport: (last.arrival_airport && last.arrival_airport.id) || null,
    arrTime: arr.label,
    // a red-eye landing the next day, so the arrival time is not misread
    dayOffset: dep.date && arr.date && arr.date !== dep.date ? 1 : 0,
    stops: Math.max(legs.length - 1, 0),
    layovers,
    aircraft,
    totalDuration: Number(it.total_duration) || null,
    totalDurationLabel: fmtDuration(it.total_duration),
    nonstop: legs.length === 1,
    widebody: aircraft.some(isWidebody),
    longLayover: layovers.some((l) => l.minutes != null && l.minutes > LONG_LAYOVER_MIN),
  };
}

// The key may come from the server's environment OR from the device making
// the request. The per-device path exists because this runs under launchd,
// which never sees a shell export — and because a key held in one browser's
// localStorage is not sitting on a public-facing box at all. It arrives as a
// header rather than a query parameter so it stays out of request logs.
function serpKeyFrom(req) {
  const h = req && req.headers && req.headers["x-serpapi-key"];
  if (typeof h === "string" && h.trim()) return h.trim();
  return SERPAPI_KEY;
}

async function serpFlights(from, to, date, cabin, apiKey) {
  const key = [from, to, date, cabin].join("|");
  const hit = flightCache.get(key);
  if (hit && Date.now() - hit.at < FLIGHT_CACHE_MS) return hit.itineraries;

  const url = new URL("https://serpapi.com/search.json");
  url.searchParams.set("engine", "google_flights");
  url.searchParams.set("departure_id", from);
  url.searchParams.set("arrival_id", to);
  url.searchParams.set("outbound_date", date);
  url.searchParams.set("type", "2"); // one way
  url.searchParams.set("travel_class", String(SERP_CLASS[cabin]));
  url.searchParams.set("currency", "USD");
  url.searchParams.set("hl", "en");
  url.searchParams.set("api_key", apiKey);

  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.error) {
    throw new Error(json.error || `serpapi ${res.status}`);
  }
  const raw = [].concat(json.best_flights || [], json.other_flights || []);
  const itineraries = raw.map((it) => normalizeItinerary(it, cabin)).filter(Boolean);
  flightCache.set(key, { at: Date.now(), itineraries });
  return itineraries;
}

// Flags are computed across the MERGED list, because "cheapest coach" only
// means anything once both cabins are in hand. Ties all get flagged.
function flagItineraries(list) {
  for (const cabin of Object.keys(SERP_CLASS)) {
    const inCabin = list.filter((r) => r.cabin === cabin && r.price != null);
    if (!inCabin.length) continue;
    const min = Math.min(...inCabin.map((r) => r.price));
    for (const r of inCabin) if (r.price === min) r.cheapest = true;
  }
  return list;
}

async function searchFlights(from, to, date, apiKey) {
  const settled = await Promise.allSettled([
    serpFlights(from, to, date, "economy", apiKey),
    serpFlights(from, to, date, "first", apiKey),
  ]);
  const list = [];
  const errors = [];
  settled.forEach((r, i) => {
    if (r.status === "fulfilled") list.push(...r.value);
    else errors.push((i === 0 ? "economy: " : "first: ") + r.reason.message);
  });
  // one cabin failing should not sink the whole search
  if (!list.length && errors.length) throw new Error(errors.join("; "));
  flagItineraries(list);
  list.sort((a, b) => (a.price || 1e9) - (b.price || 1e9) || (a.totalDuration || 0) - (b.totalDuration || 0));
  return { itineraries: list, partialError: errors.length ? errors.join("; ") : null };
}

// ---------- hotels (SerpApi Google Hotels) ----------
//
// Same key as flights — serpKeyFrom() is shared, so a key stored on the device
// for one search works for the other with nothing further to enter. One
// request per search here rather than the flights' two, since there is no
// cabin axis to sweep.

const HOTEL_CACHE_MS = Number(process.env.HOTEL_CACHE_MS) || FLIGHT_CACHE_MS;
const hotelCache = new Map(); // key -> { at, properties }
const MAX_AMENITIES = 8;

// Rate objects arrive as { lowest: "$189", extracted_lowest: 189 }. Only the
// extracted number is trustworthy to compute with — the string carries a
// currency symbol and thousands separators.
function rateAmount(o) {
  if (!o) return null;
  const n = Number(o.extracted_lowest != null ? o.extracted_lowest : o.extracted_before_taxes_fees);
  return Number.isFinite(n) && n > 0 ? n : null;
}

// ---------- amenities ----------
//
// Google's wording drifts ("Pet-friendly" / "Pet friendly" / "Pets allowed"),
// so every comparison goes through a key with the punctuation and spacing
// stripped out. Matching is on the WHOLE key, never a substring, because
// "Accessible pool" must not read as a pool.
function amenityKey(s) {
  return String(s || "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

// Things all but universal, plus payment methods, languages and the
// per-facility accessibility entries. They crowd out what actually
// distinguishes one stay from another.
const AMENITY_HIDE = new Set([
  "Kid friendly", "Elevator", "Parking", "Accessible", "Accessible parking",
  "Accessible pool", "Air conditioning", "WiFi in public areas", "Credit cards",
  "Debit cards", "Cash", "Checks", "Front desk", "Activities for kids",
  "Accessible elevator", "Private bathroom", "English", "Bathtub in some rooms",
  "Shower in some rooms", "Smoke-free property", "Currency exchange", "Spanish",
].map(amenityKey));

// "Parking" goes, "Valet parking" stays — whole-key matching is what keeps
// those apart.
function usefulAmenities(list) {
  return (Array.isArray(list) ? list : []).filter((a) => a && !AMENITY_HIDE.has(amenityKey(a)));
}

// The few worth calling out on the card itself. Order here is the order they
// render in.
// Each carries its own colour so the row can be read by hue at a glance.
// Pets are an icon rather than a word: it is the one people scan for.
const AMENITY_BADGES = [
  { label: "Pet friendly", icon: "paw", cls: "b-paw",
    keys: ["petfriendly", "petsallowed", "dogsallowed", "petsallowedfree"] },
  { label: "Indoor pool", cls: "b-pool", keys: ["indoorpool"] },
  { label: "Outdoor pool", cls: "b-pool", keys: ["outdoorpool"] },
  // only when neither of the specific ones matched, so a hotel with an indoor
  // pool is not badged "Indoor pool · Pool"
  { label: "Pool", cls: "b-pool", keys: ["pool"], unless: ["indoorpool", "outdoorpool"] },
  { label: "Spa", cls: "b-spa", keys: ["spa"] },
  { label: "Minibar", cls: "b-bar", keys: ["minibar", "minibarinroom"] },
  { label: "Turndown", cls: "b-turn", keys: ["turndownservice", "turndown"] },
  { label: "In-room dining", cls: "b-dine", keys: ["roomservice", "inroomdining"] },
  { label: "Casino", cls: "b-casino", keys: ["casino"] },
];

function amenityBadges(list) {
  const have = new Set((Array.isArray(list) ? list : []).map(amenityKey));
  const out = [];
  for (const b of AMENITY_BADGES) {
    if (!b.keys.some((k) => have.has(k))) continue;
    if (b.unless && b.unless.some((k) => have.has(k))) continue;
    const badge = { label: b.label, title: b.label, cls: b.cls };
    if (b.icon) badge.icon = b.icon;
    out.push(badge);
  }
  return out;
}

// Anything a badge can speak for is dropped from the card's text line, so the
// row does not say "Pool · Spa" directly above badges reading POOL and SPA.
// The full listing keeps them: it has no badges to defer to.
const BADGE_KEYS = new Set(AMENITY_BADGES.reduce((a, b) => a.concat(b.keys), []));

// Card-only. Not junk — you want to know a place does breakfast — but so
// nearly universal that four cards in a row read the same four things and the
// line stops telling you anything. The full listing still shows them.
const CARD_AMENITY_HIDE = new Set([
  "Free breakfast", "Breakfast", "Breakfast included",
  "Free Wi-Fi", "Wi-Fi",
  "Free parking",
  "Fitness center", "Fitness centre", "Gym",
  "Restaurant",
].map(amenityKey));

function normalizeProperty(p, nights) {
  const name = p && p.name;
  if (!name) return null;
  const perNight = rateAmount(p.rate_per_night);
  // Google omits the stay total on some properties; derive it so every row can
  // answer "what does the whole trip cost" rather than only the nightly rate.
  const total = rateAmount(p.total_rate) || (perNight != null && nights ? perNight * nights : null);
  const img = Array.isArray(p.images) && p.images.length ? p.images[0] : null;
  const rating = Number(p.overall_rating);
  const reviews = Number(p.reviews);
  const stars = Number(p.extracted_hotel_class);
  const deal = p.deal || null;
  // badges read the FULL list, before it is trimmed for display
  const allAmenities = Array.isArray(p.amenities) ? p.amenities : [];
  return {
    name,
    kind: p.type === "vacation rental" ? "rental" : "hotel",
    // the token is what the details endpoint is addressed by, so it has to
    // survive normalising even though nothing in the list view shows it
    token: p.property_token || null,
    thumb: (img && (img.thumbnail || img.original_image)) || null,
    perNight,
    total,
    rating: Number.isFinite(rating) ? rating : null,
    reviews: Number.isFinite(reviews) ? reviews : null,
    stars: Number.isFinite(stars) ? stars : null,
    amenities: usefulAmenities(allAmenities)
      .filter((a) => !BADGE_KEYS.has(amenityKey(a)) && !CARD_AMENITY_HIDE.has(amenityKey(a)))
      .slice(0, MAX_AMENITIES),
    badges: amenityBadges(allAmenities),
    deal,
    dealPct: dealPercent(deal),
    eco: !!p.eco_certified,
    locationRating: Number(p.location_rating) || null,
  };
}

// "23% less than usual" -> 23. Used to rank one deal above another; a deal
// with no readable percentage still counts as a deal, it just ranks after
// the ones that quantify themselves.
function dealPercent(deal) {
  const m = /(\d+(?:\.\d+)?)\s*%/.exec(String(deal || ""));
  return m ? Number(m[1]) : null;
}

async function serpHotels(q, checkIn, checkOut, adults, apiKey) {
  const key = [q, checkIn, checkOut, adults].join("|");
  const hit = hotelCache.get(key);
  if (hit && Date.now() - hit.at < HOTEL_CACHE_MS) return hit.properties;

  const nights = nightsBetween(checkIn, checkOut);
  const url = new URL("https://serpapi.com/search.json");
  url.searchParams.set("engine", "google_hotels");
  url.searchParams.set("q", q);
  url.searchParams.set("check_in_date", checkIn);
  url.searchParams.set("check_out_date", checkOut);
  url.searchParams.set("adults", String(adults));
  url.searchParams.set("currency", "USD");
  url.searchParams.set("gl", "us");
  url.searchParams.set("hl", "en");
  url.searchParams.set("api_key", apiKey);

  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.error) {
    throw new Error(json.error || `serpapi ${res.status}`);
  }
  const raw = Array.isArray(json.properties) ? json.properties : [];
  const properties = raw.map((p) => normalizeProperty(p, nights)).filter(Boolean);
  hotelCache.set(key, { at: Date.now(), properties });
  return properties;
}

// Dates are plain YYYY-MM-DD with no zone, so they are compared as UTC noon —
// that keeps a DST boundary inside the stay from turning 2 nights into 1.
function nightsBetween(checkIn, checkOut) {
  const a = Date.parse(checkIn + "T12:00:00Z");
  const b = Date.parse(checkOut + "T12:00:00Z");
  if (!Number.isFinite(a) || !Number.isFinite(b)) return 0;
  return Math.round((b - a) / 86400000);
}

// Same idea as the flight flags: computed across the whole result set, ties
// all flagged, so "lowest" is a fact about the search rather than about a row.
function flagProperties(list) {
  const priced = list.filter((p) => p.perNight != null);
  if (priced.length) {
    const min = Math.min(...priced.map((p) => p.perNight));
    for (const p of priced) if (p.perNight === min) p.cheapest = true;
  }
  const rated = list.filter((p) => p.rating != null);
  if (rated.length) {
    const max = Math.max(...rated.map((p) => p.rating));
    // a lone result is not "top rated", it is the only one
    if (rated.length > 1) for (const p of rated) if (p.rating === max) p.topRated = true;
  }
  return list;
}

async function searchHotels(q, checkIn, checkOut, adults, apiKey) {
  const properties = await serpHotels(q, checkIn, checkOut, adults, apiKey);
  flagProperties(properties);
  properties.sort((a, b) => (a.perNight || 1e9) - (b.perNight || 1e9));
  const nights = nightsBetween(checkIn, checkOut);
  logHotelSearch(q, checkIn, checkOut, adults, nights, properties);
  return { properties, nights };
}

// Every search, and the name of everything it turned up — nothing else, so
// the terminal stays readable when several searches run back to back.
function logHotelSearch(q, checkIn, checkOut, adults, nights, properties) {
  console.log(
    `\nHotels: "${q}"  ${checkIn} -> ${checkOut}  ${nights} night${nights === 1 ? "" : "s"}` +
    `, ${adults} guest${adults === 1 ? "" : "s"}  —  ${properties.length} result` +
    `${properties.length === 1 ? "" : "s"}`
  );
  for (const p of properties) console.log(`  ${p.name}`);
}

// ---------- one hotel, in full ----------
//
// Same engine, addressed by property_token instead of a query. It answers
// with far more than the list row carries: every photo, the address and
// coordinates, the price at each booking site, the star histogram, what
// reviewers talk about, the full amenity list and what is nearby.

const detailCache = new Map(); // token|dates|adults -> { at, details }

function ratingHistogram(ratings) {
  const rows = (Array.isArray(ratings) ? ratings : [])
    .map((r) => ({ stars: Number(r.stars), count: Number(r.count) }))
    .filter((r) => Number.isFinite(r.stars) && Number.isFinite(r.count));
  const total = rows.reduce((s, r) => s + r.count, 0);
  // share is what the bar is drawn from, so compute it once here rather than
  // making the page divide by a total it would have to re-derive
  return rows
    .sort((a, b) => b.stars - a.stars)
    .map((r) => ({ ...r, share: total ? r.count / total : 0 }));
}

function priceRows(json) {
  const raw = [].concat(
    Array.isArray(json.featured_prices) ? json.featured_prices : [],
    Array.isArray(json.prices) ? json.prices : []
  );
  const seen = new Set();
  const out = [];
  for (const p of raw) {
    const source = p && p.source;
    if (!source || seen.has(source)) continue;
    seen.add(source);
    out.push({
      source,
      logo: p.logo || null,
      official: !!p.official,
      perNight: rateAmount(p.rate_per_night),
      total: rateAmount(p.total_rate),
    });
  }
  out.sort((a, b) => (a.perNight == null ? 1e9 : a.perNight) - (b.perNight == null ? 1e9 : b.perNight));
  return out;
}

function normalizeDetails(json, nights) {
  const g = json.gps_coordinates || {};
  const lat = Number(g.latitude), lon = Number(g.longitude);
  const perNight = rateAmount(json.rate_per_night);
  const rating = Number(json.overall_rating);
  const reviews = Number(json.reviews);
  const stars = Number(json.extracted_hotel_class);
  const range = json.typical_price_range || null;
  return {
    name: json.name || null,
    kind: json.type === "vacation rental" ? "rental" : "hotel",
    description: json.description || null,
    address: json.address || null,
    phone: json.phone || null,
    lat: Number.isFinite(lat) ? lat : null,
    lon: Number.isFinite(lon) ? lon : null,
    checkInTime: json.check_in_time || null,
    checkOutTime: json.check_out_time || null,
    perNight,
    total: rateAmount(json.total_rate) || (perNight != null && nights ? perNight * nights : null),
    typicalLow: range ? rateAmount({ extracted_lowest: range.extracted_lowest }) : null,
    typicalHigh: range && Number.isFinite(Number(range.extracted_highest))
      ? Number(range.extracted_highest) : null,
    // full size where offered, thumbnail as the fallback — this is a gallery,
    // not a 54px row icon
    images: (Array.isArray(json.images) ? json.images : [])
      .map((i) => i && (i.original_image || i.thumbnail))
      .filter(Boolean)
      .slice(0, 24),
    rating: Number.isFinite(rating) ? rating : null,
    reviews: Number.isFinite(reviews) ? reviews : null,
    stars: Number.isFinite(stars) ? stars : null,
    locationRating: Number(json.location_rating) || null,
    histogram: ratingHistogram(json.ratings),
    reviewTopics: (Array.isArray(json.reviews_breakdown) ? json.reviews_breakdown : [])
      .map((r) => ({
        name: r.name || null,
        mentioned: Number(r.total_mentioned) || 0,
        positive: Number(r.positive) || 0,
        negative: Number(r.negative) || 0,
        neutral: Number(r.neutral) || 0,
      }))
      .filter((r) => r.name)
      .sort((a, b) => b.mentioned - a.mentioned)
      .slice(0, 8),
    prices: priceRows(json),
    amenities: usefulAmenities(json.amenities),
    excluded: usefulAmenities(json.excluded_amenities),
    nearby: (Array.isArray(json.nearby_places) ? json.nearby_places : [])
      .map((n) => ({
        name: n.name || null,
        transport: (Array.isArray(n.transportations) ? n.transportations : [])
          .map((t) => [t.type, t.duration].filter(Boolean).join(" "))
          .filter(Boolean),
      }))
      .filter((n) => n.name)
      .slice(0, 10),
    eco: !!json.eco_certified,
  };
}

async function serpHotelDetails(token, checkIn, checkOut, adults, apiKey) {
  const key = [token, checkIn, checkOut, adults].join("|");
  const hit = detailCache.get(key);
  if (hit && Date.now() - hit.at < HOTEL_CACHE_MS) return hit.details;

  const url = new URL("https://serpapi.com/search.json");
  url.searchParams.set("engine", "google_hotels");
  url.searchParams.set("property_token", token);
  // the engine still wants a query and the dates alongside the token,
  // because the rates it quotes are for that stay
  url.searchParams.set("q", "hotel");
  url.searchParams.set("check_in_date", checkIn);
  url.searchParams.set("check_out_date", checkOut);
  url.searchParams.set("adults", String(adults));
  url.searchParams.set("currency", "USD");
  url.searchParams.set("gl", "us");
  url.searchParams.set("hl", "en");
  url.searchParams.set("api_key", apiKey);

  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.error) {
    throw new Error(json.error || `serpapi ${res.status}`);
  }
  const details = normalizeDetails(json, nightsBetween(checkIn, checkOut));
  detailCache.set(key, { at: Date.now(), details });
  return details;
}

// ---------- HTTP + WebSocket server ----------

const clients = new Set();
function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of clients) if (c.readyState === WebSocket.OPEN) c.send(data);
}
setInterval(() => broadcast({ type: "trend", ...computeForecast() }), 10000);

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  res.setHeader("Access-Control-Allow-Origin", "*");

  if (url.pathname === "/favicon.ico") { res.writeHead(204); res.end(); return; }

  // Places a real order against a real account. Requires the server-side
  // PIN on every call; no PIN configured means the route does not exist.
  if ((url.pathname === "/api/sell" || url.pathname === "/api/buy") && req.method === "POST") {
    const action = url.pathname === "/api/buy" ? "buy" : "sell";
    let body = "";
    req.on("data", (c) => {
      body += c;
      if (body.length > 4096) { req.destroy(); }
    });
    req.on("end", async () => {
      const send = (code, obj) => {
        res.writeHead(code, { "Content-Type": "application/json" });
        res.end(JSON.stringify(obj));
      };
      if (!portfolioState.sellEnabled) return send(403, { error: "trading is not enabled on this server" });
      let payload;
      try { payload = JSON.parse(body || "{}"); } catch { return send(400, { error: "bad request" }); }

      // constant-time compare so a wrong PIN can't be found by timing
      const given = Buffer.from(String(payload.pin || ""));
      const want = Buffer.from(SELL_PIN);
      const pinOk = given.length === want.length && crypto.timingSafeEqual(given, want);
      if (!pinOk) {
        console.warn(`Rejected /api/${action}: bad PIN`);
        return send(401, { error: "incorrect PIN" });
      }
      if (!payload.ticker) return send(400, { error: "ticker required" });

      try {
        send(200, await tradePosition(String(payload.ticker), action));
      } catch (e) {
        console.error(`${action.toUpperCase()} failed:`, e.message);
        send(502, { error: e.message });
      }
    });
    return;
  }

  // Decoy thumbnails, dropped in beside this file as pica.png … picz.png.
  // Read fresh each time rather than listed at boot, so adding one is a matter
  // of copying the file in — no restart.
  if (url.pathname === "/api/decoy") {
    let pics = [];
    try {
      pics = fs.readdirSync(__dirname).filter((f) => /^pic[a-z]\.png$/i.test(f)).sort();
    } catch (e) { pics = []; }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ pics }));
    return;
  }
  // Whitelisted by shape and reduced to a basename, so nothing outside this
  // directory is reachable through it.
  if (/^\/pic[a-z]\.png$/i.test(url.pathname)) {
    fs.readFile(path.join(__dirname, path.basename(url.pathname)), (err, buf) => {
      if (err) { res.writeHead(404); res.end(); return; }
      res.writeHead(200, { "Content-Type": "image/png", "Cache-Control": "public, max-age=3600" });
      res.end(buf);
    });
    return;
  }
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
    res.end(JSON.stringify(computeForecast()));
    return;
  }
  if (url.pathname === "/api/brti") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(brtiState));
    return;
  }
  if (url.pathname === "/api/kalshi") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ...kalshiState, quote: quoteSnapshot() }));
    return;
  }
  if (url.pathname === "/api/flights") {
    const send = (code, obj) => {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(obj));
    };
    const apiKey = serpKeyFrom(req);
    // no key anywhere: tell the page so it can offer to store one per device
    if (!apiKey) return send(200, { enabled: false, needsKey: true });
    // uppercase and de-space so "lga, jfk" and "LGA,JFK" are the same search
    const clean = (v) => String(v || "").toUpperCase().replace(/\s+/g, "").replace(/,+/g, ",").replace(/^,|,$/g, "");
    const from = clean(url.searchParams.get("from"));
    const to = clean(url.searchParams.get("to"));
    const date = String(url.searchParams.get("date") || "");
    if (!from || !to) return send(400, { enabled: true, error: "need both a departure and an arrival" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return send(400, { enabled: true, error: "need a date as YYYY-MM-DD" });
    searchFlights(from, to, date, apiKey)
      .then((r) => send(200, { enabled: true, from, to, date, ...r }))
      .catch((e) => send(200, { enabled: true, from, to, date, itineraries: [], error: e.message }));
    return;
  }
  if (url.pathname === "/api/hotels") {
    const send = (code, obj) => {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(obj));
    };
    // the very same key the flight search uses, from env or from the device
    const apiKey = serpKeyFrom(req);
    if (!apiKey) return send(200, { enabled: false, needsKey: true });
    const q = String(url.searchParams.get("q") || "").trim().replace(/\s+/g, " ");
    const checkIn = String(url.searchParams.get("checkIn") || "");
    const checkOut = String(url.searchParams.get("checkOut") || "");
    const adults = Math.min(Math.max(parseInt(url.searchParams.get("adults"), 10) || 2, 1), 8);
    const ymd = /^\d{4}-\d{2}-\d{2}$/;
    if (!q) return send(400, { enabled: true, error: "need somewhere to stay" });
    if (!ymd.test(checkIn) || !ymd.test(checkOut)) {
      return send(400, { enabled: true, error: "need both dates as YYYY-MM-DD" });
    }
    if (nightsBetween(checkIn, checkOut) < 1) {
      return send(400, { enabled: true, error: "check-out must be after check-in" });
    }
    searchHotels(q, checkIn, checkOut, adults, apiKey)
      .then((r) => send(200, { enabled: true, q, checkIn, checkOut, adults, ...r }))
      .catch((e) => send(200, { enabled: true, q, checkIn, checkOut, adults, properties: [], error: e.message }));
    return;
  }
  if (url.pathname === "/api/hotel") {
    const send = (code, obj) => {
      res.writeHead(code, { "Content-Type": "application/json" });
      res.end(JSON.stringify(obj));
    };
    const apiKey = serpKeyFrom(req);
    if (!apiKey) return send(200, { enabled: false, needsKey: true });
    const token = String(url.searchParams.get("token") || "").trim();
    const checkIn = String(url.searchParams.get("checkIn") || "");
    const checkOut = String(url.searchParams.get("checkOut") || "");
    const adults = Math.min(Math.max(parseInt(url.searchParams.get("adults"), 10) || 2, 1), 8);
    const ymd = /^\d{4}-\d{2}-\d{2}$/;
    if (!token) return send(400, { enabled: true, error: "need a property token" });
    if (!ymd.test(checkIn) || !ymd.test(checkOut)) {
      return send(400, { enabled: true, error: "need both dates as YYYY-MM-DD" });
    }
    if (nightsBetween(checkIn, checkOut) < 1) {
      return send(400, { enabled: true, error: "check-out must be after check-in" });
    }
    serpHotelDetails(token, checkIn, checkOut, adults, apiKey)
      .then((details) => send(200, { enabled: true, details }))
      .catch((e) => send(200, { enabled: true, details: null, error: e.message }));
    return;
  }
  if (url.pathname === "/api/portfolio") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(portfolioState));
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
    ws.send(JSON.stringify({ type: "bootstrap", snapshot: snapshot(), trades: recentTrades, trend: computeForecast(), portfolio: portfolioState, brti: brtiPublic() }));
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
  padding: 8px 10px 22px;
  display: flex;
  flex-direction: column;
  gap: 7px;
}

/* ── brand row ── */
.brand-row { display: flex; align-items: center; justify-content: center; gap: 7px; padding: 0 2px; }
.brand-text { font-size: 10.5px; letter-spacing: 2px; color: var(--text3); font-weight: 800; text-transform: uppercase; }
.brand-sep { color: var(--text3); font-size: 11px; }
.status-word { font-size: 10.5px; letter-spacing: 2px; font-weight: 800; text-transform: uppercase; color: var(--green); margin-left: 1px; }
.status-word.delay { color: var(--yellow); }
.status-word.down { color: var(--red); }
/* The health light. Green = BRTI printing in real time, yellow = the feed
   has gone quiet (DELAY), red = no feed at all (OFFLINE).
   Each state is a lit bead rather than a flat disc: the radial gradient puts
   a near-white specular highlight up and to the left and darkens toward the
   lower rim, which is what reads as depth at this size. background-color
   stays set underneath as the flat fallback. */
.status-dot { width: 7px; height: 7px; border-radius: 50%; background-color: var(--text3); flex-shrink: 0; }
.status-dot.live {
  background-color: #2fe36a;
  background-image: radial-gradient(circle at 32% 26%, #e4fff0 0%, #7bf7ab 26%, #2fe36a 55%, #0f9c45 100%);
  animation: pulseGreen 1.25s ease-out infinite;
}
.status-dot.delay {
  background-color: var(--yellow);
  background-image: radial-gradient(circle at 32% 26%, #fffadf 0%, #ffe98c 26%, #f5c518 55%, #9a7a08 100%);
  animation: pulseYellow 1.25s ease-out infinite;
}
.status-dot.down {
  background-color: var(--red);
  background-image: radial-gradient(circle at 32% 26%, #ffe6e6 0%, #ff9d9d 26%, #ef4444 55%, #8f1a1a 100%);
  animation: pulseRed 1s ease-out infinite;
}
@keyframes pulse {
  0%   { box-shadow: 0 0 0 0 rgba(34,197,94,0.45); }
  70%  { box-shadow: 0 0 0 6px rgba(34,197,94,0); }
  100% { box-shadow: 0 0 0 0 rgba(34,197,94,0); }
}
/* Pulse strength sits between where it started (a bare 7px ring, easy to
   miss) and where it went (a 9px ring plus a hard glow at scale 1.18, which
   drew the eye off the price): a soft halo, a 5px ring, and a scale of 1.08. */
@keyframes pulseGreen {
  0%   { box-shadow: 0 0 3px 0.5px rgba(47,227,106,0.7), 0 0 0 0 rgba(47,227,106,0.5); transform: scale(1); }
  70%  { box-shadow: 0 0 5px 1px rgba(47,227,106,0.35), 0 0 0 5px rgba(47,227,106,0); transform: scale(1.08); }
  100% { box-shadow: 0 0 3px 0.5px rgba(47,227,106,0.7), 0 0 0 0 rgba(47,227,106,0); transform: scale(1); }
}
@keyframes pulseYellow {
  0%   { box-shadow: 0 0 3px 0.5px rgba(245,197,24,0.7), 0 0 0 0 rgba(245,197,24,0.5); transform: scale(1); }
  70%  { box-shadow: 0 0 5px 1px rgba(245,197,24,0.35), 0 0 0 5px rgba(245,197,24,0); transform: scale(1.08); }
  100% { box-shadow: 0 0 3px 0.5px rgba(245,197,24,0.7), 0 0 0 0 rgba(245,197,24,0); transform: scale(1); }
}
@keyframes pulseRed {
  0%   { box-shadow: 0 0 3px 0.5px rgba(239,68,68,0.75), 0 0 0 0 rgba(239,68,68,0.55); transform: scale(1); }
  70%  { box-shadow: 0 0 5px 1px rgba(239,68,68,0.4), 0 0 0 5px rgba(239,68,68,0); transform: scale(1.1); }
  100% { box-shadow: 0 0 3px 0.5px rgba(239,68,68,0.75), 0 0 0 0 rgba(239,68,68,0); transform: scale(1); }
}

/* ── big price ── */
.price-flash-wrap { width: 100vw; padding: 8px 0; margin: -8px calc(50% - 50vw) 0; background-color: transparent; }

/* Full-screen camera flash on a big move.
   At rest this element carries NO background color: iOS Safari tints its
   status bar / toolbar by sampling the page, and a lingering colored
   (even fully transparent) overlay leaves that chrome stuck green or red
   long after the flash. The color is set only for the duration of the
   animation and cleared on animationend. */
/* Deliberately inset from the top and bottom of the viewport: iOS Safari
   tints its status bar / toolbar from the pixels the page paints at those
   edges, sampling while the flash is live, so any color there sticks to
   the chrome. Leaving 9% strips untouched keeps the sampled regions the
   page's own black; the remaining 82% is still an unmissable flash. */
.screen-flash { position: fixed; left: 0; right: 0; top: 9%; bottom: 9%; pointer-events: none; z-index: 9999; opacity: 0; background: none; }
.screen-flash.go { animation: screenFlash 0.45s ease-out; }
@keyframes screenFlash {
  0%   { opacity: 0; }
  12%  { opacity: 0.85; }
  100% { opacity: 0; }
}
.price-big { font-size: clamp(46px, 15.5vw, 64px); font-weight: 800; letter-spacing: -1.5px; line-height: 1; text-align: center; color: var(--text1); font-variant-numeric: tabular-nums; transition: font-size 0.35s ease; }
/* During the settlement minute the live BRTI print steps aside: it shrinks to
   half size and the 60-second average takes over as the headline number. */
.price-big.half { font-size: clamp(23px, 7.75vw, 32px); letter-spacing: -0.5px; color: var(--text2); }
.price-big-tag { display: none; font-size: 9px; letter-spacing: 2px; font-weight: 800; color: var(--text3); text-transform: uppercase; text-align: center; margin-top: 3px; }
.price-big.half + .price-big-tag { display: block; }

/* ── settlement-minute 60s average ── */
.minute-avg {
  width: 100vw;
  margin: 4px calc(50% - 50vw) 0;
  padding: 11px 0 10px;
  text-align: center;
  background: var(--yellow);
  display: none;
}
.minute-avg.on { display: block; }
.minute-avg-label { font-size: 11px; letter-spacing: 2.5px; font-weight: 900; text-transform: uppercase; color: rgba(0,0,0,0.72); }
.minute-avg-price { font-size: clamp(46px, 15.5vw, 64px); font-weight: 800; letter-spacing: -1.5px; line-height: 1.05; color: #000; font-variant-numeric: tabular-nums; }
.minute-avg-sub { font-size: 10.5px; font-weight: 700; color: rgba(0,0,0,0.62); font-variant-numeric: tabular-nums; }
.minute-avg.locked .minute-avg-label::after { content: " · final"; }

/* ── settlement-minute capture list ── */
.capture-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 14px; }
.capture-hdr { display: flex; align-items: center; gap: 8px; margin-bottom: 7px; }
.capture-title { font-size: 11px; letter-spacing: 1.5px; color: var(--text3); font-weight: 800; text-transform: uppercase; }
.capture-when { flex: 1; font-size: 10px; color: var(--text3); font-variant-numeric: tabular-nums; }
.capture-export {
  border: 1px solid rgba(255,255,255,0.22); background: rgba(255,255,255,0.09); color: #d6dae1;
  font-size: 10.5px; font-weight: 800; letter-spacing: 0.8px; padding: 4px 12px; border-radius: 6px;
}
.capture-export:disabled { opacity: 0.4; }
.capture-list { max-height: 132px; overflow-y: auto; -webkit-overflow-scrolling: touch; font-variant-numeric: tabular-nums; }
.capture-row { display: flex; align-items: center; gap: 8px; padding: 2.5px 2px; font-size: 12px; border-bottom: 1px solid rgba(255,255,255,0.03); }
.capture-row:last-child { border-bottom: none; }
.capture-sec { width: 62px; flex-shrink: 0; color: var(--text3); }
.capture-idx { width: 34px; flex-shrink: 0; color: var(--text3); font-size: 10.5px; }
.capture-px { flex: 1; text-align: right; font-weight: 700; color: var(--text1); }
.capture-empty { font-size: 11.5px; color: var(--text3); font-style: italic; }

/* ── hero row: delta + countdown ── */
.hero-row { display: flex; align-items: center; justify-content: space-between; padding: 4px 4px 0; }
.price-delta { display: inline-flex; align-items: center; gap: 4px; padding: 4px 10px; border-radius: 20px; font-size: 13px; font-weight: 700; }
.price-delta.up   { background: var(--green-dim); color: var(--green); }
.price-delta.down { background: var(--red-dim); color: var(--red); }
.price-delta.flat { background: rgba(255,255,255,0.06); color: var(--text2); }
.countdown { font-size: 30px; font-weight: 800; font-variant-numeric: tabular-nums; letter-spacing: -0.5px; padding: 1px 8px; border-radius: 8px; transition: color 0.9s ease-out, background-color 0.9s ease-out; }
/* last ten minutes: invert to black on solid red */
.countdown.urgent { background: var(--red); color: #000; }
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
.chart-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 8px 8px 5px; }
.chart-hdr { display: flex; justify-content: space-between; align-items: baseline; padding: 0 4px 4px; font-size: 10px; color: var(--text3); }
.chart-hdr .hi { color: var(--green); font-weight: 700; }
.chart-hdr .lo { color: var(--red); font-weight: 700; }
canvas#chart { width: 100%; height: 158px; display: block; }
.chart-axis { display: flex; justify-content: space-between; padding: 3px 4px 0; font-size: 9.5px; color: var(--text3); }

/* ── outlook (chance) card ── */
.outlook-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 10px 13px; }
.trend-headline { font-size: 14px; font-weight: 700; line-height: 1.35; color: var(--text1); }
.trend-headline b.up { color: var(--green); }
.trend-headline b.down { color: var(--red); }
.gauge { position: relative; height: 7px; border-radius: 4px; margin: 8px 0 5px; background: linear-gradient(90deg, var(--red) 0%, var(--yellow) 50%, var(--green) 100%); }
.gauge-pointer { position: absolute; top: -4px; width: 3px; height: 16px; background: #fff; border-radius: 2px; box-shadow: 0 0 4px rgba(0,0,0,0.6); transform: translateX(-50%); animation: gaugePulse 1.4s ease-in-out infinite; }
@keyframes gaugePulse { 0%, 100% { opacity: 1; transform: translateX(-50%) scaleY(1); } 50% { opacity: 0.5; transform: translateX(-50%) scaleY(1.3); } }
.gauge-labels { display: flex; justify-content: space-between; font-size: 10px; color: var(--text3); }
.stat-chips { display: flex; gap: 6px; margin-top: 8px; flex-wrap: wrap; }
.chip { font-size: 10px; font-weight: 700; padding: 4px 9px; border-radius: 20px; background: rgba(255,255,255,0.05); color: var(--text2); }
.chip.green { color: var(--green); background: var(--green-dim); }
.chip.red { color: var(--red); background: var(--red-dim); }
.chip.yellow { color: var(--yellow); background: var(--yellow-dim); }
.trend-warmup { font-size: 13px; color: var(--text2); padding: 4px 0; }

/* ── kalshi vs model ── */
.kalshi-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 9px 12px; }
.kalshi-hdr { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 6px; }
.kalshi-title { font-size: 11px; letter-spacing: 1.5px; color: var(--text3); font-weight: 800; text-transform: uppercase; }
.kalshi-close { font-size: 10px; color: var(--text3); }
.kalshi-compare { display: flex; align-items: stretch; gap: 8px; }
.kalshi-cell { flex: 1; background: var(--panel2); border: 1px solid var(--border); border-radius: 10px; padding: 5px 9px; text-align: center; }
.kalshi-cell .lbl { font-size: 9px; letter-spacing: 1px; color: var(--text3); font-weight: 700; text-transform: uppercase; }
.kalshi-cell .val { font-size: 17px; font-weight: 800; margin-top: 1px; font-variant-numeric: tabular-nums; color: var(--text1); }
.kalshi-cell.edge .val.up { color: var(--green); }
.kalshi-cell.edge .val.down { color: var(--red); }
.kalshi-cell.edge .val.flat { color: var(--yellow); }
.kalshi-detail { font-size: 10px; color: var(--text2); margin-top: 6px; text-align: center; }
.kalshi-detail b { color: var(--text1); }
.kalshi-off { font-size: 11.5px; color: var(--text3); font-style: italic; margin-top: 2px; }

/* ── my kalshi portfolio ── */
.portfolio-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 9px 12px; }
.port-row { display: flex; align-items: center; gap: 8px; padding: 5px 0; border-bottom: 1px solid rgba(255,255,255,0.04); font-variant-numeric: tabular-nums; }
.port-row:last-of-type { border-bottom: none; }
.port-side { font-size: 10px; font-weight: 800; letter-spacing: 0.5px; padding: 2px 7px; border-radius: 5px; flex-shrink: 0; }
.port-side.yes { background: var(--green-dim); color: var(--green); }
.port-side.no { background: var(--red-dim); color: var(--red); }
.port-desc { flex: 1; font-size: 12.5px; color: var(--text1); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.port-val { font-size: 14px; font-weight: 800; color: var(--text1); text-align: right; }
.port-val span { display: block; font-size: 10px; font-weight: 600; color: var(--text3); }
.port-pnl { font-size: 12.5px; font-weight: 800; text-align: right; min-width: 62px; }
.port-pnl.up { color: var(--green); }
.port-pnl.down { color: var(--red); }
.port-pnl.flat { color: var(--text3); }
.port-actions { display: flex; flex-direction: column; gap: 3px; flex-shrink: 0; margin-left: 7px; }
.port-sell, .port-buy {
  padding: 3px 9px; border-radius: 6px; min-width: 54px; text-align: center;
  font-size: 10.5px; font-weight: 800; letter-spacing: 0.3px;
}
.port-sell { border: 1px solid rgba(245,197,24,0.5); background: rgba(245,197,24,0.12); color: var(--yellow); }
.port-sell.confirm { background: var(--red); border-color: var(--red); color: #fff; }
.port-buy { border: 1px solid rgba(34,197,94,0.5); background: rgba(34,197,94,0.12); color: var(--green); }
.port-buy.confirm { background: var(--green); border-color: var(--green); color: #fff; }
.port-sell:disabled, .port-buy:disabled { opacity: 0.45; }
.port-sell-msg { font-size: 11px; margin-top: 6px; text-align: right; }
.port-sell-msg.ok { color: var(--green); }
.port-sell-msg.err { color: var(--red); }
.port-empty { font-size: 11.5px; color: var(--text3); font-style: italic; }
.port-empty.err { color: var(--yellow); font-style: normal; }
/* a decided position: won rows carry a green field, lost ones step back */
.port-row.won { background: linear-gradient(90deg, rgba(34,197,94,0.18), rgba(34,197,94,0.02)); border-radius: 8px; }
.port-row.lost { opacity: 0.6; }
.port-badge { font-size: 9.5px; font-weight: 900; letter-spacing: 1px; padding: 3px 8px; border-radius: 5px; text-align: center; }
.port-badge.won { background: var(--green); color: #042a12; }
.port-badge.lost { background: var(--red-dim); color: var(--red); }
.port-badge.settling { background: rgba(245,197,24,0.16); color: var(--yellow); }
.port-prov { font-size: 8.5px; color: var(--text3); text-align: center; letter-spacing: 0.3px; }

/* the win announcement — lives outside the positions card so the 1/sec
   rebuild of that card can't restart its animation mid-flight */
.win-toast {
  display: none;
  width: 100vw; margin: 0 calc(50% - 50vw); padding: 11px 14px;
  background: linear-gradient(90deg, #16a34a, #22c55e);
  color: #04220f; text-align: center;
}
.win-toast.on { display: block; animation: winIn 0.45s ease-out, winGlow 1.1s ease-in-out 0.45s 4; }
.win-toast .wt-top { font-size: 11px; font-weight: 900; letter-spacing: 2.5px; text-transform: uppercase; opacity: 0.8; }
.win-toast .wt-amt { font-size: 27px; font-weight: 800; line-height: 1.15; font-variant-numeric: tabular-nums; }
.win-toast .wt-sub { font-size: 10.5px; font-weight: 700; opacity: 0.78; font-variant-numeric: tabular-nums; }
@keyframes winIn { from { opacity: 0; transform: translateY(-8px); } to { opacity: 1; transform: none; } }
@keyframes winGlow { 0%, 100% { filter: brightness(1); } 50% { filter: brightness(1.22); } }

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

/* ── flight search ── */
.jump-flights {
  position: absolute; right: 0; top: 50%; transform: translateY(-50%);
  width: 26px; height: 26px; border-radius: 8px;
  border: 1px solid var(--border); background: var(--panel2); color: var(--text2);
  font-size: 13px; line-height: 1; display: flex; align-items: center; justify-content: center;
}
.jump-flights:active { background: var(--panel); color: var(--text1); }
.stealth-btn { right: 32px; font-size: 14px; }

.flights-section { margin-top: 16px; scroll-margin-top: 10px; }
.flights-hdr { display: flex; align-items: center; gap: 8px; padding: 0 4px 8px; }
/* the section owns the blue the Search button uses, so that colour reads as
   this feature's rather than as one stray accent */
.flights-title { font-size: 12px; letter-spacing: 2px; font-weight: 800; text-transform: uppercase; color: #5ac8fa; }
.flights-sub { font-size: 10px; color: var(--text3); }
.fl-keybtn {
  margin-left: auto; flex-shrink: 0;
  width: 24px; height: 24px; border-radius: 7px;
  border: 1px solid var(--border); background: var(--panel2); color: var(--text3);
  display: flex; align-items: center; justify-content: center;
}
.fl-keybtn.set { color: var(--text2); }
.fl-keybtn.needed { color: var(--yellow); border-color: rgba(245,197,24,0.4); }
.fl-keybtn:active { background: var(--panel); }

.fl-card { background: var(--panel); border: 1px solid var(--border); border-radius: 14px; padding: 12px 13px; }
.fl-field { margin-bottom: 9px; }
.fl-label { font-size: 9px; letter-spacing: 1.4px; font-weight: 800; text-transform: uppercase; color: var(--text3); margin-bottom: 4px; }
.fl-input {
  width: 100%; background: var(--panel2); border: 1px solid var(--border); border-radius: 9px;
  color: var(--text1); font-size: 15px; font-weight: 700; letter-spacing: 1.2px;
  padding: 9px 11px; text-transform: uppercase;
}
.fl-input::placeholder { color: var(--text3); font-weight: 500; letter-spacing: 0.5px; text-transform: none; }
.fl-input:focus { outline: none; border-color: rgba(90,200,250,0.55); background: rgba(90,200,250,0.06); }
.fl-chips { display: flex; gap: 5px; margin-top: 6px; flex-wrap: wrap; }
.fl-chip {
  font-size: 10.5px; font-weight: 800; letter-spacing: 0.8px;
  padding: 4px 10px; border-radius: 20px;
  border: 1px solid var(--border); background: var(--panel2); color: var(--text2);
}
.fl-chip.on { background: var(--text1); color: #000; border-color: var(--text1); }

.fl-row2 { display: flex; gap: 9px; align-items: flex-end; }
.fl-row2 > .fl-field { flex: 1; margin-bottom: 0; }
/* the native picker sits invisibly on top of the styled label so iOS opens
   its own date wheel on tap while the page keeps the relative wording */
.fl-date-wrap { position: relative; }
.fl-date-display {
  background: var(--panel2); border: 1px solid var(--border); border-radius: 9px;
  color: var(--text1); font-size: 15px; font-weight: 700; padding: 9px 11px;
  white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
}
.fl-date-wrap input[type="date"] {
  position: absolute; inset: 0; width: 100%; height: 100%;
  opacity: 0; border: 0; padding: 0; margin: 0;
}
.fl-go {
  width: 100%; margin-top: 11px; padding: 11px; border-radius: 10px;
  border: 1px solid rgba(90,200,250,0.5); background: rgba(90,200,250,0.14); color: #5ac8fa;
  font-size: 12.5px; font-weight: 800; letter-spacing: 1.6px; text-transform: uppercase;
}
.fl-go:active { background: rgba(90,200,250,0.24); }
.fl-go:disabled { opacity: 0.5; }
.fl-key { display: none; margin-top: 10px; padding-top: 10px; border-top: 1px solid var(--border); }
.fl-key.on { display: block; }
.fl-key-row { display: flex; gap: 7px; }
.fl-key-row input {
  flex: 1; min-width: 0; background: var(--panel2); border: 1px solid var(--border); border-radius: 9px;
  color: var(--text1); font-size: 13px; padding: 8px 10px; letter-spacing: 1px;
}
.fl-key-row input:focus { outline: none; border-color: rgba(90,200,250,0.55); }
.fl-key-row button {
  padding: 8px 14px; border-radius: 9px; font-size: 11px; font-weight: 800; letter-spacing: 1px;
  border: 1px solid rgba(90,200,250,0.5); background: rgba(90,200,250,0.14); color: #5ac8fa;
}
.fl-key-note { font-size: 9.5px; color: var(--text3); margin-top: 6px; line-height: 1.45; }
.fl-msg { font-size: 11.5px; color: var(--text3); margin-top: 9px; text-align: center; font-style: italic; }
.fl-msg.err { color: var(--red); font-style: normal; }

.fl-results { display: flex; flex-direction: column; gap: 7px; margin-top: 9px; }

.fl-sorts { display: flex; gap: 6px; margin-bottom: 8px; }
.fl-sort { flex: 1; padding: 8px 12px; border: 1px solid var(--border); border-radius: 8px; background: transparent; color: var(--text2); font-size: 13px; font-weight: 600; cursor: pointer; transition: all 0.2s; }
.fl-sort:hover { border-color: var(--blue); color: var(--blue); }
.fl-sort.active { background: var(--blue); border-color: var(--blue); color: #fff; }

/* the two headline fares, one per cabin, above the split lists */
.fl-tiles { display: flex; gap: 8px; }
.fl-tile {
  flex: 1; min-width: 0; text-align: left;
  background: var(--panel); border: 1px solid var(--border); border-radius: 13px; padding: 10px 12px;
}
.fl-tile .lbl { font-size: 8.5px; letter-spacing: 1.2px; font-weight: 900; text-transform: uppercase; color: var(--text3); }
.fl-tile .amt { font-size: 22px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; letter-spacing: -0.4px; margin-top: 1px; }
.fl-tile .sub { font-size: 9.5px; color: var(--text3); margin-top: 2px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.fl-tile.first-cabin { border-color: rgba(192,132,252,0.3); background: linear-gradient(180deg, rgba(192,132,252,0.05), rgba(0,0,0,0)); }
.fl-tile.first-cabin .lbl { color: #c084fc; }
.fl-tile:disabled { opacity: 0.4; }
.fl-tile:not(:disabled):active { border-color: rgba(90,200,250,0.6); }

.fl-group { font-size: 9.5px; letter-spacing: 1.8px; font-weight: 900; text-transform: uppercase;
  color: var(--text3); padding: 8px 4px 0; }
.fl-group.first-cabin { color: #c084fc; }

/* where a tile lands you */
.fl-item.flash { animation: flItemFlash 1.8s ease-out; }
@keyframes flItemFlash {
  0%, 22% { border-color: rgba(90,200,250,0.85); background-color: rgba(90,200,250,0.13); }
  100% { border-color: var(--border); background-color: transparent; }
}
.fl-item { position: relative; background: var(--panel); border: 1px solid var(--border); border-radius: 13px; padding: 10px 12px; }
.fl-item.first-cabin { border-color: rgba(192,132,252,0.32); background: linear-gradient(180deg, rgba(192,132,252,0.055), rgba(0,0,0,0)); }
.fl-top { display: flex; align-items: center; gap: 9px; }
.fl-logo { width: 26px; height: 26px; border-radius: 7px; background: #fff; object-fit: contain; flex-shrink: 0; padding: 2px; }
.fl-carrier { flex: 1; min-width: 0; }
.fl-carrier .nm { font-size: 12.5px; font-weight: 700; color: var(--text1); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.fl-carrier .fn { font-size: 9.5px; color: var(--text3); letter-spacing: 0.4px; }
.fl-price { text-align: right; flex-shrink: 0; }
.fl-price .amt { font-size: 19px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; letter-spacing: -0.3px; }
.fl-price .cab { font-size: 8.5px; font-weight: 900; letter-spacing: 1.2px; text-transform: uppercase; color: var(--text3); }
.fl-item.first-cabin .fl-price .cab { color: #c084fc; }

.fl-times { display: flex; align-items: center; gap: 8px; margin-top: 9px; }
.fl-end { text-align: center; }
.fl-end .t { font-size: 15px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; }
.fl-end .a { font-size: 9.5px; font-weight: 800; letter-spacing: 1px; color: var(--text3); }
.fl-end .t sup { font-size: 8.5px; color: var(--yellow); font-weight: 800; }
.fl-path { flex: 1; text-align: center; position: relative; }
.fl-path .bar { height: 1px; background: linear-gradient(90deg, rgba(255,255,255,0.05), rgba(255,255,255,0.28), rgba(255,255,255,0.05)); margin: 7px 0 5px; position: relative; }
.fl-path .bar::after { content: ""; position: absolute; right: -1px; top: -2px; width: 5px; height: 5px; border-radius: 50%; background: rgba(255,255,255,0.4); }
.fl-path .dur { font-size: 9.5px; color: var(--text3); font-variant-numeric: tabular-nums; }
.fl-path .via { font-size: 10px; color: var(--text2); font-weight: 700; }
.fl-path .via.direct { color: var(--green); }

.fl-meta { font-size: 10px; color: var(--text3); margin-top: 7px; line-height: 1.45; }
.fl-meta b { color: var(--text2); font-weight: 700; }
.fl-flags { display: flex; gap: 4px; flex-wrap: wrap; margin-top: 8px; }
.fl-flag { font-size: 8.5px; font-weight: 900; letter-spacing: 0.9px; padding: 3px 7px; border-radius: 5px; text-transform: uppercase; }
.fl-flag.cheap { background: var(--green); color: #042a12; }
.fl-flag.wide { background: rgba(90,200,250,0.15); color: #5ac8fa; }
.fl-flag.longlay { background: rgba(245,197,24,0.15); color: var(--yellow); }

/* ── hotels ──
   Its own accent rather than the flights blue, so the two travel panels read
   as siblings instead of one long section. The key itself is shared. */
.hotels-section { margin-top: 18px; scroll-margin-top: 10px; }
.hotels-title { font-size: 12px; letter-spacing: 2px; font-weight: 800; text-transform: uppercase; color: #2dd4bf; }
/* a place name, not an airport code — so no uppercasing or letter-spacing */
.ht-where { text-transform: none; letter-spacing: 0.2px; font-size: 14.5px; }
.ht-where::placeholder { letter-spacing: 0.2px; }
.hotels-section .fl-input:focus { border-color: rgba(45,212,191,0.55); background: rgba(45,212,191,0.06); }
.hotels-section .fl-chip.on { background: #2dd4bf; color: #04231f; border-color: #2dd4bf; }
/* the date row zeroes its own bottom margin, so this label needs the gap */
.ht-guests { margin-top: 11px; margin-bottom: 0; }
.ht-guests .fl-chips { margin-top: 0; }
.ht-go { border-color: rgba(45,212,191,0.5); background: rgba(45,212,191,0.14); color: #2dd4bf; }
.ht-go:active { background: rgba(45,212,191,0.24); }
.hotels-section .fl-sort:hover { border-color: #2dd4bf; color: #2dd4bf; }
.hotels-section .fl-sort.active { background: #2dd4bf; border-color: #2dd4bf; color: #04231f; }

.ht-item { background: var(--panel); border: 1px solid var(--border); border-radius: 13px; padding: 10px 12px; }
.ht-item.flash { animation: flItemFlash 1.8s ease-out; }
.ht-top { display: flex; align-items: flex-start; gap: 10px; }
.ht-thumb { width: 54px; height: 54px; border-radius: 9px; object-fit: cover; flex-shrink: 0; background: var(--panel2); }
.ht-thumb.ph { display: flex; align-items: center; justify-content: center; font-size: 17px; color: var(--text3); }
.ht-id { flex: 1; min-width: 0; }
.ht-id .nm { font-size: 13px; font-weight: 700; color: var(--text1); line-height: 1.3; }
.ht-id .cls { font-size: 9.5px; color: var(--text3); letter-spacing: 0.4px; margin-top: 2px; }
.ht-stars { color: var(--yellow); letter-spacing: 1px; }
.ht-rate { display: flex; align-items: baseline; gap: 5px; margin-top: 3px; }
.ht-rate .sc { font-size: 12px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; }
.ht-rate .rv { font-size: 9.5px; color: var(--text3); }
.ht-price { text-align: right; flex-shrink: 0; }
.ht-price .amt { font-size: 19px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; letter-spacing: -0.3px; }
.ht-price .per { font-size: 8.5px; font-weight: 900; letter-spacing: 1.2px; text-transform: uppercase; color: var(--text3); }
.ht-price .tot { font-size: 9.5px; color: var(--text2); margin-top: 3px; font-variant-numeric: tabular-nums; }
.ht-amen { font-size: 10px; color: var(--text3); margin-top: 8px; line-height: 1.45; }
.fl-flag.rated { background: rgba(45,212,191,0.15); color: #2dd4bf; }
.fl-flag.deal { background: rgba(249,115,22,0.16); color: var(--orange); }
/* amenity badges, each its own hue so the row reads by colour at a glance */
.fl-flag.b-paw    { background: rgba(245,197,24,0.16);  color: var(--yellow); padding: 3px 8px; }
.fl-flag.b-pool   { background: rgba(56,189,248,0.16);  color: #38bdf8; }
.fl-flag.b-spa    { background: rgba(244,114,182,0.16); color: #f472b6; }
.fl-flag.b-bar    { background: rgba(167,139,250,0.16); color: #a78bfa; }
.fl-flag.b-turn   { background: rgba(148,163,184,0.18); color: #94a3b8; }
.fl-flag.b-dine   { background: rgba(163,230,53,0.15);  color: #a3e635; }
.fl-flag.b-casino { background: rgba(232,121,249,0.16); color: #e879f9; }
.paw-svg { width: 13px; height: 13px; display: block; }
.fl-tile.ht-tile.best { border-color: rgba(45,212,191,0.32); background: linear-gradient(180deg, rgba(45,212,191,0.055), rgba(0,0,0,0)); }
.fl-tile.ht-tile.best .lbl { color: #2dd4bf; }
.ht-item.tappable { cursor: pointer; }
.ht-item.tappable:active { background: var(--panel2); }

/* ── the detail sheet ── */
.ht-sheet { position: fixed; inset: 0; z-index: 60; display: none; }
.ht-sheet.on { display: block; }
.ht-scrim { position: absolute; inset: 0; background: rgba(0,0,0,0.72); backdrop-filter: blur(2px); }
.ht-panel {
  position: absolute; left: 0; right: 0; bottom: 0; top: 24px;
  background: var(--bg); border-top: 1px solid var(--border);
  border-radius: 18px 18px 0 0; overflow: hidden;
  animation: htUp 0.24s cubic-bezier(0.2, 0.8, 0.3, 1);
}
@keyframes htUp { from { transform: translateY(26px); opacity: 0; } to { transform: none; opacity: 1; } }
.ht-grip { width: 34px; height: 4px; border-radius: 3px; background: var(--border); margin: 8px auto 0; }
.ht-close {
  position: absolute; top: 8px; right: 10px; z-index: 2;
  width: 30px; height: 30px; border-radius: 50%;
  border: 1px solid var(--border); background: var(--panel2); color: var(--text2);
  font-size: 19px; line-height: 1; display: flex; align-items: center; justify-content: center;
}
.ht-close:active { background: var(--panel); color: var(--text1); }
.ht-body {
  position: absolute; inset: 20px 0 0; overflow-y: auto; -webkit-overflow-scrolling: touch;
  padding: 4px 13px calc(26px + env(safe-area-inset-bottom));
}
.ht-load { text-align: center; color: var(--text3); font-size: 12px; font-style: italic; padding: 40px 0; }
.ht-load.err { color: var(--red); font-style: normal; }

/* gallery: a snapping filmstrip rather than a grid, so one photo reads big */
.ht-gal { display: flex; gap: 7px; overflow-x: auto; scroll-snap-type: x mandatory; margin: 0 -13px; padding: 0 13px 2px; }
.ht-gal::-webkit-scrollbar { display: none; }
.ht-gal img { width: 82%; height: 190px; flex: 0 0 auto; object-fit: cover; border-radius: 12px; scroll-snap-align: center; background: var(--panel2); }
.ht-count { font-size: 9.5px; color: var(--text3); text-align: right; margin-top: 4px; letter-spacing: 0.5px; }

.ht-d-name { font-size: 19px; font-weight: 800; color: var(--text1); line-height: 1.25; margin-top: 10px; letter-spacing: -0.2px; }
.ht-d-sub { font-size: 10.5px; color: var(--text3); margin-top: 3px; }
.ht-d-rate { display: flex; align-items: baseline; gap: 6px; margin-top: 6px; }
.ht-d-rate .big { font-size: 17px; font-weight: 800; color: var(--text1); }
.ht-d-rate .of { font-size: 10px; color: var(--text3); }

.ht-sec { margin-top: 18px; }
.ht-sec-h { font-size: 9.5px; letter-spacing: 1.5px; font-weight: 800; text-transform: uppercase; color: #2dd4bf; margin-bottom: 8px; }
.ht-kv { display: flex; gap: 10px; font-size: 11.5px; color: var(--text2); padding: 6px 0; border-bottom: 1px solid var(--border); line-height: 1.45; }
.ht-kv:last-child { border-bottom: 0; }
.ht-kv .k { flex: 0 0 84px; color: var(--text3); font-weight: 700; }
.ht-kv .v { flex: 1; min-width: 0; }
/* place names need the room a field label does not */
.ht-kv.ht-near .k { flex: 1 1 auto; color: var(--text1); font-weight: 600; }
.ht-kv.ht-near .v { flex: 0 0 auto; text-align: right; color: var(--text3); }

.ht-map { width: 100%; height: 190px; border: 1px solid var(--border); border-radius: 12px; background: var(--panel2); display: block; }
.ht-coord { font-size: 9.5px; color: var(--text3); margin-top: 5px; font-variant-numeric: tabular-nums; }

.ht-src { display: flex; align-items: center; gap: 9px; padding: 8px 0; border-bottom: 1px solid var(--border); }
.ht-src:last-child { border-bottom: 0; }
.ht-src img { width: 20px; height: 20px; border-radius: 5px; background: #fff; object-fit: contain; padding: 1px; flex-shrink: 0; }
.ht-src .nm { flex: 1; min-width: 0; font-size: 12px; color: var(--text1); font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.ht-src .off { font-size: 8px; font-weight: 900; letter-spacing: 0.8px; color: #2dd4bf; text-transform: uppercase; }
.ht-src .pr { font-size: 13px; font-weight: 800; color: var(--text1); font-variant-numeric: tabular-nums; flex-shrink: 0; }
.ht-src .pr small { display: block; font-size: 9px; font-weight: 600; color: var(--text3); text-align: right; }

.ht-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 5px; }
.ht-bar .st { font-size: 10px; color: var(--text3); width: 26px; flex-shrink: 0; font-variant-numeric: tabular-nums; }
.ht-bar .track { flex: 1; height: 6px; border-radius: 3px; background: var(--panel2); overflow: hidden; }
.ht-bar .fill { height: 100%; background: var(--yellow); border-radius: 3px; }
.ht-bar .ct { font-size: 9.5px; color: var(--text3); width: 44px; text-align: right; flex-shrink: 0; font-variant-numeric: tabular-nums; }

.ht-topic { padding: 7px 0; border-bottom: 1px solid var(--border); }
.ht-topic:last-child { border-bottom: 0; }
.ht-topic .th { display: flex; justify-content: space-between; align-items: baseline; gap: 8px; }
.ht-topic .nm { font-size: 11.5px; font-weight: 700; color: var(--text1); }
.ht-topic .mn { font-size: 9.5px; color: var(--text3); }
.ht-split { display: flex; height: 5px; border-radius: 3px; overflow: hidden; margin-top: 5px; background: var(--panel2); }
.ht-split i { display: block; height: 100%; }
.ht-split .pos { background: var(--green); }
.ht-split .neu { background: var(--text3); }
.ht-split .neg { background: var(--red); }

.ht-chiplist { display: flex; flex-wrap: wrap; gap: 5px; }
.ht-chiplist span { font-size: 10.5px; padding: 5px 9px; border-radius: 7px; background: var(--panel2); border: 1px solid var(--border); color: var(--text2); }
.ht-chiplist.no span { color: var(--text3); text-decoration: line-through; opacity: 0.75; }
.ht-desc { font-size: 12px; color: var(--text2); line-height: 1.55; }

/* ── passcode ──
   A privacy screen, not access control: it keeps the dashboard off the glass
   when someone else is holding the phone. The data behind it is still served
   by the same open endpoints. */
/* shown at first paint, removed once unlocked — the other way round would
   flash the dashboard before the script ran */
.pin { position: fixed; inset: 0; z-index: 200; background: var(--bg);
  display: flex; align-items: center; justify-content: center; }
.pin.off { display: none; }
.pin-inner { width: 100%; max-width: 300px; padding: 0 20px calc(20px + env(safe-area-inset-bottom)); }
.pin-title { text-align: center; font-size: 13px; letter-spacing: 0.6px; color: var(--text2); margin-bottom: 18px; }
.pin-dots { display: flex; gap: 16px; justify-content: center; margin-bottom: 34px; }
.pin-dots i {
  width: 11px; height: 11px; border-radius: 50%;
  border: 1px solid var(--text3); background: transparent; transition: background 0.12s, border-color 0.12s;
}
.pin-dots i.on { background: var(--text1); border-color: var(--text1); }
.pin.bad .pin-dots { animation: pinShake 0.4s; }
.pin.bad .pin-dots i { border-color: var(--red); background: var(--red); }
@keyframes pinShake {
  0%,100% { transform: none; } 20% { transform: translateX(-9px); }
  40% { transform: translateX(9px); } 60% { transform: translateX(-6px); } 80% { transform: translateX(6px); }
}
.pin-pad { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; justify-items: center; }
.pin-key {
  width: 72px; height: 72px; border-radius: 50%;
  border: 1px solid var(--border); background: var(--panel2); color: var(--text1);
  display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 1px;
  -webkit-tap-highlight-color: transparent;
}
.pin-key:active { background: var(--panel); }
.pin-key .n { font-size: 27px; font-weight: 400; line-height: 1; }
.pin-key .l { font-size: 9px; font-weight: 700; letter-spacing: 1.6px; color: var(--text3); height: 10px; }
.pin-key.blank { border: 0; background: none; }
.pin-key.act { border: 0; background: none; }
.pin-key.act .n { font-size: 14px; color: var(--text2); font-weight: 600; letter-spacing: 0.4px; }

/* ── stealth ──
   A decoy front page over the whole screen with the real numbers reduced to a
   ticker strip along the bottom, where a page like this would carry one. */
.stealth { position: fixed; inset: 0; z-index: 150; background: #f6f4ef; display: none; }
.stealth.on { display: block; }
.dk {
  position: absolute; inset: 0; overflow-y: auto; -webkit-overflow-scrolling: touch;
  padding: calc(8px + env(safe-area-inset-top)) 16px 74px;
  background: #f6f4ef; color: #14110c;
  font-family: Georgia, "Times New Roman", serif;
}
.dk-rail { display: flex; gap: 14px; overflow-x: auto; font-family: -apple-system, sans-serif;
  font-size: 9.5px; letter-spacing: 0.4px; color: #6b6559; padding-bottom: 7px; border-bottom: 1px solid #d9d3c6; }
.dk-rail > span { white-space: nowrap; flex: 0 0 auto; }
.dk-rail::-webkit-scrollbar { display: none; }
.dk-rail b { color: #14110c; font-weight: 700; }
.dk-rail .up { color: #0a7a35; } .dk-rail .dn { color: #b3261e; }
.dk-mast { text-align: center; font-size: 33px; font-weight: 700; letter-spacing: -0.5px; margin: 13px 0 3px; }
.dk-sub { text-align: center; font-family: -apple-system, sans-serif; font-size: 9px; letter-spacing: 1.6px;
  text-transform: uppercase; color: #6b6559; border-bottom: 2px solid #14110c; padding-bottom: 9px; }
.dk-lead { padding: 15px 0 14px; border-bottom: 1px solid #d9d3c6; }
.dk-photo { width: 100%; height: 176px; border-radius: 2px; margin-bottom: 11px; }
.dk-lead h1 { font-size: 27px; line-height: 1.16; font-weight: 700; letter-spacing: -0.4px; }
.dk-dek { font-size: 14px; line-height: 1.5; color: #4a4438; margin-top: 7px; }
.dk-by { font-family: -apple-system, sans-serif; font-size: 8.5px; letter-spacing: 1.4px;
  text-transform: uppercase; color: #8a8375; margin-top: 9px; }
.dk-sec { font-family: -apple-system, sans-serif; font-size: 9.5px; font-weight: 800; letter-spacing: 2px;
  text-transform: uppercase; color: #b3261e; margin: 19px 0 9px; }
.dk-item { display: flex; gap: 12px; padding: 12px 0; border-bottom: 1px solid #e4dfd3; }
.dk-item .tx { flex: 1; min-width: 0; }
.dk-item h2 { font-size: 16.5px; line-height: 1.25; font-weight: 700; }
.dk-item p { font-size: 12.5px; line-height: 1.45; color: #4a4438; margin-top: 4px; }
.dk-item .th { width: 74px; height: 60px; border-radius: 2px; flex-shrink: 0; }
.dk-quote { border-left: 3px solid #14110c; padding: 3px 0 3px 13px; margin: 17px 0; font-size: 17px; line-height: 1.35; font-style: italic; }
.dk-tbl { width: 100%; border-collapse: collapse; font-family: -apple-system, sans-serif; font-size: 11.5px; }
.dk-tbl th { text-align: left; font-size: 8.5px; letter-spacing: 1.2px; text-transform: uppercase;
  color: #8a8375; border-bottom: 1px solid #14110c; padding: 5px 0; }
.dk-tbl td { padding: 6px 0; border-bottom: 1px solid #e4dfd3; font-variant-numeric: tabular-nums; }
.dk-tbl td.r { text-align: right; }
.dk-tbl .up { color: #0a7a35; } .dk-tbl .dn { color: #b3261e; }
.dk-end { text-align: center; font-family: -apple-system, sans-serif; font-size: 9px; letter-spacing: 1.5px;
  text-transform: uppercase; color: #8a8375; padding: 26px 0 8px; }

.st-hud {
  position: absolute; left: 0; right: 0; bottom: 0; z-index: 2;
  display: flex; align-items: center; gap: 9px;
  padding: 6px 13px calc(6px + env(safe-area-inset-bottom));
  background: #14110c; color: #cfc9bb;
  font-family: -apple-system, sans-serif; font-size: 10px; font-variant-numeric: tabular-nums;
  overflow-x: auto; white-space: nowrap; -webkit-tap-highlight-color: transparent;
  /* iOS answers a long press on text with the selection loupe and a
     pointercancel, which would kill the hold before it ever completed —
     leaving no way out of stealth on an actual phone. */
  -webkit-touch-callout: none; -webkit-user-select: none; user-select: none;
  touch-action: manipulation;
}
.st-hud::-webkit-scrollbar { display: none; }
.st-hud .px { font-weight: 800; color: #fff; font-size: 11px; }
.st-hud .tr { font-weight: 700; }
.st-hud .tr.up { color: #4ade80; } .st-hud .tr.dn { color: #f87171; } .st-hud .tr.fl { color: #8a8375; }
.st-hud .sep { color: #4a4438; }
.st-hud .pos { color: #cfc9bb; }
.st-hud .gain { font-weight: 700; } .st-hud .gain.up { color: #4ade80; } .st-hud .gain.dn { color: #f87171; }
.st-hud .tot { margin-left: auto; padding-left: 10px; font-weight: 800; }
.st-hud .tot.up { color: #4ade80; } .st-hud .tot.dn { color: #f87171; }

/* ── footer ── */
.footer { display: flex; flex-direction: column; gap: 2px; padding: 4px 4px 0; font-size: 10.5px; color: var(--text3); }
.footer-row { display: flex; justify-content: space-between; align-items: baseline; }
.footer-brand { font-weight: 800; letter-spacing: 1.5px; color: var(--text2); }
.footer-note { font-size: 9.5px; color: var(--text3); opacity: 0.75; }
.footer a { color: var(--text3); text-decoration: none; }

</style>
</head>
<body>

<div class="pin" id="pinGate">
  <div class="pin-inner">
    <div class="pin-title">Enter Passcode</div>
    <div class="pin-dots" id="pinDots"><i></i><i></i><i></i><i></i></div>
    <div class="pin-pad" id="pinPad"></div>
  </div>
</div>
<script>
  // synchronous, and before the dashboard markup is parsed: an already
  // unlocked session must never see the keypad flash past
  try { if (sessionStorage.getItem("btcUnlocked") === "1") document.getElementById("pinGate").classList.add("off"); } catch (e) {}
</script>

<div class="stealth" id="stealth">
  <div class="dk" id="decoy"></div>
  <div class="st-hud" id="stHud"></div>
</div>

<div class="screen-flash" id="screenFlash"></div>
<div id="app">

  <div class="brand-row" style="position:relative">
    <span class="brand-text">CF Benchmark BRTI</span>
    <span class="brand-sep">|</span>
    <span class="status-dot" id="statusDot"></span>
    <span class="status-word" id="statusWord">LIVE</span>
    <button class="jump-flights" id="jumpFlights" aria-label="Jump to flight search">&#9992;</button>
    <button class="jump-flights stealth-btn" id="stealthBtn" aria-label="Stealth mode">&#9680;</button>
  </div>

  <div class="price-flash-wrap" id="priceFlashWrap">
    <div class="price-big" id="bigPrice">—</div>
    <div class="price-big-tag">BRTI Live</div>
  </div>

  <div class="minute-avg" id="minuteAvg">
    <div class="minute-avg-label" id="minuteAvgLabel">60 Second Avg</div>
    <div class="minute-avg-price" id="minuteAvgPrice">—</div>
    <div class="minute-avg-sub" id="minuteAvgSub">&nbsp;</div>
  </div>

  <div class="hero-row">
    <div class="price-delta flat" id="priceDelta">— %</div>
    <div class="countdown" id="countdown">60:00</div>
  </div>

  <div class="outlook-card" id="outlookCard">
    <div id="outlookBody"><div class="trend-warmup">Gathering data for a projection…</div></div>
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

  <div class="kalshi-card" id="kalshiCard"></div>

  <div class="win-toast" id="winToast"></div>

  <div class="portfolio-card" id="portfolioCard" style="display:none"></div>

  <div class="trades-card">
    <div class="trades-hdr"><span class="live-chip"></span>Live Trade Feed</div>
    <div class="trades-list" id="tradesList"></div>
  </div>

  <div class="capture-card">
    <div class="capture-hdr">
      <span class="capture-title">60s Capture</span>
      <span class="capture-when" id="captureWhen">—</span>
      <button class="capture-export" id="captureExport" disabled>EXPORT</button>
    </div>
    <div class="capture-list" id="captureList">
      <div class="capture-empty">Fills each hour from :59:00 to :59:59</div>
    </div>
  </div>

  <div class="flights-section" id="flights">
    <div class="flights-hdr">
      <span class="flights-title">Flights</span>
      <span class="flights-sub">one way &middot; economy + first</span>
      <button class="fl-keybtn" id="flKeyBtn" aria-label="SerpApi key">
        <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor"
             stroke-width="2" stroke-linecap="round"><circle cx="7" cy="12" r="3.5"></circle><path d="M10.5 12H21M17 12v3.5M20.5 12v2.5"></path></svg>
      </button>
    </div>

    <div class="fl-card">
      <div class="fl-field">
        <div class="fl-label">From</div>
        <input class="fl-input" id="flFrom" placeholder="airport code" autocomplete="off"
               autocapitalize="characters" spellcheck="false" inputmode="text">
        <div class="fl-chips" id="flFromChips"></div>
      </div>

      <div class="fl-field">
        <div class="fl-label">To</div>
        <input class="fl-input" id="flTo" placeholder="airport code" autocomplete="off"
               autocapitalize="characters" spellcheck="false" inputmode="text">
        <div class="fl-chips" id="flToChips"></div>
      </div>

      <div class="fl-field">
        <div class="fl-label">Depart</div>
        <div class="fl-date-wrap">
          <div class="fl-date-display" id="flDateDisplay">&mdash;</div>
          <input type="date" id="flDate">
        </div>
      </div>

      <button class="fl-go" id="flGo">Search</button>
      <div class="fl-msg" id="flMsg"></div>

      <div class="fl-key" id="flKeyBox">
        <div class="fl-label">SerpApi key</div>
        <div class="fl-key-row">
          <input type="password" id="flKeyInput" placeholder="paste key" autocomplete="off"
                 autocapitalize="off" spellcheck="false">
          <button id="flKeySave">Save</button>
        </div>
        <div class="fl-key-note">Stored on this device only, never on the server. Sent as a
          header with each search so it stays out of request logs.</div>
      </div>

    </div>

    <div class="fl-results" id="flResults"></div>
  </div>

  <div class="hotels-section" id="hotels">
    <div class="flights-hdr">
      <span class="hotels-title">Hotels</span>
      <span class="flights-sub">same SerpApi key as flights</span>
    </div>

    <div class="fl-card">
      <div class="fl-field">
        <div class="fl-label">Where</div>
        <input class="fl-input ht-where" id="htWhere" placeholder="city, area or hotel"
               autocomplete="off" autocapitalize="words" spellcheck="false">
        <div class="fl-chips" id="htWhereChips"></div>
      </div>

      <div class="fl-row2">
        <div class="fl-field">
          <div class="fl-label">Check in</div>
          <div class="fl-date-wrap">
            <div class="fl-date-display" id="htInDisplay">&mdash;</div>
            <input type="date" id="htIn">
          </div>
        </div>
        <div class="fl-field">
          <div class="fl-label">Check out</div>
          <div class="fl-date-wrap">
            <div class="fl-date-display" id="htOutDisplay">&mdash;</div>
            <input type="date" id="htOut">
          </div>
        </div>
      </div>

      <div class="fl-field ht-guests">
        <div class="fl-label">Guests</div>
        <div class="fl-chips" id="htAdultChips"></div>
      </div>

      <button class="fl-go ht-go" id="htGo">Search</button>
      <div class="fl-msg" id="htMsg"></div>
    </div>

    <div class="fl-results" id="htResults"></div>
  </div>

  <div class="ht-sheet" id="htSheet" role="dialog" aria-modal="true" aria-label="Hotel details">
    <div class="ht-scrim" id="htScrim"></div>
    <div class="ht-panel">
      <div class="ht-grip"></div>
      <button class="ht-close" id="htClose" aria-label="Close">&times;</button>
      <div class="ht-body" id="htBody"></div>
    </div>
  </div>

  <div class="footer">
    <div class="footer-row">
      <span class="footer-brand">HINER.NYC</span>
      <span id="lastUpdated">—</span>
    </div>
    <div class="footer-note">No claims made to accuracy or timeliness of market data</div>
  </div>

</div>

<script>
(function () {
  "use strict";

  var state = {
    range: "1h",
    history: [],
    live: { coinbase: null, bitstamp: null },
    rangeStartAvg: null,
    connCoinbase: false,
    connBitstamp: false,
    high24: null,
    low24: null,
    lastMsgAt: 0,
    lastSnapshotAvg: null,
    brti: null,
    brtiSeenAt: 0, // local arrival time of the newest BRTI print
    trend: null,     // { dir, minutes } — how long the price has run this way
    portfolio: null, // last portfolio payload, for the stealth readout
  };

  var BIG_MOVE_THRESHOLD = 10; // dollars of blended-price change within one snapshot tick (~1s)
  var flashTimeout = null;
  var screenFlashReset = null;
  // iOS Safari tints its status bar / toolbar from the page's edge colors.
  // A full-viewport colored overlay gets sampled, and the tint stays stuck
  // after the flash unless the color is removed and Safari is nudged to
  // re-evaluate (toggling theme-color forces that).
  var themeMeta = document.querySelector('meta[name="theme-color"]');
  function resetChromeTint() {
    var screen = document.getElementById("screenFlash");
    screen.classList.remove("go");
    screen.style.background = "none";
    if (themeMeta) {
      themeMeta.setAttribute("content", "#000001");
      requestAnimationFrame(function () { themeMeta.setAttribute("content", "#000000"); });
    }
  }

  function triggerBigMoveFlash(direction) {
    // Full-screen camera flash at the moment of the move.
    // iOS Safari tints its status bar / toolbar by sampling the pixels the
    // page paints at the very top and bottom of the viewport, and it does
    // that sampling WHILE the flash is on screen — so clearing the color
    // afterwards came too late and the chrome stayed green. Paint the color
    // as a gradient that stays fully transparent in the top and bottom
    // strips: the middle ~86% still reads as a full-screen flash, but the
    // edges Safari samples never stop being the page's black background.
    var screen = document.getElementById("screenFlash");
    screen.style.background = direction === "up" ? "#00c800" : "#ff0000";
    screen.classList.remove("go");
    void screen.offsetWidth; // restart the animation even mid-run
    screen.classList.add("go");
    // clear the color as soon as the flash is done; the timeout is a
    // belt-and-braces path for when animationend never fires (e.g. the tab
    // was backgrounded mid-animation)
    clearTimeout(screenFlashReset);
    screenFlashReset = setTimeout(resetChromeTint, 700);

    // then the held background bar behind the price, as before
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

  document.getElementById("screenFlash").addEventListener("animationend", resetChromeTint);
  // if the tab was backgrounded mid-flash the animation never finishes;
  // clear any leftover color the moment it comes back
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) resetChromeTint();
  });

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
  // ---------- headline price ----------

  // Mirrors the server's staleness weighting. The decay itself is computed
  // server-side and arrives with each snapshot; a trade from an exchange
  // proves that feed is live again, so it resets to full weight here
  // without waiting for the next snapshot.
  function recomputeAverage() {
    var c = state.live.coinbase, b = state.live.bitstamp;
    var wc = c == null ? 0 : (state.wCoinbase != null ? state.wCoinbase : 1);
    var wb = b == null ? 0 : (state.wBitstamp != null ? state.wBitstamp : 1);
    var total = wc + wb;
    if (total > 0) return ((c || 0) * wc + (b || 0) * wb) / total;
    if (c != null) return c;
    if (b != null) return b;
    return null;
  }

  // BRTI — CF Benchmarks' Bitcoin Real Time Index — is the only price that
  // matters here: it is the index Kalshi settles these markets on. Coinbase
  // and Bitstamp still run behind the scenes (they drive the chart history,
  // the big-move flash and the trade feed) but they are no longer shown as
  // prices of their own.
  function headlinePrice() {
    var b = state.brti;
    // Only stand in the blend when BRTI isn't configured at all. With it
    // configured, the header says CF BENCHMARK BRTI — printing a
    // Coinbase/Bitstamp number under that label would be a lie, so show
    // nothing and let the light say DELAY or OFFLINE.
    if (!b || !b.enabled) return recomputeAverage();
    return b.value;
  }

  // How long since the last BRTI print actually reached this browser. Measured
  // against local arrival time rather than the server's own timestamp so a
  // clock difference between the Mac and the phone can't fake a delay — and so
  // a dead socket (no snapshots at all) reads as delayed too.
  var BRTI_DELAY_MS = 4000, BRTI_OFFLINE_MS = 15000;
  function brtiHealth() {
    var b = state.brti;
    if (!b || !b.enabled) {
      // fall back to the exchange feeds, which are what's driving the price
      if (state.connCoinbase && state.connBitstamp) return "live";
      return (state.connCoinbase || state.connBitstamp) ? "delay" : "down";
    }
    if (!b.connected || b.value == null) return "down";
    if (!state.brtiSeenAt) return "down";
    var age = Date.now() - state.brtiSeenAt;
    if (age > BRTI_OFFLINE_MS) return "down";
    if (age > BRTI_DELAY_MS) return "delay";
    return "live";
  }

  function refreshHeaderAndRows() {
    paintHud();
    var px = headlinePrice();
    var deltaEl = document.getElementById("priceDelta");

    // fmtUSD renders null as an em dash: when the benchmark has no price to
    // report, say so. Leaving the last number up while the light goes red
    // reads as a live quote, which is the one thing it isn't.
    document.getElementById("bigPrice").textContent = fmtUSD(px);

    if (state.rangeStartAvg && px != null) {
      var pct = ((px - state.rangeStartAvg) / state.rangeStartAvg) * 100;
      var cls = pct > 0 ? "up" : pct < 0 ? "down" : "flat";
      var arrow = pct > 0 ? "\\u25B2" : pct < 0 ? "\\u25BC" : "\\u2013";
      deltaEl.className = "price-delta " + cls;
      deltaEl.textContent = arrow + " " + fmtPct(pct) + " (" + state.range.toUpperCase() + ")";
    } else if (px == null) {
      deltaEl.className = "price-delta flat";
      deltaEl.textContent = "\\u2013 (" + state.range.toUpperCase() + ")";
    }
  }

  function updateStatus() {
    var dot = document.getElementById("statusDot");
    var word = document.getElementById("statusWord");
    var h = brtiHealth();
    dot.className = "status-dot " + h;
    if (h === "live") { word.className = "status-word"; word.textContent = "LIVE"; }
    else if (h === "delay") { word.className = "status-word delay"; word.textContent = "DELAY"; }
    else { word.className = "status-word down"; word.textContent = "OFFLINE"; }
  }
  // the dot has to be able to go yellow/red on its own — nothing arriving is
  // exactly the condition it reports, so it can't wait for a message to redraw
  setInterval(updateStatus, 1000);

  function tickLastUpdated() {
    if (!state.lastMsgAt) return;
    var secs = Math.round((Date.now() - state.lastMsgAt) / 1000);
    document.getElementById("lastUpdated").textContent = secs <= 1 ? "updated just now" : "updated " + secs + "s ago";
  }
  setInterval(tickLastUpdated, 1000);

  // ---------- countdown to top of hour ----------

  function countdownColor(remainingSec) {
    // The clock's yellow is its own, deliberately hotter than the --yellow
    // used elsewhere on the page (245,197,24 — that one is a marigold). This
    // is a neon: near-full red and green, almost no blue, so it reads
    // electric against black rather than amber.
    var WHITE = [255, 255, 255], YELLOW = [255, 245, 40], ORANGE = [249, 115, 22], RED = [239, 68, 68];
    function lerp(a, b, t) { return [0, 1, 2].map(function (i) { return Math.round(a[i] + (b[i] - a[i]) * t); }); }
    function rgb(c) { return "rgb(" + c[0] + "," + c[1] + "," + c[2] + ")"; }
    // 30:00 white -> neon, and the neon HOLDS all the way to 20:00. The slide
    // then runs 20:00 -> 10:00, through orange at the midpoint, arriving at
    // full red exactly at 10:00 — where the clock inverts to black-on-red.
    if (remainingSec >= 1800) return rgb(WHITE);
    if (remainingSec >= 1200) return rgb(YELLOW);
    if (remainingSec >= 900) return rgb(lerp(YELLOW, ORANGE, (1200 - remainingSec) / 300));
    if (remainingSec >= 600) return rgb(lerp(ORANGE, RED, (900 - remainingSec) / 300));
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
    // Under 10:00 the clock inverts — black numerals on a solid red field —
    // so the color is carried by the background and the text must not also be
    // painted red on top of it.
    var urgent = remaining <= 600;
    el.classList.toggle("urgent", urgent);
    el.style.color = urgent ? "" : countdownColor(remaining);
    el.classList.toggle("flashing", remaining <= 300);
  }
  setInterval(updateCountdown, 1000);
  updateCountdown();

  // ---------- the settlement minute ----------
  // Kalshi settles on "the simple average of the sixty seconds of BRTI before
  // {hour}". So from HH:59:00 to HH:59:59 the page changes shape: the live
  // BRTI print shrinks to half size and the running 60-second average takes
  // over as the headline number on a yellow field. At HH:59:59 the last sample
  // lands, the average freezes exactly where it is, holds for 30 seconds, and
  // then the page goes back to the live price.
  //
  // Every one of those sixty prints is also kept in a scrollable list below
  // the trade feed and stays there until the next hour replaces it.

  var HOLD_MS = 30000;
  var CAPTURE_KEY = "brtiCapture";

  var settle = {
    phase: "idle",   // idle | collecting | frozen
    hourKey: null,   // ISO hour this capture settles into
    hourLabel: null,
    samples: [],     // [{ t: epochMs, px: number }]
    sum: 0,
    lastSec: null,   // epoch-second of the most recent capture, so one per second
    frozenUntil: 0,
    frozenAvg: null,
  };

  var elMinuteAvg = document.getElementById("minuteAvg");
  var elMinuteAvgPrice = document.getElementById("minuteAvgPrice");
  var elMinuteAvgSub = document.getElementById("minuteAvgSub");
  var elBigPrice = document.getElementById("bigPrice");
  var elCaptureList = document.getElementById("captureList");
  var elCaptureWhen = document.getElementById("captureWhen");
  var elCaptureExport = document.getElementById("captureExport");

  function hhmmss(t) {
    return new Date(t).toLocaleTimeString([], { hour12: false });
  }

  function settleShow(on, locked) {
    elMinuteAvg.classList.toggle("on", on);
    elMinuteAvg.classList.toggle("locked", !!locked);
    elBigPrice.classList.toggle("half", on);
  }

  function settleAvg() {
    return settle.samples.length ? settle.sum / settle.samples.length : null;
  }

  function settleStart(now) {
    // the hour this minute settles INTO — :59 belongs to the next hour's close
    var close = new Date(now.getTime());
    close.setMinutes(0, 0, 0);
    close.setHours(close.getHours() + 1);
    settle.phase = "collecting";
    settle.hourKey = close.toISOString();
    settle.hourLabel = close.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    settle.samples = [];
    settle.sum = 0;
    settle.lastSec = null;
    settle.frozenAvg = null;
    renderCapture();
    settleShow(true, false);
  }

  function settleCapture(now, px) {
    if (px == null || !isFinite(px)) return;
    var sec = Math.floor(now.getTime() / 1000);
    if (settle.lastSec === sec) return; // exactly one print per second
    settle.lastSec = sec;
    settle.samples.push({ t: now.getTime(), px: px });
    settle.sum += px;
    appendCaptureRow(settle.samples.length - 1);
    saveCapture();
  }

  function settleFreeze() {
    settle.phase = "frozen";
    settle.frozenAvg = settleAvg();
    settle.frozenUntil = Date.now() + HOLD_MS;
    saveCapture();
  }

  function renderMinuteAvg() {
    var avg = settle.phase === "frozen" ? settle.frozenAvg : settleAvg();
    elMinuteAvgPrice.textContent = fmtUSD(avg);
    var n = settle.samples.length;
    var sub = n + "/60";
    if (settle.hourLabel) sub += " \\u00b7 settles " + settle.hourLabel;
    // CF publishes its own trailing-60s figure; show it alongside ours as a
    // cross-check rather than swapping the headline number mid-minute
    var b = state.brti;
    if (b && b.avg60 != null) sub += " \\u00b7 CF " + fmtUSDShort(b.avg60);
    elMinuteAvgSub.textContent = sub;
  }

  function updateSettleMinute() {
    var now = new Date();
    var min = now.getMinutes();
    var sec = now.getSeconds();

    if (min === 59 && settle.phase === "idle") settleStart(now);

    if (settle.phase === "collecting") {
      if (min === 59) {
        settleCapture(now, headlinePrice());
        renderMinuteAvg();
        if (sec === 59) { settleFreeze(); renderMinuteAvg(); settleShow(true, true); }
      } else {
        // the tab was backgrounded or the clock jumped straight past :59:59 —
        // close the window out rather than leaving it collecting forever
        settleFreeze();
        renderMinuteAvg();
        settleShow(true, true);
      }
    } else if (settle.phase === "frozen") {
      if (Date.now() >= settle.frozenUntil) {
        settle.phase = "idle";
        settleShow(false, false);
      }
    }
  }
  setInterval(updateSettleMinute, 250); // sub-second so :59:59 is never missed
  updateSettleMinute();

  // ---------- the captured 60 prints ----------

  function captureRowHTML(i) {
    var s = settle.samples[i];
    return '<div class="capture-row">' +
      '<span class="capture-idx">' + (i + 1) + "</span>" +
      '<span class="capture-sec">' + hhmmss(s.t) + "</span>" +
      '<span class="capture-px">' + fmtUSDShort(s.px) + "</span>" +
      "</div>";
  }

  function appendCaptureRow(i) {
    if (i === 0) elCaptureList.innerHTML = "";
    elCaptureList.insertAdjacentHTML("beforeend", captureRowHTML(i));
    elCaptureList.scrollTop = elCaptureList.scrollHeight;
    elCaptureWhen.textContent = settle.hourLabel ? "settles " + settle.hourLabel : "";
    elCaptureExport.disabled = false;
  }

  function renderCapture() {
    if (!settle.samples.length) {
      elCaptureList.innerHTML = '<div class="capture-empty">Fills each hour from :59:00 to :59:59</div>';
      elCaptureWhen.textContent = settle.hourLabel ? "settles " + settle.hourLabel : "\\u2014";
      elCaptureExport.disabled = true;
      return;
    }
    var html = "";
    for (var i = 0; i < settle.samples.length; i++) html += captureRowHTML(i);
    elCaptureList.innerHTML = html;
    elCaptureList.scrollTop = elCaptureList.scrollHeight;
    elCaptureWhen.textContent = settle.hourLabel ? "settles " + settle.hourLabel : "";
    elCaptureExport.disabled = false;
  }

  // Survive a reload: the list is meant to stay put for the whole hour, and a
  // refresh in the middle of one shouldn't throw the sixty prints away.
  function saveCapture() {
    try {
      localStorage.setItem(CAPTURE_KEY, JSON.stringify({
        hourKey: settle.hourKey,
        hourLabel: settle.hourLabel,
        samples: settle.samples,
      }));
    } catch (e) {}
  }

  function loadCapture() {
    var raw;
    try { raw = localStorage.getItem(CAPTURE_KEY); } catch (e) { return; }
    if (!raw) return;
    var saved;
    try { saved = JSON.parse(raw); } catch (e) { return; }
    if (!saved || !Array.isArray(saved.samples) || !saved.samples.length) return;
    // drop it once its hour is more than an hour behind us — it has been
    // superseded even if this tab was closed when that happened
    var closeMs = Date.parse(saved.hourKey);
    if (isFinite(closeMs) && Date.now() - closeMs > 3600000) {
      try { localStorage.removeItem(CAPTURE_KEY); } catch (e) {}
      return;
    }
    settle.hourKey = saved.hourKey;
    settle.hourLabel = saved.hourLabel;
    settle.samples = saved.samples;
    settle.sum = saved.samples.reduce(function (a, s) { return a + s.px; }, 0);
    renderCapture();
  }
  loadCapture();

  function captureCSV() {
    var rows = ["settle_hour,second,timestamp_iso,timestamp_local,brti_price,running_avg"];
    var sum = 0;
    for (var i = 0; i < settle.samples.length; i++) {
      var s = settle.samples[i];
      sum += s.px;
      rows.push([
        settle.hourKey || "",
        i + 1,
        new Date(s.t).toISOString(),
        hhmmss(s.t),
        s.px.toFixed(2),
        (sum / (i + 1)).toFixed(4),
      ].join(","));
    }
    // the last row's running_avg IS the 60-second settlement average, so the
    // file needs no separate summary line to carry it
    return rows.join("\\n") + "\\n";
  }

  elCaptureExport.addEventListener("click", function () {
    if (!settle.samples.length) return;
    var blob = new Blob([captureCSV()], { type: "text/csv;charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "brti-60s-" + (settle.hourKey || "capture").replace(/[:.]/g, "-") + ".csv";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
  });

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

  var RANGE_MS = { "1h": 3600000, "3h": 3 * 3600000, "24h": 24 * 3600000 };

  function drawChart() {
    var rect = canvas.getBoundingClientRect();
    var w = rect.width, h = rect.height;
    ctx.clearRect(0, 0, w, h);
    if (!state.history.length) return;

    // The X axis is a FIXED window ending at now — not the span of whatever
    // data happens to be in hand. Normalizing to the data's own span meant
    // that while the buffer was still filling (after any restart, when the
    // server holds less than an hour), every new point on the right stretched
    // the span and shoved the whole existing line leftward, since nothing was
    // old enough to age off yet. Measured: the median gap between plotted
    // points shrank 1.051px -> 0.948px over three minutes, so the left end
    // kept compressing while the right kept growing. With the window pinned,
    // a point's position depends only on its timestamp: old data scrolls off
    // the left at exactly the rate new data arrives on the right, and a
    // partly-filled buffer simply starts partway across instead of being
    // stretched to fit.
    var spanMs = RANGE_MS[state.range] || RANGE_MS["1h"];
    var data = state.history.filter(function (p) { return p.t >= Date.now() - spanMs; });
    if (!data.length) return;
    // tolerate a little clock skew between this browser and the server rather
    // than clipping the newest point off the right edge
    var tLast = Math.max(Date.now(), data[data.length - 1].t);
    var tFirst = tLast - spanMs;

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
    // Position points by TIME, not by array index. The points are not
    // evenly spaced in time — server history arrives as ~12s buckets while
    // live points land every second — so index spacing made the newest
    // stretch of data occupy a growing share of the width and visibly
    // squeeze the older data toward the left edge.
    var x = function (i) { return leftPad + ((data[i].t - tFirst) / spanMs) * plotW; };
    var y = function (v) { return chartH - ((v - minP) / (maxP - minP)) * chartH; };

    // volume bars
    ctx.fillStyle = "rgba(255,255,255,0.10)";
    // width from the data's own cadence — with a fixed window, dividing the
    // full plot by the point count would fatten the bars whenever the buffer
    // is only partly filled
    var dataSpan = n > 1 ? data[n - 1].t - data[0].t : spanMs;
    var barW = Math.max((dataSpan / Math.max(n - 1, 1) / spanMs) * plotW - 1, 1);
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
    document.getElementById("axisStart").textContent = axisFmt(tFirst);
    document.getElementById("axisEnd").textContent = axisFmt(tLast);
  }

  window.addEventListener("resize", function () { resizeCanvas(); drawChart(); });
  // the window ends at now, so it has to keep scrolling even when nothing is
  // arriving — otherwise a quiet feed freezes the axis and the line drifts
  // out of step with the clock
  setInterval(drawChart, 1000);

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
    if (avg == null || !isFinite(avg)) return; // benchmark has no price right now
    var now = Date.now();
    // Coalesce into one point per second, matching the server's own tick
    // resolution. This used to push a point per trade — hundreds a minute —
    // which both bloated the series and packed the right-hand side with far
    // more points than the historical buckets on the left.
    var sec = Math.floor(now / 1000) * 1000;
    var last = state.history[state.history.length - 1];
    if (last && last.t >= sec) {
      last.avg = avg;
      last.high = Math.max(last.high, avg);
      last.low = Math.min(last.low, avg);
      last.vol = (last.vol || 0) + (vol || 0);
    } else {
      state.history.push({ t: sec, avg: avg, high: avg, low: avg, vol: vol || 0 });
    }
    var cutoff = now - 60 * 60 * 1000;
    while (state.history.length && state.history[0].t < cutoff) state.history.shift();
    if (state.history.length && state.rangeStartAvg == null) state.rangeStartAvg = state.history[0].avg;
    drawChart();
  }

  // ---------- trend widget ----------
  // The probability itself is computed server-side (trend.model) — one
  // source of truth shared with /api/trend and the Python lab. This just
  // renders it.

  function renderTrend(trend) {
    var body = document.getElementById("outlookBody");
    if (!trend || trend.insufficient || !trend.model) {
      var have = trend ? trend.sampleSeconds || 0 : 0;
      body.innerHTML = '<div class="trend-warmup">Gathering data for a projection\\u2026 (' + have + 's of the first 60s collected)</div>';
      renderKalshi(trend && trend.kalshi, null);
      return;
    }

    var m = trend.model;
    var probPct = Math.round(m.probAbove * 100); // always "P(above)" — matches the gauge and the Kalshi card
    var direction = probPct >= 50 ? "up" : "down";
    var headlinePct = direction === "up" ? probPct : 100 - probPct; // P(the stated direction)
    var earlyRead = trend.sampleMinutes < 10;

    var targetLabel = new Date(m.settleTs).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });

    var edge = Math.abs(m.probAbove - 0.5);
    var confidence = edge > 0.25 ? "Strong" : edge > 0.1 ? "Leaning" : "Toss-up";

    var headline =
      '<div class="trend-headline">' +
      headlinePct + "% chance BTC is <b class=\\"" + direction + "\\">" + (direction === "up" ? "above" : "below") + "</b> " +
      "$" + Math.round(m.threshold).toLocaleString("en-US") +
      " by <b>" + targetLabel + "</b></div>";

    var pointerPct = Math.min(Math.max(probPct, 2), 98);
    var gauge =
      '<div class="gauge"><div class="gauge-pointer" style="left:' + pointerPct + '%"></div></div>' +
      '<div class="gauge-labels"><span>Below</span><span>' + confidence + "</span><span>Above</span></div>";

    var chips = '<div class="stat-chips">';
    if (trend.rsi != null) {
      var rsiCls = trend.rsi > 70 ? "red" : trend.rsi < 30 ? "green" : "yellow";
      chips += '<span class="chip ' + rsiCls + '">RSI ' + Math.round(trend.rsi) + "</span>";
    }
    if (trend.streakCount > 1 && trend.streakDirection !== "flat") {
      var streakCls = trend.streakDirection === "up" ? "green" : "red";
      chips += '<span class="chip ' + streakCls + '">' + (trend.streakDirection === "up" ? "Up " : "Down ") + trend.streakCount + " min</span>";
    }
    if (m.contested) chips += '<span class="chip yellow">Mixed trend</span>';
    chips += '<span class="chip">' + Math.round(trend.sampleMinutes) + "m data</span>";
    if (earlyRead) chips += '<span class="chip yellow">Warming up</span>';
    chips += "</div>";

    body.innerHTML = headline + gauge + chips;
    lastModelPct = probPct;
    renderKalshi(trend.kalshi, probPct);
  }

  // remembered so the 1/sec Kalshi refresh can redraw the comparison card
  // without waiting for the next (10s) model broadcast
  var lastModelPct = null;

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

  // ---------- my kalshi portfolio ----------

  function renderPortfolio(p) {
    state.portfolio = p;      // the stealth readout reads it from here
    paintHud();
    var card = document.getElementById("portfolioCard");
    // No credentials on the server means the feature is off, and staying
    // hidden is right. An ERROR is different: the positions are meant to be
    // here and are not, and vanishing without a word is how this reads as
    // "the tracker broke" rather than "Kalshi did not answer".
    if (!p || !p.enabled) {
      card.style.display = "none";
      return;
    }
    if (p.error) {
      card.style.display = "";
      card.innerHTML = '<div class="kalshi-hdr"><span class="kalshi-title">Open Positions</span></div>' +
        '<div class="port-empty err">Kalshi: ' + esc(p.error) + "</div>";
      return;
    }
    if (p.positions == null) {
      card.style.display = "none";
      return;
    }
    var html = '<div class="kalshi-hdr"><span class="kalshi-title">Open Positions</span></div>';
    if (!p.positions.length) {
      html += '<div class="port-empty">No open contracts</div>';
    } else {
      html += p.positions.map(function (r) {
        // A decided contract does not have a quote — it has a payout. Showing
        // the closed market's bid here is what made a winner read as a wipeout.
        var decided = r.won === true || r.won === false;
        var per;
        if (decided) {
          per = (r.pending ? "resolves " : "settled ") + (r.won ? "$1.00" : "$0.00");
        } else {
          per = r.perContract != null ? Math.round(r.perContract * 100) + "\\u00a2" : "no quote";
        }
        if (r.avgCost != null && (decided || r.perContract != null)) per += " \\u00b7 paid " + Math.round(r.avgCost * 100) + "\\u00a2";
        var val = r.value != null ? "$" + r.value.toFixed(2) : "\\u2013";
        // never print a raw null/NaN count into the badge
        var qty = (typeof r.count === "number" && isFinite(r.count)) ? " \\u00d7" + r.count : "";
        var pnlHtml = "";
        if (r.pnl != null && isFinite(r.pnl)) {
          var pc = r.pnl > 0.004 ? "up" : r.pnl < -0.004 ? "down" : "flat";
          var sign = r.pnl > 0 ? "+" : r.pnl < 0 ? "\\u2212" : "";
          pnlHtml = '<span class="port-pnl ' + pc + '">' + sign + "$" + Math.abs(r.pnl).toFixed(2) + "</span>";
        }
        // Once a market closes there is nothing to trade, so the buttons give
        // way to the outcome.
        var sellBtn;
        if (decided) {
          sellBtn = '<div class="port-actions"><span class="port-badge ' + (r.won ? "won" : "lost") + '">' +
            (r.won ? "WON" : "LOST") + "</span>" +
            (r.pending ? '<span class="port-prov">unofficial</span>' : "") + "</div>";
        } else if (r.pending || r.closed) {
          sellBtn = '<div class="port-actions"><span class="port-badge settling">SETTLING</span></div>';
        } else if (p.sellEnabled) {
          sellBtn = '<div class="port-actions">' +
              '<button class="port-buy" data-ticker="' + r.ticker + '">+10</button>' +
              '<button class="port-sell" data-ticker="' + r.ticker + '">SELL</button>' +
            "</div>";
        } else {
          sellBtn = "";
        }
        // compact strike ("$65,000+") when we know it, full label otherwise
        var desc = r.subtitle;
        if (r.strike != null && isFinite(r.strike) && r.strikeDir) {
          desc = "$" + Math.round(r.strike).toLocaleString("en-US") +
            (r.strikeDir === "above" ? "+" : "\\u2212");
        }
        var rowCls = "port-row" + (r.won === true ? " won" : r.won === false ? " lost" : "");
        return '<div class="' + rowCls + '">' +
          '<span class="port-side ' + r.side + '">' + r.side.toUpperCase() + qty + "</span>" +
          '<span class="port-desc">' + desc + "</span>" +
          '<span class="port-val">' + val + "<span>" + per + "</span></span>" +
          pnlHtml + sellBtn +
          "</div>";
      }).join("");
    }
    html += '<div class="port-sell-msg" id="sellMsg"></div>';
    card.innerHTML = html;
    announceWins(p.positions);
    card.style.display = "";
    if (sellMsg.text) {
      var el = document.getElementById("sellMsg");
      el.textContent = sellMsg.text;
      el.className = "port-sell-msg " + sellMsg.cls;
    }
    // This card is rebuilt every second by the live quote feed, which would
    // otherwise wipe an armed button back to its resting state after ~1s.
    // Re-apply the armed look so it stays lit for the full confirm window.
    if (armed.ticker) {
      var cls = armed.action === "buy" ? ".port-buy" : ".port-sell";
      var armedBtn = card.querySelector(cls + '[data-ticker="' + armed.ticker + '"]');
      if (armedBtn) {
        armedBtn.classList.add("confirm");
        armedBtn.textContent = "CONFIRM?";
      } else {
        disarm(); // the position is gone (sold or settled)
      }
    }
    if (p.sellEnabled) ensurePin();
  }

  // A win is the one thing on this page worth interrupting for, and the
  // positions card is rebuilt every second by the quote feed — an animation
  // living inside it would restart mid-flight. The banner sits outside.
  var celebrated = {};
  var winToastTimer = null;

  function announceWins(rows) {
    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];
      if (r.won !== true || celebrated[r.ticker]) continue;
      celebrated[r.ticker] = true;
      showWinToast(r);
    }
  }

  function showWinToast(r) {
    var el = document.getElementById("winToast");
    var payout = (typeof r.count === "number" && isFinite(r.count)) ? r.count : null;
    var profit = (r.value != null && r.costBasis != null) ? r.value - r.costBasis : null;
    var what = r.strike != null && r.strikeDir
      ? "$" + Math.round(r.strike).toLocaleString("en-US") + (r.strikeDir === "above" ? "+" : "\u2212")
      : r.subtitle;
    var sub = [];
    if (payout != null) sub.push(r.side.toUpperCase() + " \u00d7" + payout + " \u00d7 $1.00");
    if (r.settleAvg != null) sub.push("settled " + fmtUSDShort(r.settleAvg));
    if (r.pending) sub.push("unofficial");
    el.innerHTML =
      '<div class="wt-top">Position won \u00b7 ' + what + "</div>" +
      '<div class="wt-amt">' + (profit != null ? (profit >= 0 ? "+" : "\u2212") + "$" + Math.abs(profit).toFixed(2) : "$" + (r.value != null ? r.value.toFixed(2) : "\u2013")) + "</div>" +
      '<div class="wt-sub">' + sub.join(" \u00b7 ") + "</div>";
    el.classList.add("on");
    clearTimeout(winToastTimer);
    winToastTimer = setTimeout(function () { el.classList.remove("on"); }, 20000);
  }

  // ---------- selling ----------
  // Two taps to fire: the first arms the button, the second sends the
  // order immediately. The PIN is asked for once when the page loads and
  // kept in localStorage, so selling never blocks on a prompt — the whole
  // point is to get out fast. The PIN is never in the page source.

  var sellMsg = { text: "", cls: "" };
  var armed = { ticker: null, action: null, timer: null };
  var pinAsked = false;

  function ensurePin() {
    if (pinAsked || localStorage.getItem("sellPin")) return;
    pinAsked = true;
    var pin = window.prompt("Sell PIN (stored on this device)");
    if (pin) localStorage.setItem("sellPin", pin);
  }

  function setSellMsg(text, cls) {
    sellMsg = { text: text, cls: cls || "" };
    var el = document.getElementById("sellMsg");
    if (el) { el.textContent = text; el.className = "port-sell-msg " + sellMsg.cls; }
  }

  function disarm() {
    if (armed.timer) clearTimeout(armed.timer);
    armed = { ticker: null, action: null, timer: null };
    document.querySelectorAll(".port-sell").forEach(function (b) {
      b.classList.remove("confirm");
      b.textContent = "SELL";
    });
    document.querySelectorAll(".port-buy").forEach(function (b) {
      b.classList.remove("confirm");
      b.textContent = "+10";
    });
  }

  document.getElementById("portfolioCard").addEventListener("click", function (ev) {
    var btn = ev.target.closest(".port-sell, .port-buy");
    if (!btn) return;
    var action = btn.classList.contains("port-buy") ? "buy" : "sell";
    var ticker = btn.getAttribute("data-ticker");

    // arming is per (position, action) — arming a buy must not fire a sell
    if (armed.ticker !== ticker || armed.action !== action) {
      disarm();
      armed.ticker = ticker;
      armed.action = action;
      btn.classList.add("confirm");
      btn.textContent = "CONFIRM?";
      armed.timer = setTimeout(function () { disarm(); }, 5000);
      return;
    }

    // second tap — send it straight away, no prompt in the hot path
    disarm();
    var pin = localStorage.getItem("sellPin");
    if (!pin) {
      pinAsked = false;
      ensurePin();
      pin = localStorage.getItem("sellPin");
      if (!pin) { setSellMsg("No PIN set", "err"); return; }
    }
    setSellMsg(action === "buy" ? "Buying\\u2026" : "Selling\\u2026", "");
    document.querySelectorAll(".port-sell, .port-buy").forEach(function (b) { b.disabled = true; });

    fetch(action === "buy" ? "/api/buy" : "/api/sell", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ticker: ticker, pin: pin }),
    })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (res.ok) {
          localStorage.setItem("sellPin", pin);
          // immediate-or-cancel can fill partially; say what actually filled
          var f = res.j.filled;
          var verb = res.j.action === "buy" ? "Bought" : "Sold";
          if (f != null && Number(f) < Number(res.j.count)) {
            setSellMsg("Filled " + f + " of " + res.j.count + " @ " + res.j.priceCents + "\\u00a2", "ok");
          } else {
            setSellMsg(verb + " " + res.j.count + " @ " + res.j.priceCents + "\\u00a2", "ok");
          }
        } else {
          // a rejected PIN is cleared so the next page load asks again
          if (res.j && /PIN/i.test(res.j.error || "")) {
            localStorage.removeItem("sellPin");
            pinAsked = false;
          }
          setSellMsg(res.j && res.j.error ? res.j.error : "Sell failed", "err");
        }
      })
      .catch(function (e) { setSellMsg("Sell failed: " + e.message, "err"); })
      .then(function () {
        document.querySelectorAll(".port-sell, .port-buy").forEach(function (b) { b.disabled = false; });
      });
  });

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

  // ---------- flight search ----------

  var AIRPORTS = [
    { label: "LGA", codes: "LGA" },
    { label: "NYC", codes: "LGA,JFK,EWR" },
    { label: "GSO", codes: "GSO" },
    { label: "RDU", codes: "RDU" },
    { label: "OC", codes: "GSO,RDU" }
  ];
  var WD_FULL = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  var WD_SHORT = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

  function esc(v) {
    return String(v == null ? "" : v).replace(/[&<>"\u0027]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "\u0027": "&#39;" }[c];
    });
  }
  function pad2(n) { return (n < 10 ? "0" : "") + n; }
  function toYMD(d) { return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate()); }

  // Before noon the useful default is today; after noon it is tomorrow,
  // because by then most of today has already gone.
  function defaultDepartDate() {
    var now = new Date();
    var d = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    if (now.getHours() >= 12) d.setDate(d.getDate() + 1);
    return toYMD(d);
  }

  // Today / Tomorrow / a weekday name while that is still unambiguous, and a
  // real date once it is not.
  function dayLabel(ymd) {
    var p = String(ymd).split("-");
    if (p.length !== 3) return ymd;
    var d = new Date(+p[0], +p[1] - 1, +p[2]);
    var today = new Date(); today.setHours(0, 0, 0, 0);
    var diff = Math.round((d - today) / 86400000);
    if (diff === 0) return "Today";
    if (diff === 1) return "Tomorrow";
    if (diff >= 2 && diff <= 5) return WD_FULL[d.getDay()];
    return WD_SHORT[d.getDay()] + " " + pad2(d.getMonth() + 1) + "/" + pad2(d.getDate());
  }

  var elFrom = document.getElementById("flFrom");
  var elTo = document.getElementById("flTo");
  var elDate = document.getElementById("flDate");
  var elDateDisplay = document.getElementById("flDateDisplay");
  var elGo = document.getElementById("flGo");
  var elMsg = document.getElementById("flMsg");
  var elResults = document.getElementById("flResults");
  var flightsEnabled = true;
  var hotelsEnabled = true;

  // One key serves both panels, so one probe settles both. Looked up by id
  // rather than through a variable because the hotel block is defined further
  // down and this runs during the first probe.
  function setSerpEnabled(enabled) {
    flightsEnabled = enabled;
    hotelsEnabled = enabled;
    var f = document.getElementById("flGo"), h = document.getElementById("htGo");
    if (f) f.disabled = !enabled;
    if (h) h.disabled = !enabled;
  }

  function normCodes(v) {
    // \\s, not \s: this whole script lives in a template literal, and an
    // unrecognised escape there collapses to the bare letter — /\s+/ would
    // reach the browser as /s+/ and start deleting the letter s.
    return String(v || "").toUpperCase().replace(/\\s+/g, "").replace(/,+/g, ",").replace(/^,|,$/g, "");
  }

  function renderChips(host, input) {
    host.innerHTML = AIRPORTS.map(function (a) {
      var on = normCodes(input.value) === a.codes ? " on" : "";
      return '<button class="fl-chip' + on + '" data-codes="' + a.codes + '">' + a.label + "</button>";
    }).join("");
  }
  function refreshChips() {
    renderChips(document.getElementById("flFromChips"), elFrom);
    renderChips(document.getElementById("flToChips"), elTo);
  }
  function wireChips(hostId, input) {
    document.getElementById(hostId).addEventListener("click", function (e) {
      var btn = e.target.closest(".fl-chip");
      if (!btn) return;
      input.value = btn.getAttribute("data-codes");
      refreshChips();
      saveFlightPrefs();
    });
  }
  wireChips("flFromChips", elFrom);
  wireChips("flToChips", elTo);
  elFrom.addEventListener("input", function () { refreshChips(); });
  elTo.addEventListener("input", function () { refreshChips(); });

  function syncDate() { elDateDisplay.textContent = dayLabel(elDate.value); }
  elDate.addEventListener("change", function () { syncDate(); saveFlightPrefs(); });

  function saveFlightPrefs() {
    try {
      localStorage.setItem("flightPrefs", JSON.stringify({ from: elFrom.value, to: elTo.value }));
    } catch (e) {}
  }
  function loadFlightPrefs() {
    try {
      var p = JSON.parse(localStorage.getItem("flightPrefs") || "{}");
      if (p.from) elFrom.value = p.from;
      if (p.to) elTo.value = p.to;
    } catch (e) {}
  }

  elDate.value = defaultDepartDate();
  elDate.min = toYMD(new Date());
  loadFlightPrefs();
  syncDate();
  refreshChips();

  function flagHTML(r) {
    var out = [];
    // just "Lowest" — the row is already badged Economy or First beside the
    // price, so naming the cabin again in the flag says it twice
    if (r.cheapest) out.push('<span class="fl-flag cheap">Lowest</span>');
    if (r.widebody) out.push('<span class="fl-flag wide">Widebody</span>');
    if (r.longLayover) out.push('<span class="fl-flag longlay">Long layover</span>');
    return out.length ? '<div class="fl-flags">' + out.join("") + "</div>" : "";
  }

  function itineraryHTML(r) {
    var viaTxt = r.nonstop
      ? '<div class="via direct">Nonstop</div>'
      : '<div class="via">via ' + esc(r.layovers.map(function (l) { return l.id || l.name; }).join(", ")) + "</div>";

    var meta = [];
    if (!r.nonstop && r.layovers.length) {
      meta.push(r.layovers.map(function (l) {
        return esc(l.durationLabel || "?") + " in " + esc(l.id || l.name) + (l.overnight ? " (overnight)" : "");
      }).join(" &middot; "));
    }
    if (r.aircraft.length) {
      meta.push(r.aircraft.map(function (a) { return "<b>" + esc(a) + "</b>"; }).join(" &middot; "));
    }

    return '<div class="fl-item' + (r.cabin === "first" ? " first-cabin" : "") + '" id="' + r.domId + '">' +
      '<div class="fl-top">' +
        (r.logo ? '<img class="fl-logo" src="' + esc(r.logo) + '" alt="" loading="lazy">' : '<div class="fl-logo"></div>') +
        '<div class="fl-carrier">' +
          '<div class="nm">' + esc(r.airlines.join(" / ")) + "</div>" +
          '<div class="fn">' + esc(r.flightNumbers.join(" \u00b7 ")) + "</div>" +
        "</div>" +
        '<div class="fl-price">' +
          '<div class="amt">' + (r.price != null ? "$" + r.price.toLocaleString("en-US") : "\u2013") + "</div>" +
          '<div class="cab">' + (r.cabin === "first" ? "First" : "Economy") + "</div>" +
        "</div>" +
      "</div>" +
      '<div class="fl-times">' +
        '<div class="fl-end"><div class="t">' + esc(r.depTime) + '</div><div class="a">' + esc(r.depAirport) + "</div></div>" +
        '<div class="fl-path">' + viaTxt + '<div class="bar"></div><div class="dur">' + esc(r.totalDurationLabel || "") + "</div></div>" +
        '<div class="fl-end"><div class="t">' + esc(r.arrTime) + (r.dayOffset ? "<sup>+1</sup>" : "") +
          '</div><div class="a">' + esc(r.arrAirport) + "</div></div>" +
      "</div>" +
      (meta.length ? '<div class="fl-meta">' + meta.join(" &middot; ") + "</div>" : "") +
      flagHTML(r) +
      "</div>";
  }

  // ---- the key, held per device ----
  // Kept in localStorage rather than on the server: this box runs under
  // launchd, which never sees a shell export, and a key sitting in one
  // browser is not on a public-facing machine at all.
  function flightKey() {
    try { return localStorage.getItem("serpapiKey") || ""; } catch (e) { return ""; }
  }
  function flightHeaders() {
    var k = flightKey();
    return k ? { "X-Serpapi-Key": k } : {};
  }
  // The key lives behind the key button in the section header rather than as
  // a standing line of link text — it is a once-a-year action and does not
  // deserve to be the loudest thing in the panel.
  function showKeyBox(needed) {
    var btn = document.getElementById("flKeyBtn");
    if (needed) document.getElementById("flKeyBox").classList.add("on");
    else document.getElementById("flKeyBox").classList.remove("on");
    btn.classList.toggle("needed", needed);
    btn.classList.toggle("set", !needed && !!flightKey());
  }
  document.getElementById("flKeyBtn").addEventListener("click", function () {
    var box = document.getElementById("flKeyBox");
    if (box.classList.toggle("on")) document.getElementById("flKeyInput").focus();
  });
  document.getElementById("flKeySave").addEventListener("click", function () {
    var v = document.getElementById("flKeyInput").value.trim();
    if (!v) return;
    try { localStorage.setItem("serpapiKey", v); } catch (e) {}
    document.getElementById("flKeyInput").value = "";
    setSerpEnabled(true);
    showKeyBox(false);
    setFlightMsg("Key saved on this device");
    setHotelMsg("");
    probeFlights();
  });

  // Two headline fares above the results, one per cabin, each a shortcut to
  // the itinerary it names. Below them the cabins are listed separately —
  // mixing them in one price-sorted column buried every first-class option
  // under the entire economy list.
  function tileHTML(label, r, cabinCls) {
    if (!r) {
      return '<button class="fl-tile ' + cabinCls + '" disabled>' +
        '<div class="lbl">' + label + '</div><div class="amt">\u2013</div>' +
        '<div class="sub">none found</div></button>';
    }
    var sub = [r.airlines.join(" / "), r.nonstop ? "nonstop" : r.stops + " stop" + (r.stops > 1 ? "s" : ""), r.depTime]
      .filter(Boolean).join(" \u00b7 ");
    return '<button class="fl-tile ' + cabinCls + '" data-target="' + r.domId + '">' +
      '<div class="lbl">' + label + '</div>' +
      '<div class="amt">$' + r.price.toLocaleString("en-US") + '</div>' +
      '<div class="sub">' + esc(sub) + '</div></button>';
  }

  var sortBy = "price"; // "price" or "arrival"
  var results = [];

  function renderResults(list) {
    var isNew = list !== results;
    if (isNew) {
      results = list.slice();
      sortBy = "price"; // reset to price when new results come in
    }
    results.sort(compareResults);
    for (var i = 0; i < results.length; i++) results[i].domId = "fl-it-" + i;
    var econ = results.filter(function (r) { return r.cabin !== "first"; });
    var first = results.filter(function (r) { return r.cabin === "first"; });
    var cheapE = econ.filter(function (r) { return r.cheapest; })[0] || econ[0];
    var cheapF = first.filter(function (r) { return r.cheapest; })[0] || first[0];

    var html = '<div class="fl-sorts">' +
      '<button data-sort="price" class="fl-sort' + (sortBy === "price" ? " active" : "") + '">Price</button>' +
      '<button data-sort="arrival" class="fl-sort' + (sortBy === "arrival" ? " active" : "") + '">Earliest arrival</button>' +
      '</div>' +
      '<div class="fl-tiles">' +
      tileHTML("Lowest coach", cheapE, "") +
      tileHTML("Lowest first", cheapF, "first-cabin") +
      "</div>";
    if (econ.length) html += '<div class="fl-group">Economy</div>' + econ.map(itineraryHTML).join("");
    if (first.length) html += '<div class="fl-group first-cabin">First</div>' + first.map(itineraryHTML).join("");
    elResults.innerHTML = html;
  }

  function compareResults(a, b) {
    if (sortBy === "price") {
      return a.price - b.price;
    } else if (sortBy === "arrival") {
      var aMin = parseInt(a.arrTime.split(":")[0]);
      var aMin2 = parseInt(a.arrTime.split(":")[1]);
      var bMin = parseInt(b.arrTime.split(":")[0]);
      var bMin2 = parseInt(b.arrTime.split(":")[1]);
      var aTime = aMin * 60 + aMin2 + (a.dayOffset ? 1440 : 0);
      var bTime = bMin * 60 + bMin2 + (b.dayOffset ? 1440 : 0);
      return aTime - bTime;
    }
    return 0;
  }

  elResults.addEventListener("click", function (e) {
    var sortBtn = e.target.closest("[data-sort]");
    if (sortBtn) {
      sortBy = sortBtn.getAttribute("data-sort");
      renderResults(results);
      return;
    }
    var tile = e.target.closest(".fl-tile[data-target]");
    if (!tile) return;
    var row = document.getElementById(tile.getAttribute("data-target"));
    if (!row) return;
    row.scrollIntoView({ behavior: "smooth", block: "center" });
    row.classList.remove("flash");
    void row.offsetWidth; // restart the animation on a repeat tap
    row.classList.add("flash");
  });

  function setFlightMsg(text, isErr) {
    elMsg.textContent = text || "";
    elMsg.className = "fl-msg" + (isErr ? " err" : "");
  }

  function searchFlights() {
    if (!flightsEnabled) return;
    var from = normCodes(elFrom.value), to = normCodes(elTo.value);
    if (!from || !to) { setFlightMsg("Pick a departure and an arrival", true); return; }
    elGo.disabled = true;
    setFlightMsg("Searching economy and first\u2026");
    elResults.innerHTML = "";
    fetch("/api/flights?from=" + encodeURIComponent(from) + "&to=" + encodeURIComponent(to) +
          "&date=" + encodeURIComponent(elDate.value), { headers: flightHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d.enabled === false) { flightsEnabled = false; showKeyBox(true); setFlightMsg("Add a SerpApi key to search", true); return; }
        if (d.error && (!d.itineraries || !d.itineraries.length)) { setFlightMsg(d.error, true); return; }
        var list = d.itineraries || [];
        if (!list.length) { setFlightMsg("No flights found for that route and date", true); return; }
        renderResults(list);
        setFlightMsg(list.length + " itineraries \u00b7 " + from + " \u2192 " + to + " \u00b7 " + dayLabel(d.date) +
          (d.partialError ? " \u00b7 one cabin unavailable" : ""));
      })
      .catch(function (e) { setFlightMsg("Search failed: " + e.message, true); })
      .then(function () { elGo.disabled = false; });
  }
  elGo.addEventListener("click", searchFlights);

  // is a key available from either side? cheap probe, no search burned
  function probeFlights() {
    fetch("/api/flights", { headers: flightHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        var needsKey = !!(d && d.enabled === false);
        setSerpEnabled(!needsKey);
        showKeyBox(needsKey);
        if (needsKey) {
          setFlightMsg("Add a SerpApi key to search", true);
          setHotelMsg("Add a SerpApi key above to search", true);
        } else {
          if (!elResults.children.length) setFlightMsg("");
          if (!elHtResults.children.length) setHotelMsg("");
        }
      })
      .catch(function () {});
  }

  // ---------- hotels ----------
  // Shares the flights key, the date wording and the card styling; only the
  // accent colour and the result shape differ.

  var HOTEL_SPOTS = [
    { label: "New York", q: "New York, NY" },
    { label: "Greensboro", q: "Greensboro, NC" },
    { label: "Raleigh", q: "Raleigh, NC" }
  ];
  var HOTEL_PARTY = [1, 2, 3, 4];

  var elHtWhere = document.getElementById("htWhere");
  var elHtIn = document.getElementById("htIn");
  var elHtOut = document.getElementById("htOut");
  var elHtInDisp = document.getElementById("htInDisplay");
  var elHtOutDisp = document.getElementById("htOutDisplay");
  var elHtGo = document.getElementById("htGo");
  var elHtMsg = document.getElementById("htMsg");
  var elHtResults = document.getElementById("htResults");
  var htAdults = 2;

  function setHotelMsg(text, isErr) {
    elHtMsg.textContent = text || "";
    elHtMsg.className = "fl-msg" + (isErr ? " err" : "");
  }

  function addDays(ymd, n) {
    var p = String(ymd).split("-");
    var d = new Date(+p[0], +p[1] - 1, +p[2]);
    d.setDate(d.getDate() + n);
    return toYMD(d);
  }
  function nightsBetween(a, b) {
    var pa = String(a).split("-"), pb = String(b).split("-");
    if (pa.length !== 3 || pb.length !== 3) return 0;
    var da = new Date(+pa[0], +pa[1] - 1, +pa[2]), db = new Date(+pb[0], +pb[1] - 1, +pb[2]);
    return Math.round((db - da) / 86400000);
  }

  function refreshHotelChips() {
    document.getElementById("htWhereChips").innerHTML = HOTEL_SPOTS.map(function (s) {
      var on = elHtWhere.value.trim().toLowerCase() === s.q.toLowerCase() ? " on" : "";
      return '<button class="fl-chip' + on + '" data-q="' + esc(s.q) + '">' + esc(s.label) + "</button>";
    }).join("");
    document.getElementById("htAdultChips").innerHTML = HOTEL_PARTY.map(function (n) {
      return '<button class="fl-chip' + (n === htAdults ? " on" : "") + '" data-adults="' + n + '">' +
        n + (n === 1 ? " guest" : " guests") + "</button>";
    }).join("");
  }
  document.getElementById("htWhereChips").addEventListener("click", function (e) {
    var btn = e.target.closest(".fl-chip");
    if (!btn) return;
    elHtWhere.value = btn.getAttribute("data-q");
    refreshHotelChips();
    saveHotelPrefs();
  });
  document.getElementById("htAdultChips").addEventListener("click", function (e) {
    var btn = e.target.closest(".fl-chip");
    if (!btn) return;
    htAdults = parseInt(btn.getAttribute("data-adults"), 10) || 2;
    refreshHotelChips();
    saveHotelPrefs();
  });
  elHtWhere.addEventListener("input", function () { refreshHotelChips(); });

  // Check-out has to stay after check-in, so moving check-in drags it along
  // rather than leaving an impossible stay on screen.
  function syncHotelDates(fromCheckIn) {
    if (fromCheckIn && nightsBetween(elHtIn.value, elHtOut.value) < 1) {
      elHtOut.value = addDays(elHtIn.value, 1);
    }
    elHtOut.min = addDays(elHtIn.value, 1);
    elHtInDisp.textContent = dayLabel(elHtIn.value);
    elHtOutDisp.textContent = dayLabel(elHtOut.value);
  }
  elHtIn.addEventListener("change", function () { syncHotelDates(true); saveHotelPrefs(); });
  elHtOut.addEventListener("change", function () { syncHotelDates(false); saveHotelPrefs(); });

  function saveHotelPrefs() {
    try {
      localStorage.setItem("hotelPrefs", JSON.stringify({ q: elHtWhere.value, adults: htAdults }));
    } catch (e) {}
  }
  function loadHotelPrefs() {
    try {
      var p = JSON.parse(localStorage.getItem("hotelPrefs") || "{}");
      if (p.q) elHtWhere.value = p.q;
      if (p.adults) htAdults = p.adults;
    } catch (e) {}
  }

  elHtIn.value = defaultDepartDate();
  elHtOut.value = addDays(elHtIn.value, 1);
  elHtIn.min = toYMD(new Date());
  loadHotelPrefs();
  syncHotelDates(false);
  refreshHotelChips();

  function money(n) { return "$" + Number(n).toLocaleString("en-US"); }

  // Drawn rather than the 🐾 emoji: an emoji glyph carries its own colour and
  // cannot be tinted, and it renders differently on every platform.
  var PAW_SVG =
    '<svg class="paw-svg" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">' +
    '<ellipse cx="5.6" cy="11.2" rx="2.5" ry="3.1"/>' +
    '<ellipse cx="10" cy="7.4" rx="2.6" ry="3.4"/>' +
    '<ellipse cx="15.2" cy="7.4" rx="2.6" ry="3.4"/>' +
    '<ellipse cx="19.5" cy="11.2" rx="2.5" ry="3.1"/>' +
    '<path d="M12.5 12.6c3.1 0 5.9 2.6 5.9 5.1 0 1.9-1.6 2.9-3.4 2.9-1.1 0-1.8-.4-2.5-.4s-1.4.4-2.5.4c-1.8 0-3.4-1-3.4-2.9 0-2.5 2.8-5.1 5.9-5.1z"/>' +
    "</svg>";

  function hotelFlagHTML(p) {
    var out = [];
    if (p.cheapest) out.push('<span class="fl-flag cheap">Lowest</span>');
    if (p.topRated) out.push('<span class="fl-flag rated">Top rated</span>');
    if (p.deal) out.push('<span class="fl-flag deal">' + esc(p.deal) + "</span>");
    // the amenities worth seeing without opening the listing
    (p.badges || []).forEach(function (b) {
      out.push('<span class="fl-flag ' + esc(b.cls) + '" title="' + esc(b.title) +
        '" aria-label="' + esc(b.title) + '">' +
        (b.icon === "paw" ? PAW_SVG : esc(b.label)) + "</span>");
    });
    return out.length ? '<div class="fl-flags">' + out.join("") + "</div>" : "";
  }

  function propertyHTML(p) {
    var cls = [];
    if (p.stars) cls.push('<span class="ht-stars">' + new Array(p.stars + 1).join("★") + "</span>");
    if (p.kind === "rental") cls.push("Vacation rental");
    if (p.locationRating != null) cls.push("Location " + p.locationRating.toFixed(1));

    var rate = p.rating != null
      ? '<div class="ht-rate"><span class="sc">' + p.rating.toFixed(1) + "</span>" +
        (p.reviews != null ? '<span class="rv">' + p.reviews.toLocaleString("en-US") + " reviews</span>" : "") +
        "</div>"
      : "";

    var price = p.perNight != null
      ? '<div class="amt">' + money(p.perNight) + '</div><div class="per">/night</div>' +
        (p.total != null ? '<div class="tot">' + money(p.total) + " total</div>" : "")
      : '<div class="amt">–</div><div class="per">no rate</div>';

    return '<div class="ht-item' + (p.token ? " tappable" : "") + '" id="' + p.domId + '"' +
        (p.token ? ' data-token="' + esc(p.token) + '" role="button" tabindex="0"' : "") + ">" +
      '<div class="ht-top">' +
        (p.thumb
          ? '<img class="ht-thumb" src="' + esc(p.thumb) + '" alt="" loading="lazy">'
          : '<div class="ht-thumb ph">⌂</div>') +
        '<div class="ht-id">' +
          '<div class="nm">' + esc(p.name) + "</div>" +
          (cls.length ? '<div class="cls">' + cls.join(" &middot; ") + "</div>" : "") +
          rate +
        "</div>" +
        '<div class="ht-price">' + price + "</div>" +
      "</div>" +
      (p.amenities.length ? '<div class="ht-amen">' + esc(p.amenities.slice(0, 4).join(" · ")) + "</div>" : "") +
      hotelFlagHTML(p) +
      "</div>";
  }

  function hotelTileHTML(label, p, extraCls) {
    if (!p) {
      return '<button class="fl-tile ht-tile ' + extraCls + '" disabled>' +
        '<div class="lbl">' + label + '</div><div class="amt">–</div>' +
        '<div class="sub">none found</div></button>';
    }
    var sub = [p.name, p.rating != null ? p.rating.toFixed(1) + "★" : null]
      .filter(Boolean).join(" · ");
    return '<button class="fl-tile ht-tile ' + extraCls + '" data-target="' + p.domId + '">' +
      '<div class="lbl">' + label + '</div>' +
      '<div class="amt">' + (p.perNight != null ? money(p.perNight) : "–") + "</div>" +
      '<div class="sub">' + esc(sub) + "</div></button>";
  }

  var HT_SORTS = [
    { id: "deals", label: "Deals" },
    { id: "price", label: "Price" },
    { id: "rating", label: "Rating" }
  ];
  var htSortBy = "deals"; // deals lead by default
  var htResults = [];

  function byPrice(a, b) {
    return (a.perNight == null ? 1e9 : a.perNight) - (b.perNight == null ? 1e9 : b.perNight);
  }

  function compareHotels(a, b) {
    if (htSortBy === "rating") {
      // unrated properties sort last rather than to the top as 0
      return (b.rating == null ? -1 : b.rating) - (a.rating == null ? -1 : a.rating) || byPrice(a, b);
    }
    if (htSortBy === "deals") {
      // anything Google marks as under its usual price leads, steepest
      // discount first; a deal that does not say how much still beats a
      // full-price room. Everything else falls back to price.
      var ad = a.deal ? 1 : 0, bd = b.deal ? 1 : 0;
      if (ad !== bd) return bd - ad;
      if (ad === 1) {
        var ap = a.dealPct == null ? -1 : a.dealPct, bp = b.dealPct == null ? -1 : b.dealPct;
        if (ap !== bp) return bp - ap;
      }
      return byPrice(a, b);
    }
    return byPrice(a, b);
  }

  function renderHotels(list) {
    if (list !== htResults) {
      htResults = list.slice();
      htSortBy = "deals";
    }
    htResults.sort(compareHotels);
    for (var i = 0; i < htResults.length; i++) htResults[i].domId = "ht-it-" + i;

    var cheapest = htResults.filter(function (p) { return p.cheapest; })[0] || htResults[0];
    var best = htResults.filter(function (p) { return p.topRated; })[0] || null;

    var html = '<div class="fl-sorts">' +
      HT_SORTS.map(function (s) {
        return '<button data-sort="' + s.id + '" class="fl-sort' +
          (htSortBy === s.id ? " active" : "") + '">' + s.label + "</button>";
      }).join("") +
      "</div>" +
      '<div class="fl-tiles">' +
      hotelTileHTML("Lowest nightly", cheapest, "") +
      hotelTileHTML("Top rated", best, "best") +
      "</div>";
    html += htResults.map(propertyHTML).join("");
    elHtResults.innerHTML = html;
  }

  elHtResults.addEventListener("click", function (e) {
    var sortBtn = e.target.closest("[data-sort]");
    if (sortBtn) {
      htSortBy = sortBtn.getAttribute("data-sort");
      renderHotels(htResults);
      return;
    }
    var tile = e.target.closest(".fl-tile[data-target]");
    if (tile) {
      var row = document.getElementById(tile.getAttribute("data-target"));
      if (!row) return;
      row.scrollIntoView({ behavior: "smooth", block: "center" });
      row.classList.remove("flash");
      void row.offsetWidth;
      row.classList.add("flash");
      return;
    }
    var card = e.target.closest(".ht-item[data-token]");
    if (card) openHotel(card.getAttribute("data-token"));
  });
  // the card carries role=button, so the keyboard has to reach it too
  elHtResults.addEventListener("keydown", function (e) {
    if (e.key !== "Enter" && e.key !== " ") return;
    var card = e.target.closest(".ht-item[data-token]");
    if (!card) return;
    e.preventDefault();
    openHotel(card.getAttribute("data-token"));
  });

  // ---- the detail sheet ----

  var elSheet = document.getElementById("htSheet");
  var elSheetBody = document.getElementById("htBody");
  var htDetailSeq = 0; // so a slow response cannot overwrite a newer one

  function closeHotel() {
    elSheet.classList.remove("on");
    document.body.style.overflow = "";
    htDetailSeq++;
  }
  document.getElementById("htClose").addEventListener("click", closeHotel);
  document.getElementById("htScrim").addEventListener("click", closeHotel);
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && elSheet.classList.contains("on")) closeHotel();
  });

  function openHotel(token) {
    var mine = ++htDetailSeq;
    elSheet.classList.add("on");
    document.body.style.overflow = "hidden";
    elSheetBody.scrollTop = 0;
    elSheetBody.innerHTML = '<div class="ht-load">Loading the full listing…</div>';
    fetch("/api/hotel?token=" + encodeURIComponent(token) +
          "&checkIn=" + encodeURIComponent(elHtIn.value) +
          "&checkOut=" + encodeURIComponent(elHtOut.value) +
          "&adults=" + htAdults, { headers: flightHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (mine !== htDetailSeq) return; // closed, or another card was tapped
        if (d.error || !d.details) {
          elSheetBody.innerHTML = '<div class="ht-load err">' + esc(d.error || "No details available") + "</div>";
          return;
        }
        elSheetBody.innerHTML = detailHTML(d.details);
      })
      .catch(function (err) {
        if (mine !== htDetailSeq) return;
        elSheetBody.innerHTML = '<div class="ht-load err">Could not load: ' + esc(err.message) + "</div>";
      });
  }

  function section(title, inner) {
    if (!inner) return "";
    return '<div class="ht-sec"><div class="ht-sec-h">' + title + "</div>" + inner + "</div>";
  }
  function kv(k, v) {
    if (!v) return "";
    return '<div class="ht-kv"><div class="k">' + k + '</div><div class="v">' + esc(v) + "</div></div>";
  }

  // OpenStreetMap's embed needs no key of its own — the SerpApi key is the
  // only credential this page asks anyone for.
  function mapHTML(d) {
    if (d.lat == null || d.lon == null) return "";
    var dx = 0.005, dy = 0.004;
    var bbox = [d.lon - dx, d.lat - dy, d.lon + dx, d.lat + dy].join(",");
    var src = "https://www.openstreetmap.org/export/embed.html?bbox=" + encodeURIComponent(bbox) +
      "&layer=mapnik&marker=" + encodeURIComponent(d.lat + "," + d.lon);
    return '<iframe class="ht-map" src="' + esc(src) + '" loading="lazy" title="Map"></iframe>' +
      '<div class="ht-coord">' + d.lat.toFixed(5) + ", " + d.lon.toFixed(5) + "</div>";
  }

  function detailHTML(d) {
    var out = "";

    if (d.images.length) {
      out += '<div class="ht-gal">' + d.images.map(function (src) {
        return '<img src="' + esc(src) + '" alt="" loading="lazy">';
      }).join("") + "</div>" +
      '<div class="ht-count">' + d.images.length + (d.images.length === 1 ? " photo" : " photos") + "</div>";
    }

    var sub = [];
    if (d.stars) sub.push('<span class="ht-stars">' + new Array(d.stars + 1).join("★") + "</span>");
    if (d.kind === "rental") sub.push("Vacation rental");
    if (d.eco) sub.push("Eco certified");
    out += '<div class="ht-d-name">' + esc(d.name || "This stay") + "</div>";
    if (sub.length) out += '<div class="ht-d-sub">' + sub.join(" &middot; ") + "</div>";
    if (d.rating != null) {
      out += '<div class="ht-d-rate"><span class="big">' + d.rating.toFixed(1) + "</span>" +
        '<span class="of">out of 5' + (d.reviews != null ? " · " + d.reviews.toLocaleString("en-US") + " reviews" : "") +
        "</span></div>";
    }

    // what the stay costs
    var priceRows = "";
    if (d.perNight != null) priceRows += kv("Per night", money(d.perNight));
    if (d.total != null) priceRows += kv("Stay total", money(d.total));
    if (d.typicalLow != null && d.typicalHigh != null) {
      priceRows += kv("Usual range", money(d.typicalLow) + " – " + money(d.typicalHigh));
    }
    out += section("Rates", priceRows);

    // the same room at each site that sells it
    if (d.prices.length) {
      out += section("Where it is listed", d.prices.map(function (p) {
        return '<div class="ht-src">' +
          (p.logo ? '<img src="' + esc(p.logo) + '" alt="" loading="lazy">' : "") +
          '<div class="nm">' + esc(p.source) + (p.official ? ' <span class="off">Official</span>' : "") + "</div>" +
          '<div class="pr">' + (p.perNight != null ? money(p.perNight) : "–") +
          (p.total != null ? "<small>" + money(p.total) + " total</small>" : "") +
          "</div></div>";
      }).join(""));
    }

    out += section("Where it is",
      kv("Address", d.address) + kv("Phone", d.phone) +
      kv("Check in", d.checkInTime) + kv("Check out", d.checkOutTime) +
      (d.locationRating != null ? kv("Location", d.locationRating.toFixed(1) + " out of 5") : ""));

    out += section("On the map", mapHTML(d));

    if (d.histogram.length) {
      out += section("How it is rated", d.histogram.map(function (r) {
        return '<div class="ht-bar"><div class="st">' + r.stars + "★</div>" +
          '<div class="track"><div class="fill" style="width:' + (r.share * 100).toFixed(1) + '%"></div></div>' +
          '<div class="ct">' + r.count.toLocaleString("en-US") + "</div></div>";
      }).join(""));
    }

    if (d.reviewTopics.length) {
      out += section("What reviewers mention", d.reviewTopics.map(function (t) {
        var tot = t.positive + t.negative + t.neutral;
        var pct = function (n) { return tot ? (n / tot * 100).toFixed(1) + "%" : "0%"; };
        return '<div class="ht-topic"><div class="th">' +
          '<span class="nm">' + esc(t.name) + "</span>" +
          '<span class="mn">' + t.mentioned.toLocaleString("en-US") + " mentions</span></div>" +
          (tot ? '<div class="ht-split">' +
            '<i class="pos" style="width:' + pct(t.positive) + '"></i>' +
            '<i class="neu" style="width:' + pct(t.neutral) + '"></i>' +
            '<i class="neg" style="width:' + pct(t.negative) + '"></i></div>' : "") +
          "</div>";
      }).join(""));
    }

    if (d.amenities.length) {
      out += section("Amenities", '<div class="ht-chiplist">' +
        d.amenities.map(function (a) { return "<span>" + esc(a) + "</span>"; }).join("") + "</div>");
    }
    if (d.excluded.length) {
      out += section("Not available", '<div class="ht-chiplist no">' +
        d.excluded.map(function (a) { return "<span>" + esc(a) + "</span>"; }).join("") + "</div>");
    }

    if (d.nearby.length) {
      out += section("Nearby", d.nearby.map(function (n) {
        return '<div class="ht-kv ht-near"><div class="k">' + esc(n.name) + "</div>" +
          '<div class="v">' + esc(n.transport.join(" · ") || "—") + "</div></div>";
      }).join(""));
    }

    if (d.description) {
      out += section("About", '<div class="ht-desc">' + esc(d.description) + "</div>");
    }
    return out;
  }

  function searchHotels() {
    if (!hotelsEnabled) return;
    var q = elHtWhere.value.trim().replace(/\\s+/g, " ");
    if (!q) { setHotelMsg("Say where you want to stay", true); return; }
    var nights = nightsBetween(elHtIn.value, elHtOut.value);
    if (nights < 1) { setHotelMsg("Check-out has to be after check-in", true); return; }
    elHtGo.disabled = true;
    setHotelMsg("Searching stays…");
    elHtResults.innerHTML = "";
    fetch("/api/hotels?q=" + encodeURIComponent(q) +
          "&checkIn=" + encodeURIComponent(elHtIn.value) +
          "&checkOut=" + encodeURIComponent(elHtOut.value) +
          "&adults=" + htAdults, { headers: flightHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d.enabled === false) {
          setSerpEnabled(false);
          showKeyBox(true);
          setHotelMsg("Add a SerpApi key above to search", true);
          return;
        }
        if (d.error && (!d.properties || !d.properties.length)) { setHotelMsg(d.error, true); return; }
        var list = d.properties || [];
        if (!list.length) { setHotelMsg("No stays found for those dates", true); return; }
        renderHotels(list);
        setHotelMsg(list.length + " stays · " + q + " · " + dayLabel(d.checkIn) + " → " +
          dayLabel(d.checkOut) + " · " + d.nights + (d.nights === 1 ? " night" : " nights"));
      })
      .catch(function (e) { setHotelMsg("Search failed: " + e.message, true); })
      .then(function () { elHtGo.disabled = false; });
  }
  elHtGo.addEventListener("click", searchHotels);

  probeFlights();

  document.getElementById("jumpFlights").addEventListener("click", function () {
    document.getElementById("flights").scrollIntoView({ behavior: "smooth", block: "start" });
  });

  // ---------- passcode ----------
  //
  // This is a privacy screen, not access control. It keeps the dashboard off
  // the glass when the phone is in someone else's hand; it does not protect
  // the data, which the same endpoints still serve to anyone who asks.

  var PIN = "1123";
  var elPin = document.getElementById("pinGate");
  var pinBuf = "";

  // a telephone pad, letters and all
  var PIN_KEYS = [
    ["1", ""], ["2", "ABC"], ["3", "DEF"],
    ["4", "GHI"], ["5", "JKL"], ["6", "MNO"],
    ["7", "PQRS"], ["8", "TUV"], ["9", "WXYZ"],
    ["", ""], ["0", "+"], ["del", ""]
  ];
  document.getElementById("pinPad").innerHTML = PIN_KEYS.map(function (k) {
    if (k[0] === "") return '<div class="pin-key blank"></div>';
    if (k[0] === "del") return '<button class="pin-key act" data-k="del"><span class="n">Delete</span></button>';
    return '<button class="pin-key" data-k="' + k[0] + '">' +
      '<span class="n">' + k[0] + '</span><span class="l">' + k[1] + "</span></button>";
  }).join("");

  function paintDots() {
    var dots = elPin.querySelectorAll(".pin-dots i");
    for (var i = 0; i < dots.length; i++) dots[i].classList.toggle("on", i < pinBuf.length);
  }
  function pinPush(k) {
    if (k === "del") { pinBuf = pinBuf.slice(0, -1); paintDots(); return; }
    if (pinBuf.length >= 4) return;
    pinBuf += k;
    paintDots();
    if (pinBuf.length < 4) return;
    if (pinBuf === PIN) {
      pinBuf = "";
      paintDots();
      try { sessionStorage.setItem("btcUnlocked", "1"); } catch (e) {}
      elPin.classList.add("off");
    } else {
      elPin.classList.add("bad");
      setTimeout(function () {
        elPin.classList.remove("bad");
        pinBuf = "";
        paintDots();
      }, 450);
    }
  }
  document.getElementById("pinPad").addEventListener("click", function (e) {
    var b = e.target.closest(".pin-key[data-k]");
    if (b) pinPush(b.getAttribute("data-k"));
  });
  document.addEventListener("keydown", function (e) {
    if (elPin.classList.contains("off")) return;
    if (e.key >= "0" && e.key <= "9") pinPush(e.key);
    else if (e.key === "Backspace") pinPush("del");
  });

  // ---------- stealth ----------
  //
  // A decoy front page over the whole screen, with the real numbers reduced to
  // the ticker strip a paper like this would carry along the bottom. Every
  // pixel is drawn here — no outside requests, so nothing to load, nothing to
  // fail, and no third party told what is on the screen.

  var elStealth = document.getElementById("stealth");
  var elHud = document.getElementById("stHud");
  var decoyPics = [];

  function setStealth(on) {
    elStealth.classList.toggle("on", on);
    document.body.style.overflow = on ? "hidden" : "";
    try { localStorage.setItem("btcStealth", on ? "1" : "0"); } catch (e) {}
    if (!on) return;
    renderDecoy(); // redrawn every time, so it is never the same page twice
    paintHud();
  }

  // Which thumbnails are actually on disk. Until this answers — or if nothing
  // is there — the decoy uses its drawn blocks, so it is never showing a
  // broken image.
  fetch("/api/decoy")
    .then(function (r) { return r.json(); })
    .then(function (d) {
      decoyPics = (d && d.pics) || [];
      if (decoyPics.length && elStealth.classList.contains("on")) renderDecoy();
    })
    .catch(function () {});
  document.getElementById("stealthBtn").addEventListener("click", function () { setStealth(true); });

  // Held, not tapped: a stray thumb on the strip should not blow the cover.
  var holdTimer = null;
  elHud.addEventListener("pointerdown", function () {
    holdTimer = setTimeout(function () { setStealth(false); }, 700);
  });
  ["pointerup", "pointercancel", "pointerleave"].forEach(function (ev) {
    elHud.addEventListener(ev, function () { clearTimeout(holdTimer); });
  });

  function signed(n) {
    return (n >= 0 ? "+" : "−") + "$" + Math.abs(n).toFixed(2);
  }

  function paintHud() {
    // refreshHeaderAndRows can fire before this block has run
    if (!elStealth || !elStealth.classList.contains("on")) return;
    var bits = [];

    var px = headlinePrice();
    bits.push('<span class="px">' +
      (px != null ? "$" + Math.round(px).toLocaleString("en-US") : "—") + "</span>");

    var t = state.trend || { dir: 0, minutes: 0 };
    var tc = t.dir > 0 ? "up" : t.dir < 0 ? "dn" : "fl";
    var tri = t.dir > 0 ? "▲" : t.dir < 0 ? "▼" : "▬";
    bits.push('<span class="tr ' + tc + '">' + tri + " " + (t.minutes || 0) + "m</span>");

    var pf = state.portfolio;
    if (pf && pf.enabled && pf.positions && pf.positions.length) {
      pf.positions.forEach(function (r) {
        var strike = r.strike != null ? (Math.round(r.strike / 100) / 10) + "k" : "?";
        var side = (r.side || "").charAt(0).toUpperCase();
        var g = r.pnl;
        var gc = g > 0 ? "up" : g < 0 ? "dn" : "";
        bits.push('<span class="sep">·</span><span class="pos">' + esc(strike) + " " + esc(side) +
          (isFinite(r.count) ? " ×" + r.count : "") + " " +
          '<span class="gain ' + gc + '">' + (g == null ? "—" : signed(g)) + "</span></span>");
      });
    }
    if (pf && pf.unrealizedPnl != null) {
      bits.push('<span class="tot ' + (pf.unrealizedPnl >= 0 ? "up" : "dn") + '">' +
        signed(pf.unrealizedPnl) + "</span>");
    }
    elHud.innerHTML = bits.join("");
  }

  // The decoy's own content. Invented masthead and evergreen copy: it has to
  // read as a newspaper at arm's length without passing itself off as a real
  // publication or inventing news about anyone.
  var DK_TINTS = [
    "linear-gradient(135deg,#3d4a5c,#1c232d)", "linear-gradient(135deg,#6b5a45,#2e261c)",
    "linear-gradient(135deg,#4a5c4e,#1e2a20)", "linear-gradient(135deg,#5c4a52,#2a1e23)",
    "linear-gradient(135deg,#45566b,#1c2531)", "linear-gradient(135deg,#6b6145,#2e291c)"
  ];
  var DK_RAIL = [
    ["BROAD 500", "5,412.08", 0.42], ["TECH 100", "18,940.22", 0.88],
    ["INDUSTRIALS", "39,118.40", -0.19], ["SMALL CAP", "2,104.55", -0.63],
    ["10-YR", "4.218%", 0.03], ["DOLLAR IDX", "104.61", -0.11],
    ["CRUDE", "$78.44", 1.27], ["GOLD", "$2,388.10", 0.35]
  ];
  var DK_LEADS = [
    { h: "Central Bankers Signal a Slower Path as Inflation Cools",
      d: "Policymakers left the door open to a longer pause, saying they want more evidence that price pressures have durably eased before moving again.",
      by: "By the Economics Staff" },
    { h: "Investors Look Past a Soft Quarter to the Year Ahead",
      d: "Positioning suggests the market has already written off the current period and is trading on what comes after it.",
      by: "By the Markets Desk" },
    { h: "The Long Wait for Capacity Reshapes Industrial Planning",
      d: "Firms that once ordered to demand are now ordering to the calendar, booking years ahead of what they can forecast.",
      by: "By the Industry Desk" },
    { h: "A Quiet Rotation Is Under Way Beneath the Headline Index",
      d: "Breadth has narrowed and then widened twice this quarter, a churn that the top-line number has almost entirely concealed.",
      by: "By the Markets Desk" }
  ];
  var DK_ITEMS = [
    { s: "Markets", h: "Treasury Yields Ease as Traders Weigh the Rate Path",
      p: "The long end retraced much of last week's move, with the curve steepening modestly through the afternoon session." },
    { s: "Markets", h: "Earnings Season Opens to a Cautious Reception",
      p: "Guidance, not results, is doing the work this quarter, and companies are being unusually careful with their language." },
    { s: "Markets", h: "Volatility Gauge Slips to a Three-Month Low",
      p: "Options desks report thinner demand for downside protection heading into the end of the month." },
    { s: "Business", h: "Freight Rates Soften on Improving Port Throughput",
      p: "Container volumes normalized after a congested spring, easing one of the more stubborn cost pressures for importers." },
    { s: "Business", h: "Commercial Landlords Test the Conversion Math",
      p: "Owners of older office stock are running the numbers on residential conversions, with mixed results across submarkets." },
    { s: "Business", h: "Regional Lenders Rebuild Deposit Bases",
      p: "Funding costs remain elevated, but the outflows that defined last year have largely stopped." },
    { s: "Technology", h: "Data-Center Buildout Runs Into the Power Grid",
      p: "Utilities are fielding interconnection requests years ahead of the capacity they can actually deliver." },
    { s: "Technology", h: "Chip Equipment Orders Point to a Measured Recovery",
      p: "Lead times have shortened, though customers are still spreading commitments across a longer horizon." },
    { s: "Economy", h: "Job Openings Drift Lower Without a Rise in Layoffs",
      p: "The labor market is cooling through reduced hiring rather than separations, an unusual pattern by past standards." },
    { s: "Economy", h: "Households Trade Down Without Cutting Back",
      p: "Spending volumes are holding up even as shoppers move toward private label and smaller pack sizes." },
    { s: "Economy", h: "Wage Growth Settles Into a Narrower Band",
      p: "Average hourly earnings have moved sideways for three months, a pace policymakers have called consistent with the target." },
    { s: "World", h: "Export Orders Stabilize Across the Euro Area",
      p: "New orders returned to expansion for the first time since the winter, led by capital goods." },
    { s: "World", h: "Shipping Insurers Reprice Long-Haul Routes",
      p: "Premiums on certain corridors have doubled, a cost that is beginning to appear in landed prices." },
    { s: "World", h: "Commodity Exporters Weigh Currency Interventions",
      p: "Several central banks have signaled discomfort with the pace, if not the direction, of recent moves." },
    { s: "Real Estate", h: "Mortgage Applications Rise on a Modest Rate Retreat",
      p: "Refinancing led the increase, though volumes remain far below the levels of three years ago." },
    { s: "Real Estate", h: "Industrial Rents Flatten After a Long Climb",
      p: "Warehouse completions finally caught up with demand in several of the largest logistics markets." },
    { s: "Personal Finance", h: "Cash Yields Slip but Still Beat the Alternatives",
      p: "Money-market funds continue to attract balances even as the highest quoted rates come down." },
    { s: "Personal Finance", h: "A Closer Look at Target-Date Glide Paths",
      p: "Two funds with the same year on the label can hold strikingly different mixes a decade out." },
    { s: "Opinion", h: "The Case for Patience in Monetary Policy",
      p: "Acting too quickly on a single quarter of data has a poor track record. The committee is right to wait." },
    { s: "Opinion", h: "What the Productivity Numbers Do and Do Not Show",
      p: "A strong print is welcome, but one quarter tells you very little about the underlying trend." },
    { s: "Opinion", h: "Regulation Written for the Last Crisis",
      p: "Rules built around a decade-old failure mode leave the newer one almost entirely unaddressed." },
    { s: "Opinion", h: "In Defense of Boring Infrastructure",
      p: "The projects that never make the front page are the ones that determine what everything else costs." }
  ];
  var DK_TABLE = [
    ["Broad 500", "5,412.08", "+22.61", "+0.42"], ["Tech 100", "18,940.22", "+165.30", "+0.88"],
    ["Industrials", "39,118.40", "-74.12", "-0.19"], ["Small Cap", "2,104.55", "-13.29", "-0.63"],
    ["World ex-US", "2,388.77", "+9.04", "+0.38"], ["Emerging", "1,061.42", "+4.11", "+0.39"],
    ["10-Yr Note", "4.218%", "+0.03", "+0.72"], ["2-Yr Note", "4.611%", "-0.02", "-0.43"],
    ["Dollar Index", "104.61", "-0.12", "-0.11"], ["Crude Oil", "78.44", "+0.98", "+1.27"],
    ["Natural Gas", "2.614", "-0.041", "-1.54"], ["Gold", "2,388.10", "+8.32", "+0.35"],
    ["Silver", "28.44", "+0.19", "+0.67"], ["Copper", "4.512", "-0.028", "-0.62"]
  ];

  function shuffled(a) {
    var out = a.slice();
    for (var i = out.length - 1; i > 0; i--) {
      var j = Math.floor(Math.random() * (i + 1));
      var t = out[i]; out[i] = out[j]; out[j] = t;
    }
    return out;
  }

  // Enough thumbnails to go round, reshuffled on each pass through the short
  // list so it does not read as a repeating cycle, and never repeating one
  // directly beneath itself.
  function picRun(n) {
    if (!decoyPics.length) return [];
    var out = [];
    while (out.length < n) {
      var batch = shuffled(decoyPics);
      if (out.length && batch.length > 1 && batch[0] === out[out.length - 1]) {
        var t = batch[0]; batch[0] = batch[1]; batch[1] = t;
      }
      out = out.concat(batch);
    }
    return out.slice(0, n);
  }

  function dkBox(pic, i) {
    return pic
      ? "background-image:url(/" + esc(pic) + ");background-size:cover;background-position:center"
      : "background:" + DK_TINTS[i % DK_TINTS.length];
  }

  function dkItemHTML(it, i, pic) {
    return '<div class="dk-item"><div class="tx"><h2>' + esc(it.h) + "</h2><p>" + esc(it.p) +
      '</p></div><div class="th" style="' + dkBox(pic, i) + '"></div></div>';
  }

  function renderDecoy() {
    var d = new Date();
    var days = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
    var mons = ["January", "February", "March", "April", "May", "June",
                "July", "August", "September", "October", "November", "December"];
    var dateLine = days[d.getDay()] + ", " + mons[d.getMonth()] + " " + d.getDate() + ", " + d.getFullYear();

    var html = '<div class="dk-rail">' + DK_RAIL.map(function (r) {
      return "<span><b>" + esc(r[0]) + "</b> " + esc(r[1]) +
        ' <span class="' + (r[2] >= 0 ? "up" : "dn") + '">' +
        (r[2] >= 0 ? "+" : "") + r[2].toFixed(2) + "%</span></span>";
    }).join("") + "</div>";

    html += '<div class="dk-mast">The Market Ledger</div>' +
      '<div class="dk-sub">' + esc(dateLine) + " &middot; Late Edition</div>";

    // Shuffled within each section and the sections themselves reordered, so
    // the page is laid out afresh every time rather than being the same front
    // page on Tuesday that it was on Monday. Grouping survives the shuffle —
    // a paper does not scatter its sections.
    var groups = {}, order = [];
    DK_ITEMS.forEach(function (it) {
      if (!groups[it.s]) { groups[it.s] = []; order.push(it.s); }
      groups[it.s].push(it);
    });
    var laid = [];
    shuffled(order).forEach(function (s) {
      laid.push({ sec: s });
      shuffled(groups[s]).forEach(function (it) { laid.push(it); });
    });

    // one for the lead, then one per story. Counted in stories rather than in
    // laid-out rows: section headings take no picture, and indexing past them
    // would let the same one land twice in a row.
    var pics = picRun(DK_ITEMS.length + 1);
    var lead = shuffled(DK_LEADS)[0];

    html += '<div class="dk-lead"><div class="dk-photo" style="' + dkBox(pics[0], 0) + '"></div>' +
      "<h1>" + esc(lead.h) + '</h1><div class="dk-dek">' + esc(lead.d) + "</div>" +
      '<div class="dk-by">' + esc(lead.by) + "</div></div>";

    var placed = 0;
    laid.forEach(function (row) {
      if (row.sec) { html += '<div class="dk-sec">' + esc(row.sec) + "</div>"; return; }
      html += dkItemHTML(row, placed, pics[placed + 1]);
      placed++;
      if (placed === 4) {
        html += '<div class="dk-quote">"The data have been kind to us lately, but we have been ' +
          'wrong-footed by a single quarter before."</div>';
      }
    });

    html += '<div class="dk-sec">Market Data</div><table class="dk-tbl"><tr>' +
      "<th>Instrument</th><th>Last</th><th>Chg</th><th>%</th></tr>" +
      DK_TABLE.map(function (r) {
        var up = r[3].charAt(0) !== "-";
        return "<tr><td>" + esc(r[0]) + '</td><td class="r">' + esc(r[1]) +
          '</td><td class="r ' + (up ? "up" : "dn") + '">' + esc(r[2]) +
          '</td><td class="r ' + (up ? "up" : "dn") + '">' + esc(r[3]) + "%</td></tr>";
      }).join("") + "</table>";

    html += '<div class="dk-end">&#9632;</div>';
    document.getElementById("decoy").innerHTML = html;
  }

  // A way back that needs no gesture at all: btc.hiner.nyc/?normal drops
  // stealth and forgets it. If the hold ever fails on a device, this is the
  // door out that cannot itself break.
  // plain indexOf rather than a regex: this script lives in a template
  // literal, where \b in a pattern would reach the browser as a bare "b"
  var wantsNormal = (location.search + location.hash).indexOf("normal") >= 0;
  if (wantsNormal) {
    try { localStorage.setItem("btcStealth", "0"); } catch (e) {}
    setStealth(false);
  } else {
    try {
      if (localStorage.getItem("btcStealth") === "1") setStealth(true);
    } catch (e) {}
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
        applyBrti(snap.brti);
        if (snap.trend) state.trend = snap.trend;
        state.high24 = snap.high24;
        state.low24 = snap.low24;
        state.lastMsgAt = Date.now();
        checkBigMove(headlinePrice());
        refreshHeaderAndRows();
        updateStatus();
      }).catch(function () {});
    }, 3000);
  }
  function stopPollingFallback() {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  }

  // A BRTI print is only "fresh" from the moment it lands here — the server's
  // own timestamp is on a different clock, and a snapshot repeating a value the
  // feed already went quiet on must not reset the age.
  function applyBrti(b) {
    if (!b) return;
    var prev = state.brti;
    if (b.value != null && (!prev || prev.ts !== b.ts || prev.value !== b.value)) {
      state.brtiSeenAt = Date.now();
    }
    state.brti = b;
  }

  function applySnapshot(snap) {
    state.live.coinbase = snap.coinbase;
    state.live.bitstamp = snap.bitstamp;
    state.connCoinbase = snap.coinbaseConnected;
    state.connBitstamp = snap.bitstampConnected;
    state.wCoinbase = snap.coinbaseWeight;
    state.wBitstamp = snap.bitstampWeight;
    if (snap.trend) state.trend = snap.trend;
    applyBrti(snap.brti);
    state.high24 = snap.high24;
    state.low24 = snap.low24;
    state.lastMsgAt = Date.now();
    var px = headlinePrice();
    checkBigMove(px);
    // BRTI only arrives on the snapshot, so the chart's right edge advances
    // here rather than on trades — it must keep moving through a quiet book
    if (px != null) appendLivePoint(px, 0);
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
        if (msg.brti) applyBrti(msg.brti);
        renderPortfolio(msg.portfolio);
        fetchHistory();
        armAutoRefresh();
        return;
      }
      if (msg.type === "snapshot") { applySnapshot(msg); return; }
      if (msg.type === "trade") {
        var exKey = msg.ex === "coinbase" ? "coinbase" : "bitstamp";
        state.live[exKey] = msg.price;
        // a fresh print means this feed is live — restore its full weight
        if (exKey === "coinbase") state.wCoinbase = 1; else state.wBitstamp = 1;
        state.lastMsgAt = Date.now();
        // an exchange print carries volume but must not move the BRTI line —
        // the point keeps the benchmark's own price and only accrues size
        appendLivePoint(headlinePrice(), msg.size);
        pushTradeRow(msg);
        return;
      }
      if (msg.type === "trend") { renderTrend(msg); return; }
      if (msg.type === "portfolio") { renderPortfolio(msg); return; }
      if (msg.type === "kalshi") {
        if (msg.brti) applyBrti(msg.brti);
        // 1/sec market refresh: redraw the comparison card against the last
        // known model probability, and reprice the portfolio
        renderKalshi(msg.kalshi, lastModelPct);
        renderPortfolio(msg.portfolio);
        return;
      }
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
