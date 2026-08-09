# BTC/USD Live Ticker

A live Bitcoin price ticker that blends real-time trade streams from
**Coinbase** and **Bitstamp**, built to run on your iMac and be exposed at
`btc.hiner.nyc` the same way `trains/` serves `amtrak.hiner.nyc`.

Everything — the API, the WebSocket feed, and the mobile UI — is served by
one Node process (`server.js`). No build step, no framework, no external
JS libraries loaded in the browser.

## What it does

- Opens outbound WebSocket connections to Coinbase's public `ticker`
  channel and Bitstamp's `live_trades_btcusd` channel and keeps them alive
  with automatic reconnect + backoff (and a watchdog that force-reconnects
  a feed that's gone silent for 20s).
- Blends the two exchanges into a running average, recomputed on every
  single trade from either exchange — that's the number shown big at the
  top.
- Keeps a rolling in-memory history: 1 hour at 1-second resolution, and 24
  hours at 1-minute resolution (bucketed further for smooth chart
  rendering). History is flushed to `history.json` every 60s so a restart
  doesn't lose the chart.
- Pushes live updates to connected browsers over `/stream` (WebSocket);
  falls back to polling `/api/latest` every 3s if the socket drops.
- Computes a "Next Hour Outlook": a linear regression on the last ~20
  minutes of blended price gives a drift + volatility estimate, projected
  forward (random-walk style, drift·t ± vol·√t) to the top of the *next*
  hour in the viewer's own timezone. Turned into a probability via the
  normal CDF. Also reports a 14-period RSI and a streak counter ("4
  up-minutes in a row"). This is a fun heuristic, not a trading signal —
  the UI says so.

## Local setup

```bash
cd bitcoin
npm install
npm start
```

Visit `http://localhost:3001`.

## Deploying on the iMac

### 1. Keep it running

Use `launchd` so it restarts on crash and on login, same idea as any other
always-on service on the Mac. Create
`~/Library/LaunchAgents/nyc.hiner.btc-ticker.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>nyc.hiner.btc-ticker</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/node</string>
    <string>/Users/YOUR_USER/hiner-resume/bitcoin/server.js</string>
  </array>
  <key>WorkingDirectory</key><string>/Users/YOUR_USER/hiner-resume/bitcoin</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>/tmp/btc-ticker.log</string>
  <key>StandardErrorPath</key><string>/tmp/btc-ticker.err.log</string>
</dict>
</plist>
```

Then:

```bash
launchctl load ~/Library/LaunchAgents/nyc.hiner.btc-ticker.plist
```

(Swap in the real path to your `node` binary — `which node` — and your
actual username.)

### 2. Expose it

Same pattern as `trains/cloudflared-config.yml`: merge the `btc.hiner.nyc`
ingress rule into your existing Cloudflare Tunnel config (see
`cloudflared-config.yml` in this folder), then:

```bash
cloudflared tunnel route dns YOUR_TUNNEL_ID btc.hiner.nyc
```

Cloudflare terminates TLS for you, so the page loads over `https://` and
the in-page WebSocket upgrades to `wss://` automatically — no certs to
manage on the Mac itself.

### 3. Link it from the site (optional)

Nothing in the main site currently points at it; add a link from
`index.html` / `index3.html` if you want it reachable from the front page,
or just bookmark `btc.hiner.nyc` straight to the iPhone home screen — the
page is already tagged `apple-mobile-web-app-capable` for a clean
full-screen launch.

## API

- `GET /api/latest` — current snapshot (both exchange prices, blended
  average, bid/ask, 24h high/low, connection status).
- `GET /api/history?range=1h|3h|24h` — chart data for that window, already
  bucketed to ~300 points with per-bucket high/low/avg/volume.
- `GET /api/trend` — regression drift, volatility, RSI, streak.
- `WS /stream` — live push: `snapshot` (1/sec), `trade` (per trade, either
  exchange), `trend` (every 10s), plus a `bootstrap` message on connect.

## Notes

- Coinbase's `ticker` channel gives per-trade price, best bid/ask, and 24h
  high/low for free with no auth. Bitstamp's `live_trades_btcusd` gives
  per-trade price and size; it doesn't publish bid/ask on that channel, so
  the "Bid/Ask" figures shown are Coinbase's.
- Range High/Low on the chart is computed from whatever's actually in the
  displayed window, not a fixed 24h figure — so it rescales correctly even
  right after a restart when history is still short.
- `history.json` (gitignored) is local ticker state, not something to
  commit.
