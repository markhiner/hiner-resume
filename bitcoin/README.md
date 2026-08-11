# BTC/USD Live Ticker

A live Bitcoin price ticker headlined by **CF Benchmarks' BRTI** — the
index Kalshi settles its hourly BTC markets on — built to run on your iMac
and be exposed at `btc.hiner.nyc` the same way `trains/` serves
`amtrak.hiner.nyc`.

Coinbase and Bitstamp still stream in the background (they drive the chart,
the big-move flash and the trade feed) but they are no longer shown as
prices of their own.

Everything — the API, the WebSocket feed, and the mobile UI — is served by
one Node process (`server.js`). No build step, no framework, no external
JS libraries loaded in the browser.

## What it does

- Opens outbound WebSocket connections to Coinbase's public `ticker`
  channel and Bitstamp's `live_trades_btcusd` channel and keeps them alive
  with automatic reconnect + backoff (and a watchdog that force-reconnects
  a feed that's gone silent for 20s).
- Subscribes to Kalshi's republished CF Benchmarks feed and shows BRTI big
  at the top. Without Kalshi credentials there is no BRTI, and the headline
  falls back to a Coinbase/Bitstamp blend so the page still works.
- Blends the two exchanges into a running average, recomputed on every
  single trade from either exchange. That average is what the chart, the
  forecast model and the big-move flash run on.
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

- Checks the model against reality: the server polls Kalshi's hourly
  "Bitcoin above/below" market (`KXBTCD`, public API, no key needed) and
  interpolates the strike ladder at the current blended price. The outlook
  card shows the model's probability, the market's implied probability,
  and the gap between them ("edge"). The model's displayed probability is
  clamped to 3–97% and flagged "Early read" until it has a 10-minute
  sample.

## API

- `GET /api/latest` — current snapshot (BRTI + its 60s average, both
  exchange prices, blended average, bid/ask, 24h high/low, connection
  status). BRTI rides this 1/sec heartbeat rather than the Kalshi market
  poll, which returns early when there is nothing to quote and would leave
  the headline price stranded.
- `GET /api/history?range=1h|3h|24h` — chart data for that window, already
  bucketed to ~300 points with per-bucket high/low/avg/volume.
- `GET /api/trend` — regression drift, volatility, RSI, streak, plus the
  current Kalshi state.
- `GET /api/kalshi` — the live hourly market: implied probability at the
  current price, nearest strike's quotes/volume, settle time.
- `WS /stream` — live push: `snapshot` (1/sec), `trade` (per trade, either
  exchange), `trend` (every 10s, includes Kalshi), plus a `bootstrap`
  message on connect.

## Kalshi portfolio card (optional)

With API credentials, the page adds a "My Kalshi" card showing your open
contracts marked to the live bid, plus cash and total. Create a key at
kalshi.com → Settings → API keys, then start the server with the same
two env vars the Python lab uses:

```bash
export KALSHI_API_KEY_ID="..."
export KALSHI_PRIVATE_KEY_PATH="$HOME/.kalshi/kalshi_private_key"
npm start
```

If you run it under launchd, add them to the plist instead:

```xml
<key>EnvironmentVariables</key>
<dict>
  <key>KALSHI_API_KEY_ID</key><string>...</string>
  <key>KALSHI_PRIVATE_KEY_PATH</key><string>/Users/YOUR_USER/.kalshi/kalshi_private_key</string>
</dict>
```

Without the env vars the card never renders and no authenticated calls
are made. The key is only used to *read* balance/positions — the server
never places orders.

**Privacy note:** btc.hiner.nyc is public. With this enabled, anyone who
finds the URL sees your balance and positions. If that matters, put the
hostname behind Cloudflare Access (free for personal use) or run the
card only on the LAN.

## BRTI — the price Kalshi actually settles on

Kalshi's hourly BTC markets do **not** settle on Coinbase or Bitstamp.
Their rules read: *"the simple average of the sixty seconds of CF
Benchmarks' BRTI before {hour}"*. So BRTI is the headline price, and the
page is built around that sixty-second window.

With Kalshi credentials set, the server subscribes to Kalshi's republished
CF Benchmarks feed using the same key as the portfolio. Note the documented
URL (`wss://external-api-ws.kalshi.com/cfbenchmarks_value`) 404s; the feed
is a channel on the main socket at `/trade-api/ws/v2`, and its `data` field
arrives as a JSON *string*, not an object.

`GET /api/brti` exposes the same state, including the last raw frame seen
so a stalled feed can be diagnosed without guessing.

### The health light

`CF BENCHMARK BRTI | ● LIVE` in the header is the page's honesty signal —
it is the only thing that says whether the big number can be trusted:

| light | word | meaning |
| --- | --- | --- |
| green | `LIVE` | a BRTI print landed in the last 4 seconds |
| yellow | `DELAY` | nothing for 4s — the price on screen is aging |
| red | `OFFLINE` | nothing for 15s, or the feed reports itself down |

Age is measured from when a print reached the *browser*, not from the
server's own timestamp, so a clock difference between the Mac and the phone
can't fake a delay — and a dead socket (no messages at all) correctly reads
as offline rather than staying green on the last value.

### The settlement minute

From `HH:59:00` the page changes shape:

- the live BRTI print shrinks to half size and moves to the top,
- beneath it, on a yellow field, the running 60-second average takes over as
  the headline number under the label **60 SECOND AVG**,
- at `HH:59:59` the last of the sixty samples lands, the average **freezes**
  exactly where it is, holds for 30 seconds, and the page returns to live.

The number shown is the mean of the sixty per-second BRTI prints — the same
window settlement uses. CF's own trailing-60s figure is printed small
alongside it as a cross-check rather than swapped in mid-minute, which
would make the headline jump.

### The capture list

Every one of those sixty prints is kept in a scrollable list under the
trade feed, numbered and timestamped, and stays there until the next hour
replaces it. It survives a page reload (localStorage) and is discarded once
its hour is more than an hour behind.

**EXPORT** downloads it as CSV:

```
settle_hour,second,timestamp_iso,timestamp_local,brti_price,running_avg
2026-08-11T11:00:00.000Z,1,2026-08-11T10:59:00.041Z,10:59:00,64000.00,64000.0000
...
2026-08-11T11:00:00.000Z,60,2026-08-11T10:59:59.038Z,10:59:59,64059.00,64029.5000
```

The last row's `running_avg` **is** the sixty-second settlement average, so
the file needs no separate summary line to carry it.

## Sell button (optional, off by default)

Each open position can show two buttons: **SELL**, which liquidates the
whole position, and **+10**, which buys 10 more contracts of the side you
already hold. Both trade at the current going price and place **real
orders against your real account**, so they are disabled unless you set a
PIN:

```bash
export SELL_PIN="some-passphrase"
```

How it is protected:

- **Off without the PIN.** No `SELL_PIN`, and `/api/sell` returns 403 and
  no buttons render.
- **The PIN is never in the page.** It lives only in the server's
  environment. The browser prompts for it, and only stores it locally
  after the server has accepted it once. Every order must carry it, and
  it is compared in constant time.
- **Two taps.** The first tap arms that button ("CONFIRM?"), the second
  sends the order. Arming expires by itself after 5 seconds, and is
  tracked per button, so arming +10 and then tapping SELL only arms the
  sell rather than firing anything.
- **Buys are capped by lot size.** A sell can use `reduce_only`, which
  makes it impossible to open a position by mistake. A buy is meant to
  increase one, so it cannot -- its protection is the fixed lot: since a
  contract can never cost more than $1, one tap commits at most $10
  (override with `BUY_COUNT`).
- **Limit, not market.** The order is priced to cross the spread so it
  fills right away, but unlike a market order it can never fill at an
  arbitrarily bad price if the book is thin. It is `immediate_or_cancel`,
  so nothing is left resting, and `reduce_only`, so it can only ever
  shrink a position and never open a new one.

Uses Kalshi's V2 order API (`external-api.kalshi.com`,
`POST /portfolio/events/orders`). The older `/portfolio/orders` endpoint
now returns `410 deprecated_v1_order_endpoint`. In V2 everything is
quoted on the YES leg — `ask` sells YES, `bid` buys YES — so closing a
NO position is placed as a YES buy.

A PIN is a speed bump, not real authentication — anyone who reaches the
URL can still see the button and guess at it. **If you enable selling,
put the hostname behind Cloudflare Access.** It is free for personal use
and is the only thing here that actually keeps strangers out.

The Python lab's `kalshi_edge.py --live` is a separate path with its own
credentials and guardrails; the two do not share state.

## Python lab

`lab/` holds the research/execution side — a lumibot strategy that trades
the trend signal (backtest + Alpaca paper), OpenBB helpers for historical
data, and a Kalshi edge monitor that paper-trades model-vs-market
disagreements and scores the model's calibration after each hour settles.
See `lab/README.md`.

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
