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
- Subscribes to Kalshi's republished CF Benchmarks feed and runs everything
  off BRTI: the headline price, the chart, the forecast model, the big-move
  flash and the strike the market check is interpolated at. Without Kalshi
  credentials there is no BRTI, and it all falls back to a Coinbase/Bitstamp
  blend so the page still works.
- Blends the two exchanges into a running average, recomputed on every
  single trade from either exchange. That average is the fallback benchmark,
  and Coinbase/Bitstamp trades still supply the live trade feed and the
  per-bar volume either way.
- Keeps a rolling in-memory history: 1 hour at 1-second resolution, and 24
  hours at 1-minute resolution (bucketed further for smooth chart
  rendering). History is flushed to `history.json` every 60s so a restart
  doesn't lose the chart.
- Pushes live updates to connected browsers over `/stream` (WebSocket);
  falls back to polling `/api/latest` every 3s if the socket drops.
- Computes a "Next Hour Outlook": a linear regression on the last ~20
  minutes of benchmark price gives a drift + volatility estimate, projected
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

### One benchmark, no quiet substitutions

`benchmarkPrice()` is the single source of truth everything downstream
reads. It returns BRTI when BRTI is live, and the exchange blend only when
BRTI is not configured or has never printed.

What it deliberately does **not** do is fall back mid-series. BRTI and the
blend differ by tens of dollars, so swapping one for the other when the feed
goes quiet would put a step in the history that reads as a real move — a
false crash flash, and a volatility estimate poisoned for the next twenty
minutes. Instead a print is held for up to 90 seconds, and past that the
server stops appending and leaves an honest gap. The header light is
already saying DELAY or OFFLINE while that happens.

That includes **startup**: with credentials set but no frame yet, it returns
nothing rather than opening the series on a blend tick. Measured against a
stubbed feed, opening on one blend tick and stepping to BRTI a second later
reported `driftPerMin 1212` and `volPerMin 20609` on a series actually
moving $24/min — the splice inflated volatility roughly 2900x, and the model
carried that for its whole 20-minute lookback. Waiting instead gives
`driftPerMin 6.7`, `volPerMin 7.2`.

For the same reason the page shows an em dash, not a Coinbase/Bitstamp
number, if BRTI is configured but silent — the header says CF BENCHMARK
BRTI, and printing something else under that label would be a lie. Once
BRTI has printed once, its last value is held (and flagged by the light), so
the dash only appears when there is genuinely nothing.

For the same reason `history.json` records which series it holds. A
BRTI-priced file will not load into a blend-mode run (or the reverse) — the
chart resets once rather than showing a cliff at the restart point. Expect
one empty chart the first time you start the server with credentials.

### The 1h window is backfilled

`secondTicks` is capped at a **count** (3600), not a duration. Across restarts
and downtime those entries end up spanning many hours, so only a fraction land
inside the hour the chart draws. Measured on a real history file: 3600 ticks
covering 20 hours, of which 958 were in the window — 16 minutes of data on a
60-minute axis, bunched into the right quarter.

The fixed-width axis didn't cause that, it revealed it. Beforehand the chart
normalised to whatever data was in hand, so 16 minutes was stretched across the
full width and silently relabelled as an hour.

`minuteBars` covers 24h and survives the same gaps, so the part of the window
the tick buffer doesn't reach is filled from it — the left of the chart degrades
to minute resolution instead of going blank, and a gap remains only where
nothing was ever recorded. Ticks already outside the window are also dropped at
load rather than occupying slots fresh ones need.

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
  exactly where it is and the label picks up a `· FINAL` marker, holds for
  30 seconds, and the page returns to live.

The number shown is the mean of the sixty per-second BRTI prints — the same
window settlement uses. CF's own trailing-60s figure is printed small
alongside it as a cross-check rather than swapped in mid-minute, which
would make the headline jump.

### The countdown

The clock to the top of the hour escalates on its own schedule:

| remaining | look |
| --- | --- |
| 30:00+ | white |
| 30:00 to 20:00 | neon yellow, held |
| 20:00 to 10:00 | slides yellow to orange to red |
| under 10:00 | inverts: black numerals on solid red |
| under 5:00 | same, flashing |

The neon is the countdown's own yellow, hotter than the `--yellow` used
elsewhere on the page, and it is held rather than lerped straight off the
turn — a lerp reaches amber within a minute, so the neon would only ever
have existed for the instant of the changeover.

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

## Settlement — a contract resolves, it doesn't trade

A binary contract does not have a closing price. At expiry it **resolves**:
$1 to the winning side, $0 to the losing one. A closed market's bid is 0 on
*both* sides, so marking a held position to the bid the way an open one is
marked reports a winner as a total wipeout — value $0, P&L equal to the
entire stake. That is exactly what a NO position on "$64,300 or above" showed
when the benchmark settled at $64,281: it had won, and the card said
`$0.00 / -$29.36`.

Positions are now priced by state, not by quote:

| market | priced at |
| --- | --- |
| open | the live bid on the held side, as before |
| closed, Kalshi has published `result` | $1.00 if the side won, $0.00 if it lost |
| closed, result not published yet | called from this server's own settlement window, flagged **unofficial** |
| closed, too little of that window recorded | no call, no invented price — shows `SETTLING` |

The provisional call uses `settlementAverage()`: the mean of the sixty
benchmark prints before the close, the same window Kalshi settles on, taken
from the ticks this server already records. It needs at least 30 of those 60
seconds before it will commit to an answer. Strike comparison follows
Kalshi's own convention — "or above" is inclusive, so an exact tie is a YES.

A won position gets a green field and a `WON` badge, a lost one steps back
with `LOST`, and trading buttons withdraw once a market closes since there is
nothing left to trade. A win also raises a full-width banner above the card
naming the profit and the settlement figure that decided it — it lives
outside the positions card because that card is rebuilt every second by the
quote feed, which would restart any animation inside it mid-flight.

Settled positions drop out of Kalshi's list the moment they pay out, which
would blink the result off screen at the moment it becomes interesting, so a
decided row is held for two minutes after it disappears.

### Only what's actually open

`/portfolio/positions` returns position *history*, not just what is open now,
so the request carries `settlement_status=unsettled`. Without it every market
ever traded comes back — and since the quantity field parses to a non-zero
number for those old rows rather than the `undefined` it used to yield, each
one rendered as a settled loss. Hours of them, stacked up on the card.

A second guard drops any row whose market closed longer ago than the two
minute hold, in case that parameter isn't honoured by a given API version.
Either mechanism alone is enough; both are tested, including that a market
which closed thirty seconds ago still shows its WON/LOST outcome.

The hold for a decided row is anchored to the **market's own close time**,
never to "now". `portfolioState.positions` already contains the rows the hold
added on the previous poll, so deriving the expiry from the current time made
every poll push it forward again — the row kept itself alive indefinitely and
last hour's settled position never cleared. Anchored to close time the expiry
is a fixed instant, re-setting it is a no-op, and it agrees with the
close-time filter above: everything decided is visible until close +
`RESOLVED_HOLD_MS`, then gone. Overridable with `KALSHI_RESOLVED_HOLD_MS`.

### Polling cadence

| what | how often | why |
| --- | --- | --- |
| quotes + resolution state (`/markets?tickers=`) | **400ms** | drives unrealized P&L; one batched request whatever the position count |
| market structure / strike ladder | 20s | strikes don't move |
| `/portfolio/positions` + balance | 15s | count and cost basis don't change while you hold |

Unrealized P&L rides the 400ms loop, not the 15s one. `/portfolio/positions`
only supplies quantity and cost basis; the mark comes from the quote batch,
which also carries `status` and `result` — so a settlement is recognised, and
a win announced, within a fraction of a second rather than waiting on the
portfolio poll.

400ms was measured, not guessed. Against a stubbed Kalshi at 300ms
round-trip, a 1s interval delivered 1.10 P&L updates/sec to the browser and
400ms delivered 2.60/sec with zero skipped ticks. Going lower is wasted
work: at 250ms the interval falls under the round-trip and the loop churns —
47 skipped ticks in 20 seconds and no more updates than 400ms.

All three are overridable: `KALSHI_QUOTE_MS`, `KALSHI_STRUCTURE_MS`,
`KALSHI_PORTFOLIO_MS`.

`setInterval` fires on a timer, not on completion, so the quote loop carries
an in-flight guard. Without it a request slower than the interval stacks
behind the next one and two can land out of order, an older quote
overwriting a newer one — P&L that jumps backwards or sits still. Measured
at 1500ms of latency the guard skipped 10 ticks in 20 seconds and the
delivered cadence degraded cleanly to the round-trip time instead of
thrashing. `GET /api/kalshi` reports what the loop is actually achieving
under `quote`: `intervalMs`, `lastRoundTripMs`, `ageMs`, `ticks`, `skipped`,
`errors` — if `skipped` is climbing on your machine, Kalshi is slower than
the interval and the real refresh rate is `lastRoundTripMs`.

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

## Flight search

A one-way flight search lives at the bottom of the page, below everything
BTC. The plane button at the right of the header scrolls straight to it.

The key can come from **either side**, and the device is the easier one.

### Per device (no server config)

Open the flight section, paste the key into the SerpApi field, Save. It lives
in that browser's `localStorage` and rides along as an `X-Serpapi-Key` header
on each search — a header rather than a query parameter so it stays out of
request logs. It is never written to the server, never appears in the page
source, and another phone hitting the same URL simply sees the prompt to add
its own.

The server still has to relay the call: SerpApi sends no CORS headers, so a
browser cannot reach it directly. The request path is device to your Mac to
SerpApi. Note the server's `Access-Control-Allow-Origin: *` is deliberately
*not* paired with `Access-Control-Allow-Headers`, so a custom header can only
be sent from the page itself — no other site can preflight its way into
proxying through your box.

### Or on the server

```bash
export SERPAPI_KEY="..."
npm start
```

**Under launchd, `export` in a shell will not reach it.** The agent has its
own environment, so the key belongs in the plist next to the Kalshi ones:

```xml
<key>EnvironmentVariables</key>
<dict>
  <key>KALSHI_API_KEY_ID</key><string>...</string>
  <key>KALSHI_PRIVATE_KEY_PATH</key><string>/Users/YOUR_USER/.kalshi/kalshi_private_key</string>
  <key>SERPAPI_KEY</key><string>...</string>
</dict>
```

```bash
launchctl kickstart -k gui/$(id -u)/nyc.hiner.btc-ticker
```

A device key wins over the environment when both are present. With neither,
the section renders but says so, the button is disabled, and no request is
ever made. To check what the running process sees — this endpoint tests the
key before validating anything else, so it costs no search:

```bash
curl -s localhost:3001/api/flights
# {"enabled":false,"needsKey":true}         -> no key on the server side
# {"enabled":true,"error":"need both ..."}  -> loaded
```

**Two searches per query.** SerpApi's `travel_class` takes a single value per
request, so every search runs twice — economy (`1`) and first (`4`) — and the
two result sets are merged into one list sorted by price. That puts the
cheapest fare of either cabin at the top rather than burying first class in a
separate tab, and it is why the "lowest in cabin" flags exist. If one cabin
errors and the other succeeds, the results still render and the message says
one cabin was unavailable. Two searches means two API credits, so results are
cached per route/date/cabin for `FLIGHT_CACHE_MS` (default 5 min).

**Airports.** Free-text input plus shortcuts: `LGA`, `NYC` (LGA + JFK + EWR),
`GSO`, `RDU`, `OC` (GSO + RDU). A shortcut fills the input with the actual
codes, so what will be searched is always visible and still editable.

**The date** defaults to today before noon and tomorrow after — by the
afternoon, most of today's departures have gone. It reads back relatively:
`Today`, `Tomorrow`, then a weekday name while that stays unambiguous, and
`Eee mm/dd` beyond it. The native picker sits invisibly over the styled label
so iOS opens its own date wheel while the page keeps the relative wording.

**Each result** shows price, airline logo, departure and arrival time with
airports, connection city, layover duration and aircraft, with four flags:

| flag | meaning |
| --- | --- |
| `Lowest Coach` / `Lowest First` | cheapest in that cabin; ties all get it |
| `Nonstop` | single leg |
| `Widebody` | any leg on a twin-aisle aircraft |
| `Long layover` | any connection over 90 minutes |

Widebody detection is an explicit pattern list rather than a clever regex,
because a loose one like `7[0-9]7` would sweep in the 737 and 757, which are
single-aisle. Departure and arrival times are parsed by hand from SerpApi's
`"YYYY-MM-DD HH:MM"` strings and never passed through `Date()`: they are
local times at each airport, and parsing them would reinterpret them in the
server's zone and shift the clock. A flight landing the next day is marked
`+1` rather than silently showing an earlier-looking arrival.

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
- The chart's X axis is a **fixed window ending at now**, not the span of
  whatever data is in hand. Normalizing to the data's own span meant that
  while the buffer was still filling — after any restart, when the server
  holds less than an hour — every new point on the right stretched the span
  and shoved the existing line leftward, because nothing was yet old enough
  to age off. Measured against the live page, the median gap between plotted
  points shrank 1.051px to 0.948px over three minutes while the right-hand
  third gained 161 points and the left gained 11. With the window pinned,
  that gap holds constant and the line extends rightward at exactly the rate
  the clock advances. The trade-off is visible and intended: a partly-filled
  buffer starts partway across the canvas instead of being stretched to fit,
  so expect blank space on the left for the first hour after a restart.
- `history.json` (gitignored) is local ticker state, not something to
  commit.
