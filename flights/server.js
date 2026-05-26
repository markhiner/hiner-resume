// flights/server.js
// Run: SERPAPI_KEY=your_key node server.js

const http = require('http');
const { URL } = require('url');

const PORT = 3001;
const SERPAPI_KEY = process.env.SERPAPI_KEY || '';

const NYC_AIRPORTS  = ['LGA', 'JFK', 'EWR'];
const HOME_AIRPORTS = ['RDU', 'GSO'];
const ECONOMY = 1;
const FIRST   = 4;

// ── allowed airlines ─────────────────────────────────────────
const ALLOWED_CODES = new Set(['DL', 'AA', 'UA', 'B6']);
const ALLOWED_NAMES = new Set([
  'delta', 'delta air lines', 'delta airlines',
  'american', 'american airlines',
  'united', 'united airlines',
  'jetblue', 'jetblue airways',
]);
function legAllowed(leg) {
  const name = (leg.airline || '').toLowerCase().trim();
  if (ALLOWED_NAMES.has(name)) return true;
  const fn = (leg.flight_number || '').trim();
  return ALLOWED_CODES.has(fn.slice(0, 2).toUpperCase());
}

// ── airport expansion ────────────────────────────────────────
function expand(code) {
  const u = (code || '').toUpperCase().trim();
  if (u === 'NYC')  return NYC_AIRPORTS;
  if (u === 'HOME') return HOME_AIRPORTS;
  return u ? [u] : [];
}

// ── SerpAPI ──────────────────────────────────────────────────
async function serpSearch(dep, arr, date, cabin) {
  const params = new URLSearchParams({
    engine: 'google_flights', departure_id: dep, arrival_id: arr,
    outbound_date: date, currency: 'USD', hl: 'en', gl: 'us',
    type: '2', adults: '1', travel_class: String(cabin), api_key: SERPAPI_KEY,
  });
  try {
    const res = await fetch('https://serpapi.com/search?' + params, {
      signal: AbortSignal.timeout(30000),
    });
    return await res.json();
  } catch (e) {
    console.error(`  SerpAPI error (${dep}→${arr} cabin=${cabin}):`, e.message);
    return {};
  }
}

// ── format helpers ───────────────────────────────────────────
function fmtTime(s) {
  if (!s) return '—';
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return s;
  let h = parseInt(m[1], 10);
  const min = m[2], ampm = h >= 12 ? 'pm' : 'am';
  if (h > 12) h -= 12;
  if (h === 0) h = 12;
  return h + ':' + min + ' ' + ampm;
}
function fmtDur(mins) {
  if (!mins) return '';
  const h = Math.floor(mins / 60), m = mins % 60;
  return h === 0 ? m + 'm' : m === 0 ? h + 'h' : h + 'h ' + m + 'm';
}
function fmtLayover(mins) {
  if (!mins) return '';
  const h = Math.floor(mins / 60), m = mins % 60;
  if (h === 0) return m + ' min';
  if (m === 0) return h + ' hr';
  return h + ' hr, ' + m + ' min';
}
const WIDEBODY_RE = /747|767|777|787|330|350|380/;
function isWidebody(ac) { return WIDEBODY_RE.test(ac); }

function shortAircraft(name) {
  if (!name) return '';
  var s = name;
  // Remove manufacturer prefix
  s = s.replace(/^(boeing|airbus|embraer|bombardier|mcdonnell\s*douglas|canadair|atr)\s*/i, '');
  // Remove "Passenger" and "Sharklets"
  s = s.replace(/\s*\bpassenger\b\s*/gi, ' ');
  s = s.replace(/\s*\bsharklets?\b\s*/gi, ' Transcon ');
  // 737MAX X → 737-X00  (e.g. MAX 8 → 737-800, MAX 10 → 737-1000)
  s = s.replace(/737\s*max\s*(\d+)/i, function(_, n) { return '737-' + parseInt(n, 10) + '00'; });
  // Contains 550 → CRJ 550
  if (/550/.test(s)) return 'CRJ 550';
  // Standalone 170 or 175 → E170 / E175
  s = s.replace(/\b(170|175)\b/, 'E$1');
  // RJ prefix → CRJ (skips existing CRJ via word-boundary — C is a word char so \bRJ won't fire inside CRJ)
  s = s.replace(/\bRJ(\d*)/g, 'CRJ$1');
  // Safety: collapse any accidental CCRJ
  s = s.replace(/CCRJ/g, 'CRJ');
  // Normalize CRJ: always "CRJ NNN" with a space and no dash
  s = s.replace(/CRJ[\s-]*(\d)/g, 'CRJ $1');
  return s.replace(/\s+/g, ' ').trim();
}

// ── parse flights ────────────────────────────────────────────
function parseFlights(data, cabin) {
  const results = [], seen = new Set();
  for (const section of ['best_flights', 'other_flights']) {
    for (const opt of (data[section] || [])) {
      const legs = opt.flights || [], price = opt.price;
      if (!legs.length || !price) continue;
      if (!legs.some(legAllowed)) continue;
      const fl = legs[0], ll = legs[legs.length - 1];
      const key = [fl.departure_airport?.id, fl.departure_airport?.time,
                   ll.arrival_airport?.id,   ll.arrival_airport?.time,
                   price, cabin].join('|');
      if (seen.has(key)) continue;
      seen.add(key);
      const layovers = opt.layovers || [];
      results.push({
        price, cabin,
        airline:  fl.airline      || '',
        logo:     fl.airline_logo || '',
        dep:      fl.departure_airport?.id   || '',
        depTime:  fmtTime(fl.departure_airport?.time),
        depRaw:   fl.departure_airport?.time || '',
        arr:      ll.arrival_airport?.id     || '',
        arrTime:  fmtTime(ll.arrival_airport?.time),
        arrRaw:   ll.arrival_airport?.time   || '',
        duration: fmtDur(opt.total_duration),
        nonstop:  legs.length === 1,
        stops: layovers.map(lv => ({ id: lv.id || '', dur: fmtLayover(lv.duration) })),
        aircraft: legs.map(l => shortAircraft(l.airplane || '')).filter(Boolean),
        widebodyTypes: [...new Set(legs.map(l => shortAircraft(l.airplane || '')).filter(isWidebody))],
        legs: legs.map((leg, i) => {
          const rawFn  = leg.flight_number || '';
          const rawAl  = leg.airline       || '';
          const isAA   = /^AA[\s-]?\d/i.test(rawFn) || /american/i.test(rawAl);
          const aaNum  = rawFn.replace(/^AA[\s-]*/i, '').replace(/\D/g, '');
          console.log(`    leg fn="${rawFn}" al="${rawAl}" isAA=${isAA} aaNum="${aaNum}"`);
          return {
            dep:       leg.departure_airport?.id || '?',
            depT:      fmtTime(leg.departure_airport?.time),
            arr:       leg.arrival_airport?.id  || '?',
            arrT:      fmtTime(leg.arrival_airport?.time),
            fn:        rawFn,
            ac:        shortAircraft(leg.airplane || ''),
            al:        rawAl,
            dur:       fmtDur(leg.duration),
            layover:   layovers[i] ? fmtLayover(layovers[i].duration) : null,
            layoverId: layovers[i]?.id || null,
            isAA,
            aaNum,
          };
        }),
      });
    }
  }
  return results;
}

// ── search orchestration ─────────────────────────────────────
async function doSearch(depCode, arrCode, date, cabinParam) {
  if (!SERPAPI_KEY) return { error: 'SERPAPI_KEY not configured on server.' };
  if (!depCode || !arrCode || !date) return { error: 'Missing required parameters.' };
  const deps = expand(depCode), arrs = expand(arrCode);
  const cabins = cabinParam === 'all' ? [ECONOMY, FIRST] : cabinParam === 'first' ? [FIRST] : [ECONOMY];
  const routes = [];
  for (const d of deps) for (const a of arrs) if (d !== a) routes.push([d, a]);
  if (!routes.length) return { error: 'No valid routes — departure and arrival cannot be the same.' };
  console.log(`\nSearch: ${depCode}→${arrCode} ${date} [${cabinParam}] — ${routes.length}×${cabins.length} calls`);
  const econFlights = [], firstFlights = [];
  for (const [d, a] of routes) {
    for (const cab of cabins) {
      process.stdout.write(`  ${d}→${a} [${cab === ECONOMY ? 'Econ' : 'First'}] ... `);
      const data = await serpSearch(d, a, date, cab);
      const r    = parseFlights(data, cab);
      console.log(r.length + ' results');
      (cab === ECONOMY ? econFlights : firstFlights).push(...r);
    }
  }
  econFlights.sort((a, b) => a.price - b.price);
  firstFlights.sort((a, b) => a.price - b.price);
  return { economy: econFlights, firstClass: firstFlights };
}

// ── HTML ─────────────────────────────────────────────────────

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>fly.hiner.nyc</title>
<style>
:root {
  --bg:       #0a0a0a;
  --surf:     #111111;
  --card:     #181818;
  --border:   #282828;
  --text:     #f0f0f0;
  --muted:    #999999;
  --dim:      #888888;
  --accent:   #facc15;
  --accent-fg:#000000;
  --green:    #4ade80;
  --red:      #f87171;
}
* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
html { font-size: 16px; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text); min-height: 100svh;
  padding: 20px max(16px,env(safe-area-inset-right)) max(20px,env(safe-area-inset-bottom)) max(16px,env(safe-area-inset-left));
}
#app { max-width: 960px; margin: 0 auto; }

.page-title { font-size: 16px; font-weight: 900; letter-spacing: 3px; color: var(--accent); margin-bottom: 20px; }

/* ── search bar (collapsed state) ── */
.search-bar {
  display: none; align-items: center; gap: 12px;
  background: var(--surf); border: 1px solid var(--border);
  border-radius: 12px; padding: 12px 14px; margin-bottom: 16px;
  cursor: pointer;
}
.search-bar-text { flex: 1; font-size: 14px; font-weight: 600; color: var(--text); }
.search-bar-edit {
  font-size: 12px; font-weight: 700; color: var(--accent);
  background: none; border: none; cursor: pointer;
  padding: 4px 8px; letter-spacing: .5px; min-height: 36px;
  touch-action: manipulation;
}

/* ── search panel ── */
.search-panel {
  background: var(--surf); border: 1px solid var(--border);
  border-radius: 14px; padding: 18px 16px; margin-bottom: 24px;
}

/* date */
.date-row { display: flex; align-items: center; gap: 10px; margin-bottom: 18px; flex-wrap: wrap; }
.row-label { font-size: 10px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--muted); white-space: nowrap; }
.date-input {
  font-size: 16px; background: var(--card); border: 1px solid var(--border);
  border-radius: 9px; color: var(--text); padding: 9px 12px; outline: none;
  cursor: pointer; color-scheme: dark; min-height: 44px; touch-action: manipulation;
}
.date-input:focus { border-color: var(--accent); }
.date-label-text { font-size: 12px; color: var(--accent); font-weight: 600; }

/* airports row */
.airports-row {
  display: grid; grid-template-columns: 1fr auto 1fr;
  gap: 10px; align-items: start; margin-bottom: 18px;
}
.airport-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 14px 12px 12px; }
.ac-label { font-size: 10px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--muted); margin-bottom: 6px; }
.ac-value { font-size: 22px; font-weight: 800; color: #fff; letter-spacing: .4px; line-height: 1; margin-bottom: 3px; }
.ac-desc  { font-size: 11px; color: var(--dim); margin-bottom: 12px; min-height: 15px; }
.btn-row  { display: flex; flex-wrap: wrap; gap: 6px; }
.btn-row + .btn-row { margin-top: 6px; }

.ap-btn {
  background: #1e1e1e; border: 1px solid #2e2e2e; border-radius: 7px;
  color: var(--muted); font-size: 12px; font-weight: 600;
  padding: 0 10px; min-height: 36px; cursor: pointer; white-space: nowrap;
  touch-action: manipulation; transition: background .1s, color .1s, border-color .1s;
}
.ap-btn:active { opacity: .7; }
.ap-btn.active { background: var(--accent); border-color: var(--accent); color: var(--accent-fg); }
.ap-btn.group-btn { padding: 0 14px; }

.custom-input {
  display: none; width: 100%; margin-top: 8px; font-size: 16px;
  background: #1e1e1e; border: 1px solid var(--accent); border-radius: 7px;
  color: var(--text); padding: 9px 10px; outline: none;
  text-transform: uppercase; letter-spacing: 1px; min-height: 44px; touch-action: manipulation;
}
.custom-input::placeholder { text-transform: none; color: var(--muted); letter-spacing: 0; }

/* swap */
.swap-wrap { display: flex; align-items: flex-start; justify-content: center; padding-top: 40px; }
.swap-btn {
  background: var(--card); border: 1px solid var(--border); border-radius: 50%;
  color: var(--dim); font-size: 18px; width: 42px; height: 42px; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background .1s, color .1s, transform .2s;
  flex-shrink: 0; touch-action: manipulation;
}
.swap-btn:active { opacity: .7; }

/* cabin */
.cabin-row { display: flex; gap: 8px; margin-bottom: 18px; flex-wrap: wrap; }
.cabin-btn {
  background: var(--card); border: 1px solid var(--border); border-radius: 9px;
  color: var(--muted); font-size: 14px; font-weight: 600;
  padding: 0 18px; min-height: 44px; cursor: pointer;
  touch-action: manipulation; transition: background .1s, color .1s, border-color .1s;
}
.cabin-btn:active { opacity: .7; }
.cabin-btn.active { background: var(--accent); border-color: var(--accent); color: var(--accent-fg); }

/* go button */
.search-btn {
  width: 100%; background: var(--accent); border: none; border-radius: 12px;
  color: var(--accent-fg); font-size: 18px; font-weight: 800; letter-spacing: 1px;
  min-height: 52px; cursor: pointer; touch-action: manipulation;
  transition: opacity .1s;
}
.search-btn:active   { opacity: .8; }
.search-btn:disabled { background: #3a3a00; color: #777; }

/* ── results area ── */
.results-loading { text-align: center; color: var(--dim); padding: 40px 20px; font-size: 13px; line-height: 1.9; }
.results-error   { background: #2d1515; border: 1px solid #5c2020; border-radius: 10px; color: #f87171; padding: 14px 18px; font-size: 14px; }

/* highlights box */
.highlights-box {
  display: none;
  background: var(--surf); border: 1px solid var(--border);
  border-radius: 10px; padding: 12px 16px;
  margin-bottom: 14px; gap: 0;
}
.hl-grid { display: flex; flex-wrap: wrap; gap: 16px 24px; }
.hl-stat { display: flex; flex-direction: column; gap: 2px; }
.hl-label { font-size: 9px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--muted); }
.hl-value { font-size: 18px; font-weight: 800; color: #fff; }
.hl-widebody { font-size: 11px; font-weight: 700; color: var(--accent); margin-top: 10px; letter-spacing: .3px; }

/* sort bar */
.sort-bar {
  display: none; align-items: center; gap: 8px; flex-wrap: wrap;
  margin-bottom: 16px;
}
.sort-label { font-size: 10px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase; color: var(--muted); margin-right: 2px; }
.sort-btn {
  background: var(--card); border: 1px solid var(--border); border-radius: 7px;
  color: var(--muted); font-size: 12px; font-weight: 600;
  padding: 0 12px; min-height: 34px; cursor: pointer;
  touch-action: manipulation; transition: background .1s, color .1s, border-color .1s;
}
.sort-btn:active { opacity: .7; }
.sort-btn.active { background: var(--accent); border-color: var(--accent); color: var(--accent-fg); }

/* two-col or single col results */
.results-cols   { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; align-items: start; }
.results-single { }
.col-head {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px; text-transform: uppercase;
  color: var(--muted); display: flex; align-items: center; gap: 8px; margin-bottom: 10px;
}
.col-badge {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 999px; padding: 1px 8px; font-size: 10px;
  font-weight: 400; color: var(--muted); letter-spacing: 0; text-transform: none;
}

/* ── flight card ── */
.fcard {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 10px; margin-bottom: 7px; cursor: pointer; overflow: hidden;
  transition: border-color .1s;
}
.fcard:active { opacity: .9; }
.fcard.open   { border-color: #3a3a3a; }
.fcard-face   { padding: 10px 12px 8px; }

/* 3-col grid: [badge+logo] [route+meta] [price+lowest] */
.fc-row-main {
  display: grid;
  grid-template-columns: 84px 1fr auto;
  gap: 10px; align-items: center;
}
.fc-col-left {
  display: flex; flex-direction: column; align-items: flex-start; gap: 5px;
}
.fc-logo-wrap { display: flex; align-items: center; }
.fc-logo      { height: 40px; max-width: 80px; object-fit: contain; object-position: left; display: block; }
.fc-al-text   { font-size: 12px; font-weight: 700; color: var(--dim); }

.fc-col-center { min-width: 0; }
.fc-route-row  { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.fc-ep         { display: flex; flex-direction: column; gap: 1px; }
.fc-ep-code    { font-size: 20px; font-weight: 800; color: #fff; line-height: 1; }
.fc-ep-time    { font-size: 12px; font-weight: 700; color: #ccc; }
.fc-ep-arrow   { color: #444; font-size: 15px; flex-shrink: 0; }

.fc-col-right  { display: flex; flex-direction: column; align-items: flex-end; gap: 5px; flex-shrink: 0; }
.fc-price      { font-size: 22px; font-weight: 800; color: #fff; white-space: nowrap; }

/* badges */
.fc-nonstop { font-size: 9px; font-weight: 800; letter-spacing: .8px; color: var(--accent-fg); background: var(--accent); padding: 2px 6px; border-radius: 4px; white-space: nowrap; }
.fc-lowest  { font-size: 9px; font-weight: 800; letter-spacing: .8px; color: #4ade80; background: #052e16; padding: 2px 6px; border-radius: 4px; white-space: nowrap; }

/* meta: via / duration / aircraft */
.fc-meta { font-size: 11px; color: var(--dim); line-height: 1.5; }
.fc-via  { color: var(--accent); font-weight: 600; }

/* expanded detail */
.fcard-detail { display: none; border-top: 1px solid #1e1e1e; padding: 10px 14px 12px; background: #111; }
.fcard.open .fcard-detail { display: block; }
.leg-row { display: flex; gap: 4px; align-items: baseline; font-size: 12px; color: var(--dim); padding: 3px 0; flex-wrap: wrap; }
.leg-ap  { font-weight: 800; color: #ccc; min-width: 2.4rem; }
.leg-tm  { color: #aaa; min-width: 3.6rem; }
.leg-arr { display: contents; }
.leg-info { font-size: 11px; color: #999; margin-left: auto; }
.lv-row  { font-size: 11px; padding: 2px 0 2px 8px; border-left: 2px solid #78350f; margin: 2px 0; }
.lv-loc  { font-weight: 700; color: #d97706; }
.lv-dur  { font-weight: 600; color: #b45309; }
.seatmap-link {
  font-size: 9px; font-weight: 800; letter-spacing: .8px;
  color: #000; background: var(--accent);
  padding: 2px 6px; border-radius: 4px;
  text-decoration: none; white-space: nowrap; margin-left: 4px;
}

.empty { color: #2a2a2a; font-style: italic; font-size: 13px; padding: 8px 0; }

@media (max-width: 600px) {
  .airports-row { grid-template-columns: 1fr; }
  .swap-wrap    { padding-top: 0; justify-content: flex-start; }
  .results-cols { grid-template-columns: 1fr; }
  .fc-price     { font-size: 20px; }
  .fc-ep-code   { font-size: 19px; }
  .fc-ep-time   { font-size: 13px; }
}
</style>
</head>
<body>
<div id="app">
  <div class="page-title">FLY.HINER.NYC</div>

  <!-- collapsed search bar (shown after search) -->
  <div class="search-bar" id="search-bar">
    <span class="search-bar-text" id="search-bar-text"></span>
    <button class="search-bar-edit" id="edit-search-btn">Edit</button>
  </div>

  <!-- full search panel -->
  <div class="search-panel" id="search-panel">

    <div class="date-row">
      <span class="row-label">Date</span>
      <input type="date" class="date-input" id="date-input">
      <span class="date-label-text" id="date-label"></span>
    </div>

    <div class="airports-row">

      <div class="airport-card">
        <div class="ac-label">Departs</div>
        <div class="ac-value" id="dep-value">NYC</div>
        <div class="ac-desc"  id="dep-desc">LGA &middot; JFK &middot; EWR</div>
        <div class="btn-row">
          <button class="ap-btn group-btn active" data-for="dep" data-code="NYC">NYC</button>
          <button class="ap-btn group-btn"        data-for="dep" data-code="HOME">Home</button>
        </div>
        <div class="btn-row">
          <button class="ap-btn" data-for="dep" data-code="RDU">RDU</button>
          <button class="ap-btn" data-for="dep" data-code="GSO">GSO</button>
          <button class="ap-btn" data-for="dep" data-code="LGA">LGA</button>
          <button class="ap-btn" data-for="dep" data-code="DCA">DCA</button>
          <button class="ap-btn" data-for="dep" data-code="LAS">LAS</button>
          <button class="ap-btn" id="dep-other-btn" data-for="dep">+</button>
        </div>
        <input type="text" class="custom-input" id="dep-custom" placeholder="Airport code" maxlength="4">
      </div>

      <div class="swap-wrap">
        <button class="swap-btn" id="swap-btn" title="Swap">&#8644;</button>
      </div>

      <div class="airport-card">
        <div class="ac-label">Arrives</div>
        <div class="ac-value" id="arr-value">Home</div>
        <div class="ac-desc"  id="arr-desc">RDU &middot; GSO</div>
        <div class="btn-row">
          <button class="ap-btn group-btn"        data-for="arr" data-code="NYC">NYC</button>
          <button class="ap-btn group-btn active" data-for="arr" data-code="HOME">Home</button>
        </div>
        <div class="btn-row">
          <button class="ap-btn" data-for="arr" data-code="RDU">RDU</button>
          <button class="ap-btn" data-for="arr" data-code="GSO">GSO</button>
          <button class="ap-btn" data-for="arr" data-code="LGA">LGA</button>
          <button class="ap-btn" data-for="arr" data-code="DCA">DCA</button>
          <button class="ap-btn" data-for="arr" data-code="LAS">LAS</button>
          <button class="ap-btn" id="arr-other-btn" data-for="arr">+</button>
        </div>
        <input type="text" class="custom-input" id="arr-custom" placeholder="Airport code" maxlength="4">
      </div>

    </div>

    <div class="cabin-row">
      <button class="cabin-btn"        data-cabin="economy">Economy</button>
      <button class="cabin-btn"        data-cabin="first">First Class</button>
      <button class="cabin-btn active" data-cabin="all">All</button>
    </div>

    <button class="search-btn" id="search-btn">GO</button>
  </div>

  <!-- sort + highlights (shown with results) -->
  <div class="sort-bar" id="sort-bar">
    <span class="sort-label">Sort</span>
    <button class="sort-btn active" data-sort="price">Price</button>
    <button class="sort-btn" data-sort="arrival">Early</button>
    <button class="sort-btn" data-sort="departure">Late</button>
  </div>

  <div class="highlights-box" id="highlights-box">
    <div class="hl-grid" id="hl-grid"></div>
    <div class="hl-widebody" id="hl-widebody" style="display:none"></div>
  </div>

  <div id="results"></div>
</div>

<script>
// ── state ──────────────────────────────────────────────────────
var state = { dep: 'NYC', arr: 'HOME', cabin: 'all' };
var searchData = { economy: [], firstClass: [], date: '' };
var currentSort = 'price';

// ── date logic ─────────────────────────────────────────────────
function todayStr()   { return new Date().toISOString().slice(0, 10); }
function tmrwStr()    { var d = new Date(); d.setDate(d.getDate()+1); return d.toISOString().slice(0, 10); }

function updateDateLabel() {
  var val   = document.getElementById('date-input').value;
  var label = document.getElementById('date-label');
  if      (val === todayStr()) label.textContent = 'Today';
  else if (val === tmrwStr())  label.textContent = 'Tomorrow';
  else                         label.textContent = '';
}

(function initDate() {
  var now = new Date();
  var d   = new Date(now);
  if (now.getHours() >= 17) d.setDate(d.getDate() + 1);
  document.getElementById('date-input').value = d.toISOString().slice(0, 10);
  updateDateLabel();
})();
document.getElementById('date-input').addEventListener('change', updateDateLabel);

// ── helpers ────────────────────────────────────────────────────
function airportDisplay(c) { return c === 'HOME' ? 'Home' : c; }
function airportDesc(c) {
  if (c === 'NYC')  return 'LGA \xb7 JFK \xb7 EWR';
  if (c === 'HOME') return 'RDU \xb7 GSO';
  return '';
}
function esc(s) {
  return String(s||'').replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}
function expandCount(c) { return c==='NYC'?3:c==='HOME'?2:1; }
function isWidebody(ac) { return /747|767|777|787|330|350|380/.test(ac); }

function dateSummary() {
  var val = document.getElementById('date-input').value;
  if (val === todayStr()) return 'Today';
  if (val === tmrwStr())  return 'Tomorrow';
  return val;
}

// ── airport selection ──────────────────────────────────────────
function setAirport(which, code) {
  state[which] = code;
  document.getElementById(which+'-value').textContent = airportDisplay(code);
  document.getElementById(which+'-desc').innerHTML    = airportDesc(code);
  document.querySelectorAll('[data-for="'+which+'"]').forEach(function(btn) {
    btn.classList.toggle('active', !!(btn.dataset.code && btn.dataset.code === code));
  });
  if (code !== '__custom__') document.getElementById(which+'-custom').style.display = 'none';
}

document.querySelectorAll('.ap-btn').forEach(function(btn) {
  if (btn.id === 'dep-other-btn' || btn.id === 'arr-other-btn') return;
  btn.addEventListener('click', function() { setAirport(btn.dataset.for, btn.dataset.code); });
});

['dep','arr'].forEach(function(which) {
  var ob = document.getElementById(which+'-other-btn');
  var ci = document.getElementById(which+'-custom');
  ob.addEventListener('click', function() {
    var vis = ci.style.display !== 'none';
    ci.style.display = vis ? 'none' : 'block';
    if (!vis) setTimeout(function(){ ci.focus(); }, 50);
  });
  ci.addEventListener('input', function() {
    var val = ci.value.replace(/[^A-Za-z]/g,'').toUpperCase().slice(0,4);
    ci.value = val;
    if (val.length >= 3) {
      state[which] = val;
      document.getElementById(which+'-value').textContent = val;
      document.getElementById(which+'-desc').textContent  = '';
      document.querySelectorAll('[data-for="'+which+'"]').forEach(function(b){ b.classList.remove('active'); });
      ob.classList.add('active');
    }
  });
});

// ── swap ───────────────────────────────────────────────────────
document.getElementById('swap-btn').addEventListener('click', function() {
  var dc = document.getElementById('dep-custom');
  var ac = document.getElementById('arr-custom');
  var tc = state.dep, tv = dc.value, ts = dc.style.display;
  state.dep = state.arr; setAirport('dep', state.dep);
  state.arr = tc;        setAirport('arr', state.arr);
  dc.value = ac.value; dc.style.display = ac.style.display;
  ac.value = tv;       ac.style.display = ts;
});

// ── cabin ──────────────────────────────────────────────────────
document.querySelectorAll('.cabin-btn').forEach(function(btn) {
  btn.addEventListener('click', function() {
    state.cabin = btn.dataset.cabin;
    document.querySelectorAll('.cabin-btn').forEach(function(b){ b.classList.remove('active'); });
    btn.classList.add('active');
  });
});

// ── sort ───────────────────────────────────────────────────────
function sortList(flights, method) {
  var s = flights.slice();
  if (method === 'price')     s.sort(function(a,b){ return a.price - b.price; });
  else if (method === 'arrival')   s.sort(function(a,b){ return a.arrRaw < b.arrRaw ? -1 : 1; });
  else if (method === 'departure') s.sort(function(a,b){ return a.depRaw > b.depRaw ? -1 : 1; });
  return s;
}

document.querySelectorAll('.sort-btn').forEach(function(btn) {
  btn.addEventListener('click', function() {
    currentSort = btn.dataset.sort;
    document.querySelectorAll('.sort-btn').forEach(function(b){ b.classList.remove('active'); });
    btn.classList.add('active');
    renderFlightResults(searchData.economy, searchData.firstClass);
  });
});

// ── collapse/expand search ─────────────────────────────────────
function collapseSearch() {
  var cabinLabel = state.cabin==='all'?'All':state.cabin==='first'?'First':'Economy';
  document.getElementById('search-bar-text').textContent =
    airportDisplay(state.dep) + ' → ' + airportDisplay(state.arr) +
    '  \xb7  ' + dateSummary() + '  \xb7  ' + cabinLabel;
  document.getElementById('search-bar').style.display  = 'flex';
  document.getElementById('search-panel').style.display = 'none';
}
function expandSearch() {
  document.getElementById('search-bar').style.display  = 'none';
  document.getElementById('search-panel').style.display = 'block';
}
document.getElementById('search-bar').addEventListener('click', expandSearch);

// ── highlights ─────────────────────────────────────────────────
function calcMedian(prices) {
  if (!prices.length) return null;
  var s = prices.slice().sort(function(a,b){return a-b;});
  var m = Math.floor(s.length/2);
  return s.length%2 ? s[m] : Math.round((s[m-1]+s[m])/2);
}

function buildHighlights(econ, first) {
  var stats = [];
  var allPrices = econ.concat(first).map(function(f){return f.price;});

  if (econ.length && first.length) {
    stats.push({ label:'Lowest Coach', value: '$'+econ[0].price });
    stats.push({ label:'Lowest First', value: '$'+first[0].price });
  } else if (econ.length) {
    stats.push({ label:'Lowest',  value: '$'+econ[0].price });
  } else if (first.length) {
    stats.push({ label:'Lowest',  value: '$'+first[0].price });
  }

  var med = calcMedian(allPrices);
  if (med !== null) stats.push({ label:'Median', value: '$'+med });

  if (allPrices.length) {
    var hi = Math.max.apply(null, allPrices);
    stats.push({ label:'Highest', value: '$'+hi });
  }

  document.getElementById('hl-grid').innerHTML = stats.map(function(s){
    return '<div class="hl-stat"><span class="hl-label">'+esc(s.label)+'</span>' +
           '<span class="hl-value">'+esc(s.value)+'</span></div>';
  }).join('');

  // Widebody flag
  var wideMap = {};
  econ.concat(first).forEach(function(f) {
    (f.widebodyTypes || []).forEach(function(t) { wideMap[t] = true; });
  });
  var wideTypes = Object.keys(wideMap).sort();
  var wideEl = document.getElementById('hl-widebody');
  if (wideTypes.length) {
    wideEl.textContent = 'WIDEBODY AVAILABLE \xb7 ' + wideTypes.join(', ');
    wideEl.style.display = 'block';
  } else {
    wideEl.style.display = 'none';
  }

  document.getElementById('highlights-box').style.display = stats.length ? 'block' : 'none';
}

// ── AA seatmap URL ─────────────────────────────────────────────
function aaSeaatmapUrl(num, date, dep, arr) {
  if (!num) return null;
  var parts = date.split('-');
  if (parts.length < 3) return null;
  return 'https://www.aa.com/seats/view?' +
    'flightNumber=' + encodeURIComponent(num) +
    '&departureMonth=' + parseInt(parts[1], 10) +
    '&departureDay='   + parseInt(parts[2], 10) +
    '&originAirport='      + encodeURIComponent(dep) +
    '&destinationAirport=' + encodeURIComponent(arr);
}

// ── render card ────────────────────────────────────────────────
function renderCard(f, isLowest) {
  var logoHtml = f.logo
    ? '<img class="fc-logo" src="'+esc(f.logo)+'" alt="'+esc(f.airline)+'" loading="lazy">'
    : '<span class="fc-al-text">'+esc(f.airline)+'</span>';

  // meta: stops · duration · aircraft
  var metaParts = [];
  if (!f.nonstop) {
    if (f.stops && f.stops.length === 1) {
      metaParts.push('<span class="fc-via">via '+esc(f.stops[0].id)+'</span> '+esc(f.stops[0].dur));
    } else if (f.stops && f.stops.length > 1) {
      var vias = f.stops.map(function(s){return esc(s.id);}).join(', ');
      var lvs  = f.stops.map(function(s){return esc(s.id)+': '+esc(s.dur);}).join(' \xb7 ');
      metaParts.push('<span class="fc-via">via '+vias+'</span> \xb7 '+lvs);
    }
  }
  if (f.duration) metaParts.push(esc(f.duration));
  if (f.aircraft && f.aircraft.length) {
    metaParts.push(f.aircraft.map(function(ac) {
      return isWidebody(ac)
        ? '<span style="color:var(--accent);font-weight:700">'+esc(ac)+'</span>'
        : esc(ac);
    }).join(' / '));
  }

  // expanded detail
  var detailHtml = '';
  (f.legs||[]).forEach(function(leg,i){
    var infoParts = [leg.fn, leg.al, leg.ac, leg.dur].filter(Boolean).map(esc).join(' \xb7 ');
    var seatmapHtml = '';
    if (leg.isAA && leg.aaNum && searchData.date) {
      var smUrl = aaSeaatmapUrl(leg.aaNum, searchData.date, leg.dep, leg.arr);
      if (smUrl) seatmapHtml = '<a href="'+esc(smUrl)+'" target="_blank" rel="noopener" class="seatmap-link">SEATMAP</a>';
    }
    detailHtml +=
      '<div class="leg-row">' +
        '<span class="leg-ap">'+esc(leg.dep)+'</span>' +
        '<span class="leg-tm">'+esc(leg.depT)+'</span>' +
        '<span style="color:#666">→</span>' +
        '<span class="leg-ap">'+esc(leg.arr)+'</span>' +
        '<span class="leg-tm">'+esc(leg.arrT)+'</span>' +
        '<span class="leg-info">'+infoParts+'</span>' +
        seatmapHtml +
      '</div>';
    if (leg.layoverId) {
      detailHtml += '<div class="lv-row"><span class="lv-loc">Layover at '+esc(leg.layoverId)+'</span> \xb7 <span class="lv-dur">'+esc(leg.layover)+'</span></div>';
    }
  });

  return (
    '<div class="fcard">' +
      '<div class="fcard-face">' +
        '<div class="fc-row-main">' +
          '<div class="fc-col-left">' +
            '<div class="fc-logo-wrap">'+logoHtml+'</div>' +
          '</div>' +
          '<div class="fc-col-center">' +
            '<div class="fc-route-row">' +
              '<div class="fc-ep">' +
                '<span class="fc-ep-code">'+esc(f.dep)+'</span>' +
                '<span class="fc-ep-time">'+esc(f.depTime)+'</span>' +
              '</div>' +
              '<span class="fc-ep-arrow">→</span>' +
              '<div class="fc-ep">' +
                '<span class="fc-ep-code">'+esc(f.arr)+'</span>' +
                '<span class="fc-ep-time">'+esc(f.arrTime)+'</span>' +
              '</div>' +
            '</div>' +
            (metaParts.length ? '<div class="fc-meta">'+metaParts.join(' \xb7 ')+'</div>' : '') +
          '</div>' +
          '<div class="fc-col-right">' +
            '<span class="fc-price">$'+f.price+'</span>' +
            (isLowest ? '<span class="fc-lowest">LOWEST</span>' : '') +
          '</div>' +
        '</div>' +
      '</div>' +
      '<div class="fcard-detail">'+detailHtml+'</div>' +
    '</div>'
  );
}

function renderList(flights) {
  if (!flights.length) return '<p class="empty">No results found.</p>';
  var sorted   = sortList(flights, currentSort);
  var minPrice = Math.min.apply(null, flights.map(function(f){return f.price;}));
  return sorted.map(function(f){ return renderCard(f, f.price === minPrice); }).join('');
}

// ── render results ─────────────────────────────────────────────
function renderFlightResults(econ, first) {
  var el = document.getElementById('results');
  if (!econ.length && !first.length) {
    el.innerHTML = '<div class="results-error">No matching flights found.</div>';
    return;
  }
  var html = '';
  if (econ.length && first.length) {
    html =
      '<div class="results-cols">' +
        '<div><div class="col-head">Economy <span class="col-badge">'+econ.length+'</span></div>'+renderList(econ)+'</div>' +
        '<div><div class="col-head">First Class <span class="col-badge">'+first.length+'</span></div>'+renderList(first)+'</div>' +
      '</div>';
  } else {
    var flights = econ.length ? econ : first;
    var label   = econ.length ? 'Economy' : 'First Class';
    html = '<div class="results-single"><div class="col-head">'+label+' <span class="col-badge">'+flights.length+'</span></div>'+renderList(flights)+'</div>';
  }
  el.innerHTML = html;
}

// event delegation — no re-binding needed after re-renders
document.getElementById('results').addEventListener('click', function(e) {
  if (e.target.closest('a')) return;
  var card = e.target.closest('.fcard');
  if (card) card.classList.toggle('open');
});

// ── search ─────────────────────────────────────────────────────
document.getElementById('search-btn').addEventListener('click', function() {
  var date = document.getElementById('date-input').value;
  if (!date)          { alert('Please select a date.'); return; }
  if (!state.dep)     { alert('Please select a departure.'); return; }
  if (!state.arr)     { alert('Please select an arrival.'); return; }
  if (state.dep === state.arr) { alert('Departure and arrival cannot be the same.'); return; }

  var routes = expandCount(state.dep) * expandCount(state.arr);
  var cabins = state.cabin === 'all' ? 2 : 1;
  var total  = routes * cabins;
  var secs   = Math.max(15, total * 4);

  var btn = document.getElementById('search-btn');
  btn.disabled = true; btn.textContent = '…';

  document.getElementById('results').innerHTML =
    '<div class="results-loading">' +
      'Searching '+routes+' route'+(routes!==1?'s':'')+' \xd7 '+cabins+' cabin'+(cabins!==1?'s':'')+
      ' ('+total+' API call'+(total!==1?'s':'')+')' +
      '<br><span style="color:#444;font-size:11px">Est. ~'+secs+'s</span>' +
    '</div>';

  document.getElementById('highlights-box').style.display = 'none';
  document.getElementById('sort-bar').style.display       = 'none';

  var qs = 'dep='+encodeURIComponent(state.dep)+'&arr='+encodeURIComponent(state.arr)+
           '&date='+encodeURIComponent(date)+'&cabin='+encodeURIComponent(state.cabin);

  fetch('/api/search?'+qs)
    .then(function(r){ return r.json(); })
    .then(function(data) {
      btn.disabled = false; btn.textContent = 'GO';

      if (data.error) {
        document.getElementById('results').innerHTML =
          '<div class="results-error">'+esc(data.error)+'</div>';
        return;
      }

      searchData = { economy: data.economy||[], firstClass: data.firstClass||[], date: date };
      currentSort = 'price';
      document.querySelectorAll('.sort-btn').forEach(function(b){
        b.classList.toggle('active', b.dataset.sort === 'price');
      });

      collapseSearch();

      buildHighlights(searchData.economy, searchData.firstClass);
      document.getElementById('sort-bar').style.display = 'flex';
      renderFlightResults(searchData.economy, searchData.firstClass);
    })
    .catch(function(e) {
      document.getElementById('results').innerHTML =
        '<div class="results-error">Request failed: '+esc(e.message)+'</div>';
      btn.disabled = false; btn.textContent = 'GO';
    });
});
</script>
</body>
</html>`;

// ── HTTP server ──────────────────────────────────────────────

const server = http.createServer(async (req, res) => {
  if (req.url === '/favicon.ico') { res.writeHead(204); res.end(); return; }
  const u = new URL(req.url, 'http://localhost');
  if (req.method === 'GET' && u.pathname === '/api/search') {
    try {
      const result = await doSearch(
        u.searchParams.get('dep')   || '',
        u.searchParams.get('arr')   || '',
        u.searchParams.get('date')  || '',
        u.searchParams.get('cabin') || 'all',
      );
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(result));
    } catch (e) {
      console.error('Search error:', e);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: e.message }));
    }
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(htmlPage);
});

server.listen(PORT, () => {
  if (!SERPAPI_KEY) console.warn('\n⚠  SERPAPI_KEY not set.\n');
  console.log('\nfly.hiner.nyc server → http://localhost:' + PORT + '\n');
});
