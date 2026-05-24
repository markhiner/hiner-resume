// flights/server.js
// Flight search UI backed by SerpAPI Google Flights
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
    engine:        'google_flights',
    departure_id:  dep,
    arrival_id:    arr,
    outbound_date: date,
    currency:      'USD',
    hl:            'en',
    gl:            'us',
    type:          '2',   // one-way
    adults:        '1',
    travel_class:  String(cabin),
    api_key:       SERPAPI_KEY,
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

// ── parse / format helpers ───────────────────────────────────

function fmtTime(s) {
  if (!s) return '—';
  const m = s.match(/(\d{1,2}):(\d{2})/);
  if (!m) return s;
  let h = parseInt(m[1], 10);
  const min = m[2];
  const ampm = h >= 12 ? 'pm' : 'am';
  if (h > 12) h -= 12;
  if (h === 0) h = 12;
  return h + ':' + min + ' ' + ampm;   // narrow no-break space before am/pm
}

function fmtDur(mins) {
  if (!mins) return '';
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  if (h === 0) return m + 'm';
  if (m === 0) return h + 'h';
  return h + 'h ' + m + 'm';
}

function fmtLayover(mins) {
  if (!mins) return '';
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  if (h === 0) return m + ' min';
  if (m === 0) return h + ' hr';
  return h + ' hr, ' + m + ' min';
}

function shortAircraft(name) {
  if (!name) return '';
  return name
    .replace(/^(boeing|airbus|embraer|bombardier|mcdonnell\s*douglas|canadair|atr)\s*/i, '')
    .trim();
}

// ── parse flights (with airline filter) ──────────────────────

function parseFlights(data, cabin) {
  const results = [];
  const seen    = new Set();

  for (const section of ['best_flights', 'other_flights']) {
    for (const opt of (data[section] || [])) {
      const legs  = opt.flights || [];
      const price = opt.price;
      if (!legs.length || !price) continue;

      // filter: at least one leg must be Delta, AA, United, or JetBlue
      if (!legs.some(legAllowed)) continue;

      const fl = legs[0];
      const ll = legs[legs.length - 1];
      const key = [
        fl.departure_airport?.id,
        fl.departure_airport?.time,
        ll.arrival_airport?.id,
        ll.arrival_airport?.time,
        price, cabin,
      ].join('|');
      if (seen.has(key)) continue;
      seen.add(key);

      const layovers = opt.layovers || [];
      const aircraft = legs.map(l => shortAircraft(l.airplane || '')).filter(Boolean);

      results.push({
        price, cabin,
        airline:  fl.airline      || '',
        logo:     fl.airline_logo || '',
        dep:      fl.departure_airport?.id   || '',
        depTime:  fmtTime(fl.departure_airport?.time),
        arr:      ll.arrival_airport?.id     || '',
        arrTime:  fmtTime(ll.arrival_airport?.time),
        duration: fmtDur(opt.total_duration),
        nonstop:  legs.length === 1,
        stops: layovers.map(lv => ({
          id:  lv.id || '',
          dur: fmtLayover(lv.duration),
        })),
        aircraft,    // array, one per leg
        legs: legs.map((leg, i) => ({
          dep:  leg.departure_airport?.id   || '?',
          depT: fmtTime(leg.departure_airport?.time),
          arr:  leg.arrival_airport?.id     || '?',
          arrT: fmtTime(leg.arrival_airport?.time),
          fn:   leg.flight_number || '',
          ac:   shortAircraft(leg.airplane || ''),
          al:   leg.airline || '',
          dur:  fmtDur(leg.duration),
          layover: layovers[i] ? fmtLayover(layovers[i].duration) : null,
          layoverId: layovers[i]?.id || null,
        })),
      });
    }
  }
  return results;
}

// ── search orchestration ─────────────────────────────────────

async function doSearch(depCode, arrCode, date, cabinParam) {
  if (!SERPAPI_KEY) return { error: 'SERPAPI_KEY not configured on server.' };
  if (!depCode || !arrCode || !date) return { error: 'Missing required parameters.' };

  const deps   = expand(depCode);
  const arrs   = expand(arrCode);
  const cabins = cabinParam === 'all'   ? [ECONOMY, FIRST]
               : cabinParam === 'first' ? [FIRST]
               :                          [ECONOMY];

  const routes = [];
  for (const d of deps) for (const a of arrs) if (d !== a) routes.push([d, a]);

  if (!routes.length) return { error: 'No valid routes — departure and arrival cannot be the same airport.' };

  console.log(`\nSearch: ${depCode} → ${arrCode}  ${date}  [${cabinParam}]`);
  console.log(`${routes.length} route(s) × ${cabins.length} cabin(s) = ${routes.length * cabins.length} API calls`);

  const econFlights  = [];
  const firstFlights = [];

  for (const [d, a] of routes) {
    for (const cab of cabins) {
      process.stdout.write(`  ${d}→${a} [${cab === ECONOMY ? 'Economy' : 'First'}] ... `);
      const data    = await serpSearch(d, a, date, cab);
      const results = parseFlights(data, cab);
      console.log(`${results.length} results`);
      (cab === ECONOMY ? econFlights : firstFlights).push(...results);
    }
  }

  econFlights.sort((a, b) => a.price - b.price);
  firstFlights.sort((a, b) => a.price - b.price);

  return { economy: econFlights, firstClass: firstFlights };
}

// ── HTML page ────────────────────────────────────────────────

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
<title>Flight Search \xb7 hiner.nyc</title>
<style>
:root {
  --bg:      #0a0a0a;
  --surf:    #111111;
  --card:    #181818;
  --border:  #242424;
  --text:    #e8e8e8;
  --muted:   #555555;
  --dim:     #333333;
  --blue:    #1d4ed8;
  --blue-h:  #2563eb;
  --green:   #22c55e;
  --red:     #ef4444;
  --neutral: #cbd5e1;
}
* { box-sizing: border-box; margin: 0; padding: 0; -webkit-tap-highlight-color: transparent; }
html { font-size: 16px; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Text', 'Segoe UI', sans-serif;
  background: var(--bg); color: var(--text);
  min-height: 100svh;
  padding: 20px 16px max(20px, env(safe-area-inset-bottom)) 16px;
  padding-left:  max(16px, env(safe-area-inset-left));
  padding-right: max(16px, env(safe-area-inset-right));
}
#app { max-width: 960px; margin: 0 auto; }

.page-title { font-size: 17px; font-weight: 800; letter-spacing: 2.5px; color: #fff; margin-bottom: 3px; }
.page-sub   { font-size: 12px; color: var(--muted); margin-bottom: 22px; }

/* ── search panel ── */
.search-panel {
  background: var(--surf); border: 1px solid var(--border);
  border-radius: 14px; padding: 18px 16px; margin-bottom: 24px;
}

/* date */
.date-row { display: flex; align-items: center; gap: 10px; margin-bottom: 18px; }
.row-label {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: var(--muted); white-space: nowrap;
}
.date-input {
  font-size: 16px; /* prevents iOS zoom */
  background: var(--card); border: 1px solid var(--border);
  border-radius: 9px; color: var(--text);
  padding: 9px 12px; outline: none; cursor: pointer;
  color-scheme: dark; min-height: 44px;
  touch-action: manipulation;
}
.date-input:focus { border-color: var(--blue); }

/* airports row */
.airports-row {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  gap: 10px; align-items: start;
  margin-bottom: 18px;
}
.airport-card {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 12px; padding: 14px 12px 12px;
}
.ac-label {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: var(--muted); margin-bottom: 6px;
}
.ac-value {
  font-size: 22px; font-weight: 800; color: #fff;
  letter-spacing: .4px; line-height: 1; margin-bottom: 3px;
}
.ac-desc { font-size: 11px; color: var(--dim); margin-bottom: 12px; min-height: 15px; }

.btn-row { display: flex; flex-wrap: wrap; gap: 6px; }
.btn-row + .btn-row { margin-top: 6px; }

.ap-btn {
  background: #1e1e1e; border: 1px solid #2c2c2c;
  border-radius: 7px; color: #888; font-size: 12px; font-weight: 600;
  padding: 0 10px; min-height: 36px; cursor: pointer; white-space: nowrap;
  touch-action: manipulation;
  transition: background .12s, color .12s, border-color .12s;
}
.ap-btn:active { opacity: .7; }
.ap-btn.active { background: var(--blue); border-color: var(--blue-h); color: #fff; }
.ap-btn.group-btn { padding: 0 14px; }

.custom-input {
  display: none; width: 100%; margin-top: 8px;
  font-size: 16px; /* prevent iOS zoom */
  background: #1e1e1e; border: 1px solid var(--blue);
  border-radius: 7px; color: var(--text);
  padding: 9px 10px; outline: none; text-transform: uppercase;
  letter-spacing: 1px; min-height: 44px;
  touch-action: manipulation;
}
.custom-input::placeholder { text-transform: none; color: var(--muted); letter-spacing: 0; }

/* swap */
.swap-wrap {
  display: flex; align-items: flex-start;
  justify-content: center; padding-top: 40px;
}
.swap-btn {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 50%; color: var(--muted); font-size: 18px;
  width: 42px; height: 42px; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background .12s, color .12s, transform .2s;
  flex-shrink: 0; touch-action: manipulation;
}
.swap-btn:active { opacity: .7; }

/* cabin */
.cabin-row { display: flex; gap: 8px; margin-bottom: 18px; flex-wrap: wrap; }
.cabin-btn {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 9px; color: #777; font-size: 14px; font-weight: 600;
  padding: 0 18px; min-height: 44px; cursor: pointer;
  touch-action: manipulation;
  transition: background .12s, color .12s, border-color .12s;
}
.cabin-btn:active { opacity: .7; }
.cabin-btn.active { background: var(--blue); border-color: var(--blue-h); color: #fff; }

/* search button */
.search-btn {
  width: 100%; background: var(--blue); border: none; border-radius: 12px;
  color: #fff; font-size: 16px; font-weight: 700; letter-spacing: .2px;
  min-height: 52px; cursor: pointer; touch-action: manipulation;
  transition: background .12s;
}
.search-btn:active    { opacity: .8; }
.search-btn:disabled  { background: #172558; color: #3d5faa; }

/* ── results ── */
.results-loading {
  text-align: center; color: var(--muted);
  padding: 40px 20px; font-size: 14px; line-height: 1.8;
}
.results-error {
  background: #2d1515; border: 1px solid #5c2020;
  border-radius: 10px; color: #f87171;
  padding: 14px 18px; font-size: 14px;
}

.results-cols   { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; align-items: start; }
.results-single { }

.col-head {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: var(--muted);
  display: flex; align-items: center; gap: 8px; margin-bottom: 10px;
}
.col-badge {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 999px; padding: 1px 8px;
  font-size: 10px; font-weight: 400; color: var(--muted);
  letter-spacing: 0; text-transform: none;
}

/* ── flight card ── */
.fcard {
  background: var(--card); border: 1px solid var(--border);
  border-radius: 10px; margin-bottom: 9px;
  cursor: pointer; overflow: hidden;
  transition: border-color .12s;
}
.fcard:active  { opacity: .9; }
.fcard.open    { border-color: #334; }

.fcard-face { padding: 12px 14px; }

/* top row: logo + airline + price */
.fc-top {
  display: flex; align-items: center; gap: 10px; margin-bottom: 10px;
}
.fc-logo-wrap { flex-shrink: 0; width: 52px; }
.fc-logo      { height: 18px; max-width: 52px; object-fit: contain; object-position: left; display: block; }
.fc-al-text   { font-size: 11px; font-weight: 700; color: var(--dim); }
.fc-airline   { flex: 1; font-size: 13px; font-weight: 600; color: #aaa; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.fc-price-area { display: flex; align-items: center; gap: 6px; flex-shrink: 0; }
.fc-price      { font-size: 22px; font-weight: 800; white-space: nowrap; }
.fc-lowest     {
  font-size: 9px; font-weight: 800; letter-spacing: .8px;
  color: #4ade80; background: #052e16;
  padding: 2px 6px; border-radius: 4px; white-space: nowrap;
}

/* route row */
.fc-route {
  display: flex; align-items: baseline; gap: 4px;
  font-size: 17px; font-weight: 800; color: #fff;
  margin-bottom: 6px; letter-spacing: .3px;
}
.fc-route .fc-time { font-size: 12px; font-weight: 400; color: var(--muted); margin-left: 2px; }
.fc-route .fc-arrow { color: var(--dim); font-size: 14px; font-weight: 300; margin: 0 4px; flex-shrink: 0; }

/* meta row: stops, duration, aircraft */
.fc-meta { font-size: 12px; color: var(--muted); line-height: 1.6; }
.fc-meta .via { color: #888; }
.fc-meta .layover-info { color: #666; }

/* ── expanded detail ── */
.fcard-detail {
  display: none;
  border-top: 1px solid #1a1a1a;
  padding: 10px 14px 12px;
  background: #111;
}
.fcard.open .fcard-detail { display: block; }

.leg-row {
  display: flex; gap: 3px; align-items: baseline;
  font-size: 12px; color: #666; padding: 3px 0; flex-wrap: wrap;
}
.leg-ap   { font-weight: 800; color: #ccc; min-width: 2.4rem; }
.leg-tm   { color: var(--dim); min-width: 3.5rem; }
.leg-arr  { display: contents; }
.leg-info { font-size: 11px; color: var(--dim); margin-left: auto; }
.lv-row {
  font-size: 11px; color: #92400e;
  padding: 2px 0 2px 8px;
  border-left: 2px solid #78350f;
  margin: 2px 0;
}
.det-arr { color: var(--dim); margin: 0 3px; }

.empty { color: #2a2a2a; font-style: italic; font-size: 13px; padding: 8px 0; }

/* ── responsive ── */
@media (max-width: 600px) {
  .airports-row  { grid-template-columns: 1fr; }
  .swap-wrap     { padding-top: 0; justify-content: flex-start; }
  .results-cols  { grid-template-columns: 1fr; }
  .fc-price      { font-size: 19px; }
}
</style>
</head>
<body>
<div id="app">
  <div class="page-title">FLIGHT SEARCH</div>
  <div class="page-sub">One-way &middot; 1 adult &middot; Delta &middot; American &middot; United &middot; JetBlue</div>

  <div class="search-panel">

    <div class="date-row">
      <span class="row-label">Date</span>
      <input type="date" class="date-input" id="date-input">
    </div>

    <div class="airports-row">

      <!-- Departure -->
      <div class="airport-card">
        <div class="ac-label">From</div>
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
          <button class="ap-btn" data-for="dep" data-code="MIA">MIA</button>
          <button class="ap-btn" id="dep-other-btn" data-for="dep">+ Other</button>
        </div>
        <input type="text" class="custom-input" id="dep-custom" placeholder="Airport code (e.g. ORD)" maxlength="4">
      </div>

      <!-- Swap -->
      <div class="swap-wrap">
        <button class="swap-btn" id="swap-btn" title="Swap airports">&#8644;</button>
      </div>

      <!-- Arrival -->
      <div class="airport-card">
        <div class="ac-label">To</div>
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
          <button class="ap-btn" data-for="arr" data-code="MIA">MIA</button>
          <button class="ap-btn" id="arr-other-btn" data-for="arr">+ Other</button>
        </div>
        <input type="text" class="custom-input" id="arr-custom" placeholder="Airport code (e.g. ORD)" maxlength="4">
      </div>

    </div>

    <div class="cabin-row">
      <button class="cabin-btn active" data-cabin="economy">Economy</button>
      <button class="cabin-btn"        data-cabin="first">First Class</button>
      <button class="cabin-btn"        data-cabin="all">All</button>
    </div>

    <button class="search-btn" id="search-btn">Search Flights</button>

  </div><!-- /search-panel -->

  <div id="results"></div>
</div>

<script>
// ── constants ─────────────────────────────────────────────────
var NYC_AP  = ['LGA', 'JFK', 'EWR'];
var HOME_AP = ['RDU', 'GSO'];

// ── state ──────────────────────────────────────────────────────
var state = { dep: 'NYC', arr: 'HOME', cabin: 'economy' };

// ── default date ───────────────────────────────────────────────
(function () {
  var now = new Date();
  var d   = new Date(now);
  if (now.getHours() >= 17) d.setDate(d.getDate() + 1);
  document.getElementById('date-input').value = d.toISOString().slice(0, 10);
})();

// ── helpers ────────────────────────────────────────────────────
function airportDisplay(code) { return code === 'HOME' ? 'Home' : code; }
function airportDesc(code) {
  if (code === 'NYC')  return 'LGA \xb7 JFK \xb7 EWR';
  if (code === 'HOME') return 'RDU \xb7 GSO';
  return '';
}
function esc(s) {
  return String(s || '').replace(/[&<>"']/g, function (c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
function expandCount(code) {
  if (code === 'NYC')  return 3;
  if (code === 'HOME') return 2;
  return 1;
}

// ── airport selection ──────────────────────────────────────────
function setAirport(which, code) {
  state[which] = code;
  document.getElementById(which + '-value').textContent = airportDisplay(code);
  document.getElementById(which + '-desc').innerHTML    = airportDesc(code);
  document.querySelectorAll('[data-for="' + which + '"]').forEach(function (btn) {
    btn.classList.toggle('active', !!(btn.dataset.code && btn.dataset.code === code));
  });
  if (code !== '__custom__') {
    document.getElementById(which + '-custom').style.display = 'none';
  }
}

document.querySelectorAll('.ap-btn').forEach(function (btn) {
  if (btn.id === 'dep-other-btn' || btn.id === 'arr-other-btn') return;
  btn.addEventListener('click', function () { setAirport(btn.dataset.for, btn.dataset.code); });
});

// ── custom airport input ───────────────────────────────────────
['dep', 'arr'].forEach(function (which) {
  var otherBtn    = document.getElementById(which + '-other-btn');
  var customInput = document.getElementById(which + '-custom');

  otherBtn.addEventListener('click', function () {
    var visible = customInput.style.display !== 'none';
    customInput.style.display = visible ? 'none' : 'block';
    if (!visible) { setTimeout(function () { customInput.focus(); }, 50); }
  });

  customInput.addEventListener('input', function () {
    var val = customInput.value.replace(/[^A-Za-z]/g, '').toUpperCase().slice(0, 4);
    customInput.value = val;
    if (val.length >= 3) {
      state[which] = val;
      document.getElementById(which + '-value').textContent = val;
      document.getElementById(which + '-desc').textContent  = '';
      document.querySelectorAll('[data-for="' + which + '"]').forEach(function (b) { b.classList.remove('active'); });
      otherBtn.classList.add('active');
    }
  });
});

// ── swap ───────────────────────────────────────────────────────
document.getElementById('swap-btn').addEventListener('click', function () {
  var dc = document.getElementById('dep-custom');
  var ac = document.getElementById('arr-custom');
  var tmpCode = state.dep, tmpVal = dc.value, tmpVis = dc.style.display;
  state.dep = state.arr; setAirport('dep', state.dep);
  state.arr = tmpCode;   setAirport('arr', state.arr);
  dc.value = ac.value; dc.style.display = ac.style.display;
  ac.value = tmpVal;   ac.style.display = tmpVis;
});

// ── cabin ──────────────────────────────────────────────────────
document.querySelectorAll('.cabin-btn').forEach(function (btn) {
  btn.addEventListener('click', function () {
    state.cabin = btn.dataset.cabin;
    document.querySelectorAll('.cabin-btn').forEach(function (b) { b.classList.remove('active'); });
    btn.classList.add('active');
  });
});

// ── price color ────────────────────────────────────────────────
function priceColor(price, cabin) {
  if (cabin === 1) {           // economy
    if (price < 200) return '#22c55e';   // green
    if (price > 400) return '#ef4444';   // red
    return '#cbd5e1';                     // neutral
  } else {                     // first
    if (price < 300) return '#22c55e';
    if (price > 600) return '#ef4444';
    return '#cbd5e1';
  }
}

// ── render card ────────────────────────────────────────────────
function renderCard(f, isLowest) {
  var color = priceColor(f.price, f.cabin);

  var logoHtml = f.logo
    ? '<img class="fc-logo" src="' + esc(f.logo) + '" alt="' + esc(f.airline) + '" loading="lazy">'
    : '<span class="fc-al-text">' + esc(f.airline) + '</span>';

  var lowestBadge = isLowest ? '<span class="fc-lowest">LOWEST</span>' : '';

  // route row
  var routeHtml =
    '<div class="fc-route">' +
      '<span>' + esc(f.dep) + '</span>' +
      '<span class="fc-time">' + esc(f.depTime) + '</span>' +
      '<span class="fc-arrow">→</span>' +
      '<span>' + esc(f.arr) + '</span>' +
      '<span class="fc-time">' + esc(f.arrTime) + '</span>' +
    '</div>';

  // meta row: stop info + duration + aircraft
  var metaParts = [];
  if (f.nonstop) {
    metaParts.push('Nonstop');
  } else if (f.stops && f.stops.length) {
    var vias = f.stops.map(function (s) { return esc(s.id); }).join(', ');
    metaParts.push('Via ' + vias);
    var lvParts = f.stops.map(function (s) {
      return esc(s.id) + ': ' + esc(s.dur);
    });
    if (lvParts.length) metaParts.push(lvParts.join(' \xb7 '));
  }
  if (f.duration) metaParts.push(esc(f.duration));
  if (f.aircraft && f.aircraft.length) {
    metaParts.push(f.aircraft.map(esc).join(' / '));
  }

  var metaHtml = '<div class="fc-meta">' + metaParts.join(' \xb7 ') + '</div>';

  // expanded leg detail
  var detailHtml = '';
  (f.legs || []).forEach(function (leg, i) {
    var info = [leg.fn, leg.al, leg.ac, leg.dur].filter(Boolean).join(' \xb7 ');
    detailHtml +=
      '<div class="leg-row">' +
        '<span class="leg-ap">' + esc(leg.dep) + '</span>' +
        '<span class="leg-tm">' + esc(leg.depT) + '</span>' +
        '<span class="det-arr">→</span>' +
        '<span class="leg-ap">' + esc(leg.arr) + '</span>' +
        '<span class="leg-tm">' + esc(leg.arrT) + '</span>' +
        '<span class="leg-info">' + esc(info) + '</span>' +
      '</div>';
    if (leg.layoverId) {
      detailHtml +=
        '<div class="lv-row">Layover at ' + esc(leg.layoverId) + ' \xb7 ' + esc(leg.layover) + '</div>';
    }
  });

  return (
    '<div class="fcard">' +
      '<div class="fcard-face">' +
        '<div class="fc-top">' +
          '<div class="fc-logo-wrap">' + logoHtml + '</div>' +
          '<span class="fc-airline">' + esc(f.airline) + '</span>' +
          '<div class="fc-price-area">' +
            '<span class="fc-price" style="color:' + color + '">$' + f.price + '</span>' +
            lowestBadge +
          '</div>' +
        '</div>' +
        routeHtml +
        metaHtml +
      '</div>' +
      '<div class="fcard-detail">' + detailHtml + '</div>' +
    '</div>'
  );
}

// ── render flight list ─────────────────────────────────────────
function renderList(flights) {
  if (!flights.length) return '<p class="empty">No results found.</p>';
  var minPrice = flights[0].price;
  return flights.map(function (f) { return renderCard(f, f.price === minPrice); }).join('');
}

// ── render results ─────────────────────────────────────────────
function renderResults(data) {
  var el = document.getElementById('results');

  if (data.error) {
    el.innerHTML = '<div class="results-error">' + esc(data.error) + '</div>';
    return;
  }

  var econ  = data.economy    || [];
  var first = data.firstClass || [];

  if (!econ.length && !first.length) {
    el.innerHTML = '<div class="results-error">No flights found for this search.</div>';
    return;
  }

  var html = '';
  if (econ.length && first.length) {
    html =
      '<div class="results-cols">' +
        '<div>' +
          '<div class="col-head">Economy <span class="col-badge">' + econ.length + '</span></div>' +
          renderList(econ) +
        '</div>' +
        '<div>' +
          '<div class="col-head">First Class <span class="col-badge">' + first.length + '</span></div>' +
          renderList(first) +
        '</div>' +
      '</div>';
  } else {
    var flights = econ.length ? econ  : first;
    var label   = econ.length ? 'Economy' : 'First Class';
    html =
      '<div class="results-single">' +
        '<div class="col-head">' + label + ' <span class="col-badge">' + flights.length + '</span></div>' +
        renderList(flights) +
      '</div>';
  }

  el.innerHTML = html;
  el.querySelectorAll('.fcard').forEach(function (card) {
    card.addEventListener('click', function () { card.classList.toggle('open'); });
  });
}

// ── search ─────────────────────────────────────────────────────
document.getElementById('search-btn').addEventListener('click', function () {
  var date = document.getElementById('date-input').value;
  if (!date)          { alert('Please select a date.'); return; }
  if (!state.dep)     { alert('Please select a departure airport.'); return; }
  if (!state.arr)     { alert('Please select an arrival airport.'); return; }
  if (state.dep === state.arr) { alert('Departure and arrival cannot be the same.'); return; }

  var routes = expandCount(state.dep) * expandCount(state.arr);
  var cabins = state.cabin === 'all' ? 2 : 1;
  var total  = routes * cabins;
  var secs   = Math.max(15, total * 4);

  var btn = document.getElementById('search-btn');
  btn.disabled    = true;
  btn.textContent = 'Searching…';

  document.getElementById('results').innerHTML =
    '<div class="results-loading">' +
      '✈️ Searching ' + routes + ' route' + (routes !== 1 ? 's' : '') +
      ' \xd7 ' + cabins + ' cabin' + (cabins !== 1 ? 's' : '') +
      ' (' + total + ' API call' + (total !== 1 ? 's' : '') + ')' +
      '<br><span style="color:#2a2a2a;font-size:12px">Est. ~' + secs + 's</span>' +
    '</div>';

  var qs = 'dep='    + encodeURIComponent(state.dep)   +
           '&arr='   + encodeURIComponent(state.arr)   +
           '&date='  + encodeURIComponent(date)         +
           '&cabin=' + encodeURIComponent(state.cabin);

  fetch('/api/search?' + qs)
    .then(function (r) { return r.json(); })
    .then(function (data) {
      renderResults(data);
      btn.disabled    = false;
      btn.textContent = 'Search Flights';
    })
    .catch(function (e) {
      document.getElementById('results').innerHTML =
        '<div class="results-error">Request failed: ' + esc(e.message) + '</div>';
      btn.disabled    = false;
      btn.textContent = 'Search Flights';
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
        u.searchParams.get('cabin') || 'economy',
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
  if (!SERPAPI_KEY) console.warn('\n⚠  SERPAPI_KEY is not set — searches will return an error.\n');
  console.log('\nFlight search server → http://localhost:' + PORT + '\n');
});
