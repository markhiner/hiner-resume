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

// ── parse ────────────────────────────────────────────────────

function fmtTime(s) {
  if (!s) return '—';
  const m = s.match(/\d{1,2}:\d{2}/);
  return m ? m[0] : s;
}

function fmtDur(mins) {
  if (!mins) return '';
  return Math.floor(mins / 60) + 'h ' + (mins % 60) + 'm';
}

function parseFlights(data, cabin) {
  const results = [];
  const seen    = new Set();
  for (const section of ['best_flights', 'other_flights']) {
    for (const opt of (data[section] || [])) {
      const legs  = opt.flights || [];
      const price = opt.price;
      if (!legs.length || !price) continue;
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
      results.push({
        price, cabin,
        airline: fl.airline       || '',
        logo:    fl.airline_logo  || '',
        dep:     fl.departure_airport?.id   || '',
        depTime: fmtTime(fl.departure_airport?.time),
        arr:     ll.arrival_airport?.id     || '',
        arrTime: fmtTime(ll.arrival_airport?.time),
        duration: fmtDur(opt.total_duration),
        nonstop:  legs.length === 1,
        layovers: (opt.layovers || []).map(lv => ({
          id:  lv.id || '',
          dur: fmtDur(lv.duration),
        })),
        legs: legs.map(leg => ({
          dep:  leg.departure_airport?.id   || '?',
          depT: fmtTime(leg.departure_airport?.time),
          arr:  leg.arrival_airport?.id     || '?',
          arrT: fmtTime(leg.arrival_airport?.time),
          fn:   leg.flight_number || '',
          ac:   leg.airplane      || '',
          al:   leg.airline       || '',
          dur:  fmtDur(leg.duration),
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
  console.log(`${routes.length} routes × ${cabins.length} cabin(s) = ${routes.length * cabins.length} API calls`);

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
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Flight Search \xb7 hiner.nyc</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #0a0a0a; color: #e8e8e8;
  min-height: 100vh; padding: 24px 16px;
}
#app { max-width: 980px; margin: 0 auto; }

.page-title { font-size: 18px; font-weight: 800; letter-spacing: 2.5px; color: #fff; margin-bottom: 4px; }
.page-sub   { font-size: 12px; color: #444; margin-bottom: 24px; }

/* search panel */
.search-panel {
  background: #111; border: 1px solid #222;
  border-radius: 12px; padding: 20px; margin-bottom: 28px;
}

/* date row */
.date-row { display: flex; align-items: center; gap: 12px; margin-bottom: 20px; }
.row-label {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: #444; white-space: nowrap;
}
.date-input {
  background: #1a1a1a; border: 1px solid #2a2a2a;
  border-radius: 8px; color: #e8e8e8; font-size: 14px;
  padding: 8px 12px; outline: none; cursor: pointer;
  color-scheme: dark;
}
.date-input:focus { border-color: #3b82f6; }

/* airports row */
.airports-row {
  display: grid;
  grid-template-columns: 1fr auto 1fr;
  gap: 12px; align-items: start;
  margin-bottom: 20px;
}
.airport-card {
  background: #161616; border: 1px solid #242424;
  border-radius: 10px; padding: 14px 14px 12px;
}
.ac-label {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: #444; margin-bottom: 8px;
}
.ac-value {
  font-size: 24px; font-weight: 800; color: #fff;
  letter-spacing: .5px; line-height: 1; margin-bottom: 3px;
}
.ac-desc { font-size: 11px; color: #444; margin-bottom: 12px; min-height: 15px; }

.btn-row { display: flex; flex-wrap: wrap; gap: 6px; }
.btn-row + .btn-row { margin-top: 6px; }

.ap-btn {
  background: #1e1e1e; border: 1px solid #2e2e2e;
  border-radius: 6px; color: #999; font-size: 12px; font-weight: 600;
  padding: 5px 10px; cursor: pointer; white-space: nowrap;
  transition: background .1s, color .1s, border-color .1s;
}
.ap-btn:hover { background: #272727; color: #e8e8e8; border-color: #3a3a3a; }
.ap-btn.active { background: #1d4ed8; border-color: #2563eb; color: #fff; }
.ap-btn.group-btn { padding: 5px 14px; }

.custom-input {
  display: none; width: 100%; margin-top: 8px;
  background: #1e1e1e; border: 1px solid #3b82f6;
  border-radius: 6px; color: #e8e8e8; font-size: 13px;
  padding: 6px 10px; outline: none; text-transform: uppercase;
  letter-spacing: 1px;
}
.custom-input::placeholder { text-transform: none; color: #444; letter-spacing: 0; }

/* swap button */
.swap-wrap {
  display: flex; align-items: flex-start;
  justify-content: center; padding-top: 36px;
}
.swap-btn {
  background: #1a1a1a; border: 1px solid #2a2a2a;
  border-radius: 50%; color: #555; font-size: 18px;
  width: 40px; height: 40px; cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background .1s, color .1s, transform .25s;
  flex-shrink: 0;
}
.swap-btn:hover { background: #222; color: #3b82f6; transform: rotate(180deg); }

/* cabin */
.cabin-row { display: flex; gap: 8px; margin-bottom: 20px; }
.cabin-btn {
  background: #1a1a1a; border: 1px solid #2a2a2a;
  border-radius: 8px; color: #777; font-size: 13px; font-weight: 600;
  padding: 8px 18px; cursor: pointer;
  transition: background .1s, color .1s, border-color .1s;
}
.cabin-btn:hover { background: #222; color: #e8e8e8; }
.cabin-btn.active { background: #1d4ed8; border-color: #2563eb; color: #fff; }

/* search button */
.search-btn {
  width: 100%; background: #1d4ed8; border: none; border-radius: 10px;
  color: #fff; font-size: 15px; font-weight: 700; letter-spacing: .3px;
  padding: 13px; cursor: pointer; transition: background .1s;
}
.search-btn:hover    { background: #2563eb; }
.search-btn:disabled { background: #162a6b; color: #3d5faa; cursor: not-allowed; }

/* results */
.results-loading { text-align: center; color: #444; padding: 40px 20px; font-size: 13px; line-height: 1.6; }
.results-error   {
  background: #2d1515; border: 1px solid #5c2020;
  border-radius: 8px; color: #f87171;
  padding: 14px 18px; font-size: 13px;
}

.results-cols   { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
.results-single { }

.col-head {
  font-size: 10px; font-weight: 700; letter-spacing: 1.5px;
  text-transform: uppercase; color: #444;
  display: flex; align-items: center; gap: 8px; margin-bottom: 10px;
}
.col-badge {
  background: #1a1a1a; border: 1px solid #222;
  border-radius: 999px; padding: 1px 8px;
  font-size: 10px; font-weight: 400; color: #444;
  letter-spacing: 0; text-transform: none;
}

/* flight card */
.fcard {
  background: #161616; border: 1px solid #222;
  border-radius: 8px; margin-bottom: 8px;
  cursor: pointer; overflow: hidden;
  transition: border-color .1s;
}
.fcard:hover { border-color: #333; }
.fcard.open  { border-color: #334; }

.fcard-face {
  display: grid;
  grid-template-columns: 58px 1fr auto;
  gap: 10px; align-items: center;
  padding: 10px 14px;
}

.fcard-logo-wrap { display: flex; align-items: center; }
.fcard-logo      { height: 20px; max-width: 58px; object-fit: contain; object-position: left; }
.fcard-al-text   { font-size: 11px; font-weight: 700; color: #444; }

.fcard-airports  { font-size: 15px; font-weight: 800; color: #fff; letter-spacing: .5px; }
.fcard-airports .arr { color: #333; font-weight: 400; font-size: 12px; margin: 0 4px; }
.fcard-times     { font-size: 11px; color: #444; margin-top: 2px; }
.fcard-meta      { display: flex; gap: 6px; align-items: center; margin-top: 4px; flex-wrap: wrap; }
.stop-pill {
  background: #1a1a1a; border: 1px solid #2a2a2a;
  border-radius: 999px; padding: 1px 7px;
  font-size: 11px; color: #555;
}
.dur-text { font-size: 11px; color: #444; }

.fcard-price { font-size: 22px; font-weight: 800; text-align: right; white-space: nowrap; }

/* expanded detail */
.fcard-detail {
  display: none;
  border-top: 1px solid #1a1a1a;
  padding: 10px 14px 12px;
  background: #111;
}
.fcard.open .fcard-detail { display: block; }

.leg-row {
  display: flex; gap: 4px; align-items: baseline;
  font-size: 12px; color: #666; padding: 3px 0;
}
.leg-ap   { font-weight: 800; color: #bbb; min-width: 2.5rem; }
.leg-tm   { color: #555; min-width: 3rem; }
.leg-arrow { color: #333; margin: 0 2px; }
.leg-info { font-size: 11px; color: #333; margin-left: auto; }

.lv-row {
  font-size: 11px; color: #b45309;
  padding: 2px 0 2px 8px;
  border-left: 2px solid #78350f;
  margin: 2px 0;
}

.empty { color: #2a2a2a; font-style: italic; font-size: 13px; padding: 8px 0; }

@media (max-width: 640px) {
  .airports-row { grid-template-columns: 1fr; }
  .swap-wrap    { padding-top: 0; justify-content: flex-start; }
  .results-cols { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div id="app">
  <div class="page-title">FLIGHT SEARCH</div>
  <div class="page-sub">One-way &middot; 1 adult &middot; Powered by Google Flights via SerpAPI</div>

  <div class="search-panel">

    <div class="date-row">
      <span class="row-label">Date</span>
      <input type="date" class="date-input" id="date-input">
    </div>

    <div class="airports-row">

      <!-- Departure -->
      <div class="airport-card" id="dep-card">
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
          <button class="ap-btn" id="dep-other-btn" data-for="dep" data-code="">+ Other</button>
        </div>
        <input type="text" class="custom-input" id="dep-custom" placeholder="Airport code (e.g. ORD)" maxlength="4">
      </div>

      <!-- Swap -->
      <div class="swap-wrap">
        <button class="swap-btn" id="swap-btn" title="Swap airports">&#8644;</button>
      </div>

      <!-- Arrival -->
      <div class="airport-card" id="arr-card">
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
          <button class="ap-btn" id="arr-other-btn" data-for="arr" data-code="">+ Other</button>
        </div>
        <input type="text" class="custom-input" id="arr-custom" placeholder="Airport code (e.g. ORD)" maxlength="4">
      </div>

    </div><!-- /airports-row -->

    <div class="cabin-row">
      <button class="cabin-btn active" data-cabin="economy">Economy</button>
      <button class="cabin-btn"        data-cabin="first">First Class</button>
      <button class="cabin-btn"        data-cabin="all">All</button>
    </div>

    <button class="search-btn" id="search-btn">Search Flights</button>

  </div><!-- /search-panel -->

  <div id="results"></div>
</div><!-- /app -->

<script>
// ── constants ──────────────────────────────────────────────────
var NYC_AP  = ['LGA', 'JFK', 'EWR'];
var HOME_AP = ['RDU', 'GSO'];

// ── state ──────────────────────────────────────────────────────
var state = { dep: 'NYC', arr: 'HOME', cabin: 'economy' };

// ── default date (today before 5pm, tomorrow at/after 5pm) ─────
(function () {
  var now = new Date();
  var d   = new Date(now);
  if (now.getHours() >= 17) d.setDate(d.getDate() + 1);
  document.getElementById('date-input').value = d.toISOString().slice(0, 10);
})();

// ── helpers ────────────────────────────────────────────────────
function airportDisplay(code) {
  if (code === 'HOME') return 'Home';
  return code;
}
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
    btn.classList.toggle('active', btn.dataset.code === code);
  });
  if (code !== '__custom__') {
    document.getElementById(which + '-custom').style.display = 'none';
  }
}

document.querySelectorAll('.ap-btn').forEach(function (btn) {
  if (btn.id === 'dep-other-btn' || btn.id === 'arr-other-btn') return;
  btn.addEventListener('click', function () {
    setAirport(btn.dataset.for, btn.dataset.code);
  });
});

// ── custom airport input ───────────────────────────────────────
['dep', 'arr'].forEach(function (which) {
  var otherBtn    = document.getElementById(which + '-other-btn');
  var customInput = document.getElementById(which + '-custom');

  otherBtn.addEventListener('click', function () {
    var visible = customInput.style.display !== 'none';
    customInput.style.display = visible ? 'none' : 'block';
    if (!visible) customInput.focus();
  });

  customInput.addEventListener('input', function () {
    var val = customInput.value.replace(/[^A-Za-z]/g, '').toUpperCase();
    customInput.value = val;
    if (val.length >= 3) {
      state[which] = val;
      document.getElementById(which + '-value').textContent = val;
      document.getElementById(which + '-desc').textContent  = '';
      document.querySelectorAll('[data-for="' + which + '"]').forEach(function (b) {
        b.classList.remove('active');
      });
      otherBtn.classList.add('active');
    }
  });
});

// ── swap ───────────────────────────────────────────────────────
document.getElementById('swap-btn').addEventListener('click', function () {
  var depCustom = document.getElementById('dep-custom');
  var arrCustom = document.getElementById('arr-custom');

  var tmpCode = state.dep;
  var tmpVal  = depCustom.value;
  var tmpVis  = depCustom.style.display;

  state.dep = state.arr;
  setAirport('dep', state.dep);

  state.arr = tmpCode;
  setAirport('arr', state.arr);

  depCustom.value         = arrCustom.value;
  depCustom.style.display = arrCustom.style.display;
  arrCustom.value         = tmpVal;
  arrCustom.style.display = tmpVis;
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
  if (cabin === 1) {
    if (price <= 200) return '#22c55e';
    if (price <  300) return '#e2e8f0';
    if (price <= 500) return '#f97316';
    return '#ef4444';
  } else {
    if (price <= 300) return '#22c55e';
    if (price <  500) return '#e2e8f0';
    if (price <= 700) return '#f97316';
    return '#ef4444';
  }
}

// ── render flight card ─────────────────────────────────────────
function renderCard(f) {
  var color     = priceColor(f.price, f.cabin);
  var stopLabel = f.nonstop
    ? 'Nonstop'
    : (f.layovers && f.layovers.length
        ? f.layovers.map(function (lv) { return esc(lv.id); }).join(' \xb7 ')
        : '1 stop');

  var logoHtml = f.logo
    ? '<img class="fcard-logo" src="' + esc(f.logo) + '" alt="' + esc(f.airline) + '">'
    : '<span class="fcard-al-text">' + esc(f.airline) + '</span>';

  var detailHtml = '';
  (f.legs || []).forEach(function (leg, i) {
    var info = [leg.fn, leg.ac, leg.dur].filter(Boolean).join(' \xb7 ');
    detailHtml +=
      '<div class="leg-row">' +
        '<span class="leg-ap">' + esc(leg.dep) + '</span>' +
        '<span class="leg-tm">' + esc(leg.depT) + '</span>' +
        '<span class="leg-arrow">→</span>' +
        '<span class="leg-ap">' + esc(leg.arr) + '</span>' +
        '<span class="leg-tm">' + esc(leg.arrT) + '</span>' +
        '<span class="leg-info">' + esc(info) + '</span>' +
      '</div>';
    if (f.layovers && f.layovers[i]) {
      detailHtml +=
        '<div class="lv-row">Layover \xb7 ' + esc(f.layovers[i].id) + ' \xb7 ' + esc(f.layovers[i].dur) + '</div>';
    }
  });

  return (
    '<div class="fcard">' +
      '<div class="fcard-face">' +
        '<div class="fcard-logo-wrap">' + logoHtml + '</div>' +
        '<div>' +
          '<div class="fcard-airports">' + esc(f.dep) + '<span class="arr">→</span>' + esc(f.arr) + '</div>' +
          '<div class="fcard-times">' + esc(f.depTime) + ' – ' + esc(f.arrTime) + '</div>' +
          '<div class="fcard-meta">' +
            '<span class="stop-pill">' + stopLabel + '</span>' +
            (f.duration ? '<span class="dur-text">' + esc(f.duration) + '</span>' : '') +
          '</div>' +
        '</div>' +
        '<div class="fcard-price" style="color:' + color + '">$' + f.price + '</div>' +
      '</div>' +
      '<div class="fcard-detail">' + detailHtml + '</div>' +
    '</div>'
  );
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
          (econ.map(renderCard).join('') || '<p class="empty">No economy results.</p>') +
        '</div>' +
        '<div>' +
          '<div class="col-head">First Class <span class="col-badge">' + first.length + '</span></div>' +
          (first.map(renderCard).join('') || '<p class="empty">No first class results.</p>') +
        '</div>' +
      '</div>';
  } else {
    var flights = econ.length ? econ  : first;
    var label   = econ.length ? 'Economy' : 'First Class';
    html =
      '<div class="results-single">' +
        '<div class="col-head">' + label + ' <span class="col-badge">' + flights.length + '</span></div>' +
        flights.map(renderCard).join('') +
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
  if (!date)          { alert('Please select a date.');                         return; }
  if (!state.dep)     { alert('Please select a departure airport.');            return; }
  if (!state.arr)     { alert('Please select an arrival airport.');             return; }
  if (state.dep === state.arr) { alert('Departure and arrival cannot be the same.'); return; }

  var routes = expandCount(state.dep) * expandCount(state.arr);
  var cabins = state.cabin === 'all' ? 2 : 1;
  var total  = routes * cabins;
  var secs   = Math.max(15, total * 3);

  var btn = document.getElementById('search-btn');
  btn.disabled    = true;
  btn.textContent = 'Searching…';

  document.getElementById('results').innerHTML =
    '<div class="results-loading">' +
      'Searching ' + routes + ' route' + (routes !== 1 ? 's' : '') +
      ' × ' + cabins + ' cabin' + (cabins !== 1 ? 's' : '') +
      ' (' + total + ' API call' + (total !== 1 ? 's' : '') + ')…' +
      '<br><span style="color:#333">This may take up to ~' + secs + ' seconds.</span>' +
    '</div>';

  var qs = 'dep='   + encodeURIComponent(state.dep)   +
           '&arr='  + encodeURIComponent(state.arr)   +
           '&date=' + encodeURIComponent(date)         +
           '&cabin='+ encodeURIComponent(state.cabin);

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
  console.log('\nFlight search server running at http://localhost:' + PORT + '\n');
});
