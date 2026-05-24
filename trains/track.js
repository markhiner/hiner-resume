// NEC live tracker — all trains transiting NYP
// Run with Node 18+ (built-in fetch).

const http = require("http");

const PORT = 3000;
const TRAINS_URL = "https://api-v3.amtraker.com/v3/trains";
const STATIONS_URL = "https://api-v3.amtraker.com/v3/stations";

const NEC_STATIONS = [
  { code: "NYP", name: "New York Penn",        lat: 40.7510, lon: -73.9963 },
  { code: "NWK", name: "Newark Penn",          lat: 40.7347, lon: -74.1647 },
  { code: "EWR", name: "Newark Airport",       lat: 40.6966, lon: -74.1823 },
  { code: "MET", name: "Metropark",            lat: 40.5681, lon: -74.3296 },
  { code: "NBK", name: "New Brunswick",        lat: 40.4965, lon: -74.4463 },
  { code: "PJC", name: "Princeton Junction",   lat: 40.3159, lon: -74.6240 },
  { code: "TRE", name: "Trenton",              lat: 40.2190, lon: -74.7544 },
  { code: "PHL", name: "Philadelphia 30th St", lat: 39.9556, lon: -75.1810 },
  { code: "WIL", name: "Wilmington",           lat: 39.7373, lon: -75.5511 },
  { code: "ABE", name: "Aberdeen",             lat: 39.5084, lon: -76.1633 },
  { code: "BAL", name: "Baltimore Penn",       lat: 39.3073, lon: -76.6157 },
  { code: "BWI", name: "BWI Airport",          lat: 39.1924, lon: -76.6943 },
  { code: "NCR", name: "New Carrollton",       lat: 38.9481, lon: -76.8715 },
  { code: "WAS", name: "Washington Union",     lat: 38.8970, lon: -77.0064 },
];

// ---------- API ----------

async function fetchJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${url} → ${res.status} ${res.statusText}`);
  return res.json();
}

let stationsCache = null, stationsCacheTime = 0;
async function fetchAllStations() {
  if (stationsCache && Date.now() - stationsCacheTime < 3600000) return stationsCache;
  stationsCache = await fetchJSON(STATIONS_URL);
  stationsCacheTime = Date.now();
  return stationsCache;
}

// ---------- helpers ----------

function parseTime(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  return isNaN(d) ? null : d;
}

function haversineMi(lat1, lon1, lat2, lon2) {
  const R = 3959, toRad = d => d * Math.PI / 180;
  const dLat = toRad(lat2 - lat1), dLon = toRad(lon2 - lon1);
  const a = Math.sin(dLat/2)**2 + Math.cos(toRad(lat1))*Math.cos(toRad(lat2))*Math.sin(dLon/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function bearingDeg(lat1, lon1, lat2, lon2) {
  const toRad = d => d * Math.PI / 180, toDeg = r => r * 180 / Math.PI;
  const φ1 = toRad(lat1), φ2 = toRad(lat2), Δλ = toRad(lon2 - lon1);
  return (toDeg(Math.atan2(Math.sin(Δλ)*Math.cos(φ2), Math.cos(φ1)*Math.sin(φ2) - Math.sin(φ1)*Math.cos(φ2)*Math.cos(Δλ))) + 360) % 360;
}

function trainType(routeName) {
  if (routeName === "Acela") return "acela";
  if (routeName === "Northeast Regional") return "regional";
  if (routeName === "Keystone" || routeName === "Pennsylvanian") return "keystone";
  if (["Empire Service", "Maple Leaf", "Lake Shore Limited", "Adirondack", "Ethan Allen Express"].includes(routeName)) return "empire";
  return "longdistance";
}

function statusInfo(scheduled, actual) {
  if (!scheduled || !actual) return { label: "—", class: "unknown" };
  const diffMin = Math.round((actual - scheduled) / 60000);
  if (diffMin <= 5) return { label: "On Time", class: "ontime" };
  const h = Math.floor(diffMin / 60), m = diffMin % 60;
  const label = `${h}:${String(m).padStart(2, "0")} late`;
  const cls = diffMin <= 15 ? "minor" : diffMin <= 30 ? "moderate" : "severe";
  return { label, class: cls };
}

function isRelevantTrain(train) {
  if (!train.stations) return false;
  if (train.trainState === "Completed") return false;
  const nypIdx = train.stations.findIndex(s => s.code === "NYP");
  if (nypIdx < 0) return false;
  if (nypIdx === train.stations.length - 1) return false; // terminates at NYP, not departing
  const nypStop = train.stations[nypIdx];
  return !!(parseTime(nypStop.schDep) || parseTime(nypStop.dep));
}

function directionFromNYP(train) {
  const codes = train.stations.map(s => s.code);
  const nypIdx = codes.indexOf("NYP");
  const wasIdx = codes.indexOf("WAS");
  if (wasIdx !== -1) return nypIdx < wasIdx ? "south" : "north";
  const northRoutes = ["Empire Service", "Maple Leaf", "Lake Shore Limited", "Adirondack", "Ethan Allen Express"];
  if (northRoutes.includes(train.routeName)) return "north";
  return "south";
}

function findNextStop(train) {
  if (!train.stations) return null;
  const now = new Date();
  for (const stop of train.stations) {
    const dep = parseTime(stop.dep) || parseTime(stop.schDep);
    if (dep && dep < now) continue;
    return stop;
  }
  return train.stations[train.stations.length - 1];
}

function formatTimeShort(date) {
  if (!date) return null;
  return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" });
}

// Find the real final destination — the last stop after NYP that isn't NYP itself
function finalDestName(train, stationMap) {
  const nypIdx = train.stations.findIndex(s => s.code === "NYP");
  const stopsAfterNYP = nypIdx >= 0 ? train.stations.slice(nypIdx + 1) : [];
  if (stopsAfterNYP.length > 0) {
    const last = stopsAfterNYP[stopsAfterNYP.length - 1];
    return last.name || stationMap[last.code]?.name || last.code;
  }
  return train.destName || "—";
}

// ---------- data shaping ----------

async function getTrainData() {
  const [trainsObj, stations] = await Promise.all([
    fetchJSON(TRAINS_URL),
    fetchAllStations(),
  ]);
  const relevant = Object.values(trainsObj).flat().filter(isRelevantTrain);

  return relevant.map(train => {
    const dir = directionFromNYP(train);
    const nextStop = findNextStop(train);

    let status = { label: "—", class: "unknown" };
    if (nextStop) {
      status = statusInfo(
        parseTime(nextStop.schArr) || parseTime(nextStop.schDep),
        parseTime(nextStop.arr)    || parseTime(nextStop.dep)
      );
    }

    let distToNext = null, bearing = null;
    if (nextStop && train.lat != null && train.lon != null) {
      const sd = stations[nextStop.code];
      if (sd && sd.lat && sd.lon) {
        distToNext = haversineMi(train.lat, train.lon, sd.lat, sd.lon);
        bearing = bearingDeg(train.lat, train.lon, sd.lat, sd.lon);
      }
    }

    const nypStop = train.stations.find(s => s.code === "NYP");
    const nypSchDepTime = nypStop ? parseTime(nypStop.schDep) : null;
    const nypActDepTime = nypStop ? parseTime(nypStop.dep) : null;

    // Arrival time at final destination
    const finalStop = train.stations[train.stations.length - 1];
    const finalArrTime = parseTime(finalStop.arr) || parseTime(finalStop.schArr);

    return {
      trainNum:    train.trainNum,
      trainID:     train.trainID,
      routeName:   train.routeName,
      type:        trainType(train.routeName),
      destName:    finalDestName(train, stations),
      lat:         train.lat,
      lon:         train.lon,
      velocity:    Math.round(train.velocity || 0),
      heading:     train.heading,
      trainState:  train.trainState,
      direction:   dir,
      nextStop:    nextStop ? {
        code: nextStop.code,
        name: nextStop.name || stations[nextStop.code]?.name || nextStop.code,
      } : null,
      distToNext:  distToNext != null ? Math.round(distToNext * 10) / 10 : null,
      bearing,
      status,
      finalArr:    formatTimeShort(finalArrTime),
      nypSchDep:   nypSchDepTime ? nypSchDepTime.toISOString() : null,
      nypActDep:   nypActDepTime ? nypActDepTime.toISOString() : null,
    };
  });
}

// ---------- HTML ----------

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>NYP Live Tracker</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:       #0c0c0c;
  --surface:  #111111;
  --border:   #1e1e1e;
  --text1:    #ffffff;
  --text2:    #999999;
  --text3:    #555555;
  --acela:    #2dd4bf;
  --regional: #3b82f6;
  --keystone: #eab308;
  --empire:   #22c55e;
  --longdist: #ef4444;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Helvetica Neue", sans-serif;
  background: var(--bg);
  color: var(--text1);
  overflow: hidden;
  height: 100vh;
  -webkit-font-smoothing: antialiased;
}

#app { display: flex; height: 100vh; }
#map { flex: 1; height: 100%; }
#sidebar {
  width: 440px;
  height: 100%;
  background: var(--surface);
  border-left: 1px solid var(--border);
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
  display: flex;
  flex-direction: column;
}

/* ── Header ── */
.hdr {
  padding: 14px 18px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: flex-end;
  flex-shrink: 0;
}
.hdr-title { font-size: 13px; font-weight: 700; letter-spacing: 2px; color: var(--text1); display: flex; align-items: center; gap: 8px; }
.live-badge {
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 1.5px;
  color: #ef4444;
  animation: pulse-live 1.4s ease-in-out infinite;
}
@keyframes pulse-live {
  0%, 100% { opacity: 1; }
  50%       { opacity: 0; }
}
.hdr-sub   { font-size: 11px; color: var(--text3); margin-top: 2px; }
.hdr-count { font-size: 22px; font-weight: 800; color: var(--text1); line-height: 1; }
.hdr-count span { font-size: 11px; font-weight: 400; color: var(--text3); margin-left: 2px; }

/* ── Legend ── */
.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 5px 12px;
  padding: 10px 18px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
.legend-item { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text2); }
.dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.dot.acela      { background: var(--acela); }
.dot.regional   { background: var(--regional); }
.dot.keystone   { background: var(--keystone); }
.dot.empire     { background: var(--empire); }
.dot.longdistance { background: var(--longdist); }

/* ── Section headers ── */
.sec-hdr {
  padding: 12px 18px 8px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 2.5px;
  color: #777777;
  text-transform: uppercase;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
.sec-hdr.dep-hdr { background: #0f0f0f; }

/* ── Departure rows ── */
.dep-list { border-bottom: 1px solid var(--border); }

.dep-row {
  display: flex;
  flex-direction: row;
  align-items: center;
  padding: 12px 18px 12px 14px;
  border-left: 3px solid transparent;
  border-bottom: 1px solid var(--border);
  gap: 12px;
}
.dep-row:last-child { border-bottom: none; }
.dep-row.acela      { border-left-color: var(--acela); background: rgba(45,212,191,0.11); }
.dep-row.regional   { border-left-color: var(--regional); }
.dep-row.keystone   { border-left-color: var(--keystone); }
.dep-row.empire     { border-left-color: var(--empire); }
.dep-row.longdistance { border-left-color: var(--longdist); }

.dep-left {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 5px;
  flex-shrink: 0;
  min-width: 82px;
}
.dep-time {
  font-size: 20px;
  font-weight: 800;
  color: var(--text1);
  line-height: 1;
  white-space: nowrap;
}
.dep-mid {
  flex: 1;
  display: flex;
  flex-direction: column;
  gap: 4px;
  min-width: 0;
}
.dep-dest {
  font-size: 17px;
  font-weight: 500;
  color: #cccccc;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.dep-dest::before { content: "\\2192\\00a0"; color: var(--text3); font-size: 13px; }
.dep-route {
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 1.2px;
  color: #888888;
  text-transform: uppercase;
  white-space: nowrap;
}
.dep-right {
  flex-shrink: 0;
  text-align: right;
}
.dep-countdown {
  font-size: 19px;
  font-weight: 800;
  color: var(--text3);
  white-space: nowrap;
}
.dep-countdown.soon     { color: #fbbf24; }
.dep-countdown.lastcall { color: #fde047; }
.dep-countdown.departed { font-size: 12px; font-weight: 400; font-style: italic; color: var(--text3); }
.dep-list.collapsed .dep-row:nth-child(n+5) { display: none; }
.dep-more {
  padding: 10px 18px;
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 1.5px;
  color: var(--text2);
  cursor: pointer;
  text-align: center;
  border-bottom: 1px solid var(--border);
  text-transform: uppercase;
}
.dep-more:hover { background: #1a1a1a; color: var(--text1); }

/* ── Badges ── */
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 700;
  white-space: nowrap;
  letter-spacing: 0.3px;
}
.badge.ontime   { background: rgba(74,222,128,0.12); color: #4ade80; }
.badge.minor    { background: rgba(234,179,8,0.15);  color: #eab308; }
.badge.moderate { background: rgba(251,146,60,0.15); color: #fb923c; }
.badge.severe   { background: rgba(239,68,68,0.15);  color: #f87171; }
.badge.unknown  { background: rgba(255,255,255,0.06); color: var(--text3); }
.badge.countdown       { background: rgba(255,255,255,0.06); color: var(--text3); }
.badge.countdown.soon  { background: rgba(234,179,8,0.18);  color: #fbbf24; }
.badge.countdown.lastcall { background: rgba(234,179,8,0.22); color: #fde047; font-size: 12px; letter-spacing: 0.8px; }
.badge.countdown.departed { background: transparent; color: var(--text3); font-style: italic; font-weight: 400; }

/* ── Active train rows ── */
.active-list {}

.train-row {
  display: flex;
  flex-direction: column;
  padding: 10px 18px 10px 14px;
  border-left: 3px solid transparent;
  border-bottom: 1px solid var(--border);
  gap: 5px;
}
.train-row:last-child { border-bottom: none; }
.train-row.acela      { border-left-color: var(--acela); background: rgba(45,212,191,0.11); }
.train-row.regional   { border-left-color: var(--regional); }
.train-row.keystone   { border-left-color: var(--keystone); }
.train-row.empire     { border-left-color: var(--empire); }
.train-row.longdistance { border-left-color: var(--longdist); }

.tr-top {
  display: flex;
  align-items: center;
  gap: 10px;
}
.tr-num {
  font-size: 22px;
  font-weight: 800;
  color: var(--text1);
  line-height: 1;
  min-width: 48px;
  flex-shrink: 0;
}
.tr-mid {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 3px;
}
.tr-head {
  display: flex;
  align-items: baseline;
  gap: 6px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tr-route {
  font-size: 13px;
  font-weight: 800;
  color: var(--text1);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  flex-shrink: 0;
}
.tr-arrow { color: var(--text3); font-size: 11px; flex-shrink: 0; }
.tr-dest {
  font-size: 14px;
  font-weight: 400;
  color: #cccccc;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tr-meta {
  font-size: 11px;
  color: var(--text3);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tr-right {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
}
.tr-speed {
  font-size: 14px;
  font-weight: 700;
  color: var(--text2);
  white-space: nowrap;
}
.tr-speed.fast { color: #fbbf24; }

/* ── Empty state ── */
.empty {
  padding: 16px 18px;
  font-size: 13px;
  color: var(--text3);
  font-style: italic;
}

/* ── Leaflet ── */
.leaflet-container { background: #111; }
.train-marker { background: transparent !important; border: none !important; }
.leaflet-popup-content-wrapper,
.leaflet-tooltip {
  background: #1a1a1a;
  color: #fff;
  border: 1px solid #2a2a2a;
  font-family: inherit;
  font-size: 12px;
  border-radius: 6px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.5);
}
.leaflet-popup-tip { background: #1a1a1a; }
.leaflet-tooltip-top:before { border-top-color: #2a2a2a; }
.leaflet-bar a { background: #1a1a1a; color: #fff; border-color: #333; }
.leaflet-bar a:hover { background: #2a2a2a; }

/* ── Mobile ── */
@media (max-width: 768px) {
  body { overflow: auto; height: auto; }
  #app { flex-direction: column; height: auto; min-height: 100vh; }
  #map { flex: none; height: 42vw; min-height: 180px; max-height: 260px; width: 100%; }
  #sidebar { width: 100%; height: auto; border-left: none; border-top: 1px solid var(--border); }
  .dep-num, .tr-num { font-size: 20px; min-width: 52px; }
  .dep-bot { padding-left: 52px; }
  .tr-bot  { padding-left: 52px; }
}
</style>
</head>
<body>
<div id="app">
  <div id="map"></div>
  <div id="sidebar">
    <div class="hdr">
      <div>
        <div class="hdr-title">NORTHEAST CORRIDOR <span class="live-badge">LIVE</span></div>
        <div class="hdr-sub" id="updated">Loading&hellip;</div>
      </div>
      <div class="hdr-count"><span id="train-count">—</span><span>active</span></div>
    </div>
    <div class="legend">
      <span class="legend-item"><span class="dot acela"></span>Acela</span>
      <span class="legend-item"><span class="dot regional"></span>NE Regional</span>
      <span class="legend-item"><span class="dot keystone"></span>Keystone</span>
      <span class="legend-item"><span class="dot empire"></span>Empire</span>
      <span class="legend-item"><span class="dot longdistance"></span>Long Distance</span>
    </div>

    <div class="sec-hdr dep-hdr">New York Departures</div>
    <div class="dep-list" id="departures"><div class="empty">Loading&hellip;</div></div>

    <div class="sec-hdr">&#x2193; Southbound</div>
    <div class="active-list" id="southbound"><div class="empty">Loading&hellip;</div></div>

    <div class="sec-hdr">&#x2191; Northbound</div>
    <div class="active-list" id="northbound"><div class="empty">Loading&hellip;</div></div>
  </div>
</div>
<script>
var NEC_STATIONS = ${JSON.stringify(NEC_STATIONS)};

// ── Map ──
var map = L.map('map', { zoomControl: true });
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  attribution: '&copy; OpenStreetMap &copy; CARTO', maxZoom: 18, subdomains: 'abcd'
}).addTo(map);
var routeCoords = NEC_STATIONS.map(function(s){ return [s.lat, s.lon]; });
L.polyline(routeCoords, { color: '#444', weight: 2, opacity: 0.6 }).addTo(map);
map.fitBounds(L.latLngBounds(routeCoords), { padding: [30, 30] });
NEC_STATIONS.forEach(function(s){
  L.circleMarker([s.lat, s.lon], { radius: 3, color: '#888', weight: 1.5, fillColor: '#1a1a1a', fillOpacity: 1 })
   .bindTooltip(s.name, { direction: 'top', offset: [0, -4] }).addTo(map);
});
var trainLayer = L.layerGroup().addTo(map);
var HEADING = { N:0, NE:45, E:90, SE:135, S:180, SW:225, W:270, NW:315 };

function typeColor(t) {
  return { acela:'#2dd4bf', regional:'#3b82f6', keystone:'#eab308', empire:'#4a8c4a', longdistance:'#ef4444' }[t] || '#ef4444';
}
function trainIcon(t) {
  var color = typeColor(t.type);
  var angle = t.bearing != null ? t.bearing : (HEADING[t.heading] || 0);
  var svg = '<svg width="20" height="20" viewBox="0 0 20 20"><polygon points="10,1 17,17 10,13 3,17" fill="' + color + '" stroke="#000" stroke-width="1.5" stroke-linejoin="round"/></svg>';
  return L.divIcon({ className: 'train-marker', html: '<div style="transform:rotate(' + angle + 'deg)">' + svg + '</div>', iconSize:[20,20], iconAnchor:[10,10] });
}

function esc(s) {
  return String(s||'').replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}

// ── Departure board ──
var depData = [];
var depExpanded = false;

function minsUntil(iso) { return iso ? Math.round((new Date(iso) - Date.now()) / 60000) : null; }

function fmtMins(m) {
  if (m > 300) {
    var hrs = Math.round(m / 60);
    return hrs + ' hr' + (hrs === 1 ? '' : 's');
  }
  if (m > 60) {
    var half = Math.round(m / 30);
    var val = half / 2;
    return val + ' hr' + (val === 1 ? '' : 's');
  }
  return m + ' min';
}

function depBadgeState(t) {
  if (t.nypActDep && new Date(t.nypActDep) <= Date.now()) return { label: 'DEPARTED', cls: 'departed' };
  var m = minsUntil(t.nypSchDep);
  if (m === null || m < -2) return { label: 'DEPARTED', cls: 'departed' };
  if (m <= 2)  return { label: 'LAST CALL', cls: 'lastcall' };
  if (m <= 20) return { label: fmtMins(m), cls: 'soon' };
  return { label: fmtMins(m), cls: '' };
}

function fmtNYP(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('en-US', { hour:'numeric', minute:'2-digit', hour12:true, timeZone:'America/New_York' });
}

function renderDepartures(trains) {
  depData = trains.filter(function(t) {
    if (!t.nypSchDep) return false;
    if (t.nypActDep) return Math.round((Date.now() - new Date(t.nypActDep)) / 60000) < 10;
    var m = minsUntil(t.nypSchDep);
    return m !== null && m >= -1 && m <= 240;
  }).sort(function(a, b) { return new Date(a.nypSchDep) - new Date(b.nypSchDep); });

  var list = document.getElementById('departures');
  var moreBtn = document.getElementById('dep-more-btn');

  if (!depData.length) {
    list.innerHTML = '<div class="empty">No upcoming departures.</div>';
    list.classList.remove('collapsed');
    if (moreBtn) moreBtn.remove();
    return;
  }

  list.innerHTML = depData.map(function(t) {
    var b = depBadgeState(t);
    return (
      '<div class="dep-row ' + t.type + '">' +
        '<div class="dep-left">' +
          '<span class="dep-time">' + fmtNYP(t.nypSchDep) + '</span>' +
          '<span class="badge ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
        '</div>' +
        '<div class="dep-mid">' +
          '<span class="dep-dest">' + esc(t.destName) + '</span>' +
          '<span class="dep-route">' + esc(t.routeName) + '</span>' +
        '</div>' +
        '<div class="dep-right">' +
          '<span class="dep-countdown ' + b.cls + '" id="db-' + esc(t.trainNum) + '">' + b.label + '</span>' +
        '</div>' +
      '</div>'
    );
  }).join('');

  if (depData.length > 4) {
    if (depExpanded) {
      list.classList.remove('collapsed');
    } else {
      list.classList.add('collapsed');
    }
    if (!moreBtn) {
      moreBtn = document.createElement('div');
      moreBtn.id = 'dep-more-btn';
      moreBtn.className = 'dep-more';
      list.insertAdjacentElement('afterend', moreBtn);
    }
    if (depExpanded) {
      moreBtn.textContent = 'Show less';
      moreBtn.onclick = function() { depExpanded = false; renderDepartures(depData); };
    } else {
      moreBtn.textContent = '+ ' + (depData.length - 4) + ' more';
      moreBtn.onclick = function() { depExpanded = true; renderDepartures(depData); };
    }
  } else {
    list.classList.remove('collapsed');
    if (moreBtn) moreBtn.remove();
  }
}

// Per-second countdown
setInterval(function() {
  depData.forEach(function(t) {
    var el = document.getElementById('db-' + t.trainNum);
    if (!el) return;
    var b = depBadgeState(t);
    el.textContent = b.label;
    el.className = 'dep-countdown ' + b.cls;
  });
}, 1000);

function abbrevRoute(name) {
  return name === 'Northeast Regional' ? 'NE Regional' : name;
}

// ── Active train cards ──
function renderCard(t) {
  var speedCls = t.velocity > 100 ? ' fast' : '';
  var speedTxt = t.velocity > 0 ? t.velocity + ' mph' : '';
  var subMeta = t.distToNext != null && t.nextStop
    ? t.distToNext.toFixed(1) + ' mi to ' + esc(t.nextStop.name)
    : '';
  return (
    '<div class="train-row ' + t.type + '">' +
      '<div class="tr-top">' +
        '<span class="tr-num">' + esc(t.trainNum) + '</span>' +
        '<div class="tr-mid">' +
          '<div class="tr-head">' +
            '<span class="tr-route">' + esc(abbrevRoute(t.routeName)) + '</span>' +
            '<span class="tr-arrow">&#x25BA;</span>' +
            '<span class="tr-dest">' + esc(t.destName) + '</span>' +
          '</div>' +
          (subMeta ? '<div class="tr-meta">' + subMeta + '</div>' : '') +
        '</div>' +
        '<div class="tr-right">' +
          (speedTxt ? '<span class="tr-speed' + speedCls + '">' + speedTxt + '</span>' : '') +
          '<span class="badge ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
        '</div>' +
      '</div>' +
    '</div>'
  );
}

function tooltipFor(t) {
  var d = t.distToNext != null ? ' &middot; ' + t.distToNext.toFixed(1) + ' mi to ' + esc(t.nextStop.name) : '';
  return '<b>' + esc(t.routeName) + ' #' + esc(t.trainNum) + '</b><br>' + t.status.label + ' &middot; ' + t.velocity + ' mph' + d;
}

// ── Main refresh ──
async function refresh() {
  try {
    var res = await fetch('/api/trains');
    var trains = await res.json();

    trainLayer.clearLayers();
    trains.forEach(function(t) {
      if (t.lat == null || t.lon == null) return;
      L.marker([t.lat, t.lon], { icon: trainIcon(t) })
       .bindTooltip(tooltipFor(t), { direction: 'top', offset: [0, -10] })
       .addTo(trainLayer);
    });

    renderDepartures(trains);

    var active = trains.filter(function(t) {
      if (t.trainState === 'Predeparture') return false;
      if (t.nypActDep) return Math.round((Date.now() - new Date(t.nypActDep)) / 60000) >= 3;
      return t.trainState === 'Active';
    });

    var south = active.filter(function(t){ return t.direction === 'south'; })
      .sort(function(a,b){ return new Date(a.nypSchDep||0) - new Date(b.nypSchDep||0); });
    var north = active.filter(function(t){ return t.direction === 'north'; })
      .sort(function(a,b){ return new Date(a.nypSchDep||0) - new Date(b.nypSchDep||0); });

    document.getElementById('southbound').innerHTML =
      south.length ? south.map(renderCard).join('') : '<div class="empty">No trains en route.</div>';
    document.getElementById('northbound').innerHTML =
      north.length ? north.map(renderCard).join('') : '<div class="empty">No trains en route.</div>';

    document.getElementById('train-count').textContent = trains.length;
    document.getElementById('updated').textContent = 'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('updated').textContent = 'Error: ' + e.message;
  }
}
refresh();
setInterval(refresh, 30000);
</script>
</body>
</html>`;

// ---------- server ----------

const server = http.createServer(async (req, res) => {
  if (req.url === "/favicon.ico") { res.writeHead(204); res.end(); return; }
  if (req.url === "/api/trains") {
    try {
      const data = await getTrainData();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (err) {
      console.error("API error:", err.message);
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("Error: " + err.message);
    }
    return;
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(htmlPage);
});

server.listen(PORT, () => {
  console.log(`\nNYP tracker running at http://localhost:${PORT}\n`);
});
