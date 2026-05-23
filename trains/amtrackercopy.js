// NEC live tracker — all trains transiting NYP

const http  = require("http");
const https = require("https");

const PORT = 3000;
const TRAINS_URL   = "https://api-v3.amtraker.com/v3/trains";
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

function fetchJSON(url) {
  return new Promise((resolve, reject) => {
    console.log("[fetch]", url);
    const req = https.get(url, { headers: { Accept: "application/json" } }, (res) => {
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error("HTTP " + res.statusCode));
      }
      let raw = "";
      res.setEncoding("utf8");
      res.on("data", c => { raw += c; });
      res.on("end", () => {
        try { console.log("[ok]", url.split("/").pop(), raw.length, "bytes"); resolve(JSON.parse(raw)); }
        catch(e) { reject(new Error("JSON parse: " + e.message)); }
      });
    });
    req.setTimeout(10000, () => { req.destroy(new Error("timeout after 10s")); });
    req.on("error", e => { console.error("[err]", url, e.message); reject(e); });
  });
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
  const cls = diffMin <= 15 ? "minor" : diffMin <= 30 ? "moderate" : "severe";
  return { label: `${h}:${String(m).padStart(2,"0")} late`, class: cls };
}

// Only trains that actually depart FROM NYP — must have stops after NYP
function isRelevantTrain(train) {
  if (!train.stations) return false;
  if (train.trainState === "Completed") return false;
  const nypIdx = train.stations.findIndex(s => s.code === "NYP");
  if (nypIdx === -1) return false;
  if (nypIdx >= train.stations.length - 1) return false; // terminates at NYP, skip
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

// Real final destination: last stop AFTER NYP in the station list
function finalDestName(train, stationMap) {
  const nypIdx = train.stations.findIndex(s => s.code === "NYP");
  const after = nypIdx >= 0 ? train.stations.slice(nypIdx + 1) : [];
  if (after.length > 0) {
    const last = after[after.length - 1];
    return last.name || stationMap[last.code]?.name || last.code;
  }
  return train.destName || "—";
}

// ---------- data shaping ----------

async function getTrainData() {
  const [trainsObj, stations] = await Promise.all([fetchJSON(TRAINS_URL), fetchAllStations()]);
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

    const nypStop    = train.stations.find(s => s.code === "NYP");
    const nypSchDep  = nypStop ? parseTime(nypStop.schDep) : null;
    const nypActDep  = nypStop ? parseTime(nypStop.dep)    : null;
    const finalStop  = train.stations[train.stations.length - 1];
    const finalArr   = parseTime(finalStop.arr) || parseTime(finalStop.schArr);

    return {
      trainNum:   train.trainNum,
      routeName:  train.routeName,
      type:       trainType(train.routeName),
      destName:   finalDestName(train, stations),
      lat:        train.lat,
      lon:        train.lon,
      velocity:   Math.round(train.velocity || 0),
      heading:    train.heading,
      trainState: train.trainState,
      direction:  dir,
      nextStop:   nextStop ? { code: nextStop.code, name: nextStop.name || stations[nextStop.code]?.name || nextStop.code } : null,
      distToNext: distToNext != null ? Math.round(distToNext * 10) / 10 : null,
      bearing,
      status,
      finalArr:   formatTimeShort(finalArr),
      nypSchDep:  nypSchDep ? nypSchDep.toISOString() : null,
      nypActDep:  nypActDep ? nypActDep.toISOString() : null,
      stops: train.stations.map(s => ({
        code:   s.code,
        name:   s.name || stations[s.code]?.name || s.code,
        schArr: s.schArr || null,
        schDep: s.schDep || null,
        arr:    s.arr    || null,
        dep:    s.dep    || null,
      })),
    };
  });
}

// ---------- HTML ----------

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1">
<title>NEC Tracker</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:       #0c0c0c;
  --surface:  #111111;
  --border:   #1e1e1e;
  --text1:    #ffffff;
  --text2:    #aaaaaa;
  --text3:    #555555;
  --acela:    #2dd4bf;
  --regional: #3b82f6;
  --keystone: #eab308;
  --empire:   #4a8c4a;
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
  padding: 13px 18px;
  border-bottom: 1px solid var(--border);
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-shrink: 0;
}
.hdr-left { display: flex; flex-direction: column; gap: 3px; }
.hdr-title {
  font-size: 14px;
  font-weight: 800;
  letter-spacing: 2px;
  color: var(--text1);
  display: flex;
  align-items: center;
  gap: 10px;
}
.hdr-sub { font-size: 11px; color: var(--text3); }

.live-badge {
  font-size: 10px;
  font-weight: 800;
  letter-spacing: 2px;
  color: #ef4444;
  opacity: 0;
  transition: opacity 0.4s;
  display: flex;
  align-items: center;
  gap: 5px;
}
.live-badge.on { opacity: 1; }
.live-dot {
  width: 7px; height: 7px; border-radius: 50%; background: #ef4444; flex-shrink: 0;
}
@keyframes live-blink { 0%,100%{opacity:1} 50%{opacity:0.1} }
.live-badge.on .live-dot { animation: live-blink 1.6s ease-in-out infinite; }
.live-badge.on { animation: live-blink 1.6s ease-in-out infinite; }

.hdr-right { text-align: right; }
.hdr-count { font-size: 26px; font-weight: 800; color: var(--text1); line-height: 1; }
.hdr-active { font-size: 10px; color: var(--text3); letter-spacing: 1px; text-transform: uppercase; margin-top: 2px; }

/* ── Legend ── */
.legend {
  display: flex;
  flex-wrap: wrap;
  gap: 5px 12px;
  padding: 9px 18px;
  border-bottom: 1px solid var(--border);
  flex-shrink: 0;
}
.legend-item { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--text2); }
.dot { width: 7px; height: 7px; border-radius: 50%; flex-shrink: 0; }
.dot.acela       { background: var(--acela); }
.dot.regional    { background: var(--regional); }
.dot.keystone    { background: var(--keystone); }
.dot.empire      { background: var(--empire); }
.dot.longdistance{ background: var(--longdist); }

/* ── Section label ── */
.sec-hdr {
  padding: 11px 18px;
  font-size: 13px;
  font-weight: 700;
  color: #cccccc;
  background: #1a1a1a;
  border-bottom: 1px solid var(--border);
  border-top: 1px solid var(--border);
  flex-shrink: 0;
  letter-spacing: 0.3px;
}

/* ── Departure rows ── */
.dep-row {
  padding: 11px 16px 9px 14px;
  border-left: 3px solid transparent;
  border-bottom: 1px solid var(--border);
}
.dep-row.acela       { border-left-color: var(--acela); }
.dep-row.regional    { border-left-color: var(--regional); }
.dep-row.keystone    { border-left-color: var(--keystone); }
.dep-row.empire      { border-left-color: var(--empire); }
.dep-row.longdistance{ border-left-color: var(--longdist); }
.dep-row.is-departed { opacity: 0.55; }

/* Line 1: time | destination | countdown */
.dep-main {
  display: flex;
  align-items: baseline;
  gap: 8px;
  margin-bottom: 5px;
}
.dep-time {
  font-size: 20px;
  font-weight: 800;
  color: var(--text1);
  white-space: nowrap;
  flex-shrink: 0;
  width: 88px;
  line-height: 1;
}
.dep-dest {
  flex: 1;
  font-size: 14px;
  font-weight: 500;
  color: #d0d0d0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-bottom: 1px;
}
.dep-dest::before { content: "\\2192\\00a0"; color: var(--text3); }
.dep-countdown {
  font-size: 13px;
  font-weight: 700;
  white-space: nowrap;
  flex-shrink: 0;
  color: var(--text3);
  min-width: 72px;
  text-align: right;
}
.dep-countdown.soon    { color: #fbbf24; }
.dep-countdown.departed{ color: var(--text3); font-weight: 400; font-style: italic; font-size: 12px; }
.dep-countdown.finalcall{ color: #fde047; font-weight: 800; font-size: 12px; letter-spacing: 0.5px; }
@keyframes flash-num { 0%,100%{opacity:1} 50%{opacity:0.15} }
.dep-countdown.flash { color: #fbbf24; animation: flash-num 0.75s ease-in-out infinite; }

/* Line 2: route+number | status */
.dep-sub {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding-left: 96px;
}
.dep-train {
  font-size: 11px;
  color: #999999;
  text-transform: uppercase;
  letter-spacing: 1px;
  font-weight: 600;
}

/* ── Status badges ── */
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 700;
  white-space: nowrap;
}
.badge.ontime   { background: rgba(74,222,128,0.12);  color: #4ade80; }
.badge.minor    { background: rgba(234,179,8,0.15);   color: #fbbf24; }
.badge.moderate { background: rgba(251,146,60,0.15);  color: #fb923c; }
.badge.severe   { background: rgba(239,68,68,0.15);   color: #f87171; }
.badge.unknown  { background: rgba(255,255,255,0.05); color: var(--text3); }

/* ── Active train rows ── */
.train-row {
  padding: 11px 16px 9px 14px;
  border-left: 3px solid transparent;
  border-bottom: 1px solid var(--border);
}
.train-row.acela       { border-left-color: var(--acela); }
.train-row.regional    { border-left-color: var(--regional); }
.train-row.keystone    { border-left-color: var(--keystone); }
.train-row.empire      { border-left-color: var(--empire); }
.train-row.longdistance{ border-left-color: var(--longdist); }

.tr-main { display: flex; align-items: baseline; gap: 8px; margin-bottom: 5px; }
.tr-num {
  font-size: 20px;
  font-weight: 800;
  color: var(--text1);
  width: 58px;
  flex-shrink: 0;
  line-height: 1;
}
.tr-dest {
  flex: 1;
  font-size: 14px;
  font-weight: 500;
  color: #d0d0d0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-bottom: 1px;
}
.tr-dest::before { content: "\\2192\\00a0"; color: var(--text3); }
.tr-arr {
  font-size: 12px;
  color: var(--text2);
  white-space: nowrap;
  flex-shrink: 0;
}
.tr-arr em { font-style: normal; font-size: 10px; color: var(--text3); margin-right: 3px; }

.tr-sub { display: flex; align-items: center; justify-content: space-between; padding-left: 66px; }
.tr-meta { font-size: 11px; color: var(--text3); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.tr-meta .sep { margin: 0 4px; opacity: 0.4; }

/* ── Empty ── */
.empty { padding: 14px 18px; font-size: 13px; color: var(--text3); font-style: italic; }

/* ── Leaflet overrides ── */
.leaflet-container { background: #111; }
.train-marker { background: transparent !important; border: none !important; }
.leaflet-popup-content-wrapper, .leaflet-tooltip {
  background: #1a1a1a; color: #fff; border: 1px solid #2a2a2a;
  font-family: inherit; font-size: 12px; border-radius: 6px;
  box-shadow: 0 4px 12px rgba(0,0,0,0.6);
}
.leaflet-popup-tip { background: #1a1a1a; }
.leaflet-tooltip-top:before { border-top-color: #2a2a2a; }
.leaflet-bar a { background: #1a1a1a !important; color: #ddd !important; border-color: #333 !important; }
.leaflet-bar a:hover { background: #2a2a2a !important; }

/* ── Mobile ── */
@media (max-width: 768px) {
  body { overflow: auto; height: auto; }
  #app { flex-direction: column; height: auto; min-height: 100vh; }
  #map { flex: none; height: 42vw; min-height: 180px; max-height: 260px; width: 100%; }
  #sidebar { width: 100%; height: auto; border-left: none; border-top: 1px solid var(--border); }
  .dep-sub { padding-left: 0; }
  .tr-sub  { padding-left: 0; }
}

/* ── Train detail modal ── */
#modal-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0,0,0,0.75);
  z-index: 1000;
  display: flex;
  align-items: flex-end;
  justify-content: center;
}
#modal-overlay.hidden { display: none; }
#modal {
  background: #141414;
  width: 100%;
  max-width: 560px;
  max-height: 88vh;
  border-radius: 14px 14px 0 0;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  box-shadow: 0 -4px 40px rgba(0,0,0,0.8);
}
@media (min-width: 769px) {
  #modal-overlay { align-items: center; }
  #modal { border-radius: 14px; max-height: 82vh; }
}
.modal-hdr {
  padding: 16px 18px 14px;
  border-bottom: 1px solid var(--border);
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  flex-shrink: 0;
}
.modal-hdr-left { display: flex; flex-direction: column; gap: 5px; }
.modal-num  { font-size: 32px; font-weight: 800; color: #fff; line-height: 1; }
.modal-route{ font-size: 12px; font-weight: 600; color: #888; text-transform: uppercase; letter-spacing: 1.5px; }
.modal-meta { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 2px; }
.modal-dest { font-size: 13px; color: #ccc; }
.modal-dest::before { content: "\\2192\\00a0"; color: #555; }
.modal-close {
  background: #2a2a2a;
  border: none;
  color: #aaa;
  font-size: 16px;
  width: 30px; height: 30px;
  border-radius: 50%;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  margin-left: 12px;
}
.modal-close:hover { background: #333; color: #fff; }
.modal-vel { font-size: 12px; color: #666; }

.modal-stops {
  overflow-y: auto;
  -webkit-overflow-scrolling: touch;
  flex: 1;
  padding: 8px 0 20px;
}

.stop-row {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 8px 18px;
  position: relative;
}
.stop-spine {
  display: flex;
  flex-direction: column;
  align-items: center;
  flex-shrink: 0;
  width: 14px;
  margin-top: 3px;
}
.stop-dot {
  width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
}
.stop-line {
  width: 2px;
  flex: 1;
  min-height: 20px;
  background: var(--border);
  margin: 2px 0;
}
.stop-row.past .stop-dot     { background: #444; }
.stop-row.current .stop-dot  { background: #fff; box-shadow: 0 0 8px rgba(255,255,255,0.6); }
.stop-row.upcoming .stop-dot { background: #1e1e1e; border: 2px solid #555; }

.stop-info { flex: 1; min-width: 0; }
.stop-name {
  font-size: 14px;
  color: #bbb;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.stop-row.past .stop-name    { color: #666; font-weight: 400; }
.stop-row.current .stop-name { color: #fff; font-weight: 700; }
.stop-row.upcoming .stop-name{ color: #ddd; font-weight: 700; }

.stop-times { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 3px; }
.stop-time-item { display: flex; gap: 5px; align-items: center; font-size: 11px; }
.stop-time-label{ color: #555; }
.stop-time-val  { color: #999; }
.stop-row.upcoming .stop-time-val { color: #bbb; }
.delta { font-size: 11px; font-weight: 700; }
.delta.late  { color: #fb923c; }
.delta.early { color: #4ade80; }
</style>
</head>
<body>
<div id="app">
  <div id="map"></div>
  <div id="sidebar">

    <div class="hdr">
      <div class="hdr-left">
        <div class="hdr-title">
          NEC TRACKER
          <span class="live-badge" id="live-badge"><span class="live-dot"></span>LIVE</span>
        </div>
        <div class="hdr-sub" id="updated">Loading&hellip;</div>
      </div>
      <div class="hdr-right">
        <div class="hdr-count" id="train-count">&mdash;</div>
        <div class="hdr-active">active</div>
      </div>
    </div>

    <div class="legend">
      <span class="legend-item"><span class="dot acela"></span>Acela</span>
      <span class="legend-item"><span class="dot regional"></span>NE Regional</span>
      <span class="legend-item"><span class="dot keystone"></span>Keystone / Pennsylvanian</span>
      <span class="legend-item"><span class="dot empire"></span>Empire / Maple Leaf</span>
      <span class="legend-item"><span class="dot longdistance"></span>Long Distance</span>
    </div>

    <div class="sec-hdr">New York Penn Departures</div>
    <div id="departures"><div class="empty">Loading&hellip;</div></div>

    <div class="sec-hdr">&#x2193;&nbsp; Southbound</div>
    <div id="southbound"><div class="empty">Loading&hellip;</div></div>

    <div class="sec-hdr">&#x2191;&nbsp; Northbound</div>
    <div id="northbound"><div class="empty">Loading&hellip;</div></div>

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
L.polyline(routeCoords, { color: '#444', weight: 2, opacity: 0.7 }).addTo(map);
map.fitBounds(L.latLngBounds(routeCoords), { padding: [30, 30] });
NEC_STATIONS.forEach(function(s){
  L.circleMarker([s.lat, s.lon], { radius: 3, color: '#777', weight: 1.5, fillColor: '#1a1a1a', fillOpacity: 1 })
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
  return L.divIcon({
    className: 'train-marker',
    html: '<div style="transform:rotate(' + angle + 'deg)">' +
      '<svg width="20" height="20" viewBox="0 0 20 20">' +
        '<polygon points="10,1 17,17 10,13 3,17" fill="' + color + '" stroke="#000" stroke-width="1.5" stroke-linejoin="round"/>' +
      '</svg></div>',
    iconSize: [20,20], iconAnchor: [10,10]
  });
}

function esc(s) {
  return String(s||'').replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}

// ── Countdown formatting ──
function minsUntil(iso) {
  return iso ? Math.round((new Date(iso) - Date.now()) / 60000) : null;
}

function formatCountdown(mins) {
  if (mins === null) return { text: '&mdash;', cls: '' };
  if (mins <= 0)  return { text: 'DEPARTED', cls: 'departed' };
  if (mins <= 3)  return { text: 'FINAL CALL', cls: 'finalcall' };
  if (mins <= 10) return { text: mins + ' min', cls: 'flash' };
  if (mins <= 20) return { text: mins + ' min', cls: 'soon' };
  if (mins < 60)  return { text: mins + ' min', cls: '' };
  if (mins < 300) {
    // round to nearest 30 min
    var rounded = Math.round(mins / 30) * 30;
    var h = rounded / 60; // will be 1, 1.5, 2, 2.5, etc.
    return { text: h + ' hrs', cls: '' };
  }
  return { text: Math.round(mins / 60) + ' hrs', cls: '' };
}

// ── Departure board ──
var depData = [];

function fmtNYP(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'America/New_York'
  });
}

function renderDepartures(trains) {
  var now = Date.now();

  depData = trains.filter(function(t) {
    if (!t.nypSchDep) return false;
    // Always show predeparture trains — even if scheduled time has passed (delayed)
    if (t.trainState === 'Predeparture') return true;
    // Show DEPARTED for up to 3 minutes after actual departure
    if (t.nypActDep) {
      return Math.round((now - new Date(t.nypActDep)) / 60000) < 3;
    }
    return false;
  }).sort(function(a, b) {
    return new Date(a.nypSchDep) - new Date(b.nypSchDep);
  });

  if (!depData.length) {
    document.getElementById('departures').innerHTML = '<div class="empty">No upcoming departures.</div>';
    return;
  }

  document.getElementById('departures').innerHTML = depData.map(function(t) {
    // Only mark as departed if the train has actually left AND is no longer predeparture
    var isDep = !!(t.nypActDep && t.trainState !== 'Predeparture');
    var mins = isDep ? null : minsUntil(t.nypSchDep);
    // Predeparture but past scheduled time = delayed at station, not departed
    var cd = isDep
      ? { text: 'DEPARTED', cls: 'departed' }
      : (mins !== null && mins <= 0)
        ? { text: 'delayed', cls: 'soon' }
        : formatCountdown(mins);

    return (
      '<div class="dep-row ' + t.type + (isDep ? ' is-departed' : '') + '" style="cursor:pointer" onclick="openModal(\'' + esc(t.trainNum) + '\')">' +
        '<div class="dep-main">' +
          '<span class="dep-time">' + fmtNYP(t.nypSchDep) + '</span>' +
          '<span class="dep-dest">' + esc(t.destName) + '</span>' +
          '<span class="dep-countdown ' + cd.cls + '" id="dc-' + esc(t.trainNum) + '">' + cd.text + '</span>' +
        '</div>' +
        '<div class="dep-sub">' +
          '<span class="dep-train">' + esc(t.routeName) + ' #' + esc(t.trainNum) + '</span>' +
          '<span class="badge ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
        '</div>' +
      '</div>'
    );
  }).join('');
}

// Per-second countdown tick
setInterval(function() {
  depData.forEach(function(t) {
    var el = document.getElementById('dc-' + t.trainNum);
    if (!el) return;
    if (t.nypActDep && t.trainState !== 'Predeparture') return; // DEPARTED label is static
    var mins = minsUntil(t.nypSchDep);
    var cd = (mins !== null && mins <= 0)
      ? { text: 'delayed', cls: 'soon' }
      : formatCountdown(mins);
    el.innerHTML = cd.text;
    el.className = 'dep-countdown ' + cd.cls;
  });
}, 1000);

// ── Active train rows ──
function renderCard(t) {
  var meta = '';
  if (t.velocity > 0) meta += t.velocity + ' mph';
  if (t.distToNext != null && t.nextStop) {
    if (meta) meta += '<span class="sep">&middot;</span>';
    meta += t.distToNext.toFixed(1) + ' mi to ' + esc(t.nextStop.name);
  }
  return (
    '<div class="train-row ' + t.type + '" style="cursor:pointer" onclick="openModal(\'' + esc(t.trainNum) + '\')">' +
      '<div class="tr-main">' +
        '<span class="tr-num">' + esc(t.trainNum) + '</span>' +
        '<span class="tr-dest">' + esc(t.destName) + '</span>' +
        (t.finalArr ? '<span class="tr-arr"><em>arr</em>' + esc(t.finalArr) + '</span>' : '') +
      '</div>' +
      '<div class="tr-sub">' +
        '<span class="tr-meta">' + esc(t.routeName) + (meta ? '<span class="sep">&middot;</span>' + meta : '') + '</span>' +
        '<span class="badge ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
      '</div>' +
    '</div>'
  );
}

function tooltipFor(t) {
  var d = t.distToNext != null ? ' &middot; ' + t.distToNext.toFixed(1) + ' mi to ' + esc(t.nextStop.name) : '';
  return '<b>' + esc(t.routeName) + ' #' + esc(t.trainNum) + '</b><br>' + t.status.label + ' &middot; ' + t.velocity + ' mph' + d;
}

// ── LIVE badge ──
var liveTimer = null;
function setLive(ok) {
  var el = document.getElementById('live-badge');
  if (ok) {
    el.classList.add('on');
    clearTimeout(liveTimer);
    liveTimer = setTimeout(function(){ el.classList.remove('on'); }, 50000);
  } else {
    el.classList.remove('on');
  }
}

// ── Main refresh ──
async function refresh() {
  try {
    var res = await fetch('/api/trains');
    var trains = await res.json();
    allTrains = trains;

    trainLayer.clearLayers();
    trains.forEach(function(t) {
      if (t.lat == null || t.lon == null) return;
      L.marker([t.lat, t.lon], { icon: trainIcon(t) })
       .bindTooltip(tooltipFor(t), { direction: 'top', offset: [0,-10] })
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
    setLive(true);

  } catch(e) {
    document.getElementById('updated').textContent = 'Error: ' + e.message;
    setLive(false);
  }
}

refresh();
setInterval(refresh, 30000);

// ── Train detail modal ──
var allTrains = [];

function fmtTime(iso) {
  if (!iso) return null;
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'America/New_York'
  });
}

function deltaLabel(actualIso, schedIso) {
  if (!actualIso || !schedIso) return '';
  var d = Math.round((new Date(actualIso) - new Date(schedIso)) / 60000);
  if (Math.abs(d) <= 1) return '';
  if (d > 0) return '<span class="delta late">+' + d + 'm late</span>';
  return '<span class="delta early">' + Math.abs(d) + 'm early</span>';
}

function renderStopTime(label, timeIso, schedIso) {
  var t = fmtTime(timeIso);
  if (!t) return '';
  return '<span class="stop-time-item"><span class="stop-time-label">' + label + '</span>' +
    '<span class="stop-time-val">' + t + '</span>' +
    deltaLabel(timeIso, schedIso) + '</span>';
}

function openModal(num) {
  var t = allTrains.find(function(x){ return String(x.trainNum) === String(num); });
  if (!t || !t.stops) return;

  var typeClr = { acela:'#2dd4bf', regional:'#3b82f6', keystone:'#eab308', empire:'#4a8c4a', longdistance:'#ef4444' };
  var clr = typeClr[t.type] || '#ef4444';
  var velText = t.velocity > 0 ? '<span class="modal-vel">' + t.velocity + ' mph</span>' : '';

  document.getElementById('modal-header-content').innerHTML =
    '<div class="modal-hdr-left">' +
      '<div style="display:flex;align-items:baseline;gap:10px">' +
        '<span class="modal-num" style="color:' + clr + '">' + esc(t.trainNum) + '</span>' +
        '<span class="modal-route">' + esc(t.routeName) + '</span>' +
      '</div>' +
      '<div class="modal-meta">' +
        '<span class="modal-dest">' + esc(t.destName) + '</span>' +
        '<span class="badge ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
        velText +
      '</div>' +
    '</div>';

  // Determine current position: last stop that has a dep (or arr for terminal)
  var currentIdx = -1;
  for (var i = t.stops.length - 1; i >= 0; i--) {
    if (t.stops[i].dep) { currentIdx = i; break; }
  }
  // If no dep found but arr found, train is at that terminal
  if (currentIdx === -1) {
    for (var i = t.stops.length - 1; i >= 0; i--) {
      if (t.stops[i].arr) { currentIdx = i; break; }
    }
  }

  var html = '';
  t.stops.forEach(function(s, i) {
    var hasDep = !!s.dep;
    var hasArr = !!s.arr;
    var isTerminal = (i === t.stops.length - 1);
    var isPast    = hasDep || (hasArr && isTerminal);
    var isCurrent = hasArr && !hasDep && !isTerminal;
    var isUpcoming= !hasArr && !hasDep;

    var cls = isPast ? 'past' : isCurrent ? 'current' : 'upcoming';
    var isLast = (i === t.stops.length - 1);

    // For upcoming stops: use arr/dep as estimated times if provided, else sch
    var arrDisplay = s.arr || s.schArr;
    var depDisplay = s.dep || s.schDep;
    var arrSch = s.schArr, depSch = s.schDep;

    // For past stops, only show delta vs scheduled
    var arrDelta = (isPast || isCurrent) ? deltaLabel(s.arr, s.schArr) : deltaLabel(s.arr || s.schArr, s.schArr);
    var depDelta = isPast ? deltaLabel(s.dep, s.schDep) : deltaLabel(s.dep || s.schDep, s.schDep);

    var timesHtml = '';
    if (arrDisplay && !isCurrent) timesHtml += renderStopTime('arr', arrDisplay, arrSch);
    if (depDisplay && !isTerminal) timesHtml += renderStopTime('dep', depDisplay, depSch);

    html +=
      '<div class="stop-row ' + cls + '">' +
        '<div class="stop-spine">' +
          '<div class="stop-dot"></div>' +
          (!isLast ? '<div class="stop-line"></div>' : '') +
        '</div>' +
        '<div class="stop-info">' +
          '<div class="stop-name">' + esc(s.name) + '</div>' +
          (timesHtml ? '<div class="stop-times">' + timesHtml + '</div>' : '') +
        '</div>' +
      '</div>';
  });

  document.getElementById('modal-stops').innerHTML = html;
  document.getElementById('modal-overlay').classList.remove('hidden');

  // Scroll to current/first upcoming stop
  setTimeout(function() {
    var rows = document.querySelectorAll('#modal-stops .stop-row');
    var target = document.querySelector('#modal-stops .stop-row.current') ||
                 document.querySelector('#modal-stops .stop-row.upcoming');
    if (target) target.scrollIntoView({ block: 'center' });
  }, 50);
}

function closeModal() {
  document.getElementById('modal-overlay').classList.add('hidden');
}
</script>

<div id="modal-overlay" class="hidden" onclick="if(event.target===this)closeModal()">
  <div id="modal">
    <div class="modal-hdr">
      <div id="modal-header-content"></div>
      <button class="modal-close" onclick="closeModal()">&#x2715;</button>
    </div>
    <div class="modal-stops" id="modal-stops"></div>
  </div>
</div>
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
  if (req.url === "/api/test") {
    const t0 = Date.now();
    try {
      const data = await fetchJSON(TRAINS_URL);
      const n = Object.keys(data).length;
      res.writeHead(200, { "Content-Type": "text/plain" });
      res.end("OK — got " + n + " trains in " + (Date.now()-t0) + "ms");
    } catch(e) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("FAIL: " + e.message + " (after " + (Date.now()-t0) + "ms)");
    }
    return;
  }
  res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
  res.end(htmlPage);
});

server.listen(PORT, () => {
  console.log(`\nNEC tracker running at http://localhost:${PORT}\n`);
});
