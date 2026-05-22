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

// Any non-completed train that stops at NYP and departs from it
function isRelevantTrain(train) {
  if (!train.stations) return false;
  if (train.trainState === "Completed") return false;
  const nypStop = train.stations.find(s => s.code === "NYP");
  if (!nypStop) return false;
  return !!(parseTime(nypStop.schDep) || parseTime(nypStop.dep));
}

// Direction of travel FROM NYP
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

function cardTime(train) {
  const nypStop = train.stations ? train.stations.find(s => s.code === "NYP") : null;
  if (train.trainState === "Predeparture") {
    const t = nypStop ? (parseTime(nypStop.dep) || parseTime(nypStop.schDep)) : null;
    return { time: t, label: "Departs" };
  }
  const final = train.stations[train.stations.length - 1];
  const t = parseTime(final.arr) || parseTime(final.schArr);
  return { time: t, label: "Arrives" };
}

function formatTimeShort(date) {
  if (!date) return null;
  return date.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit", hour12: true, timeZone: "America/New_York" });
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
    const ct = cardTime(train);

    return {
      trainNum:   train.trainNum,
      trainID:    train.trainID,
      routeName:  train.routeName,
      type:       trainType(train.routeName),
      origName:   train.origName,
      destName:   train.destName,
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
      cardTime:   formatTimeShort(ct.time),
      cardLabel:  ct.label,
      nypSchDep:  nypSchDepTime ? nypSchDepTime.toISOString() : null,
      nypActDep:  nypActDepTime ? nypActDepTime.toISOString() : null,
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
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Inter", sans-serif; background: #0a0a0a; color: #f0f0f0; overflow: hidden; height: 100vh; }
#app { display: flex; height: 100vh; }
#map { flex: 1; height: 100%; background: #1a1a1a; }
#sidebar { width: 460px; height: 100%; background: #111; border-left: 1px solid #2a2a2a; overflow-y: auto; -webkit-overflow-scrolling: touch; }

.header { padding: 13px 16px; border-bottom: 1px solid #2a2a2a; }
.header h1 { font-size: 14px; font-weight: 700; letter-spacing: 1.5px; }
.header .updated { font-size: 11px; color: #777; margin-top: 3px; }

.legend { display: flex; gap: 12px; padding: 9px 16px; border-bottom: 1px solid #2a2a2a; font-size: 11px; flex-wrap: wrap; }
.legend-item { display: flex; align-items: center; gap: 5px; color: #bbb; }
.dot { width: 9px; height: 9px; border-radius: 50%; display: inline-block; flex-shrink: 0; }
.dot.acela       { background: #40E0D0; }
.dot.regional    { background: #1E90FF; }
.dot.keystone    { background: #FFD700; }
.dot.empire      { background: #4A8C4A; }
.dot.longdistance{ background: #DC143C; }

/* ── Departure Board ── */
.dep-board { padding: 12px 16px; border-bottom: 2px solid #2a2a2a; }
.dep-board h2 { font-size: 11px; font-weight: 700; letter-spacing: 2px; color: #999; margin-bottom: 10px; text-transform: uppercase; }

.dep-item { background: #181818; border-radius: 4px; margin-bottom: 7px; display: flex; overflow: hidden; align-items: stretch; transition: background 0.3s; }
.dep-flair { width: 5px; flex-shrink: 0; }
.dep-item.acela        .dep-flair { background: #40E0D0; }
.dep-item.regional     .dep-flair { background: #1E90FF; }
.dep-item.keystone     .dep-flair { background: #FFD700; }
.dep-item.empire       .dep-flair { background: #4A8C4A; }
.dep-item.longdistance .dep-flair { background: #DC143C; }

.dep-body { flex: 1; padding: 8px 11px; display: flex; align-items: center; gap: 8px; flex-wrap: wrap; min-width: 0; }
.dep-id { display: flex; flex-direction: column; min-width: 48px; }
.dep-num   { font-size: 18px; font-weight: 800; color: #fff; line-height: 1.1; }
.dep-route { font-size: 9px; color: #666; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; margin-top: 1px; white-space: nowrap; }
.dep-dest  { font-size: 12px; color: #bbb; flex: 1; min-width: 70px; }
.dep-dest b { color: #666; margin-right: 3px; font-weight: 400; }
.dep-time  { font-size: 13px; font-weight: 700; color: #f0f0f0; white-space: nowrap; }

.dep-badge { padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; white-space: nowrap; }
.dep-badge.normal   { background: #222; color: #666; }
.dep-badge.soon     { background: #3a3200; color: #ffd700; }
.dep-badge.lastcall { background: #4a3c00; color: #ffd700; font-size: 12px; letter-spacing: 0.5px; }
.dep-badge.departed { background: #1e1e1e; color: #555; font-style: italic; }

/* ── Train cards ── */
.section { padding: 11px 16px; }
.section h2 { font-size: 11px; font-weight: 700; letter-spacing: 2px; color: #888; margin-bottom: 9px; text-transform: uppercase; }
.section h2 .arrow { color: #444; margin-right: 5px; }

.train { background: #181818; border-radius: 4px; margin-bottom: 7px; display: flex; overflow: hidden; }
.train-flair { width: 13px; flex-shrink: 0; }
.train.acela        .train-flair { background: #40E0D0; }
.train.regional     .train-flair { background: #1E90FF; }
.train.keystone     .train-flair { background: #FFD700; }
.train.empire       .train-flair { background: #4A8C4A; }
.train.longdistance .train-flair { background: #DC143C; }

.train-body { flex: 1; padding: 8px 12px; min-width: 0; }
.train-row1 { display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px; gap: 8px; }
.train-id   { display: flex; align-items: baseline; gap: 7px; min-width: 0; flex-wrap: wrap; }
.train-num  { font-size: 20px; font-weight: 800; color: #fff; letter-spacing: 0.5px; line-height: 1; }
.train-route{ font-size: 11px; color: #777; text-transform: uppercase; letter-spacing: 1px; font-weight: 600; }
.train-row2 { display: flex; gap: 11px; font-size: 12px; color: #aaa; align-items: center; flex-wrap: wrap; }
.train-time { color: #eee; font-weight: 600; }

.status { padding: 2px 7px; border-radius: 10px; font-size: 11px; font-weight: 700; white-space: nowrap; }
.status.ontime   { background: #14391d; color: #5cd16e; }
.status.minor    { background: #4a4500; color: #ffd700; }
.status.moderate { background: #5c3300; color: #ff9800; }
.status.severe   { background: #5c1010; color: #ff5252; }
.status.unknown  { background: #2a2a2a; color: #777; }

.empty { color: #444; font-style: italic; font-size: 13px; padding: 6px 0; }

/* ── Leaflet ── */
.leaflet-container { background: #1a1a1a; }
.train-marker { background: transparent !important; border: none !important; }
.leaflet-popup-content-wrapper, .leaflet-tooltip { background: #1e1e1e; color: #fff; border: 1px solid #333; font-family: inherit; font-size: 12px; }
.leaflet-popup-tip { background: #1e1e1e; }
.leaflet-tooltip-top:before { border-top-color: #333; }

/* ── Mobile ── */
@media (max-width: 768px) {
  body { overflow: auto; height: auto; }
  #app { flex-direction: column; height: auto; min-height: 100vh; }
  #map { flex: none; height: 44vw; min-height: 180px; max-height: 280px; width: 100%; }
  #sidebar { width: 100%; height: auto; border-left: none; border-top: 1px solid #2a2a2a; overflow-y: visible; }
  .train-num { font-size: 17px; }
  .dep-num   { font-size: 16px; }
}
</style>
</head>
<body>
<div id="app">
  <div id="map"></div>
  <div id="sidebar">
    <div class="header">
      <h1>NEW YORK PENN TRACKER</h1>
      <div class="updated" id="updated">Loading&hellip;</div>
    </div>
    <div class="legend">
      <span class="legend-item"><span class="dot acela"></span>Acela</span>
      <span class="legend-item"><span class="dot regional"></span>NE Regional</span>
      <span class="legend-item"><span class="dot keystone"></span>Keystone / Pennsylvanian</span>
      <span class="legend-item"><span class="dot empire"></span>Empire / Maple Leaf</span>
      <span class="legend-item"><span class="dot longdistance"></span>Long Distance</span>
    </div>
    <div class="dep-board">
      <h2>&#x1F4CD; Departing New York Penn</h2>
      <div id="departures"><div class="empty">Loading&hellip;</div></div>
    </div>
    <div class="section">
      <h2><span class="arrow">&#x2193;</span>Southbound &amp; Westbound</h2>
      <div id="southbound"><div class="empty">Loading&hellip;</div></div>
    </div>
    <div class="section">
      <h2><span class="arrow">&#x2191;</span>Northbound &amp; Eastbound</h2>
      <div id="northbound"><div class="empty">Loading&hellip;</div></div>
    </div>
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
L.polyline(routeCoords, { color: '#666', weight: 2.5, opacity: 0.5 }).addTo(map);
map.fitBounds(L.latLngBounds(routeCoords), { padding: [30, 30] });
NEC_STATIONS.forEach(function(s){
  L.circleMarker([s.lat, s.lon], { radius: 3.5, color: '#bbb', weight: 1.5, fillColor: '#222', fillOpacity: 1 })
   .bindTooltip(s.name, { direction: 'top', offset: [0, -4] }).addTo(map);
});

var trainLayer = L.layerGroup().addTo(map);
var HEADING = { N:0, NE:45, E:90, SE:135, S:180, SW:225, W:270, NW:315 };

function typeColor(type) {
  if (type === 'acela')        return '#40E0D0';
  if (type === 'regional')     return '#1E90FF';
  if (type === 'keystone')     return '#FFD700';
  if (type === 'empire')       return '#4A8C4A';
  return '#DC143C';
}

function trainIcon(t) {
  var color = typeColor(t.type);
  var angle = t.bearing != null ? t.bearing : (HEADING[t.heading] || 0);
  var html = '<div style="transform:rotate(' + angle + 'deg);width:22px;height:22px;">' +
    '<svg width="22" height="22" viewBox="0 0 22 22">' +
      '<polygon points="11,1 19,19 11,14 3,19" fill="' + color + '" stroke="#000" stroke-width="1.5" stroke-linejoin="round"/>' +
    '</svg></div>';
  return L.divIcon({ className: 'train-marker', html: html, iconSize: [22,22], iconAnchor: [11,11] });
}

function esc(s) {
  return String(s).replace(/[&<>"']/g, function(c){
    return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
  });
}

// ── Train card (active list) ──
function renderCard(t) {
  var distText = t.distToNext != null && t.nextStop
    ? t.distToNext.toFixed(1) + ' mi to ' + esc(t.nextStop.name)
    : t.nextStop ? 'at ' + esc(t.nextStop.name) : '&mdash;';
  var timeText = t.cardTime
    ? '<span class="train-time">' + esc(t.cardLabel) + ' ' + esc(t.cardTime) + '</span>'
    : '';
  return '<div class="train ' + t.type + '">' +
    '<div class="train-flair"></div>' +
    '<div class="train-body">' +
      '<div class="train-row1">' +
        '<div class="train-id">' +
          '<span class="train-num">' + esc(t.trainNum) + '</span>' +
          '<span class="train-route">' + esc(t.routeName) + '</span>' +
        '</div>' +
        '<span class="status ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
      '</div>' +
      '<div class="train-row2">' + timeText +
        '<span>' + t.velocity + ' mph</span>' +
        '<span>' + distText + '</span>' +
      '</div>' +
    '</div></div>';
}

function tooltipFor(t) {
  var d = t.distToNext != null ? ' &middot; ' + t.distToNext.toFixed(1) + ' mi to ' + t.nextStop.name : '';
  return '<b>' + esc(t.routeName) + ' #' + esc(t.trainNum) + '</b><br>' +
    t.status.label + ' &middot; ' + t.velocity + ' mph' + d;
}

// ── Departure board ──
var depData = [];

function minsUntil(iso) {
  return iso ? Math.round((new Date(iso) - Date.now()) / 60000) : null;
}

function depBadge(t) {
  if (t.nypActDep) {
    var gone = Math.round((Date.now() - new Date(t.nypActDep)) / 60000);
    return { label: 'DEPARTED', cls: 'departed' };
  }
  var mins = minsUntil(t.nypSchDep);
  if (mins === null) return { label: '&mdash;', cls: 'normal' };
  if (mins <= 0)  return { label: 'DEPARTED', cls: 'departed' };
  if (mins <= 2)  return { label: 'LAST CALL', cls: 'lastcall' };
  if (mins <= 20) return { label: mins + ' MIN', cls: 'soon' };
  return { label: mins + ' MIN', cls: 'normal' };
}

function fmtNYP(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString('en-US', {
    hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'America/New_York'
  });
}

function renderDepartures(trains) {
  // Show predeparture trains + DEPARTED trains within 3 min of actual departure
  depData = trains.filter(function(t) {
    if (!t.nypSchDep) return false;
    if (t.nypActDep) {
      return Math.round((Date.now() - new Date(t.nypActDep)) / 60000) < 3;
    }
    var mins = minsUntil(t.nypSchDep);
    return mins !== null && mins >= -1 && mins <= 240;
  }).sort(function(a, b) {
    return new Date(a.nypSchDep) - new Date(b.nypSchDep);
  });

  if (!depData.length) {
    document.getElementById('departures').innerHTML = '<div class="empty">No upcoming departures.</div>';
    return;
  }

  document.getElementById('departures').innerHTML = depData.map(function(t) {
    var b = depBadge(t);
    return '<div class="dep-item ' + t.type + '" id="dc-' + esc(t.trainNum) + '">' +
      '<div class="dep-flair"></div>' +
      '<div class="dep-body">' +
        '<div class="dep-id">' +
          '<div class="dep-num">' + esc(t.trainNum) + '</div>' +
          '<div class="dep-route">' + esc(t.routeName) + '</div>' +
        '</div>' +
        '<div class="dep-dest"><b>&rarr;</b>' + esc(t.destName || '') + '</div>' +
        '<div class="dep-time">' + fmtNYP(t.nypSchDep) + '</div>' +
        '<span class="dep-badge ' + b.cls + '" id="db-' + esc(t.trainNum) + '">' + b.label + '</span>' +
        '<span class="status ' + t.status.class + '">' + esc(t.status.label) + '</span>' +
      '</div></div>';
  }).join('');
}

// Per-second countdown tick
setInterval(function() {
  depData.forEach(function(t) {
    var span = document.getElementById('db-' + t.trainNum);
    if (!span) return;
    var b = depBadge(t);
    span.textContent = b.label === '&mdash;' ? '—' : b.label;
    span.className = 'dep-badge ' + b.cls;
  });
}, 1000);

// ── Main refresh ──
async function refresh() {
  try {
    var res = await fetch('/api/trains');
    var trains = await res.json();

    // Map markers — all trains regardless of position
    trainLayer.clearLayers();
    trains.forEach(function(t) {
      if (t.lat == null || t.lon == null) return;
      L.marker([t.lat, t.lon], { icon: trainIcon(t) })
       .bindTooltip(tooltipFor(t), { direction: 'top', offset: [0,-10] })
       .addTo(trainLayer);
    });

    // Departure board: predeparture or DEPARTED <3 min
    renderDepartures(trains);

    // Active lists: departed NYP >3 min ago (or active with no dep timestamp)
    var activeTrains = trains.filter(function(t) {
      if (t.trainState === 'Predeparture') return false;
      if (t.nypActDep) {
        return Math.round((Date.now() - new Date(t.nypActDep)) / 60000) >= 3;
      }
      return t.trainState === 'Active';
    });

    var south = activeTrains.filter(function(t){ return t.direction === 'south'; })
      .sort(function(a,b){ return new Date(a.nypSchDep||0) - new Date(b.nypSchDep||0); });
    var north = activeTrains.filter(function(t){ return t.direction === 'north'; })
      .sort(function(a,b){ return new Date(a.nypSchDep||0) - new Date(b.nypSchDep||0); });

    document.getElementById('southbound').innerHTML =
      south.length ? south.map(renderCard).join('') : '<div class="empty">No trains en route.</div>';
    document.getElementById('northbound').innerHTML =
      north.length ? north.map(renderCard).join('') : '<div class="empty">No trains en route.</div>';
    document.getElementById('updated').textContent =
      'Updated ' + new Date().toLocaleTimeString() + ' · ' + trains.length + ' active';

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
