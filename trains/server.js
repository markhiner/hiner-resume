// NEC live tracker — calls api-v3.amtraker.com directly (no npm package required)
// Run with Node 18+ (built-in fetch). Place anywhere and `node nec-tracker.js`.

const http = require("http");

const PORT = 3000;
const TRAINS_URL = "https://api-v3.amtraker.com/v3/trains";
const STATIONS_URL = "https://api-v3.amtraker.com/v3/stations";

// NEC stations in southbound order (NYP → WAS)
const NEC_STATIONS = [
  { code: "NYP", name: "New York Penn",          lat: 40.7510, lon: -73.9963 },
  { code: "NWK", name: "Newark Penn",            lat: 40.7347, lon: -74.1647 },
  { code: "EWR", name: "Newark Airport",         lat: 40.6966, lon: -74.1823 },
  { code: "MET", name: "Metropark",              lat: 40.5681, lon: -74.3296 },
  { code: "NBK", name: "New Brunswick",          lat: 40.4965, lon: -74.4463 },
  { code: "PJC", name: "Princeton Junction",     lat: 40.3159, lon: -74.6240 },
  { code: "TRE", name: "Trenton",                lat: 40.2190, lon: -74.7544 },
  { code: "PHL", name: "Philadelphia 30th St",   lat: 39.9556, lon: -75.1810 },
  { code: "WIL", name: "Wilmington",             lat: 39.7373, lon: -75.5511 },
  { code: "ABE", name: "Aberdeen",               lat: 39.5084, lon: -76.1633 },
  { code: "BAL", name: "Baltimore Penn",         lat: 39.3073, lon: -76.6157 },
  { code: "BWI", name: "BWI Airport",            lat: 39.1924, lon: -76.6943 },
  { code: "NCR", name: "New Carrollton",         lat: 38.9481, lon: -76.8715 },
  { code: "WAS", name: "Washington Union",       lat: 38.8970, lon: -77.0064 },
];

// ---------- direct API calls ----------

async function fetchJSON(url) {
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`${url} → ${res.status} ${res.statusText}`);
  return res.json();
}

async function fetchAllTrains() {
  return fetchJSON(TRAINS_URL);
}

let stationsCache = null;
let stationsCacheTime = 0;
async function fetchAllStations() {
  if (stationsCache && Date.now() - stationsCacheTime < 60 * 60 * 1000) {
    return stationsCache;
  }
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
  const R = 3959;
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function bearingDeg(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const toDeg = (r) => (r * 180) / Math.PI;
  const φ1 = toRad(lat1), φ2 = toRad(lat2);
  const Δλ = toRad(lon2 - lon1);
  const y = Math.sin(Δλ) * Math.cos(φ2);
  const x = Math.cos(φ1) * Math.sin(φ2) - Math.sin(φ1) * Math.cos(φ2) * Math.cos(Δλ);
  return (toDeg(Math.atan2(y, x)) + 360) % 360;
}

function trainType(routeName) {
  if (routeName === "Acela") return "acela";
  if (routeName === "Northeast Regional") return "regional";
  return "longdistance";
}

function statusInfo(scheduled, actual) {
  if (!scheduled || !actual) return { label: "—", class: "unknown" };
  const diffMin = Math.round((actual - scheduled) / 60000);
  if (diffMin <= 5) return { label: "On Time", class: "ontime" };
  const h = Math.floor(diffMin / 60);
  const m = diffMin % 60;
  const label = `${h}:${String(m).padStart(2, "0")} late`;
  let cls;
  if (diffMin <= 15) cls = "minor";
  else if (diffMin <= 30) cls = "moderate";
  else cls = "severe";
  return { label, class: cls };
}

function lastEventTime(stop) {
  return (
    parseTime(stop.dep) ||
    parseTime(stop.arr) ||
    parseTime(stop.schDep) ||
    parseTime(stop.schArr)
  );
}

function isOnNEC(train) {
  if (!train.stations) return false;
  const codes = train.stations.map((s) => s.code);
  return codes.includes("NYP") && codes.includes("WAS");
}

function isInCorridor(train) {
  if (!isOnNEC(train)) return false;
  if (train.trainState === "Completed") return false;
  const codes = train.stations.map((s) => s.code);
  const exitIdx = Math.max(codes.indexOf("NYP"), codes.indexOf("WAS"));
  const exitTime = lastEventTime(train.stations[exitIdx]);
  return !exitTime || exitTime > new Date();
}

function direction(train) {
  const codes = train.stations.map((s) => s.code);
  return codes.indexOf("NYP") < codes.indexOf("WAS") ? "south" : "north";
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

function schedTimeAt(train, code) {
  if (!train.stations) return null;
  const stop = train.stations.find((s) => s.code === code);
  if (!stop) return null;
  return parseTime(stop.schDep) || parseTime(stop.schArr);
}

function formatTimeShort(date) {
  if (!date) return null;
  return date.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: "America/New_York",
  });
}

// Predeparture → entry departure time. Otherwise → exit arrival time.
function cardTimeFor(train, dir) {
  const entryCode = dir === "south" ? "NYP" : "WAS";
  const exitCode  = dir === "south" ? "WAS" : "NYP";
  const entryStop = train.stations.find((s) => s.code === entryCode);
  const exitStop  = train.stations.find((s) => s.code === exitCode);

  if (train.trainState === "Predeparture") {
    const t = parseTime(entryStop?.dep) || parseTime(entryStop?.schDep);
    return { time: t, label: "Departs" };
  }
  const t = parseTime(exitStop?.arr) || parseTime(exitStop?.schArr);
  return { time: t, label: "Arrives" };
}

// ---------- main shaping ----------

async function getCorridorData() {
  const [trainsObj, stations] = await Promise.all([fetchAllTrains(), fetchAllStations()]);
  const all = Object.values(trainsObj).flat();
  const corridorTrains = all.filter(isInCorridor);

  return corridorTrains.map((train) => {
    const dir = direction(train);
    const nextStop = findNextStop(train);

    let status = { label: "—", class: "unknown" };
    if (nextStop) {
      const sch = parseTime(nextStop.schArr) || parseTime(nextStop.schDep);
      const est = parseTime(nextStop.arr) || parseTime(nextStop.dep);
      status = statusInfo(sch, est);
    }

    let distToNext = null;
    let bearing = null;
    if (nextStop && train.lat != null && train.lon != null) {
      const stopData = stations[nextStop.code];
      if (stopData && stopData.lat && stopData.lon) {
        distToNext = haversineMi(train.lat, train.lon, stopData.lat, stopData.lon);
        bearing = bearingDeg(train.lat, train.lon, stopData.lat, stopData.lon);
      }
    }

    const entryCode = dir === "south" ? "NYP" : "WAS";
    const necDepTime = schedTimeAt(train, entryCode);
    const card = cardTimeFor(train, dir);

    return {
      trainNum: train.trainNum,
      trainID: train.trainID,
      routeName: train.routeName,
      type: trainType(train.routeName),
      origName: train.origName,
      destName: train.destName,
      lat: train.lat,
      lon: train.lon,
      velocity: Math.round(train.velocity || 0),
      heading: train.heading,
      trainState: train.trainState,
      direction: dir,
      nextStop: nextStop
        ? { code: nextStop.code, name: nextStop.name || stations[nextStop.code]?.name || nextStop.code }
        : null,
      distToNext: distToNext != null ? Math.round(distToNext * 10) / 10 : null,
      bearing,
      status,
      necDepTime: necDepTime ? necDepTime.toISOString() : null,
      cardTime: formatTimeShort(card.time),
      cardLabel: card.label,
    };
  });
}

// ---------- frontend HTML ----------

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>NEC Live Tracker — NYP ↔ WAS</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Inter", sans-serif; background: #0a0a0a; color: #f0f0f0; overflow: hidden; height: 100vh; }
  #app { display: flex; height: 100vh; }
  #map { flex: 1; height: 100%; background: #1a1a1a; }
  #sidebar { width: 460px; height: 100%; background: #111; border-left: 1px solid #2a2a2a; overflow-y: auto; }
  .header { padding: 18px 20px; border-bottom: 1px solid #2a2a2a; }
  .header h1 { font-size: 16px; font-weight: 700; letter-spacing: 1.5px; }
  .header .updated { font-size: 11px; color: #888; margin-top: 4px; }
  .legend { display: flex; gap: 16px; padding: 12px 20px; border-bottom: 1px solid #2a2a2a; font-size: 11px; }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .dot.acela { background: #40E0D0; }
  .dot.regional { background: #1E90FF; }
  .dot.longdistance { background: #DC143C; }
  .section { padding: 14px 20px; }
  .section h2 { font-size: 12px; font-weight: 700; letter-spacing: 2px; color: #999; margin-bottom: 12px; text-transform: uppercase; }
  .section h2 .arrow { color: #555; margin-right: 6px; font-size: 14px; }
  .train { background: #1a1a1a; border-radius: 4px; margin-bottom: 10px; display: flex; overflow: hidden; }
  .train-flair { width: 16px; flex-shrink: 0; }
  .train.acela .train-flair { background: #40E0D0; }
  .train.regional .train-flair { background: #1E90FF; }
  .train.longdistance .train-flair { background: #DC143C; }
  .train-body { flex: 1; padding: 10px 14px; min-width: 0; }
  .train-row1 { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; gap: 8px; }
  .train-id { display: flex; align-items: baseline; gap: 10px; min-width: 0; flex-wrap: wrap; }
  .train-num { font-size: 26px; font-weight: 800; color: #fff; letter-spacing: 0.5px; line-height: 1; }
  .train-route { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1.2px; font-weight: 600; }
  .train-row2 { display: flex; gap: 14px; font-size: 12px; color: #bbb; align-items: center; flex-wrap: wrap; }
  .train-time { color: #f0f0f0; font-weight: 600; }
  .status { padding: 3px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; white-space: nowrap; }
  .status.ontime    { background: #14391d; color: #5cd16e; }
  .status.minor     { background: #4a4500; color: #ffd700; }
  .status.moderate  { background: #5c3300; color: #ff9800; }
  .status.severe    { background: #5c1010; color: #ff5252; }
  .status.unknown   { background: #2a2a2a; color: #888; }
  .empty { color: #555; font-style: italic; font-size: 13px; padding: 8px 0; }
  .leaflet-container { background: #1a1a1a; }
  .train-marker { background: transparent !important; border: none !important; }
  .leaflet-popup-content-wrapper, .leaflet-tooltip { background: #1a1a1a; color: #fff; border: 1px solid #333; font-family: inherit; }
  .leaflet-popup-tip { background: #1a1a1a; }
  .leaflet-tooltip-top:before { border-top-color: #333; }
</style>
</head>
<body>
<div id="app">
  <div id="map"></div>
  <div id="sidebar">
    <div class="header">
      <h1>NEC TRACKER</h1>
      <div class="updated" id="updated">Loading…</div>
    </div>
    <div class="legend">
      <span class="legend-item"><span class="dot acela"></span>Acela</span>
      <span class="legend-item"><span class="dot regional"></span>Northeast Regional</span>
      <span class="legend-item"><span class="dot longdistance"></span>Long Distance</span>
    </div>
    <div class="section">
      <h2><span class="arrow">↓</span>Southbound (toward Washington)</h2>
      <div id="southbound"><div class="empty">Loading…</div></div>
    </div>
    <div class="section">
      <h2><span class="arrow">↑</span>Northbound (toward New York)</h2>
      <div id="northbound"><div class="empty">Loading…</div></div>
    </div>
  </div>
</div>
<script>
  var NEC_STATIONS = ${JSON.stringify(NEC_STATIONS)};
  var map = L.map('map', { zoomControl: true });
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '© OpenStreetMap, © CARTO', maxZoom: 18, subdomains: 'abcd'
  }).addTo(map);
  var routeCoords = NEC_STATIONS.map(function (s) { return [s.lat, s.lon]; });
  L.polyline(routeCoords, { color: '#888', weight: 3, opacity: 0.55 }).addTo(map);
  var necBounds = L.latLngBounds(routeCoords);
  map.fitBounds(necBounds, { padding: [30, 30] });
  map.setMaxBounds(necBounds.pad(0.5));
  NEC_STATIONS.forEach(function (s) {
    L.circleMarker([s.lat, s.lon], {
      radius: 4, color: '#ddd', weight: 1.5, fillColor: '#222', fillOpacity: 1
    }).bindTooltip(s.name, { direction: 'top', offset: [0, -4] }).addTo(map);
  });
  var trainLayer = L.layerGroup().addTo(map);
  var HEADING_ANGLE = { N: 0, NE: 45, E: 90, SE: 135, S: 180, SW: 225, W: 270, NW: 315 };
  function trainColor(type) {
    if (type === 'acela') return '#40E0D0';
    if (type === 'regional') return '#1E90FF';
    return '#DC143C';
  }
  function trainIcon(train) {
    var color = trainColor(train.type);
    var angle = train.bearing != null ? train.bearing : (HEADING_ANGLE[train.heading] || 0);
    var html = '<div style="transform: rotate(' + angle + 'deg); width: 22px; height: 22px;">' +
      '<svg width="22" height="22" viewBox="0 0 22 22">' +
        '<polygon points="11,1 19,19 11,14 3,19" fill="' + color + '" stroke="#000" stroke-width="1.5" stroke-linejoin="round"/>' +
      '</svg></div>';
    return L.divIcon({ className: 'train-marker', html: html, iconSize: [22, 22], iconAnchor: [11, 11] });
  }
  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c];
    });
  }
  function renderTrainCard(t) {
    var distText;
    if (t.distToNext != null && t.nextStop) {
      distText = t.distToNext.toFixed(1) + ' mi to ' + escapeHTML(t.nextStop.name);
    } else if (t.nextStop) {
      distText = 'at ' + escapeHTML(t.nextStop.name);
    } else {
      distText = '—';
    }
    var timeText = t.cardTime
      ? '<span class="train-time">' + escapeHTML(t.cardLabel) + ' ' + escapeHTML(t.cardTime) + '</span>'
      : '';
    return '<div class="train ' + t.type + '">' +
      '<div class="train-flair"></div>' +
      '<div class="train-body">' +
        '<div class="train-row1">' +
          '<div class="train-id">' +
            '<span class="train-num">' + escapeHTML(t.trainNum) + '</span>' +
            '<span class="train-route">' + escapeHTML(t.routeName) + '</span>' +
          '</div>' +
          '<span class="status ' + t.status.class + '">' + escapeHTML(t.status.label) + '</span>' +
        '</div>' +
        '<div class="train-row2">' +
          timeText +
          '<span>' + t.velocity + ' mph</span>' +
          '<span>' + distText + '</span>' +
        '</div>' +
      '</div>' +
    '</div>';
  }
  function tooltipFor(t) {
    var d = t.distToNext != null ? (' · ' + t.distToNext.toFixed(1) + ' mi to ' + t.nextStop.name) : '';
    return '<b>' + escapeHTML(t.routeName) + ' #' + escapeHTML(t.trainNum) + '</b><br>' +
           t.status.label + ' · ' + t.velocity + ' mph' + d;
  }
  async function refresh() {
    try {
      var res = await fetch('/api/trains');
      var trains = await res.json();
      trainLayer.clearLayers();
      trains.forEach(function (t) {
        if (t.lat == null || t.lon == null) return;
        var marker = L.marker([t.lat, t.lon], { icon: trainIcon(t) });
        marker.bindTooltip(tooltipFor(t), { direction: 'top', offset: [0, -10] });
        marker.addTo(trainLayer);
      });
      var south = trains.filter(function (t) { return t.direction === 'south'; })
        .sort(function (a, b) { return new Date(a.necDepTime || 0) - new Date(b.necDepTime || 0); });
      var north = trains.filter(function (t) { return t.direction === 'north'; })
        .sort(function (a, b) { return new Date(a.necDepTime || 0) - new Date(b.necDepTime || 0); });
      document.getElementById('southbound').innerHTML =
        south.length ? south.map(renderTrainCard).join('') : '<div class="empty">No southbound trains.</div>';
      document.getElementById('northbound').innerHTML =
        north.length ? north.map(renderTrainCard).join('') : '<div class="empty">No northbound trains.</div>';
      document.getElementById('updated').textContent =
        'Updated ' + new Date().toLocaleTimeString() + ' · ' + trains.length + ' active';
    } catch (e) {
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
      const data = await getCorridorData();
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
  console.log(`\nNEC tracker running at http://localhost:${PORT}`);
  console.log(`Using ${TRAINS_URL}\n`);
});