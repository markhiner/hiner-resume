// NEC live tracker — calls api-v3.amtraker.com directly (no npm package required)
// Run with Node 18+ (built-in fetch). node server.js

const http = require("http");

const PORT = 3000;
const TRAINS_URL = "https://api-v3.amtraker.com/v3/trains";
const STATIONS_URL = "https://api-v3.amtraker.com/v3/stations";

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

// ---------- API calls ----------

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

// ---------- NYP departures ----------

async function getNYPDepartures() {
  const trainsObj = await fetchAllTrains();
  const all = Object.values(trainsObj).flat();
  const cutoff = new Date(Date.now() - 3 * 60 * 1000);

  const results = [];
  for (const train of all) {
    if (!train.stations) continue;
    const nypIdx = train.stations.findIndex((s) => s.code === "NYP");
    if (nypIdx < 0) continue;
    if (nypIdx === train.stations.length - 1) continue; // NYP is this train's terminus

    const nypStop = train.stations[nypIdx];
    const schDep = parseTime(nypStop.schDep);
    if (!schDep) continue;

    const actualDep = parseTime(nypStop.dep);
    const effectiveDep = actualDep || schDep;
    if (effectiveDep < cutoff) continue;

    const status = statusInfo(schDep, actualDep);

    results.push({
      trainNum: train.trainNum,
      routeName: train.routeName,
      destName: train.destName,
      schDep: schDep.toISOString(),
      actualDep: actualDep ? actualDep.toISOString() : null,
      status,
    });
  }

  return results.sort((a, b) => new Date(a.schDep) - new Date(b.schDep));
}

// ---------- corridor data ----------

async function getCorridorData() {
  const [trainsObj, stations] = await Promise.all([fetchAllTrains(), fetchAllStations()]);
  const all = Object.values(trainsObj).flat();
  const corridorTrains = all.filter(isInCorridor);

  return corridorTrains.map((train) => {
    const dir = direction(train);
    const stationsArr = train.stations || [];
    const nextStop = findNextStop(train);
    const nextIdx = nextStop ? stationsArr.indexOf(nextStop) : stationsArr.length;

    let status = { label: "—", class: "unknown" };
    if (nextStop) {
      const sch = parseTime(nextStop.schArr) || parseTime(nextStop.schDep);
      const est = parseTime(nextStop.arr) || parseTime(nextStop.dep);
      status = statusInfo(sch, est);
    }

    let distToNext = null;
    let bearing = null;
    let distToLast = null;

    if (train.lat != null && train.lon != null) {
      if (nextStop) {
        const nd = stations[nextStop.code];
        if (nd?.lat && nd?.lon) {
          distToNext = haversineMi(train.lat, train.lon, nd.lat, nd.lon);
          bearing = bearingDeg(train.lat, train.lon, nd.lat, nd.lon);
        }
      }
      if (nextIdx > 0) {
        const lastStop = stationsArr[nextIdx - 1];
        const ld = stations[lastStop.code];
        if (ld?.lat && ld?.lon) {
          distToLast = haversineMi(train.lat, train.lon, ld.lat, ld.lon);
        }
      }
    }

    let locationInfo = null;
    if (train.lat != null) {
      if (distToLast != null && distToLast <= 5 && nextIdx > 0) {
        const lastStop = stationsArr[nextIdx - 1];
        locationInfo = {
          near: true,
          stationName: lastStop.name || stations[lastStop.code]?.name || lastStop.code,
          dist: Math.round(distToLast * 10) / 10,
        };
      } else if (distToNext != null && distToNext <= 5 && nextStop) {
        locationInfo = {
          near: true,
          stationName: nextStop.name || stations[nextStop.code]?.name || nextStop.code,
          dist: Math.round(distToNext * 10) / 10,
        };
      } else if (distToNext != null && nextStop) {
        locationInfo = {
          near: false,
          dist: Math.round(distToNext * 10) / 10,
          stationName: nextStop.name || stations[nextStop.code]?.name || nextStop.code,
        };
      }
    }

    const stops = stationsArr.map((s, i) => ({
      code: s.code,
      name: s.name || stations[s.code]?.name || s.code,
      passed: i < nextIdx,
      isNext: i === nextIdx,
    }));

    const entryCode = dir === "south" ? "NYP" : "WAS";
    const necDepTime = schedTimeAt(train, entryCode);

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
      locationInfo,
      stops,
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
  #sidebar { width: 480px; height: 100%; background: #111; border-left: 1px solid #2a2a2a; overflow-y: auto; }
  .header { padding: 18px 20px; border-bottom: 1px solid #2a2a2a; }
  .header h1 { font-size: 16px; font-weight: 700; letter-spacing: 1.5px; }
  .header .updated { font-size: 11px; color: #888; margin-top: 4px; }
  .legend { display: flex; gap: 16px; padding: 12px 20px; border-bottom: 1px solid #2a2a2a; font-size: 11px; }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
  .dot.acela { background: #40E0D0; }
  .dot.regional { background: #1E90FF; }
  .dot.longdistance { background: #DC143C; }
  .section { padding: 14px 20px; border-bottom: 1px solid #1e1e1e; }
  .section h2 { font-size: 12px; font-weight: 700; letter-spacing: 2px; color: #999; margin-bottom: 12px; text-transform: uppercase; }
  .section h2 .arrow { color: #555; margin-right: 6px; font-size: 14px; }
  .status { padding: 3px 8px; border-radius: 10px; font-size: 11px; font-weight: 700; white-space: nowrap; }
  .status.ontime    { background: #14391d; color: #5cd16e; }
  .status.minor     { background: #4a4500; color: #ffd700; }
  .status.moderate  { background: #5c3300; color: #ff9800; }
  .status.severe    { background: #5c1010; color: #ff5252; }
  .status.unknown   { background: #2a2a2a; color: #888; }
  .empty { color: #555; font-style: italic; font-size: 13px; padding: 8px 0; }

  /* ---- departures board ---- */
  .dep-table { width: 100%; border-collapse: collapse; table-layout: fixed; font-size: 12px; }
  .dep-table col.col-time   { width: 70px; }
  .dep-table col.col-status { width: 68px; }
  .dep-table col.col-dest   { }
  .dep-table col.col-num    { width: 28px; }
  .dep-table col.col-name   { width: 88px; }
  .dep-table col.col-in     { width: 54px; }
  .dep-head th { color: #555; font-size: 10px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; text-align: left; padding: 0 4px 8px; border-bottom: 1px solid #2a2a2a; }
  .dep-head th.right { text-align: right; }
  .dep-row td { padding: 6px 4px; border-bottom: 1px solid #1a1a1a; vertical-align: middle; }
  .dep-time { color: #bbb; white-space: nowrap; }
  .dep-dest { color: #f0f0f0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 0; }
  .dep-num  { color: #888; white-space: nowrap; }
  .dep-name { color: #666; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 0; }
  .dep-in   { text-align: right; }
  .dep-countdown { font-weight: 700; white-space: nowrap; font-size: 12px; }
  .dep-countdown.warn     { color: #ffd700; }
  .dep-countdown.flash    { color: #ffd700; animation: blink 1s step-start infinite; }
  .dep-countdown.lastcall { color: #ff5252; animation: blink 0.5s step-start infinite; }
  @keyframes blink { 50% { opacity: 0; } }

  /* ---- train cards ---- */
  .train { background: #1a1a1a; border-radius: 4px; margin-bottom: 10px; display: flex; overflow: hidden; }
  .train-flair { width: 16px; flex-shrink: 0; }
  .train.acela .train-flair      { background: #40E0D0; }
  .train.regional .train-flair   { background: #1E90FF; }
  .train.longdistance .train-flair { background: #DC143C; }
  .train-body { flex: 1; padding: 10px 14px; min-width: 0; }
  .train-row1 { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; gap: 8px; }
  .train-id { display: flex; align-items: baseline; gap: 10px; min-width: 0; flex-wrap: wrap; }
  .train-num { font-size: 26px; font-weight: 800; color: #fff; letter-spacing: 0.5px; line-height: 1; }
  .train-route { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: 1.2px; font-weight: 600; }
  .train-meta { display: flex; gap: 14px; font-size: 12px; color: #999; flex-wrap: wrap; align-items: center; }
  .train-loc-near { color: #7bcfef; }
  .train-stops { border-top: 1px solid #252525; margin-top: 9px; padding-top: 8px; }
  .stop { font-size: 11px; line-height: 1.65; padding-left: 8px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .stop.passed { color: #3a3a3a; font-weight: 300; }
  .stop.next   { color: #fff; font-weight: 700; padding-left: 0; border-left: 2px solid #fff; padding-left: 6px; }
  .stop.future { color: #888; font-weight: 400; }

  /* ---- map ---- */
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
      <h2>New York Penn — Departures</h2>
      <table class="dep-table">
        <colgroup>
          <col class="col-time">
          <col class="col-status">
          <col class="col-dest">
          <col class="col-num">
          <col class="col-name">
          <col class="col-in">
        </colgroup>
        <thead>
          <tr class="dep-head">
            <th>Time</th>
            <th>Status</th>
            <th>Destination</th>
            <th>#</th>
            <th>Train</th>
            <th class="right">In</th>
          </tr>
        </thead>
        <tbody id="departures"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
      </table>
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

  // ---- map setup ----
  var map = L.map('map', { zoomControl: true });
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap, &copy; CARTO', maxZoom: 18, subdomains: 'abcd'
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

  // ---- map helpers ----
  var HEADING_ANGLE = { N: 0, NE: 45, E: 90, SE: 135, S: 180, SW: 225, W: 270, NW: 315 };
  function trainColor(type) {
    if (type === 'acela') return '#40E0D0';
    if (type === 'regional') return '#1E90FF';
    return '#DC143C';
  }
  function trainIcon(train) {
    var color = trainColor(train.type);
    var angle = train.bearing != null ? train.bearing : (HEADING_ANGLE[train.heading] || 0);
    var html = '<div style="transform:rotate(' + angle + 'deg);width:22px;height:22px;">' +
      '<svg width="22" height="22" viewBox="0 0 22 22">' +
        '<polygon points="11,1 19,19 11,14 3,19" fill="' + color + '" stroke="#000" stroke-width="1.5" stroke-linejoin="round"/>' +
      '</svg></div>';
    return L.divIcon({ className: 'train-marker', html: html, iconSize: [22, 22], iconAnchor: [11, 11] });
  }
  function tooltipFor(t) {
    var d = t.distToNext != null && t.nextStop ? (' &middot; ' + t.distToNext.toFixed(1) + ' mi to ' + escapeHTML(t.nextStop.name)) : '';
    return '<b>' + escapeHTML(t.routeName) + ' #' + escapeHTML(t.trainNum) + '</b><br>' +
           escapeHTML(t.status.label) + ' &middot; ' + t.velocity + ' mph' + d;
  }

  // ---- shared utils ----
  function escapeHTML(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c];
    });
  }

  // ---- train cards ----
  function renderTrainCard(t) {
    var locHtml = '';
    if (t.locationInfo) {
      if (t.locationInfo.near) {
        locHtml = '<span class="train-loc-near">Near ' + escapeHTML(t.locationInfo.stationName) + '</span>' +
                  '<span style="color:#555">(' + t.locationInfo.dist + ' mi)</span>';
      } else {
        locHtml = '<span>' + t.locationInfo.dist + ' mi to ' + escapeHTML(t.locationInfo.stationName) + '</span>';
      }
    }

    var stopsHtml = '';
    if (t.stops && t.stops.length) {
      stopsHtml = '<div class="train-stops">' +
        t.stops.map(function (s) {
          var cls = s.isNext ? 'next' : (s.passed ? 'passed' : 'future');
          return '<div class="stop ' + cls + '">' + escapeHTML(s.name) + '</div>';
        }).join('') +
      '</div>';
    }

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
        '<div class="train-meta">' +
          '<span>' + t.velocity + ' mph</span>' +
          locHtml +
        '</div>' +
        stopsHtml +
      '</div>' +
    '</div>';
  }

  // ---- countdown logic ----
  function countdownInfo(depDate) {
    var diff = depDate - new Date();
    if (diff <= 0) return { text: 'Departed', cls: '' };
    var totalSecs = Math.floor(diff / 1000);
    var totalMins = Math.floor(totalSecs / 60);
    if (totalMins < 3) {
      return { text: 'Last call', cls: 'lastcall' };
    }
    if (totalMins < 10) {
      var secs = totalSecs % 60;
      return { text: totalMins + ':' + String(secs).padStart(2, '0'), cls: 'flash' };
    }
    if (totalMins < 20) {
      return { text: totalMins + ' mins', cls: 'warn' };
    }
    if (totalMins < 60) {
      return { text: totalMins + ' mins', cls: '' };
    }
    var h = Math.floor(totalMins / 60);
    var m = totalMins % 60;
    return { text: h + ':' + String(m).padStart(2, '0'), cls: '' };
  }

  function updateCountdowns() {
    document.querySelectorAll('.dep-countdown[data-dep]').forEach(function (el) {
      var dep = new Date(el.getAttribute('data-dep'));
      var cd = countdownInfo(dep);
      el.textContent = cd.text;
      el.className = 'dep-countdown' + (cd.cls ? ' ' + cd.cls : '');
    });
  }

  // ---- departures board ----
  function renderDepartures(deps) {
    if (!deps || !deps.length) {
      return '<tr><td colspan="6" class="empty">No upcoming departures.</td></tr>';
    }
    return deps.map(function (d) {
      var schDep = new Date(d.schDep);
      var estDep = d.actualDep ? new Date(d.actualDep) : schDep;
      var cd = countdownInfo(estDep);
      var timeStr = schDep.toLocaleTimeString('en-US', {
        hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'America/New_York'
      });
      return '<tr class="dep-row">' +
        '<td class="dep-time">' + escapeHTML(timeStr) + '</td>' +
        '<td><span class="status ' + d.status.class + '">' + escapeHTML(d.status.label) + '</span></td>' +
        '<td class="dep-dest">' + escapeHTML(d.destName || '—') + '</td>' +
        '<td class="dep-num">' + escapeHTML(d.trainNum) + '</td>' +
        '<td class="dep-name">' + escapeHTML(d.routeName) + '</td>' +
        '<td class="dep-in"><span class="dep-countdown ' + cd.cls + '" data-dep="' + estDep.toISOString() + '">' + escapeHTML(cd.text) + '</span></td>' +
        '</tr>';
    }).join('');
  }

  async function refreshDepartures() {
    try {
      var res = await fetch('/api/nyp-departures');
      var deps = await res.json();
      document.getElementById('departures').innerHTML = renderDepartures(deps);
    } catch (e) {
      document.getElementById('departures').innerHTML =
        '<tr><td colspan="6" class="empty">Error loading departures.</td></tr>';
    }
  }

  // ---- corridor refresh ----
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
        'Updated ' + new Date().toLocaleTimeString() + ' \xb7 ' + trains.length + ' active';
    } catch (e) {
      document.getElementById('updated').textContent = 'Error: ' + e.message;
    }
  }

  refresh();
  refreshDepartures();
  setInterval(refresh, 30000);
  setInterval(refreshDepartures, 60000);
  setInterval(updateCountdowns, 1000);
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

  if (req.url === "/api/nyp-departures") {
    try {
      const data = await getNYPDepartures();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(data));
    } catch (err) {
      console.error("NYP departures error:", err.message);
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
