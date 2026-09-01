// "Next train to..." — a live recreation of the LIRR departure board at
// Moynihan Train Hall / Penn Station. For every station in the system, shows
// the next train reachable from Penn — direct, or via a timed connection at
// Jamaica — matching the real board's J/T markers and per-branch colors.
//
// Data: MTA's own LIRR GTFS static schedule (no key needed) plus, if
// LIRR_API_KEY is set, the GTFS-realtime feed for live delay adjustment.
// Run with Node 18+ (built-in fetch). node lirr-board.js

const http = require("http");
const fs = require("fs");
const path = require("path");
const { execFileSync } = require("child_process");
const os = require("os");

const PORT = process.env.PORT || 3002;
const LIRR_API_KEY = process.env.LIRR_API_KEY || null;

const STATIC_GTFS_URL = "https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip";
const RT_JSON_URL = LIRR_API_KEY
  ? `https://mnorth.prod.acquia-sites.com/wse/LIRR/gtfsrt/realtime/${LIRR_API_KEY}/json`
  : null;

const CACHE_DIR = path.join(__dirname, ".lirr-gtfs-cache");
const STATIC_REFRESH_MS = 20 * 60 * 60 * 1000; // schedules don't change intraday
const BOARD_REFRESH_MS = 30 * 1000;
const RT_REFRESH_MS = 30 * 1000;
const TRANSFER_BUFFER_MS = 3 * 60 * 1000; // minimum time to change trains at Jamaica

const PENN_STOP_ID = "237";
const JAMAICA_STOP_ID = "102";
const NY_TZ = "America/New_York";

// Stations with no meaningful year-round scheduled Penn/Jamaica connection in
// this data (seasonal North Fork branch, event-only Belmont Park) — the real
// board's own escape hatches, not a gap in this logic.
const SPECIAL_EVENTS_ONLY = new Set(["Belmont Park"]);

// ---------- timezone helpers ----------
// The server this runs on is not guaranteed to be in America/New_York (it
// might be UTC), so every date/time computation here is explicit about the
// railroad's own timezone rather than trusting the host's local clock.

function nyOffsetMinutes(instant) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: NY_TZ, timeZoneName: "shortOffset" }).formatToParts(instant);
  const tz = parts.find((p) => p.type === "timeZoneName").value; // "GMT-4" / "GMT-5"
  const m = tz.match(/GMT([+-]\d+)/);
  return m ? parseInt(m[1], 10) * 60 : -300;
}
function nyDateParts(instant) {
  const parts = new Intl.DateTimeFormat("en-US", { timeZone: NY_TZ, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(instant);
  const get = (t) => +parts.find((p) => p.type === t).value;
  return { y: get("year"), mo: get("month"), d: get("day") };
}
function nyWallTimeToMs(dateParts, h, mi, s) {
  const guessUtcMs = Date.UTC(dateParts.y, dateParts.mo - 1, dateParts.d, h, mi, s);
  const offsetMin = nyOffsetMinutes(new Date(guessUtcMs));
  return guessUtcMs - offsetMin * 60000;
}
function ymd(p) { return `${p.y}${String(p.mo).padStart(2, "0")}${String(p.d).padStart(2, "0")}`; }
function gtfsTimeToMs(dateParts, hms) {
  // GTFS times legitimately exceed 24:00:00 for trips that run past midnight
  // as part of the PREVIOUS day's service — adding seconds past midnight to
  // that day's own midnight handles it correctly with no special-casing.
  const [h, m, s] = hms.split(":").map(Number);
  return nyWallTimeToMs(dateParts, 0, 0, 0) + (h * 3600 + m * 60 + s) * 1000;
}

// ---------- CSV (quoted-field aware — GTFS quotes every field) ----------

function parseCSV(text) {
  const lines = text.split("\n").filter((l) => l.trim());
  if (!lines.length) return [];
  const header = splitCSVLine(lines[0]);
  return lines.slice(1).map((line) => {
    const vals = splitCSVLine(line);
    const row = {};
    header.forEach((h, i) => (row[h] = vals[i]));
    return row;
  });
}
function splitCSVLine(line) {
  const out = [];
  let cur = "", inQ = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '"') { inQ = !inQ; continue; }
    if (c === "," && !inQ) { out.push(cur); cur = ""; continue; }
    cur += c;
  }
  out.push(cur);
  return out;
}

// ---------- static GTFS: download, cache, parse into an in-memory model ----------

async function ensureStaticGTFS() {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
  const zipPath = path.join(CACHE_DIR, "gtfslirr.zip");
  const stampPath = path.join(CACHE_DIR, ".fetched-at");
  let fresh = false;
  try {
    const stamp = +fs.readFileSync(stampPath, "utf8");
    fresh = Date.now() - stamp < STATIC_REFRESH_MS && fs.existsSync(path.join(CACHE_DIR, "stops.txt"));
  } catch {}
  if (fresh) return;

  console.log("LIRR: fetching static GTFS schedule...");
  const res = await fetch(STATIC_GTFS_URL);
  if (!res.ok) throw new Error(`GTFS static fetch failed: ${res.status}`);
  const buf = Buffer.from(await res.arrayBuffer());
  fs.writeFileSync(zipPath, buf);
  // `unzip` ships with macOS and virtually every Linux distro; avoids
  // pulling in a zip-parsing dependency for a file we refresh once a day.
  execFileSync("unzip", ["-o", zipPath, "-d", CACHE_DIR], { stdio: "ignore" });
  fs.writeFileSync(stampPath, String(Date.now()));
  console.log("LIRR: static GTFS refreshed");
}

let model = null; // { stops, routes, stopById, routeById, tripById, stByTrip, calDates }

function loadModel() {
  const read = (f) => fs.readFileSync(path.join(CACHE_DIR, f), "utf8");
  const stops = parseCSV(read("stops.txt"));
  const routes = parseCSV(read("routes.txt"));
  const trips = parseCSV(read("trips.txt"));
  const stopTimes = parseCSV(read("stop_times.txt"));
  const calDates = parseCSV(read("calendar_dates.txt"));

  const stopById = new Map(stops.map((s) => [s.stop_id, s]));
  const routeById = new Map(routes.map((r) => [r.route_id, r]));
  const tripById = new Map(trips.map((t) => [t.trip_id, t]));

  const stByTrip = new Map();
  for (const st of stopTimes) {
    if (!stByTrip.has(st.trip_id)) stByTrip.set(st.trip_id, []);
    stByTrip.get(st.trip_id).push(st);
  }
  for (const arr of stByTrip.values()) arr.sort((a, b) => +a.stop_sequence - +b.stop_sequence);

  const servicesByDate = new Map(); // yyyymmdd -> Set(service_id)
  for (const row of calDates) {
    if (row.exception_type !== "1") continue;
    if (!servicesByDate.has(row.date)) servicesByDate.set(row.date, new Set());
    servicesByDate.get(row.date).add(row.service_id);
  }

  model = { stops, routes, stopById, routeById, tripById, stByTrip, servicesByDate };
  console.log(`LIRR: loaded ${stops.length} stops, ${routes.length} routes, ${trips.length} trips`);
}

// ---------- next-train computation ----------

function buildCandidates(now, stByTrip, tripById, servicesByDate) {
  const nowParts = nyDateParts(now);
  const yParts = nyDateParts(new Date(now.getTime() - 86400000));
  const tParts = nyDateParts(new Date(now.getTime() + 86400000));
  const dateContexts = [yParts, nowParts, tParts].map((parts) => ({
    parts, services: servicesByDate.get(ymd(parts)) || new Set(),
  }));

  const candidates = [];
  for (const [tripId, sts] of stByTrip) {
    const trip = tripById.get(tripId);
    if (!trip) continue;
    for (const ctx of dateContexts) {
      if (!ctx.services.has(trip.service_id)) continue;
      candidates.push({
        tripId, routeId: trip.route_id,
        stops: sts.map((st) => ({
          stopId: st.stop_id, seq: +st.stop_sequence,
          depMs: gtfsTimeToMs(ctx.parts, st.departure_time),
          arrMs: gtfsTimeToMs(ctx.parts, st.arrival_time),
        })),
      });
    }
  }
  return candidates;
}

function nextDirect(candidates, destStopId, nowMs) {
  let best = null;
  for (const c of candidates) {
    const pennStop = c.stops.find((s) => s.stopId === PENN_STOP_ID);
    const destStop = c.stops.find((s) => s.stopId === destStopId);
    if (!pennStop || !destStop || pennStop.seq >= destStop.seq) continue;
    if (pennStop.depMs < nowMs) continue;
    if (!best || pennStop.depMs < best.depMs) best = { depMs: pennStop.depMs, tripId: c.tripId, routeId: c.routeId };
  }
  return best;
}

function nextViaJamaica(candidates, destStopId, nowMs) {
  let bestLeg2 = null;
  for (const c of candidates) {
    const jamStop = c.stops.find((s) => s.stopId === JAMAICA_STOP_ID);
    const destStop = c.stops.find((s) => s.stopId === destStopId);
    if (!jamStop || !destStop || jamStop.seq >= destStop.seq) continue;
    if (jamStop.depMs < nowMs) continue;
    if (!bestLeg2 || jamStop.depMs < bestLeg2.depMs) bestLeg2 = { jamDepMs: jamStop.depMs, tripId: c.tripId, routeId: c.routeId };
  }
  if (!bestLeg2) return null;
  let bestLeg1 = null;
  for (const c of candidates) {
    const pennStop = c.stops.find((s) => s.stopId === PENN_STOP_ID);
    const jamStop = c.stops.find((s) => s.stopId === JAMAICA_STOP_ID);
    if (!pennStop || !jamStop || pennStop.seq >= jamStop.seq) continue;
    if (pennStop.depMs < nowMs) continue;
    if (jamStop.arrMs + TRANSFER_BUFFER_MS > bestLeg2.jamDepMs) continue;
    if (!bestLeg1 || pennStop.depMs < bestLeg1.depMs) bestLeg1 = { depMs: pennStop.depMs, tripId: c.tripId };
  }
  if (!bestLeg1) return null;
  return { depMs: bestLeg1.depMs, tripId: bestLeg2.tripId, routeId: bestLeg2.routeId, penn2JamaicaTripId: bestLeg1.tripId };
}

// Pure and independently testable: candidates + "now" in, board out. No
// network, no filesystem — everything time-sensitive is an explicit input.
function computeBoard(model, candidates, now) {
  const nowMs = now.getTime();
  const results = [];
  for (const stop of model.stops) {
    if (stop.stop_id === PENN_STOP_ID) continue;
    if (SPECIAL_EVENTS_ONLY.has(stop.stop_name)) {
      results.push({ stopId: stop.stop_id, name: stop.stop_name, kind: "special", depMs: null, route: null });
      continue;
    }
    const direct = nextDirect(candidates, stop.stop_id, nowMs);
    if (direct) {
      const route = model.routeById.get(direct.routeId);
      results.push({
        stopId: stop.stop_id, name: stop.stop_name, kind: "direct", depMs: direct.depMs, tripId: direct.tripId,
        route: route ? { name: route.route_long_name, color: route.route_color, textColor: route.route_text_color } : null,
      });
      continue;
    }
    const viaJ = nextViaJamaica(candidates, stop.stop_id, nowMs);
    if (viaJ) {
      const route = model.routeById.get(viaJ.routeId);
      results.push({
        stopId: stop.stop_id, name: stop.stop_name, kind: "jamaica", depMs: viaJ.depMs, tripId: viaJ.penn2JamaicaTripId,
        route: route ? { name: route.route_long_name, color: route.route_color, textColor: route.route_text_color } : null,
      });
      continue;
    }
    results.push({ stopId: stop.stop_id, name: stop.stop_name, kind: "unknown", depMs: null, route: null });
  }
  results.sort((a, b) => a.name.localeCompare(b.name));
  return results;
}

// ---------- realtime (optional — needs LIRR_API_KEY) ----------
// Merges live delay seconds from the GTFS-realtime feed onto the trip a
// board row is actually using, so a delayed train's shown time reflects
// reality instead of the static schedule. Kept as a pure function (delay
// map in, board out) so it's testable without a live feed or key.

function applyRealtimeDelays(board, delayByTrip) {
  if (!delayByTrip || !delayByTrip.size) return board;
  return board.map((row) => {
    if (row.depMs == null || !row.tripId) return row;
    const delaySec = delayByTrip.get(row.tripId);
    if (!delaySec) return row;
    return { ...row, depMs: row.depMs + delaySec * 1000, delayed: true };
  });
}

// GTFS-realtime JSON shape (per the spec): { entity: [ { trip_update: {
//   trip: { trip_id }, stop_time_update: [ { stop_id, departure: { delay } } ] } } ] }
function parseDelayFeed(json) {
  const map = new Map(); // tripId -> delay seconds at Penn/Jamaica departure
  const entities = (json && json.entity) || [];
  for (const e of entities) {
    const tu = e.trip_update;
    if (!tu || !tu.trip || !tu.trip.trip_id) continue;
    const updates = tu.stop_time_update || [];
    // a single representative delay for the trip — the departure delay at
    // whichever origin-ish stop reported one — good enough for "running Xm
    // late" on a board that otherwise shows clean schedule times
    for (const u of updates) {
      const dep = u.departure || u.arrival;
      if (dep && typeof dep.delay === "number" && dep.delay !== 0) {
        map.set(tu.trip.trip_id, dep.delay);
        break;
      }
    }
  }
  return map;
}

let lastDelayByTrip = new Map();
async function refreshRealtime() {
  if (!RT_JSON_URL) return;
  try {
    const res = await fetch(RT_JSON_URL);
    if (!res.ok) throw new Error(`RT fetch ${res.status}`);
    const json = await res.json();
    lastDelayByTrip = parseDelayFeed(json);
  } catch (e) {
    console.error("LIRR realtime feed error (falling back to schedule only):", e.message);
  }
}

// ---------- board cache ----------

let boardCache = { rows: [], updatedAt: 0 };
function refreshBoard() {
  if (!model) return;
  const now = new Date();
  const candidates = buildCandidates(now, model.stByTrip, model.tripById, model.servicesByDate);
  let rows = computeBoard(model, candidates, now);
  rows = applyRealtimeDelays(rows, lastDelayByTrip);
  boardCache = { rows, updatedAt: Date.now() };
}

module.exports = {
  parseCSV, splitCSVLine, nyOffsetMinutes, nyDateParts, nyWallTimeToMs, gtfsTimeToMs, ymd,
  computeBoard, applyRealtimeDelays, parseDelayFeed, buildCandidates,
  PENN_STOP_ID, JAMAICA_STOP_ID, SPECIAL_EVENTS_ONLY,
};

// ---------- HTML ----------

const htmlPage = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Next Train To&hellip; — Moynihan Train Hall</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a1420; color: #e8edf2; min-height: 100vh;
    font-family: "Helvetica Neue", Arial, -apple-system, BlinkMacSystemFont, sans-serif;
    padding: 14px 14px 90px;
  }
  .board-hdr { display: flex; align-items: baseline; justify-content: space-between; margin-bottom: 4px; flex-wrap: wrap; gap: 6px; }
  .board-title { font-size: 20px; font-weight: 800; letter-spacing: 0.3px; }
  .board-sub { font-size: 11px; color: #6d8299; }
  .legend { font-size: 10.5px; color: #7f93a8; margin: 8px 0 16px; line-height: 1.6; }
  .legend b { color: #cfe0f0; }
  .cols { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 0 22px; }
  .letter-group { margin-bottom: 14px; break-inside: avoid; }
  .letter-hdr { font-size: 12px; font-weight: 800; color: #4d90c4; border-bottom: 1px solid #1c2f42; padding-bottom: 2px; margin-bottom: 3px; letter-spacing: 1px; }
  .row { display: flex; align-items: center; justify-content: space-between; gap: 8px; padding: 3px 0; font-size: 13.5px; }
  .row .name { color: #dfe8f0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; min-width: 0; }
  .row .info { display: flex; align-items: center; gap: 5px; flex-shrink: 0; }
  .xfer { font-size: 10.5px; font-weight: 800; color: #9fb4c8; }
  .pill { display: inline-flex; align-items: center; gap: 6px; padding: 2px 9px; border-radius: 3px; font-weight: 700; font-size: 12.5px; white-space: nowrap; font-variant-numeric: tabular-nums; }
  .pill.delayed::after { content: "LATE"; font-size: 8.5px; font-weight: 900; opacity: 0.85; margin-left: 2px; }
  .flat { color: #5c7086; font-style: italic; font-size: 12px; }
  .footer {
    position: fixed; left: 0; right: 0; bottom: 0; background: #060d16; border-top: 1px solid #1c2f42;
    display: flex; align-items: center; justify-content: space-between; padding: 10px 16px; font-size: 12.5px;
  }
  .footer .clock { font-variant-numeric: tabular-nums; font-weight: 700; }
  .footer .brand { color: #4d90c4; font-weight: 800; letter-spacing: 1.5px; font-size: 11px; }
  .footer .date { color: #7f93a8; }
  .err { color: #ef4444; font-size: 12px; padding: 10px 0; }
</style>
</head>
<body>
  <div class="board-hdr">
    <div>
      <div class="board-title">Next train to&hellip;</div>
      <div class="board-sub" id="updated">Loading&hellip;</div>
    </div>
  </div>
  <div class="legend"><b>J</b> Change at Jamaica &mdash; stations reachable only by a longer, multi-hub route show <i>Check TrainTime App</i> instead of a second transfer marker</div>
  <div class="cols" id="cols"><div class="flat">Loading&hellip;</div></div>

  <div class="footer">
    <span class="clock" id="clock">--:--:-- --</span>
    <span class="brand">MOYNIHAN TRAIN HALL</span>
    <span class="date" id="date">&nbsp;</span>
  </div>

<script>
function esc(s) { return String(s).replace(/[&<>]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;"}[c])); }

function fmtTime(ms) {
  return new Date(ms).toLocaleTimeString("en-US", { timeZone: "America/New_York", hour: "numeric", minute: "2-digit" });
}

function rowHTML(r) {
  var info;
  if (r.kind === "special") {
    info = '<span class="flat">Special Events Only</span>';
  } else if (r.kind === "unknown" || r.depMs == null) {
    info = '<span class="flat">Check TrainTime App</span>';
  } else {
    var xfer = r.kind === "jamaica" ? '<span class="xfer">J</span>' : '';
    var route = r.route || { name: "", color: "3b3b3b", textColor: "ffffff" };
    info = xfer + '<span class="pill' + (r.delayed ? ' delayed' : '') + '" style="background:#' + route.color + ';color:#' + route.textColor + '">' +
      fmtTime(r.depMs) + ' ' + esc(route.name.replace(/ Branch$/, "")) + '</span>';
  }
  return '<div class="row"><span class="name">' + esc(r.name) + '</span><span class="info">' + info + '</span></div>';
}

function render(rows) {
  var groups = [];
  var cur = null;
  rows.forEach(function (r) {
    var letter = r.name[0].toUpperCase();
    if (!cur || cur.letter !== letter) { cur = { letter: letter, rows: [] }; groups.push(cur); }
    cur.rows.push(r);
  });
  document.getElementById("cols").innerHTML = groups.map(function (g) {
    return '<div class="letter-group"><div class="letter-hdr">' + g.letter + '</div>' + g.rows.map(rowHTML).join("") + '</div>';
  }).join("");
}

function load() {
  fetch("/api/board").then(function (r) { return r.json(); }).then(function (d) {
    render(d.rows);
    document.getElementById("updated").textContent =
      d.rows.length + " destinations \\u00b7 updated " + new Date(d.updatedAt).toLocaleTimeString("en-US", { timeZone: "America/New_York" });
  }).catch(function (e) {
    document.getElementById("cols").innerHTML = '<div class="err">' + esc(e.message) + '</div>';
  });
}

function tickClock() {
  var now = new Date();
  document.getElementById("clock").textContent = now.toLocaleTimeString("en-US", { timeZone: "America/New_York" });
  document.getElementById("date").textContent = now.toLocaleDateString("en-US", { timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric" });
}

load();
tickClock();
setInterval(load, 30000);
setInterval(tickClock, 1000);
</script>
</body>
</html>`;

// ---------- server ----------

if (require.main === module) {
  const server = http.createServer((req, res) => {
    if (req.url === "/favicon.ico") { res.writeHead(204); res.end(); return; }
    if (req.url === "/api/board") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(boardCache));
      return;
    }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(htmlPage);
  });

  (async () => {
    await ensureStaticGTFS();
    loadModel();
    refreshBoard();
    setInterval(refreshBoard, BOARD_REFRESH_MS);
    if (RT_JSON_URL) {
      await refreshRealtime();
      setInterval(refreshRealtime, RT_REFRESH_MS);
      console.log("LIRR: realtime delay feed enabled");
    } else {
      console.log("LIRR: no LIRR_API_KEY set — schedule-only, no live delay adjustment");
    }
    server.listen(PORT, () => console.log(`\nLIRR "Next train to..." board running at http://localhost:${PORT}\n`));
  })().catch((e) => {
    console.error("LIRR board failed to start:", e);
    process.exit(1);
  });
}
