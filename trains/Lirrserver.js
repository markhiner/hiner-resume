'use strict';

/**
 * LIRR Real-Time Departure Board — server.js
 * ============================================
 *
 * DATA SOURCE: LIRR TrainTime API
 *   Endpoint : https://traintime.lirr.org/api/Departure?api_key=KEY&loc=STATION_CODE
 *   Format   : JSON
 *   Key      : Free — register at https://api.mta.info/
 *              1. Visit https://api.mta.info/
 *              2. Click "Sign Up" and create a free account
 *              3. Copy the API key shown on your dashboard
 *              4. export LIRR_API_KEY="your_key_here"
 *
 * Station codes are 3-letter LIRR identifiers, e.g.:
 *   NYK = New York Penn Station
 *   GCM = Grand Central Madison
 *   JAM = Jamaica
 *   ATL = Atlantic Terminal (Brooklyn)
 *   HVL = Hicksville
 *
 * USAGE:
 *   LIRR_API_KEY=yourkey node server.js
 *   Open http://localhost:3000 in Safari
 */

const http  = require('http');
const https = require('https');
const fs    = require('fs');
const path  = require('path');
const { URL } = require('url');

const PORT        = process.env.PORT || 3000;
const CACHE_TTL   = 30 * 1000; // 30 seconds
const API_BASE    = 'https://traintime.lirr.org/api/Departure';
const INDEX_PATH  = path.join(__dirname, 'index.html');

// ─────────────────────────────────────────────
//  Station list  (code → display name)
//  Codes are the 3-letter loc= codes accepted by
//  the LIRR TrainTime API.  Verified codes marked ✓
// ─────────────────────────────────────────────
const STATIONS = [
  // ── Terminals / major junctions ──────────────
  { code: 'NYK', name: 'New York - Penn Station' },      // ✓ confirmed
  { code: 'GCM', name: 'Grand Central Madison' },        // opened Jan 2023
  { code: 'ATL', name: 'Atlantic Terminal' },            // ✓ confirmed
  { code: 'JAM', name: 'Jamaica' },                      // ✓ confirmed

  // ── Shared/Queens ─────────────────────────────
  { code: 'WOS', name: 'Woodside' },
  { code: 'FRH', name: 'Forest Hills' },
  { code: 'KGN', name: 'Kew Gardens' },

  // ── Port Washington Branch ────────────────────
  { code: 'BAY', name: 'Bayside' },
  { code: 'DGL', name: 'Douglaston' },
  { code: 'LTN', name: 'Little Neck' },
  { code: 'GRN', name: 'Great Neck' },
  { code: 'KNP', name: 'Kings Point' },
  { code: 'MNH', name: 'Manhasset' },
  { code: 'PLD', name: 'Plandome' },
  { code: 'PWT', name: 'Port Washington' },

  // ── Babylon Branch ────────────────────────────
  { code: 'VLY', name: 'Valley Stream' },
  { code: 'LVN', name: 'Lynbrook' },
  { code: 'RKV', name: 'Rockville Centre' },
  { code: 'BLD', name: 'Baldwin' },
  { code: 'FRP', name: 'Freeport' },
  { code: 'MRK', name: 'Merrick' },
  { code: 'BLM', name: 'Bellmore' },
  { code: 'WTG', name: 'Wantagh' },
  { code: 'SFD', name: 'Seaford' },
  { code: 'MSP', name: 'Massapequa' },
  { code: 'MPK', name: 'Massapequa Park' },
  { code: 'LND', name: 'Lindenhurst' },
  { code: 'CPG', name: 'Copiague' },
  { code: 'AMT', name: 'Amityville' },
  { code: 'BAB', name: 'Babylon' },

  // ── Long Beach Branch ─────────────────────────
  { code: 'OCN', name: 'Oceanside' },
  { code: 'ISP', name: 'Island Park' },
  { code: 'LBH', name: 'Long Beach' },

  // ── Far Rockaway Branch ───────────────────────
  { code: 'GBS', name: 'Gibson' },
  { code: 'HWT', name: 'Hewlett' },
  { code: 'WWD', name: 'Woodmere' },
  { code: 'CDH', name: 'Cedarhurst' },
  { code: 'LWR', name: 'Lawrence' },
  { code: 'INW', name: 'Inwood' },
  { code: 'FAR', name: 'Far Rockaway' },

  // ── Hempstead Branch ──────────────────────────
  { code: 'CTL', name: 'Country Life Press' },
  { code: 'GRC', name: 'Garden City' },
  { code: 'NSD', name: 'Nassau Blvd' },
  { code: 'HEM', name: 'Hempstead' },

  // ── West Hempstead Branch ─────────────────────
  { code: 'FLP', name: 'Floral Park' },
  { code: 'BLR', name: 'Bellerose' },
  { code: 'STM', name: 'Stewart Manor' },
  { code: 'RTL', name: 'Rutland' },
  { code: 'WHM', name: 'West Hempstead' },

  // ── Main Line / Ronkonkoma Branch ────────────
  { code: 'MVN', name: 'Mineola' },
  { code: 'CRK', name: 'Carle Place' },
  { code: 'WBY', name: 'Westbury' },
  { code: 'HVL', name: 'Hicksville' },               // ✓ confirmed
  { code: 'BPG', name: 'Bethpage' },
  { code: 'FMD', name: 'Farmingdale' },
  { code: 'PLN', name: 'Pinelawn' },
  { code: 'DNP', name: 'Deer Park' },
  { code: 'WYN', name: 'Wyandanch' },
  { code: 'BRW', name: 'Brentwood' },
  { code: 'CIT', name: 'Central Islip' },
  { code: 'ISL', name: 'Islandia' },
  { code: 'RON', name: 'Ronkonkoma' },

  // ── Oyster Bay Branch ─────────────────────────
  { code: 'SYO', name: 'Syosset' },
  { code: 'CSH', name: 'Cold Spring Harbor' },
  { code: 'LVL', name: 'Locust Valley' },
  { code: 'GCV', name: 'Glen Cove' },
  { code: 'SCF', name: 'Sea Cliff' },
  { code: 'GST', name: 'Glen Street' },
  { code: 'MNK', name: 'Mill Neck' },
  { code: 'OYB', name: 'Oyster Bay' },

  // ── Port Jefferson / Huntington Branch ───────
  { code: 'HNT', name: 'Huntington' },
  { code: 'NPT', name: 'Northport' },
  { code: 'KGP', name: 'Kings Park' },
  { code: 'SMT', name: 'Smithtown' },
  { code: 'STJ', name: 'St. James' },
  { code: 'STN', name: 'Stony Brook' },              // ✓ appears in demo URL
  { code: 'POJ', name: 'Port Jefferson' },
];

// ─────────────────────────────────────────────
//  In-memory cache
// ─────────────────────────────────────────────
const cache = new Map(); // key: stationCode → { data, expiresAt }

function getCached(key) {
  const entry = cache.get(key);
  if (entry && Date.now() < entry.expiresAt) return entry.data;
  return null;
}
function setCached(key, data) {
  cache.set(key, { data, expiresAt: Date.now() + CACHE_TTL });
}

// ─────────────────────────────────────────────
//  Time helpers
// ─────────────────────────────────────────────

/**
 * Parse a time string from the API into today's Date.
 * Handles:  "17:00"  "5:00 PM"  "05:00"  ISO strings
 */
function parseTimeToday(str) {
  if (!str) return null;
  str = String(str).trim();

  // ISO / timestamp
  if (str.includes('T') || str.includes('-')) {
    const d = new Date(str);
    if (!isNaN(d)) return d;
  }

  const now = new Date();

  // "5:00 PM" or "5:00PM"
  const ampm = str.match(/^(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
  if (ampm) {
    let h = parseInt(ampm[1], 10);
    const m = parseInt(ampm[2], 10);
    const isPM = ampm[3].toUpperCase() === 'PM';
    if (isPM && h !== 12) h += 12;
    if (!isPM && h === 12) h = 0;
    const d = new Date(now);
    d.setHours(h, m, 0, 0);
    return d;
  }

  // "17:00" or "17:00:00"
  const hhmm = str.match(/^(\d{1,2}):(\d{2})(?::\d{2})?$/);
  if (hhmm) {
    const d = new Date(now);
    d.setHours(parseInt(hhmm[1], 10), parseInt(hhmm[2], 10), 0, 0);
    return d;
  }

  return null;
}

function formatHHMM(date) {
  if (!date) return '—';
  const h = String(date.getHours()).padStart(2, '0');
  const m = String(date.getMinutes()).padStart(2, '0');
  return `${h}:${m}`;
}

// ─────────────────────────────────────────────
//  Branch detection  (destination → branch name)
// ─────────────────────────────────────────────
function detectBranch(destination) {
  if (!destination) return 'Unknown';
  const d = destination.toLowerCase().trim();

  // Terminals (as destination from eastern stations)
  if (d.includes('penn') || d === 'new york' || d.includes('ny penn')) return 'Penn Station';
  if (d.includes('grand central')) return 'Penn Station';
  if (d.includes('atlantic terminal')) return 'Penn Station';

  // Far Rockaway Branch
  if (d.includes('far rockaway') || d === 'inwood' || d.includes('lawrence') ||
      d.includes('cedarhurst') || d.includes('woodmere') || d.includes('hewlett') ||
      d.includes('gibson')) return 'Far Rockaway';

  // Long Beach Branch
  if (d.includes('long beach') || d.includes('island park') || d.includes('lakeview')) return 'Long Beach';

  // West Hempstead Branch
  if (d.includes('west hempstead') || d.includes('stewart manor') || d === 'rutland' ||
      d.includes('bellerose') || d.includes('floral park')) return 'West Hempstead';

  // Hempstead Branch
  if (d === 'hempstead' || d.includes('garden city') || d.includes('nassau blvd') ||
      d.includes('country life')) return 'Hempstead';

  // Oyster Bay Branch
  if (d.includes('oyster bay') || d.includes('mill neck') || d.includes('glen street') ||
      d.includes('sea cliff') || d.includes('glen cove') || d.includes('locust valley')) return 'Oyster Bay';

  // Port Jefferson Branch (Huntington is the split point)
  if (d.includes('port jefferson') || d.includes('stony brook') || d.includes('st. james') ||
      d.includes('st james') || d.includes('smithtown') || d.includes('kings park') ||
      d.includes('northport')) return 'Port Jefferson';

  if (d.includes('huntington') || d.includes('cold spring harbor') || d.includes('syosset')) return 'Huntington';

  // Ronkonkoma Branch
  if (d.includes('ronkonkoma') || d.includes('central islip') || d.includes('islandia') ||
      d.includes('brentwood') || d.includes('wyandanch') || d.includes('deer park') ||
      d.includes('pinelawn') || d.includes('farmingdale')) return 'Ronkonkoma';

  // Babylon Branch – check Massapequa first (sub-branch color)
  if (d.includes('massapequa')) return 'Massapequa';
  if (d === 'babylon' || d.includes('amityville') || d.includes('copiague') ||
      d.includes('lindenhurst') || d.includes('seaford') || d.includes('wantagh') ||
      d.includes('bellmore') || d.includes('merrick') || d.includes('freeport') ||
      d.includes('rockville centre') || d.includes('baldwin') || d.includes('oceanside') ||
      d.includes('lynbrook') || d.includes('valley stream')) return 'Babylon';

  // Jamaica (as destination)
  if (d === 'jamaica') return 'Jamaica';

  // Port Washington
  if (d.includes('port washington') || d.includes('plandome') || d.includes('manhasset') ||
      d.includes('great neck')) return 'Port Washington';

  return 'Unknown';
}

// ─────────────────────────────────────────────
//  Parse raw API departure → normalized object
// ─────────────────────────────────────────────
function transformDeparture(dep) {
  // Accommodate possible field name variations in the API response
  const trainId     = dep.TrainID     || dep.trainId     || dep.train_id     || '';
  const destination = dep.Destination || dep.destination || dep.DestinationName || '';
  const rawSched    = dep.ScheduledDepartureTime  || dep.scheduledDepartureTime  ||
                      dep.ScheduledTime            || dep.sched                   || '';
  const rawEst      = dep.EstimatedDepartureTime  || dep.estimatedDepartureTime  ||
                      dep.EstimatedTime            || dep.estimated               || rawSched;
  const track       = dep.Track || dep.TrackNumber || dep.track || null;
  const statusRaw   = String(dep.Status || dep.status || '').trim();

  const scheduled  = parseTimeToday(rawSched);
  const estimated  = parseTimeToday(rawEst) || scheduled;
  const now        = new Date();

  // Minutes late (estimated vs scheduled)
  let minutesLate = 0;
  if (scheduled && estimated) {
    minutesLate = Math.round((estimated - scheduled) / 60000);
    if (minutesLate < 0) minutesLate = 0;
  }

  // Parse delay from status string if we don't have an explicit estimated time
  // e.g. "5 LATE"  or  "3 Minutes Late"
  if (minutesLate === 0 && rawEst === rawSched) {
    const lateMatch = statusRaw.match(/(\d+)\s*(?:min(?:utes?)?|late)/i);
    if (lateMatch) {
      minutesLate = parseInt(lateMatch[1], 10);
      if (scheduled) {
        estimated.setMinutes(estimated.getMinutes() + minutesLate);
      }
    }
  }

  const minsUntilDeparture = scheduled
    ? Math.round((estimated - now) / 60000)
    : null;

  // Determine status
  let status = 'ON_TIME';
  const su = statusRaw.toUpperCase();
  if (su.includes('CANCEL')) {
    status = 'CANCELLED';
  } else if (su === 'DEPARTED' || su === 'GONE' || (minsUntilDeparture !== null && minsUntilDeparture < -2)) {
    status = 'DEPARTED';
  } else if (minutesLate > 2) {
    status = 'LATE';
  }

  return {
    trainId:             trainId,
    destination:         destination,
    branch:              detectBranch(destination),
    scheduledDeparture:  formatHHMM(scheduled),
    estimatedDeparture:  formatHHMM(estimated),
    minutesLate:         minutesLate,
    minsUntilDeparture:  minsUntilDeparture,
    track:               track ? String(track) : null,
    status:              status,
  };
}

// ─────────────────────────────────────────────
//  Fetch departures from traintime.lirr.org
// ─────────────────────────────────────────────
function fetchFromAPI(stationCode) {
  return new Promise((resolve, reject) => {
    const apiKey = process.env.LIRR_API_KEY;
    if (!apiKey) {
      reject(new Error('LIRR_API_KEY environment variable not set.\n' +
        'Get a free key at https://api.mta.info/ then run:\n' +
        '  export LIRR_API_KEY=yourkey && node server.js'));
      return;
    }

    const reqUrl = `${API_BASE}?api_key=${encodeURIComponent(apiKey)}&loc=${encodeURIComponent(stationCode)}`;
    const parsed = new URL(reqUrl);

    const options = {
      hostname: parsed.hostname,
      path:     parsed.pathname + parsed.search,
      method:   'GET',
      headers:  { 'Accept': 'application/json', 'User-Agent': 'LIRR-Board/1.0' },
      timeout:  10000,
    };

    const req = https.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        if (res.statusCode === 401 || res.statusCode === 403) {
          return reject(new Error(`API returned ${res.statusCode}: invalid or missing API key. ` +
            'Check LIRR_API_KEY or register at https://api.mta.info/'));
        }
        if (res.statusCode === 404) {
          return reject(new Error(`Station code "${stationCode}" not found. ` +
            'Use GET /api/stations to see valid codes.'));
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`API HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
        }

        // Guard against HTML error pages
        if (body.trim().startsWith('<')) {
          return reject(new Error('API returned HTML instead of JSON — endpoint may be deprecated. ' +
            'Check https://api.mta.info/ for updates.'));
        }

        try {
          const json = JSON.parse(body);
          // Response is either { Departure: [...] } or an array directly
          const list = Array.isArray(json) ? json
                     : Array.isArray(json.Departure) ? json.Departure
                     : [];
          resolve(list.map(transformDeparture));
        } catch (e) {
          reject(new Error(`JSON parse error: ${e.message}. Body: ${body.slice(0, 200)}`));
        }
      });
    });

    req.on('error', reject);
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('API request timed out after 10 s'));
    });
    req.end();
  });
}

// ─────────────────────────────────────────────
//  Public: get departures (cached)
// ─────────────────────────────────────────────
async function getDepartures(stationCode) {
  const key = stationCode.toUpperCase();
  const hit = getCached(key);
  if (hit) return hit;

  const data = await fetchFromAPI(key);
  setCached(key, data);
  return data;
}

// ─────────────────────────────────────────────
//  HTTP utilities
// ─────────────────────────────────────────────
function json(res, statusCode, data) {
  const body = JSON.stringify(data);
  res.writeHead(statusCode, {
    'Content-Type':  'application/json',
    'Cache-Control': 'no-store',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(body);
}

function serve404(res) {
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
}

// ─────────────────────────────────────────────
//  HTTP server
// ─────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  // Strip query string for routing
  const urlObj = new URL(req.url, `http://localhost`);
  const pathname = urlObj.pathname;

  // CORS pre-flight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET' });
    return res.end();
  }

  // ── GET /api/health ─────────────────────────
  if (pathname === '/api/health') {
    return json(res, 200, {
      status: 'ok',
      apiKeySet: !!process.env.LIRR_API_KEY,
      cacheEntries: cache.size,
      time: new Date().toISOString(),
    });
  }

  // ── GET /api/stations ───────────────────────
  if (pathname === '/api/stations') {
    return json(res, 200, STATIONS);
  }

  // ── GET /api/departures?station=XXX ─────────
  if (pathname === '/api/departures') {
    const station = (urlObj.searchParams.get('station') || '').trim().toUpperCase();
    if (!station) {
      return json(res, 400, { error: 'Missing ?station= parameter. Example: /api/departures?station=NYK' });
    }
    try {
      const departures = await getDepartures(station);
      return json(res, 200, {
        station,
        fetchedAt: new Date().toISOString(),
        cacheExpiresIn: Math.round((cache.get(station)?.expiresAt - Date.now()) / 1000),
        departures,
      });
    } catch (err) {
      console.error(`[departures] ${station}: ${err.message}`);
      return json(res, 502, { error: err.message });
    }
  }

  // ── GET / → index.html ───────────────────────
  if (pathname === '/') {
    try {
      const html = fs.readFileSync(INDEX_PATH);
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      return res.end(html);
    } catch (e) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      return res.end('index.html not found next to server.js');
    }
  }

  serve404(res);
});

server.listen(PORT, () => {
  const keyStatus = process.env.LIRR_API_KEY
    ? '✓ LIRR_API_KEY is set'
    : '⚠  LIRR_API_KEY not set — get a free key at https://api.mta.info/';

  console.log(`\n🚂  LIRR Departure Board`);
  console.log(`   Listening on http://localhost:${PORT}`);
  console.log(`   ${keyStatus}`);
  console.log(`\n   Open in Safari: http://localhost:${PORT}\n`);
});

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Port ${PORT} already in use. Try: PORT=3001 node server.js`);
  } else {
    console.error('Server error:', err);
  }
  process.exit(1);
});
