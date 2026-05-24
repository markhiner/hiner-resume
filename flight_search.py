#!/usr/bin/env python3
"""
Flight price comparison using SerpAPI Google Flights.
Searches one-way flights between RDU/GSO and NYC area airports (LGA/JFK/EWR).
Airlines: Delta (DL), American (AA), United (UA).
"""

import os
import sys
import webbrowser
import tempfile
from datetime import datetime
from itertools import product
import requests

SERPAPI_KEY = os.environ.get('SERPAPI_KEY', '')

HOME_AIRPORTS = ['RDU', 'GSO']
NYC_AIRPORTS  = ['LGA', 'JFK', 'EWR']

# Match against full names returned by SerpAPI and IATA codes in flight numbers
ALLOWED_AIRLINE_NAMES = {'delta', 'american', 'american airlines', 'united', 'united airlines'}
ALLOWED_AIRLINE_CODES = {'DL', 'AA', 'UA'}
AIRLINE_NAMES = {'DL': 'Delta', 'AA': 'American', 'UA': 'United'}

# travel_class values for SerpAPI Google Flights
ECONOMY = 1
FIRST   = 4


# ── helpers ──────────────────────────────────────────────────────────────────

def ask_date() -> str:
    while True:
        raw = input("Travel date (YYYY-MM-DD): ").strip()
        try:
            datetime.strptime(raw, '%Y-%m-%d')
            return raw
        except ValueError:
            print("  Please use YYYY-MM-DD format.")


def ask_direction() -> str:
    while True:
        raw = input("Going to NYC or going home? [nyc/home]: ").strip().lower()
        if raw in ('nyc', 'home'):
            return raw
        print("  Please type 'nyc' or 'home'.")


def fmt_time(dt_str: str) -> str:
    """Extract HH:MM from 'YYYY-MM-DD HH:MM' or return as-is."""
    if dt_str and ' ' in dt_str:
        return dt_str.split(' ')[1]
    return dt_str or '—'


def fmt_duration(minutes) -> str:
    if not minutes:
        return '—'
    try:
        m = int(minutes)
        return f"{m // 60:02d}:{m % 60:02d}"
    except (ValueError, TypeError):
        return '—'


def price_color(price: int, cabin: int) -> str:
    if cabin == ECONOMY:
        if price <= 200:  return '#22c55e'   # green
        if price < 300:   return '#e2e8f0'   # neutral
        if price <= 500:  return '#f97316'   # orange
        return '#ef4444'                      # red
    else:  # FIRST
        if price <= 300:  return '#22c55e'
        if price < 500:   return '#e2e8f0'
        if price <= 700:  return '#f97316'
        return '#ef4444'


# ── SerpAPI ──────────────────────────────────────────────────────────────────

def search_flights(departure: str, arrival: str, date: str, cabin: int) -> dict:
    params = {
        'engine':         'google_flights',
        'departure_id':   departure,
        'arrival_id':     arrival,
        'outbound_date':  date,
        'currency':       'USD',
        'hl':             'en',
        'gl':             'us',
        'type':           '2',        # one-way
        'adults':         '1',
        'travel_class':   str(cabin),
        'api_key':        SERPAPI_KEY,
    }
    try:
        r = requests.get('https://serpapi.com/search', params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        print(f"    API error ({departure}→{arrival}): {exc}")
        return {}


def airline_allowed(leg: dict) -> bool:
    """Accept a leg if the airline name or flight-number prefix matches DL/AA/UA."""
    name = leg.get('airline', '').lower()
    if name in ALLOWED_AIRLINE_NAMES:
        return True
    fn = leg.get('flight_number', '')
    code = fn[:2].upper() if fn else ''
    return code in ALLOWED_AIRLINE_CODES


def parse_flights(data: dict, cabin: int, debug: bool = False) -> list:
    best  = data.get('best_flights', [])
    other = data.get('other_flights', [])
    if debug:
        print(f'    [debug] raw sections: best_flights={len(best)}, other_flights={len(other)}')
        if best:
            sample = best[0].get('flights', [{}])[0]
            print(f'    [debug] sample leg keys: {list(sample.keys())}')
            print(f'    [debug] sample airline field: {sample.get("airline")!r}')
            print(f'    [debug] sample flight_number: {sample.get("flight_number")!r}')

    results = []
    seen = set()

    for section in ('best_flights', 'other_flights'):
        for option in data.get(section, []):
            legs  = option.get('flights', [])
            price = option.get('price')
            if not legs or not price:
                continue

            # keep only itineraries where at least one leg is DL/AA/UA
            if not any(airline_allowed(leg) for leg in legs):
                if debug:
                    names = [leg.get('airline') for leg in legs]
                    print(f'    [debug] filtered out: {names}')
                continue

            first = legs[0]
            last  = legs[-1]

            dep_id   = first.get('departure_airport', {}).get('id', '')
            dep_time = first.get('departure_airport', {}).get('time', '')
            arr_id   = last.get('arrival_airport', {}).get('id', '')
            arr_time = last.get('arrival_airport', {}).get('time', '')

            key = (dep_id, dep_time, arr_id, arr_time, price, cabin)
            if key in seen:
                continue
            seen.add(key)

            layovers = option.get('layovers', [])
            airline_raw = first.get('airline', '')

            results.append({
                'price':             price,
                'cabin':             cabin,
                'airline':           airline_raw,
                'airline_name':      airline_raw,
                'airline_logo':      first.get('airline_logo', ''),
                'departure_airport': dep_id,
                'departure_time':    dep_time,
                'arrival_airport':   arr_id,
                'arrival_time':      arr_time,
                'nonstop':           len(legs) == 1,
                'layovers':          layovers,
                'legs':              legs,
            })

    return results


# ── HTML report ───────────────────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #0f172a; color: #e2e8f0;
  padding: 1.5rem; min-height: 100vh;
}
header { margin-bottom: 1.5rem; }
h1 { font-size: 1.5rem; color: #f8fafc; margin-bottom: .2rem; }
.subtitle { color: #475569; font-size: .8rem; }

.columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  align-items: start;
}
.col-head {
  font-size: .7rem; font-weight: 600; letter-spacing: .08em;
  text-transform: uppercase; color: #64748b;
  margin-bottom: .6rem;
  display: flex; align-items: center; gap: .5rem;
}
.badge {
  background: #1e293b; color: #64748b;
  border-radius: 9999px; font-size: .65rem;
  padding: .1rem .45rem; font-weight: 400; letter-spacing: 0;
  text-transform: none;
}

/* ── card ── */
.card {
  background: #1e293b; border: 1px solid #1e293b;
  border-radius: .6rem; margin-bottom: .5rem;
  cursor: pointer; overflow: hidden;
  transition: border-color .15s;
}
.card:hover { border-color: #334155; }
.card.open  { border-color: #334155; }

.card-face {
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: center;
  gap: .75rem;
  padding: .7rem .85rem;
}

.logo { height: 22px; width: 60px; object-fit: contain; object-position: left; }
.airline-text { font-size: .75rem; font-weight: 600; color: #64748b; }

.route {
  display: flex; align-items: baseline; gap: .3rem;
  font-size: .82rem;
}
.ap   { font-weight: 700; color: #f1f5f9; font-size: .9rem; }
.tm   { color: #64748b; font-size: .72rem; }
.sep  { color: #334155; margin: 0 .1rem; }

.meta {
  font-size: .68rem; color: #475569; margin-top: .15rem;
  display: flex; gap: .4rem; align-items: center;
}
.stop-pill {
  font-size: .65rem; padding: .1rem .4rem;
  border-radius: 9999px; background: #0f172a;
  color: #64748b; white-space: nowrap;
}
.plane { color: #475569; }

.price {
  font-size: 1.25rem; font-weight: 700;
  white-space: nowrap; text-align: right;
}

/* ── expanded detail ── */
.card-detail {
  display: none;
  border-top: 1px solid #0f172a;
  padding: .6rem .85rem .75rem;
  background: #172033;
}
.card.open .card-detail { display: block; }

.leg-row {
  display: flex; align-items: baseline;
  gap: .4rem; font-size: .75rem;
  color: #94a3b8; padding: .2rem 0;
}
.leg-ap   { font-weight: 700; color: #cbd5e1; min-width: 2.2rem; }
.leg-tm   { color: #64748b; min-width: 2.8rem; }
.leg-arr  { margin-left: auto; display: flex; gap: .4rem; align-items: baseline; }
.leg-info { font-size: .68rem; color: #334155; margin-left: .3rem; }
.lv-row {
  font-size: .68rem; color: #d97706;
  padding: .15rem 0 .15rem .3rem;
  border-left: 2px solid #78350f;
  margin: .1rem 0;
}

.empty { color: #334155; font-style: italic; font-size: .8rem; padding: .5rem 0; }
"""

JS = """
document.querySelectorAll('.card').forEach(card => {
  card.addEventListener('click', () => card.classList.toggle('open'));
});
"""


def render_detail(legs: list, layovers: list) -> str:
    html = '<div class="card-detail">'
    for i, leg in enumerate(legs):
        dep  = leg.get('departure_airport', {})
        arr  = leg.get('arrival_airport', {})
        fn   = leg.get('flight_number', '')
        ac   = leg.get('airplane', '')
        dur  = fmt_duration(leg.get('duration'))
        info = ' · '.join(p for p in [fn, ac, dur] if p)
        html += f'''<div class="leg-row">
          <span class="leg-ap">{dep.get('id','?')}</span>
          <span class="leg-tm">{fmt_time(dep.get('time',''))}</span>
          <span>→</span>
          <span class="leg-ap">{arr.get('id','?')}</span>
          <span class="leg-tm">{fmt_time(arr.get('time',''))}</span>
          <span class="leg-info">{info}</span>
        </div>'''
        if i < len(layovers):
            lv  = layovers[i]
            lv_id  = lv.get('id', '')
            lv_dur = fmt_duration(lv.get('duration'))
            html += f'<div class="lv-row">Layover · {lv_id} · {lv_dur}</div>'
    html += '</div>'
    return html


def render_card(f: dict) -> str:
    price  = f['price']
    cabin  = f['cabin']
    color  = price_color(price, cabin)
    legs   = f['legs']
    layovers = f['layovers']

    logo = (f'<img class="logo" src="{f["airline_logo"]}" alt="{f["airline_name"]}">'
            if f['airline_logo'] else f'<span class="airline-text">{f["airline_name"]}</span>')

    d_time = fmt_time(f['departure_time'])
    a_time = fmt_time(f['arrival_time'])

    # stop info: Nonstop or layover airport code(s)
    if f['nonstop']:
        stop_label = 'Nonstop'
    else:
        codes = [lv.get('id', '?') for lv in layovers]
        stop_label = ' · '.join(codes) if codes else '1 stop'

    # airplane types for all legs joined by /
    planes = ' / '.join(leg.get('airplane', '') for leg in legs if leg.get('airplane'))

    return f'''<div class="card">
  <div class="card-face">
    <div>{logo}</div>
    <div>
      <div class="route">
        <span class="ap">{f["departure_airport"]}</span>
        <span class="tm">{d_time}</span>
        <span class="sep">→</span>
        <span class="ap">{f["arrival_airport"]}</span>
        <span class="tm">{a_time}</span>
      </div>
      <div class="meta">
        <span class="stop-pill">{stop_label}</span>
        <span class="plane">{planes}</span>
      </div>
    </div>
    <div class="price" style="color:{color}">${price}</div>
  </div>
  {render_detail(legs, layovers)}
</div>'''


def build_html(econ: list, first: list, date: str, direction: str) -> str:
    econ.sort(key=lambda x: x['price'])
    first.sort(key=lambda x: x['price'])

    dest_label = 'NYC (LGA / JFK / EWR)' if direction == 'nyc' else 'Home (RDU / GSO)'

    econ_cards  = ''.join(render_card(f) for f in econ)  or '<p class="empty">No economy flights found.</p>'
    first_cards = ''.join(render_card(f) for f in first) or '<p class="empty">No first class flights found.</p>'

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Flights · {date} · {dest_label}</title>
  <style>{CSS}</style>
</head>
<body>
  <header>
    <h1>Flight Price Report</h1>
    <p class="subtitle">{date} &nbsp;·&nbsp; To {dest_label} &nbsp;·&nbsp; Delta · American · United &nbsp;·&nbsp; One-way · 1 adult</p>
  </header>

  <div class="columns">
    <div>
      <div class="col-head">Economy <span class="badge">{len(econ)}</span></div>
      {econ_cards}
    </div>
    <div>
      <div class="col-head">First Class <span class="badge">{len(first)}</span></div>
      {first_cards}
    </div>
  </div>

  <script>{JS}</script>
</body>
</html>'''


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    if not SERPAPI_KEY:
        sys.exit('Error: SERPAPI_KEY environment variable is not set.')

    debug = '--debug' in sys.argv

    print()
    date      = ask_date()
    direction = ask_direction()

    if direction == 'nyc':
        routes = list(product(HOME_AIRPORTS, NYC_AIRPORTS))
    else:
        routes = list(product(NYC_AIRPORTS, HOME_AIRPORTS))

    total = len(routes) * 2
    print(f'\nSearching {len(routes)} routes × 2 cabins = {total} API calls...\n')

    all_econ  = []
    all_first = []

    for dep, arr in routes:
        for cabin, label in [(ECONOMY, 'Economy'), (FIRST, 'First')]:
            print(f'  {dep} → {arr}  [{label}] ...', end=' ', flush=True)
            data    = search_flights(dep, arr, date, cabin)
            flights = parse_flights(data, cabin, debug=debug)
            print(f'{len(flights)} result{"s" if len(flights) != 1 else ""}')
            if cabin == ECONOMY:
                all_econ.extend(flights)
            else:
                all_first.extend(flights)

    print(f'\nTotal: {len(all_econ)} economy · {len(all_first)} first class\n')

    html     = build_html(all_econ, all_first, date, direction)
    tmp_file = tempfile.NamedTemporaryFile(
        mode='w', suffix='.html', delete=False,
        prefix=f'flights_{date}_', encoding='utf-8'
    )
    tmp_file.write(html)
    tmp_file.close()

    print(f'Report: {tmp_file.name}')
    webbrowser.open(f'file://{tmp_file.name}')


if __name__ == '__main__':
    main()
