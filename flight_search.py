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

ALLOWED_AIRLINES = {'DL', 'AA', 'UA'}
AIRLINE_NAMES    = {'DL': 'Delta', 'AA': 'American', 'UA': 'United'}

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
        'include_airlines': 'DL,AA,UA',
        'api_key':        SERPAPI_KEY,
    }
    try:
        r = requests.get('https://serpapi.com/search', params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as exc:
        print(f"    API error ({departure}→{arrival}): {exc}")
        return {}


def parse_flights(data: dict, cabin: int) -> list:
    results = []
    seen = set()

    for section in ('best_flights', 'other_flights'):
        for option in data.get(section, []):
            legs  = option.get('flights', [])
            price = option.get('price')
            if not legs or not price:
                continue

            # keep only flights operated by allowed airlines
            op_airlines = {leg.get('airline', '') for leg in legs}
            if not op_airlines & ALLOWED_AIRLINES:
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

            results.append({
                'price':             price,
                'cabin':             cabin,
                'airline':           first.get('airline', ''),
                'airline_name':      AIRLINE_NAMES.get(first.get('airline', ''),
                                         first.get('airline', '')),
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
  padding: 2rem; min-height: 100vh;
}
h1 { font-size: 1.8rem; color: #f8fafc; margin-bottom: .25rem; }
.subtitle { color: #64748b; font-size: .875rem; margin-bottom: 2.5rem; }
h2 {
  font-size: 1.15rem; color: #cbd5e1;
  border-bottom: 1px solid #1e293b;
  padding-bottom: .5rem; margin: 2rem 0 1rem;
  display: flex; align-items: center; gap: .6rem;
}
.badge {
  background: #1e293b; color: #94a3b8;
  border-radius: 9999px; font-size: .7rem;
  padding: .15rem .55rem; font-weight: 400;
}
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(370px, 1fr));
  gap: 1rem;
}
.card {
  background: #1e293b; border: 1px solid #334155;
  border-radius: .75rem; padding: 1rem;
  transition: border-color .2s;
}
.card:hover { border-color: #475569; }
.card-header {
  display: flex; justify-content: space-between;
  align-items: center; margin-bottom: .75rem;
}
.logo { height: 28px; object-fit: contain; }
.airline-text { font-weight: 600; color: #94a3b8; font-size: .875rem; }
.price { font-size: 1.6rem; font-weight: 700; }
.route {
  display: flex; align-items: center; flex-wrap: wrap;
  gap: .4rem; margin-bottom: .75rem;
}
.ap   { font-weight: 700; font-size: 1rem; color: #f1f5f9; }
.tm   { color: #94a3b8; font-size: .8rem; }
.arr  { margin-left: auto; text-align: right; }
.arrow { color: #475569; flex: 1; text-align: center; font-size: .8rem; }
.stop-badge {
  font-size: .72rem; padding: .15rem .5rem;
  border-radius: 9999px; background: #0f172a; color: #94a3b8;
  white-space: nowrap;
}
.legs { border-top: 1px solid #0f172a; padding-top: .75rem; }
.leg { margin-bottom: .6rem; }
.leg-route { font-size: .82rem; color: #cbd5e1; }
.leg-meta  { font-size: .72rem; color: #64748b; margin-top: .1rem; }
.layover-tag {
  display: inline-block; font-size: .72rem;
  background: #1c1917; color: #fbbf24;
  border: 1px solid #292524;
  border-radius: .25rem; padding: .2rem .45rem;
  margin: .25rem 0;
}
.empty { color: #475569; font-style: italic; padding: .5rem 0; }
"""


def render_legs(legs: list, layovers: list) -> str:
    html = '<div class="legs">'
    for i, leg in enumerate(legs):
        dep      = leg.get('departure_airport', {})
        arr      = leg.get('arrival_airport', {})
        airplane = leg.get('airplane', '')
        fn       = leg.get('flight_number', '')
        al       = leg.get('airline', '')
        dur      = fmt_duration(leg.get('duration'))
        d_time   = fmt_time(dep.get('time', ''))
        a_time   = fmt_time(arr.get('time', ''))
        dep_id   = dep.get('id', '?')
        arr_id   = arr.get('id', '?')

        meta_parts = [p for p in [al, fn, airplane, dur] if p]
        html += f'''
        <div class="leg">
          <div class="leg-route">{dep_id} {d_time} → {arr_id} {a_time}</div>
          <div class="leg-meta">{' · '.join(meta_parts)}</div>
        </div>'''

        if i < len(layovers):
            lv     = layovers[i]
            lv_dur = fmt_duration(lv.get('duration'))
            lv_id  = lv.get('id', '')
            lv_nm  = lv.get('name', lv_id)
            html += f'<div class="layover-tag">Layover · {lv_nm} ({lv_id}) · {lv_dur}</div>'

    html += '</div>'
    return html


def render_card(f: dict) -> str:
    price  = f['price']
    cabin  = f['cabin']
    color  = price_color(price, cabin)
    stops  = 'Nonstop' if f['nonstop'] else f"{len(f['layovers'])} stop{'s' if len(f['layovers'])>1 else ''}"
    logo   = (f'<img class="logo" src="{f["airline_logo"]}" alt="{f["airline_name"]}">'
              if f['airline_logo'] else f'<span class="airline-text">{f["airline_name"]}</span>')
    d_time = fmt_time(f['departure_time'])
    a_time = fmt_time(f['arrival_time'])

    return f'''
    <div class="card">
      <div class="card-header">
        <div>{logo}</div>
        <div class="price" style="color:{color}">${price}</div>
      </div>
      <div class="route">
        <div>
          <div class="ap">{f["departure_airport"]}</div>
          <div class="tm">{d_time}</div>
        </div>
        <div class="arrow">——▶</div>
        <div class="arr">
          <div class="ap">{f["arrival_airport"]}</div>
          <div class="tm">{a_time}</div>
        </div>
        <div class="stop-badge">{stops}</div>
      </div>
      {render_legs(f["legs"], f["layovers"])}
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
  <title>Flights · {date} · To {dest_label}</title>
  <style>{CSS}</style>
</head>
<body>
  <h1>Flight Price Report</h1>
  <p class="subtitle">
    {date} &nbsp;·&nbsp; To {dest_label} &nbsp;·&nbsp;
    Delta · American · United &nbsp;·&nbsp; One-way · 1 adult
  </p>

  <h2>Economy Class <span class="badge">{len(econ)}</span></h2>
  <div class="grid">{econ_cards}</div>

  <h2>First Class <span class="badge">{len(first)}</span></h2>
  <div class="grid">{first_cards}</div>
</body>
</html>'''


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    if not SERPAPI_KEY:
        sys.exit('Error: SERPAPI_KEY environment variable is not set.')

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
            flights = parse_flights(data, cabin)
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
