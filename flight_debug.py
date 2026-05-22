#!/usr/bin/env python3
"""Dump raw SerpAPI response for one flight search to debug.json"""

import os, sys, json
import requests

key = os.environ.get('SERPAPI_KEY', '')
if not key:
    sys.exit('Set SERPAPI_KEY first')

params = {
    'engine':        'google_flights',
    'departure_id':  'RDU',
    'arrival_id':    'LGA',
    'outbound_date': sys.argv[1] if len(sys.argv) > 1 else '2025-06-15',
    'currency':      'USD',
    'hl':            'en',
    'gl':            'us',
    'type':          '2',
    'adults':        '1',
    'travel_class':  '1',
    'api_key':       key,
}

print(f"Requesting: {params['departure_id']} → {params['arrival_id']}  {params['outbound_date']}")
r = requests.get('https://serpapi.com/search', params=params, timeout=30)
print(f"HTTP {r.status_code}")

data = r.json()

# Show top-level keys
print(f"\nTop-level keys: {list(data.keys())}")

# Show error if present
if 'error' in data:
    print(f"\nERROR from API: {data['error']}")

# Show flight counts
for section in ('best_flights', 'other_flights'):
    items = data.get(section, [])
    print(f"{section}: {len(items)} items")
    for i, opt in enumerate(items[:2]):
        legs = opt.get('flights', [])
        print(f"  [{i}] price={opt.get('price')}  legs={len(legs)}")
        for j, leg in enumerate(legs):
            print(f"      leg{j}: airline={leg.get('airline')!r}  "
                  f"flight_number={leg.get('flight_number')!r}  "
                  f"airplane={leg.get('airplane')!r}")
            print(f"             dep={leg.get('departure_airport',{}).get('id')}  "
                  f"arr={leg.get('arrival_airport',{}).get('id')}")

# Write full response
with open('debug.json', 'w') as f:
    json.dump(data, f, indent=2)
print("\nFull response written to debug.json")
