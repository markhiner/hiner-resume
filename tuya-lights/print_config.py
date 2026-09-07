#!/usr/bin/env python3
"""
Print a snapshot of every linked light's current state right now: online/
offline, on/off, white vs. color mode, brightness, and whichever of color
temperature (white mode) or hue+saturation (color mode) applies.

Usage: python3 print_config.py
"""

from tuya_client import TuyaClient

client = TuyaClient()
devices = sorted(client.list_devices(), key=lambda d: d["name"].lower())

if not devices:
    print("No devices found.")
    raise SystemExit(0)

COLUMNS = ("Name", "ID", "Status", "On/Off", "Mode", "Bright", "Temp", "Hue", "Sat")
WIDTHS = (28, 24, 8, 7, 6, 7, 6, 5, 5)


def row(*values):
    print("  ".join(str(v).ljust(w) for v, w in zip(values, WIDTHS)))


row(*COLUMNS)
row(*("-" * w for w in WIDTHS))

for d in devices:
    if not d["online"]:
        row(d["name"], d["id"], "offline", "-", "-", "-", "-", "-", "-")
        continue

    try:
        info = client.describe(d["id"])
    except Exception as exc:
        row(d["name"], d["id"], "error", str(exc), "", "", "", "", "")
        continue

    on_off = "-" if info["on"] is None else ("on" if info["on"] else "off")
    bright = "-" if info["brightness_pct"] is None else f"{info['brightness_pct']}%"
    temp = "-" if info["temp_pct"] is None else f"{info['temp_pct']}%"
    hue = "-" if info["hue"] is None else f"{info['hue']}°"
    sat = "-" if info["saturation_pct"] is None else f"{info['saturation_pct']}%"

    row(d["name"], d["id"], "online", on_off, info["mode"], bright, temp, hue, sat)
