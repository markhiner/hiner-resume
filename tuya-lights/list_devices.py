#!/usr/bin/env python3
"""
Print every light linked to your Tuya Cloud project, using the same
credentials as the main app (.env). No web server needed.

Usage: python3 list_devices.py
"""

import os
import sys

import tinytuya
from dotenv import load_dotenv

load_dotenv()

REQUIRED = ("TUYA_ACCESS_ID", "TUYA_ACCESS_KEY", "TUYA_SEED_DEVICE_ID")
missing = [k for k in REQUIRED if not os.environ.get(k)]
if missing:
    sys.exit(
        f"Missing {', '.join(missing)} in .env — see README.md for how to get these "
        "from your Tuya IoT Cloud project."
    )

cloud = tinytuya.Cloud(
    apiRegion=os.environ.get("TUYA_API_REGION", "us"),
    apiKey=os.environ["TUYA_ACCESS_ID"],
    apiSecret=os.environ["TUYA_ACCESS_KEY"],
    apiDeviceID=os.environ["TUYA_SEED_DEVICE_ID"],
)

resp = cloud.getdevices(verbose=True)
if not isinstance(resp, dict) or not resp.get("success", False):
    msg = resp.get("msg") or resp.get("Payload") or resp.get("Error") if isinstance(resp, dict) else "empty response"
    sys.exit(f"Tuya API error: {msg}")

devices = sorted(resp.get("result", []), key=lambda d: (d.get("name") or "").lower())

if not devices:
    print("No devices found. Did you link your Tuya app account under Cloud -> Devices?")
    sys.exit(0)

print(f"{len(devices)} device(s):\n")
for d in devices:
    status = "online" if d.get("online", d.get("isOnline", False)) else "offline"
    print(f"  {d.get('name', '(unnamed)'):<30} id={d.get('id')}  category={d.get('category')}  [{status}]")
