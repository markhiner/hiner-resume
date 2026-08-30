#!/usr/bin/env python3
"""
Scan the local network for Tuya devices.

This only works when run from a machine on the same Wi-Fi/LAN as your
lights (it listens for UDP broadcasts the devices send locally) — it does
NOT use the Tuya Cloud API and needs no credentials. Run it on your Mac,
not on a remote server.

Usage: python3 discover_local.py
"""

import json
import sys

import tinytuya


def main():
    print("Scanning local network for Tuya devices (takes ~20 seconds)...\n")
    devices = tinytuya.deviceScan(verbose=True)

    if not devices:
        print("\nNo Tuya devices found. Make sure this Mac is on the same Wi-Fi as your lights.")
        sys.exit(1)

    out_path = "discovered_devices.json"
    with open(out_path, "w") as f:
        json.dump(devices, f, indent=2)

    print(f"\n{len(devices)} device(s) found. Full details saved to {out_path}.")
    print("\nNote: a local scan only sees IP address, device ID, and protocol")
    print("version — it can't see your lights' names, since those live in your")
    print("Tuya app's cloud account, not on the device itself. For a named")
    print("list, finish the Cloud API setup in README.md, then run the app")
    print("(`python app.py`) and open http://localhost:8000 or GET /api/devices.")


if __name__ == "__main__":
    main()
