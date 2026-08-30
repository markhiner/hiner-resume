#!/usr/bin/env python3
"""
Flask backend for controlling Tuya lights via the Tuya Cloud API.
Talks to tinytuya's Cloud client; the frontend is a single iOS-optimized page.
"""

import json
import os
import threading

import tinytuya
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

load_dotenv()

ACCESS_ID = os.environ["TUYA_ACCESS_ID"]
ACCESS_KEY = os.environ["TUYA_ACCESS_KEY"]
API_REGION = os.environ.get("TUYA_API_REGION", "us")
SEED_DEVICE_ID = os.environ["TUYA_SEED_DEVICE_ID"]
PORT = int(os.environ.get("PORT", 8000))

app = Flask(__name__)

cloud = tinytuya.Cloud(
    apiRegion=API_REGION,
    apiKey=ACCESS_ID,
    apiSecret=ACCESS_KEY,
    apiDeviceID=SEED_DEVICE_ID,
)

# DP codes vary by bulb model/firmware generation; try the common ones in order.
SWITCH_CODES = ("switch_led", "switch_1", "switch")
BRIGHT_CODES = ("bright_value_v2", "bright_value")
TEMP_CODES = ("temp_value_v2", "temp_value")
COLOUR_CODES = ("colour_data_v2", "colour_data")
MODE_CODE = "work_mode"

_spec_cache = {}
_spec_lock = threading.Lock()


class TuyaError(Exception):
    pass


def _check(resp, action):
    """Raise if a Tuya API call failed.

    tinytuya returns either the raw Tuya API response (has "msg" on failure)
    or one of its own error_json() dicts (has "Error"/"Payload" instead, no
    "success" key at all — treated as falsy here).
    """
    if not isinstance(resp, dict) or not resp.get("success", False):
        if isinstance(resp, dict):
            msg = resp.get("msg") or resp.get("Payload") or resp.get("Error") or "unknown error"
        else:
            msg = "empty response"
        raise TuyaError(f"{action} failed: {msg}")
    return resp


def get_spec(device_id):
    """Fetch (and cache) which DP codes this device supports, and their ranges."""
    with _spec_lock:
        cached = _spec_cache.get(device_id)
    if cached:
        return cached

    resp = _check(cloud.getfunctions(device_id), "fetch device functions")
    functions = resp.get("result", {}).get("functions", [])
    by_code = {f["code"]: f for f in functions}

    def pick(candidates):
        return next((c for c in candidates if c in by_code), None)

    def values_of(code):
        if not code:
            return {}
        try:
            return json.loads(by_code[code].get("values", "{}"))
        except (ValueError, TypeError):
            return {}

    switch_code = pick(SWITCH_CODES)
    bright_code = pick(BRIGHT_CODES)
    temp_code = pick(TEMP_CODES)
    colour_code = pick(COLOUR_CODES)
    mode_code = MODE_CODE if MODE_CODE in by_code else None

    bright_vals = values_of(bright_code)
    temp_vals = values_of(temp_code)
    colour_vals = values_of(colour_code)
    mode_vals = values_of(mode_code)
    mode_range = mode_vals.get("range", [])

    spec = {
        "switch_code": switch_code,
        "bright_code": bright_code,
        "bright_min": bright_vals.get("min", 10),
        "bright_max": bright_vals.get("max", 1000),
        "temp_code": temp_code,
        "temp_min": temp_vals.get("min", 0),
        "temp_max": temp_vals.get("max", 1000),
        "colour_code": colour_code,
        "colour_h_max": colour_vals.get("h", {}).get("max", 360),
        "colour_s_max": colour_vals.get("s", {}).get("max", 1000),
        "colour_v_max": colour_vals.get("v", {}).get("max", 1000),
        "mode_code": mode_code,
        "mode_white": next((m for m in mode_range if "white" in m.lower()), None),
        "mode_colour": next((m for m in mode_range if m.lower().startswith("colo")), None),
    }
    with _spec_lock:
        _spec_cache[device_id] = spec
    return spec


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/devices")
def api_devices():
    resp = _check(cloud.getdevices(verbose=True), "list devices")
    devices = resp.get("result", [])
    out = [
        {
            "id": d.get("id"),
            "name": d.get("name") or d.get("id"),
            "online": bool(d.get("online", d.get("isOnline", False))),
            "category": d.get("category"),
            "product_name": d.get("product_name"),
        }
        for d in devices
    ]
    out.sort(key=lambda d: d["name"].lower())
    return jsonify(out)


@app.route("/api/devices/<device_id>/status")
def api_device_status(device_id):
    spec = get_spec(device_id)
    resp = _check(cloud.getstatus(device_id), "fetch device status")
    values = {s["code"]: s["value"] for s in resp.get("result", [])}

    return jsonify({
        "spec": spec,
        "on": bool(values.get(spec["switch_code"])) if spec["switch_code"] else None,
        "brightness": values.get(spec["bright_code"]),
        "temperature": values.get(spec["temp_code"]),
        "colour": values.get(spec["colour_code"]),
        "mode": values.get(spec["mode_code"]),
    })


@app.route("/api/devices/<device_id>/command", methods=["POST"])
def api_device_command(device_id):
    body = request.get_json(force=True, silent=True) or {}
    commands = body.get("commands")
    if not commands:
        return jsonify({"error": "commands is required"}), 400
    resp = _check(cloud.sendcommand(device_id, commands), "send command")
    return jsonify(resp)


@app.route("/api/all", methods=["POST"])
def api_all():
    body = request.get_json(force=True, silent=True) or {}
    on = bool(body.get("on"))
    resp = _check(cloud.getdevices(verbose=True), "list devices")

    results = {}
    for d in resp.get("result", []):
        device_id = d.get("id")
        spec = get_spec(device_id)
        if not spec["switch_code"]:
            continue
        try:
            _check(cloud.sendcommand(device_id, [{"code": spec["switch_code"], "value": on}]), "send command")
            results[device_id] = "ok"
        except Exception as exc:  # keep going across the rest of the devices
            results[device_id] = f"error: {exc}"
    return jsonify(results)


@app.errorhandler(Exception)
def handle_error(exc):
    if isinstance(exc, HTTPException):
        return exc
    status = 502 if isinstance(exc, TuyaError) else 500
    return jsonify({"error": str(exc)}), status


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
