#!/usr/bin/env python3
"""
Flask backend for controlling Tuya lights via the Tuya Cloud API, plus a
background automation loop that runs automation.json's schedule (fixed
times and/or sunset/sunrise) for as long as this process stays running.
"""

import json
import os
import threading
import time
import traceback
from datetime import datetime, timedelta

from astral import LocationInfo
from astral.sun import sun
from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from tuya_client import TuyaClient, TuyaError

PORT = int(os.environ.get("PORT", 8000))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTOMATION_FILE = os.path.join(BASE_DIR, "automation.json")
GROUPS_FILE = os.path.join(BASE_DIR, "groups.json")
AUTOMATION_CHECK_SECONDS = 30
GROUP_PREFIX = "group:"

app = Flask(__name__)
client = TuyaClient()

_automation_enabled = True
_fired_today = set()
_fired_date = None
_automation_lock = threading.Lock()


def load_automation():
    with open(AUTOMATION_FILE) as f:
        return json.load(f)


def load_groups():
    """{group display name: [member device names]}. Missing file = no groups."""
    try:
        with open(GROUPS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def resolve_names(names, name_to_id):
    """Expand a list that may mix individual device names and group names
    into physical device ids. Unknown names are logged and skipped."""
    groups = load_groups()
    ids = []
    for n in names:
        if n in groups:
            ids.extend(name_to_id[m] for m in groups[n] if m in name_to_id)
        elif n in name_to_id:
            ids.append(name_to_id[n])
        else:
            print(f"[automation] unknown device/group name: {n!r}")
    return ids


def resolve_trigger(rule, today):
    t = rule.get("time")
    if t in ("sunset", "sunrise"):
        try:
            loc = load_automation()["location"]
        except Exception:
            return None
        observer = LocationInfo(
            "Home", "Region", loc.get("timezone", "UTC"), loc["lat"], loc["lon"]
        ).observer
        try:
            times = sun(observer, date=today, tzinfo=loc.get("timezone", "UTC"))
        except Exception as exc:
            print(f"[automation] sun calc failed: {exc}")
            return None
        base = times[t]
        offset = rule.get("offset_minutes", 0)
        return (base + timedelta(minutes=offset)).replace(tzinfo=None)
    try:
        hour, minute = (int(p) for p in t.split(":"))
        return datetime.combine(today, datetime.min.time()).replace(hour=hour, minute=minute)
    except (ValueError, AttributeError):
        print(f"[automation] bad time value in rule {rule.get('name')!r}: {t!r}")
        return None


def apply_rule(rule, name_to_id):
    devices = rule.get("devices", "all")
    if devices == "all":
        target_ids = list(name_to_id.values())
    else:
        target_ids = resolve_names(devices, name_to_id)

    action = rule.get("action")
    for device_id in target_ids:
        try:
            if action == "on":
                client.set_switch(device_id, True)
                if "brightness_pct" in rule:
                    client.set_brightness_pct(device_id, rule["brightness_pct"])
                if "temp_pct" in rule:
                    client.set_temp_pct(device_id, rule["temp_pct"])
            elif action == "off":
                client.set_switch(device_id, False)
        except Exception as exc:
            print(f"[automation] rule {rule.get('name')!r} failed for {device_id}: {exc}")

    print(f"[automation] fired: {rule.get('name', '(unnamed rule)')}")


def automation_loop():
    global _fired_date, _fired_today
    while True:
        try:
            now = datetime.now()
            if _fired_date != now.date():
                _fired_today = set()
                _fired_date = now.date()

            with _automation_lock:
                enabled = _automation_enabled

            if enabled:
                config = load_automation()
                devices = client.list_devices()
                name_to_id = {d["name"]: d["id"] for d in devices}
                for idx, rule in enumerate(config.get("rules", [])):
                    key = (idx, rule.get("name"))
                    if key in _fired_today:
                        continue
                    trigger = resolve_trigger(rule, now.date())
                    if trigger and now >= trigger:
                        apply_rule(rule, name_to_id)
                        _fired_today.add(key)
        except Exception:
            traceback.print_exc()
        time.sleep(AUTOMATION_CHECK_SECONDS)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/devices")
def api_devices():
    devices = client.list_devices()
    by_name = {d["name"]: d for d in devices}
    groups = load_groups()
    grouped_names = {n for members in groups.values() for n in members}

    out = [d for d in devices if d["name"] not in grouped_names]
    for group_name, member_names in groups.items():
        members = [by_name[n] for n in member_names if n in by_name]
        if not members:
            continue
        out.append({
            "id": GROUP_PREFIX + group_name,
            "name": group_name,
            "online": any(m["online"] for m in members),
            "category": members[0]["category"],
            "group_size": len(members),
        })
    out.sort(key=lambda d: d["name"].lower())
    return jsonify(out)


def _group_member_ids(group_name):
    devices = client.list_devices()
    name_to_id = {d["name"]: d["id"] for d in devices}
    members = resolve_names([group_name], name_to_id)
    if not members:
        raise TuyaError(f"group {group_name!r} has no known online members")
    return members


@app.route("/api/devices/<device_id>/status")
def api_device_status(device_id):
    # A group has no state of its own — show whichever member is first, on
    # the assumption a well-behaved group is always kept in sync (every
    # command to it fans out to all members at once).
    if device_id.startswith(GROUP_PREFIX):
        device_id = _group_member_ids(device_id[len(GROUP_PREFIX):])[0]

    spec = client.get_spec(device_id)
    values = client.get_status(device_id)

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

    if device_id.startswith(GROUP_PREFIX):
        results = {}
        for member_id in _group_member_ids(device_id[len(GROUP_PREFIX):]):
            try:
                results[member_id] = client.send_command(member_id, commands)
            except Exception as exc:
                results[member_id] = {"error": str(exc)}
        return jsonify(results)

    resp = client.send_command(device_id, commands)
    return jsonify(resp)


@app.route("/api/all", methods=["POST"])
def api_all():
    body = request.get_json(force=True, silent=True) or {}
    on = bool(body.get("on"))
    return jsonify(client.set_all(on))


@app.route("/api/automation", methods=["GET", "POST"])
def api_automation():
    global _automation_enabled
    if request.method == "POST":
        body = request.get_json(force=True, silent=True) or {}
        with _automation_lock:
            _automation_enabled = bool(body.get("enabled", True))
    with _automation_lock:
        enabled = _automation_enabled
    return jsonify({"enabled": enabled})


@app.errorhandler(Exception)
def handle_error(exc):
    if isinstance(exc, HTTPException):
        return exc
    status = 502 if isinstance(exc, TuyaError) else 500
    return jsonify({"error": str(exc)}), status


if __name__ == "__main__":
    threading.Thread(target=automation_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=PORT)
