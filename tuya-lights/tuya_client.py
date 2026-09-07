"""
Shared Tuya Cloud client used by the menu bar app (and reusable by any other
script in this folder). Wraps tinytuya's Cloud client with device-capability
detection so callers can just say "turn this on" / "set brightness to 70%"
without worrying about which DP codes a given bulb model uses.
"""

import json
import os
import threading

import tinytuya
from dotenv import load_dotenv

load_dotenv()

SWITCH_CODES = ("switch_led", "switch_1", "switch")
BRIGHT_CODES = ("bright_value_v2", "bright_value")
TEMP_CODES = ("temp_value_v2", "temp_value")
COLOUR_CODES = ("colour_data_v2", "colour_data")
MODE_CODE = "work_mode"


class TuyaError(Exception):
    pass


class TuyaClient:
    def __init__(self):
        self.cloud = tinytuya.Cloud(
            apiRegion=os.environ.get("TUYA_API_REGION", "us"),
            apiKey=os.environ["TUYA_ACCESS_ID"],
            apiSecret=os.environ["TUYA_ACCESS_KEY"],
            apiDeviceID=os.environ["TUYA_SEED_DEVICE_ID"],
        )
        self._spec_cache = {}
        self._spec_lock = threading.Lock()

    def _check(self, resp, action):
        if not isinstance(resp, dict) or not resp.get("success", False):
            if isinstance(resp, dict):
                msg = resp.get("msg") or resp.get("Payload") or resp.get("Error") or "unknown error"
            else:
                msg = "empty response"
            raise TuyaError(f"{action} failed: {msg}")
        return resp

    def list_devices(self):
        resp = self._check(self.cloud.getdevices(verbose=True), "list devices")
        devices = resp.get("result", [])
        out = [
            {
                "id": d.get("id"),
                "name": d.get("name") or d.get("id"),
                "online": bool(d.get("online", d.get("isOnline", False))),
                "category": d.get("category"),
            }
            for d in devices
        ]
        out.sort(key=lambda d: d["name"].lower())
        return out

    def get_status(self, device_id):
        """Raw current DP values for a device, as a {code: value} dict."""
        resp = self._check(self.cloud.getstatus(device_id), "fetch device status")
        return {s["code"]: s["value"] for s in resp.get("result", [])}

    def get_spec(self, device_id):
        """Fetch (and cache) which DP codes this device supports, and their ranges."""
        with self._spec_lock:
            cached = self._spec_cache.get(device_id)
        if cached:
            return cached

        resp = self._check(self.cloud.getfunctions(device_id), "fetch device functions")
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
        with self._spec_lock:
            self._spec_cache[device_id] = spec
        return spec

    def send_command(self, device_id, commands):
        # tinytuya JSON-encodes whatever we pass here as-is (no wrapping), but
        # Tuya's /commands endpoint requires the {"commands": [...]} envelope.
        return self._check(
            self.cloud.sendcommand(device_id, {"commands": commands}), "send command"
        )

    def set_switch(self, device_id, on):
        spec = self.get_spec(device_id)
        if not spec["switch_code"]:
            raise TuyaError("device has no switch capability")
        self.send_command(device_id, [{"code": spec["switch_code"], "value": bool(on)}])

    def set_brightness_pct(self, device_id, pct):
        """pct: 0-100. Also nudges the device into white mode, if it has one."""
        spec = self.get_spec(device_id)
        if not spec["bright_code"]:
            raise TuyaError("device has no brightness capability")
        pct = max(0, min(100, pct))
        value = round(spec["bright_min"] + (spec["bright_max"] - spec["bright_min"]) * pct / 100)
        commands = []
        if spec["mode_code"] and spec["mode_white"]:
            commands.append({"code": spec["mode_code"], "value": spec["mode_white"]})
        commands.append({"code": spec["bright_code"], "value": value})
        self.send_command(device_id, commands)

    def set_temp_pct(self, device_id, pct):
        """pct: 0 (warmest) - 100 (coolest)."""
        spec = self.get_spec(device_id)
        if not spec["temp_code"]:
            raise TuyaError("device has no color-temperature capability")
        pct = max(0, min(100, pct))
        value = round(spec["temp_min"] + (spec["temp_max"] - spec["temp_min"]) * pct / 100)
        commands = []
        if spec["mode_code"] and spec["mode_white"]:
            commands.append({"code": spec["mode_code"], "value": spec["mode_white"]})
        commands.append({"code": spec["temp_code"], "value": value})
        self.send_command(device_id, commands)

    def describe(self, device_id):
        """A snapshot of a device's current state, as plain percentages
        rather than raw DP values: on/off, white-vs-color mode, brightness,
        and whichever of color-temp or hue+saturation actually applies."""
        spec = self.get_spec(device_id)
        values = self.get_status(device_id)

        on = bool(values.get(spec["switch_code"])) if spec["switch_code"] else None

        mode = values.get(spec["mode_code"]) if spec["mode_code"] else None
        colour = values.get(spec["colour_code"])
        if isinstance(colour, str):
            try:
                colour = json.loads(colour)
            except (ValueError, TypeError):
                colour = None

        is_colour = False
        if spec["colour_code"]:
            if not spec["bright_code"]:
                is_colour = True
            elif spec["mode_code"] and spec["mode_colour"]:
                is_colour = mode == spec["mode_colour"]

        brightness_pct = hue = saturation_pct = temp_pct = None

        if is_colour and colour:
            v_max = spec["colour_v_max"] or 1000
            s_max = spec["colour_s_max"] or 1000
            brightness_pct = round((colour.get("v", 0) / v_max) * 100)
            hue = colour.get("h", 0)
            saturation_pct = round((colour.get("s", 0) / s_max) * 100)
        else:
            bright_raw = values.get(spec["bright_code"])
            if isinstance(bright_raw, (int, float)) and spec["bright_max"] != spec["bright_min"]:
                brightness_pct = round(
                    (bright_raw - spec["bright_min"]) / (spec["bright_max"] - spec["bright_min"]) * 100
                )
            temp_raw = values.get(spec["temp_code"])
            if isinstance(temp_raw, (int, float)) and spec["temp_max"] != spec["temp_min"]:
                temp_pct = round(
                    (temp_raw - spec["temp_min"]) / (spec["temp_max"] - spec["temp_min"]) * 100
                )

        return {
            "on": on,
            "mode": "color" if is_colour else "white",
            "brightness_pct": brightness_pct,
            "hue": hue,
            "saturation_pct": saturation_pct,
            "temp_pct": temp_pct,
        }

    def set_all(self, on):
        """Best-effort: switches every device that has a switch capability."""
        results = {}
        for d in self.list_devices():
            try:
                self.set_switch(d["id"], on)
                results[d["id"]] = "ok"
            except Exception as exc:
                results[d["id"]] = f"error: {exc}"
        return results
