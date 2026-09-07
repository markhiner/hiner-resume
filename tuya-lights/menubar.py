#!/usr/bin/env python3
"""
macOS menu bar app for the Tuya lights: manual on/off/brightness per light
from the menu bar, plus a JSON-configured schedule (automation.json) that
runs in the background all day (fixed times and/or sunset/sunrise).

Run with: python3 menubar.py
(macOS only — uses rumps, which wraps PyObjC/AppKit.)
"""

import json
import os
import subprocess
import threading
import time
import traceback
from datetime import datetime, timedelta

import rumps
from astral import LocationInfo
from astral.sun import sun

from tuya_client import TuyaClient, TuyaError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUTOMATION_FILE = os.path.join(BASE_DIR, "automation.json")
GROUPS_FILE = os.path.join(BASE_DIR, "groups.json")
AUTOMATION_CHECK_SECONDS = 30


def load_automation():
    with open(AUTOMATION_FILE) as f:
        return json.load(f)


def save_automation(config):
    """Write atomically so the background loop never reads a half-written file."""
    tmp_path = AUTOMATION_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(config, f, indent=2)
    os.replace(tmp_path, AUTOMATION_FILE)


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


class LightsMenuBarApp(rumps.App):
    def __init__(self):
        super().__init__("💡", quit_button="Quit")
        self.client = TuyaClient()
        self.name_to_id = {}
        self._fired_today = set()
        self._fired_date = None

        self.refresh_item = rumps.MenuItem("Refresh Lights", callback=self.refresh_lights)
        self.all_on_item = rumps.MenuItem("All On", callback=lambda _: self.run_all(True))
        self.all_off_item = rumps.MenuItem("All Off", callback=lambda _: self.run_all(False))
        self.automation_toggle = rumps.MenuItem(
            "Automation Enabled", callback=self.toggle_automation
        )
        try:
            self.automation_toggle.state = load_automation().get("enabled", False)
        except Exception:
            self.automation_toggle.state = False
        self.edit_schedule_item = rumps.MenuItem(
            "Edit Schedule...", callback=self.open_schedule
        )
        self.reload_schedule_item = rumps.MenuItem(
            "Validate Schedule", callback=self.validate_schedule
        )

        self.menu = [
            self.refresh_item,
            None,
            self.all_on_item,
            self.all_off_item,
            None,
        ]

        self.refresh_lights(None)

        self.menu.add(None)
        self.menu.add(self.automation_toggle)
        self.menu.add(self.edit_schedule_item)
        self.menu.add(self.reload_schedule_item)

        threading.Thread(target=self.automation_loop, daemon=True).start()

    # ---- notifications (best-effort; never let a failure kill the app) ----

    def notify(self, title, message):
        try:
            rumps.notification("Lights", title, message)
        except Exception:
            print(f"[notify] {title}: {message}")

    # ---- menu construction ----

    def refresh_lights(self, _):
        try:
            devices = self.client.list_devices()
        except Exception as exc:
            self.notify("Couldn't load lights", str(exc))
            return

        self.name_to_id = {d["name"]: d["id"] for d in devices}

        # Drop any previously-built light submenus, keep the fixed items.
        for name in list(self.menu.keys()):
            item = self.menu[name]
            if getattr(item, "_is_light_item", False):
                del self.menu[name]

        insert_before = self.all_on_item
        for device in devices:
            item = self.make_light_menu(device)
            self.menu.insert_before(insert_before.title, item)

    def make_light_menu(self, device):
        device_id = device["id"]
        label = device["name"] if device["online"] else f"{device['name']} (offline)"
        submenu = rumps.MenuItem(label)
        submenu._is_light_item = True

        submenu.add(rumps.MenuItem("On", callback=lambda _: self.run_switch(device_id, True)))
        submenu.add(rumps.MenuItem("Off", callback=lambda _: self.run_switch(device_id, False)))
        submenu.add(None)
        for preset_label, pct in (("Dim (25%)", 25), ("Half (50%)", 50), ("Full (100%)", 100)):
            submenu.add(
                rumps.MenuItem(
                    preset_label,
                    callback=lambda _, pct=pct: self.run_brightness(device_id, pct),
                )
            )
        return submenu

    # ---- manual controls ----

    def run_switch(self, device_id, on):
        try:
            self.client.set_switch(device_id, on)
        except Exception as exc:
            self.notify("Command failed", str(exc))

    def run_brightness(self, device_id, pct):
        try:
            self.client.set_brightness_pct(device_id, pct)
        except Exception as exc:
            self.notify("Command failed", str(exc))

    def run_all(self, on):
        results = self.client.set_all(on)
        failures = [k for k, v in results.items() if not v.startswith("ok")]
        if failures:
            self.notify("Some lights failed", f"{len(failures)} of {len(results)} didn't respond")

    def toggle_automation(self, sender):
        try:
            config = load_automation()
        except Exception as exc:
            self.notify("Couldn't read schedule", str(exc))
            return
        config["enabled"] = not config.get("enabled", False)
        save_automation(config)
        sender.state = config["enabled"]

    def open_schedule(self, _):
        subprocess.run(["open", AUTOMATION_FILE])

    def validate_schedule(self, _):
        # The background loop re-reads automation.json every cycle, so edits
        # take effect on their own within ~30s — this just checks it parses.
        try:
            config = load_automation()
            n = len(config.get("rules", []))
            state = "ON" if config.get("enabled", False) else "OFF"
            self.notify("Schedule OK", f"{n} rule(s) loaded, automation is {state}")
        except Exception as exc:
            self.notify("Schedule error", str(exc))

    # ---- automation ----

    def resolve_trigger(self, rule, today):
        t = rule.get("time")
        if t in ("sunset", "sunrise"):
            try:
                config = load_automation()
                loc = config["location"]
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

    def apply_rule(self, rule):
        devices = rule.get("devices", "all")
        if devices == "all":
            target_ids = list(self.name_to_id.values())
        else:
            target_ids = resolve_names(devices, self.name_to_id)

        action = rule.get("action")
        for device_id in target_ids:
            try:
                if action == "on":
                    self.client.set_switch(device_id, True)
                    if "brightness_pct" in rule:
                        self.client.set_brightness_pct(device_id, rule["brightness_pct"])
                    if "temp_pct" in rule:
                        self.client.set_temp_pct(device_id, rule["temp_pct"])
                elif action == "off":
                    self.client.set_switch(device_id, False)
            except Exception as exc:
                print(f"[automation] rule {rule.get('name')!r} failed for {device_id}: {exc}")

        self.notify("Automation", rule.get("name", "Rule fired"))

    def automation_loop(self):
        while True:
            try:
                now = datetime.now()
                if self._fired_date != now.date():
                    self._fired_today = set()
                    self._fired_date = now.date()

                config = load_automation()
                if config.get("enabled", False):
                    for idx, rule in enumerate(config.get("rules", [])):
                        key = (idx, rule.get("name"))
                        if key in self._fired_today:
                            continue
                        trigger = self.resolve_trigger(rule, now.date())
                        if trigger and now >= trigger:
                            self.apply_rule(rule)
                            self._fired_today.add(key)
            except Exception:
                traceback.print_exc()
            time.sleep(AUTOMATION_CHECK_SECONDS)


if __name__ == "__main__":
    LightsMenuBarApp().run()
