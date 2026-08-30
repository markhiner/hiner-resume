# Lights

Tools for controlling Tuya-based smart lights (the ones you normally
control from the Tuya / Smart Life app) without opening that app. Two
front ends share the same Tuya Cloud setup and the same `.env`:

- **`menubar.py`** — a macOS menu bar app: click a light to turn it on/off
  or set brightness, plus a JSON-configured schedule that runs automations
  throughout the day (fixed times and/or sunset/sunrise). This is the
  primary way to use this folder right now.
- **`app.py`** — a Flask web app optimized for iOS Safari (Home Screen
  install, native color picker). Parked for now but still there if you
  want it later — see "Web app" below.

Both talk to the [Tuya Cloud API](https://developer.tuya.com/en/docs/iot)
via [`tinytuya`](https://github.com/jasonacox/tinytuya)'s `Cloud` client —
no local device keys or LAN discovery required.

## 1. Set up a Tuya Cloud project

1. Create a free account at [iot.tuya.com](https://iot.tuya.com) and sign in.
2. **Cloud → Create Cloud Project.** Pick the data center region that
   matches where your Tuya/Smart Life account was created (e.g. "Western
   America" for `us`).
3. On the project's **Service API** tab, make sure **IoT Core** is
   subscribed. It's free, but Tuya may put the subscription/renewal
   through a manual review — that's normal, just wait for it to clear.
4. On the project's **Overview** tab, copy the **Access ID/Client ID** and
   **Access Secret/Client Secret** — you'll need these for `.env`.
5. Go to the project's **Devices** tab → **Link Tuya App Account** → scan
   the QR code with the Tuya app or Smart Life app on your phone (the same
   account your lights are already set up in). Once linked, your lights
   will appear in the device list.
6. Copy the **Device ID** of any one *online* light from that list — it's
   only used to bootstrap access to the rest of your account.

## 2. Configure

```bash
cd tuya-lights
cp .env.example .env
```

Fill in `.env` with the Access ID, Access Secret, region, and the one
device ID from steps above.

```bash
python3 -m pip install --break-system-packages -r requirements.txt
```

(Drop `--break-system-packages` if you're using a virtualenv instead of
your system Python.)

Sanity-check the setup before moving on:

```bash
python3 list_devices.py
```

This should print every linked light by name — no web server or menu bar
app needed for this step.

## 3. Menu bar app

```bash
python3 menubar.py
```

A 💡 icon appears in your menu bar. Click it for:

- **Refresh Lights** — reload the device list (e.g. after adding/renaming
  a light in the Tuya app)
- One entry per light, each with a submenu: **On**, **Off**, and (for
  dimmable bulbs) **Dim (25%)** / **Half (50%)** / **Full (100%)**
- **All On** / **All Off**
- **Automation Enabled** — check to pause all scheduled automations
  without quitting the app
- **Edit Schedule...** — opens `automation.json` in your default editor
- **Validate Schedule** — checks the file parses and reports how many
  rules it found (edits take effect on their own within ~30 seconds,
  this is just a sanity check)

Leave it running in the menu bar; automations fire in the background as
long as it's open.

### Automation schedule

Edit `automation.json`. First, set your real location so sunset/sunrise
rules fire at the right time — right-click your house on Google Maps and
it'll show you the lat/lon to paste in.

Each rule:

```json
{
  "name": "Front Porch on at dusk",
  "time": "sunset",
  "offset_minutes": 0,
  "devices": ["Front Porch"],
  "action": "on"
}
```

- `time`: `"sunset"`, `"sunrise"`, or a fixed 24-hour `"HH:MM"`
- `offset_minutes` (optional, sunset/sunrise only): shift earlier (negative)
  or later (positive)
- `devices`: a list of light names (must match names in the Tuya app
  exactly) or the string `"all"`
- `action`: `"on"` or `"off"`
- `brightness_pct` / `temp_pct` (optional, `"on"` rules only): 0-100, only
  applied to devices that support it

The included example rules assume your actual room/light names from the
Tuya app — edit device names in `devices` to match whatever you named
your lights.

### Run it automatically at login (optional)

Create `~/Library/LaunchAgents/nyc.hiner.tuyalights.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>nyc.hiner.tuyalights</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/python3</string>
    <string>/Users/mark/hiner-resume/tuya-lights/menubar.py</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict>
</plist>
```

Then: `launchctl load ~/Library/LaunchAgents/nyc.hiner.tuyalights.plist`.
Adjust the python3 path if `which python3` gives something different.

## Web app (optional, parked for now)

```bash
python app.py
```

Open `http://localhost:8000` (or `http://<your-mac's-lan-ip>:8000` from
your iPhone). In Safari, Share → **Add to Home Screen** for a full-screen,
app-like experience. See `static/`, `templates/`, and `app.py` for the
implementation — it uses the same `.env` and Tuya setup as the menu bar
app above.

### Expose it outside your home network

This repo's `trains/` tool uses a [Cloudflare
Tunnel](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/)
to publish a locally-running server at a subdomain without opening any
ports; `cloudflared-config.yml` here follows the same pattern for
`lights.hiner.nyc`:

```bash
cloudflared tunnel login
cloudflared tunnel create lights
# copy cloudflared-config.yml to ~/.cloudflared/config.yml and fill in the
# tunnel ID + credentials path it prints
cloudflared tunnel route dns lights lights.hiner.nyc
cloudflared tunnel run lights
```

Keep `app.py` running in the background (e.g. via `pm2`, `systemd`, or
`tmux`) alongside the tunnel.

## Finding devices on your local network (optional)

If you just want to see what Tuya devices are reachable on your Wi-Fi
directly — no Tuya Cloud credentials needed — run this on a machine
that's on the same network as your lights:

```bash
python3 discover_local.py
```

It listens for the UDP broadcasts Tuya devices send on the local network.
Many home routers (mesh systems especially) block this with AP/client
isolation, in which case it'll find nothing even though your Cloud setup
above works fine — that's expected, just use the Cloud path instead.

## Notes

- **Rate limits:** free/trial Tuya Cloud projects are limited to a few
  requests per second. Device capabilities (which DP codes a bulb
  supports) are fetched lazily and cached — the menu bar app only makes a
  Tuya API call when you actually click something or an automation rule
  fires, never on a timer or on startup beyond the initial device list.
- **Which lights show up:** every device linked to the Tuya app account
  appears — not just bulbs. Simple on/off switches or plugs will only
  offer On/Off, since brightness controls only show up for devices that
  report that capability.
- **Timezone:** the menu bar app compares sunset/sunrise times against
  your Mac's local clock, so `automation.json`'s `timezone` should match
  whatever timezone your Mac is actually set to.
- **`rumps` (the menu bar library) is macOS-only** — `menubar.py` won't
  run on Linux/Windows. `app.py` and the other scripts in this folder are
  plain Python and work anywhere.
