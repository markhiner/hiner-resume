# Lights

Tools for controlling Tuya-based smart lights (the ones you normally
control from the Tuya / Smart Life app) without opening that app. Two
front ends share the same Tuya Cloud setup, the same `.env`, and the same
`automation.json` schedule:

- **`app.py`** — a Flask web app: tappable on/off, brightness, color
  temperature, and a full rainbow color slider per light, plus a
  background scheduler that runs `automation.json` (fixed times and/or
  sunset/sunrise) for as long as the process is running. This is the
  primary way to use this folder right now.
- **`menubar.py`** — a macOS menu bar equivalent: click a light in the
  menu bar for on/off/brightness, same schedule engine. An alternative to
  the web app, not required alongside it — see "Menu bar app" below.

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

## 3. Run the web app

```bash
python app.py
```

Open `http://localhost:8000` (or `http://<your-mac's-lan-ip>:8000` from
your iPhone or any other device on your network). Each light gets:

- An iOS-style **on/off switch**
- A **Brightness** slider (0-100%) — works whether the bulb is currently
  in white or color mode
- A **Warm ↔ Cool** color-temperature slider (white mode)
- A **Color** slider showing the full rainbow (hue 0-360°) for bulbs that
  support it, with a live swatch preview as you drag
- A **Saturation** slider (0-100%, gray to fully vivid) that tracks
  whatever hue is currently selected
- **All On** / **All Off** at the top

Touching the rainbow or saturation slider switches that bulb into color
mode automatically; touching the warm/cool slider switches it back to
white mode — same as how the Tuya app itself behaves. The brightness
slider works no matter which mode you're in.

In Safari, Share → **Add to Home Screen** for a full-screen, app-like
experience on your phone.

### Linking lights together

Edit `groups.json` to make a set of lights show up as a single card,
controlled together (e.g. all 5 Parlor Ceiling fixtures at once):

```json
{
  "Parlor Ceiling": [
    "Parlor Ceiling 1",
    "Parlor Ceiling 2",
    "Parlor Ceiling 3",
    "Parlor Ceiling 4",
    "Parlor Ceiling 5"
  ]
}
```

The key is the name the merged card shows; the list is the exact device
names (from the Tuya app) it controls. Every command sent to that card —
on/off, brightness, color, temperature — goes to all listed devices at
once. Its status card just reads back whichever member is first in the
list, since a group is only ever commanded as a whole. Group names also
work inside `automation.json`'s `devices` lists, in place of listing every
member individually.

Leave `python app.py` running in the background (e.g. via `pm2`,
`systemd`, or `tmux`, or see the Cloudflare Tunnel section below) —
that's also what runs the automation schedule below.

### Automation schedule

**Automation ships OFF by default** (`automation.json`'s top-level
`"enabled": false`), and starts with an empty rule list. A toggle switch
in the web app's header shows and controls this — it always reflects
what's actually saved in `automation.json`, and flipping it writes
straight back to that file, so the setting survives restarting
`app.py`/`menubar.py` (it does **not** silently reset to on, or to
whatever example rules used to be there, the way an earlier version of
this did — that's what turned all your lights off unannounced. Sorry
about that.).

Location is already set to Sutherlin, VA for sunset/sunrise math; update
it in `automation.json` if that's wrong, or if you move.

Add rules to the `"rules"` array. Each one:

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
- `devices`: a list of light or group names (must match names in the Tuya
  app, or a key in `groups.json`) or the string `"all"`
- `action`: `"on"` or `"off"`
- `brightness_pct` / `temp_pct` (optional, `"on"` rules only): 0-100, only
  applied to devices that support it

Only flip the header toggle to ON once you've actually reviewed what's in
`"rules"` — nothing fires while it's off, no matter what's in the file.

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
    <string>/Users/mark/hiner-resume/tuya-lights/app.py</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
</dict>
</plist>
```

Then: `launchctl load ~/Library/LaunchAgents/nyc.hiner.tuyalights.plist`.
Adjust the python3 path if `which python3` gives something different.

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

## Menu bar app (alternative to the web app)

```bash
python3 menubar.py
```

A 💡 icon appears in your menu bar instead of a web page — same lights,
same `automation.json` schedule, same on/off/brightness controls, just as
native macOS menu items instead of a browser tab. Click it for:

- **Refresh Lights** — reload the device list
- One entry per light with **On** / **Off** / **Dim (25%)** / **Half (50%)**
  / **Full (100%)** (no rainbow color slider here — menu items can't do a
  drag gesture, so color is web-app-only for now)
- **All On** / **All Off**
- **Automation Enabled** — pause scheduled automations without quitting
- **Edit Schedule...** / **Validate Schedule**

Only run one of `app.py` / `menubar.py` at a time if you're worried about
doubled-up automation firing — both run the same scheduler loop
independently.

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

## Printing the current state of every light

```bash
python3 print_config.py
```

Prints a table of every light's name, device ID, online/offline, on/off,
white-vs-color mode, brightness %, and whichever of color temperature %
(white mode) or hue°/saturation % (color mode) currently applies. Useful
for a quick sanity check, or as a reference for what values to put in an
`automation.json` rule.

## Notes

- **Rate limits:** free/trial Tuya Cloud projects are limited to a few
  requests per second. Device capabilities (which DP codes a bulb
  supports) are fetched lazily and cached — a command is only sent when
  you actually release a slider/switch or an automation rule fires, never
  on a timer or on startup beyond the initial device list.
- **Which lights show up:** every device linked to the Tuya app account
  appears — not just bulbs. Simple on/off switches or plugs will only
  offer On/Off, since brightness/color controls only show up for devices
  that report that capability.
- **Color vs. brightness vs. white:** touching the rainbow slider switches
  a bulb into color mode; touching Warm↔Cool switches it back to white
  mode. The Brightness slider works in either mode — it adjusts whichever
  DP actually controls brightness for the bulb's current mode.
- **Timezone:** sunset/sunrise times are compared against the local clock
  of whatever machine is running `app.py`/`menubar.py`, so
  `automation.json`'s `timezone` should match that machine's actual
  timezone setting.
- **`rumps` (the menu bar library) is macOS-only** — `menubar.py` won't
  run on Linux/Windows. `app.py` and the other scripts in this folder are
  plain Python and work anywhere.
