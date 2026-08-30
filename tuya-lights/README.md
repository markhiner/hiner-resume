# Lights

A small Flask web app for controlling Tuya-based smart lights (the ones you
normally control from the Tuya / Smart Life app) from a single page,
optimized for iOS Safari — big touch targets, an iOS-style toggle switch, a
native color picker, and a manifest so it can be added to your Home Screen
and run full-screen like a real app.

It talks to the [Tuya Cloud API](https://developer.tuya.com/en/docs/iot),
the same official API the Tuya app itself uses, via
[`tinytuya`](https://github.com/jasonacox/tinytuya)'s `Cloud` client — no
local device keys or LAN discovery required.

## 1. Set up a Tuya Cloud project

1. Create a free account at [iot.tuya.com](https://iot.tuya.com) and sign in.
2. **Cloud → Create Cloud Project.** Pick the data center region that
   matches where your Tuya/Smart Life account was created (e.g. "Western
   America" for `us`).
3. On the project's **Service API** tab, make sure **IoT Core** is
   subscribed (it's free and usually added automatically for trial
   projects).
4. On the project's **Overview** tab, copy the **Access ID/Client ID** and
   **Access Secret/Client Secret** — you'll need these for `.env`.
5. Go to the project's **Devices** tab → **Link Tuya App Account** → scan
   the QR code with the Tuya app or Smart Life app on your phone (the same
   account your lights are already set up in). Once linked, your lights
   will appear in the device list.
6. Copy the **Device ID** of any one of your lights from that list — the
   app only needs one to bootstrap access to the rest of your account.

## 2. Configure

```bash
cd tuya-lights
cp .env.example .env
```

Fill in `.env` with the Access ID, Access Secret, region, and the one
device ID from steps above.

## 3. Run it

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open `http://localhost:8000` (or `http://<your-computer's-lan-ip>:8000`
from your iPhone, if it's on the same Wi-Fi).

## 4. Add to your iPhone's Home Screen

In Safari, open the app, tap the **Share** button, then **Add to Home
Screen**. It'll launch full-screen with no browser chrome, like a native
app.

## 5. (Optional) Find devices on your local network

If you just want to see what Tuya devices are reachable on your Wi-Fi right
now — no Tuya Cloud credentials needed — run this on a machine that's on
the same network as your lights:

```bash
pip install tinytuya
python3 discover_local.py
```

It listens for the UDP broadcasts Tuya devices send on the local network
and prints each one's IP, device ID, and protocol version, and saves the
full results to `discovered_devices.json`. It can't show friendly names
(those only exist in your Tuya app's cloud account) — for a named list,
finish the Cloud setup above and use the running app's device list.

## 6. (Optional) Expose it outside your home network

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

## Notes

- **Rate limits:** free/trial Tuya Cloud projects are limited to a few
  requests per second. The app only calls the API when you actually
  release a slider or the native color picker (not while dragging), and
  polls nothing in the background, so normal use stays well under that.
- **Which lights show up:** every device linked to the Tuya app account
  from step 5 above appears here — not just bulbs. Simple on/off switches
  or plugs will just show a toggle with no brightness/color controls,
  since those controls only render for devices that report a matching
  capability.
- **Colors:** the color picker is the browser's native `<input
  type="color">`, so on iOS it opens Apple's system color wheel. Whatever
  you pick is converted to the device's own HSV range and, if the bulb
  supports a white/color mode switch, the app also flips it into color
  mode for you (and back to white mode when you touch brightness or
  warm/cool).
