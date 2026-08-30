(() => {
  'use strict';

  const grid = document.getElementById('lights');
  const emptyState = document.getElementById('emptyState');
  const banner = document.getElementById('banner');
  const refreshBtn = document.getElementById('refreshBtn');
  const allOnBtn = document.getElementById('allOnBtn');
  const allOffBtn = document.getElementById('allOffBtn');

  const cards = new Map(); // device id -> { el, refs, spec }

  function showBanner(msg) {
    banner.textContent = msg;
    banner.hidden = false;
    clearTimeout(showBanner._t);
    showBanner._t = setTimeout(() => { banner.hidden = true; }, 4000);
  }

  async function api(path, options) {
    const res = await fetch(path, options);
    let body = null;
    try { body = await res.json(); } catch (_) { /* no body */ }
    if (!res.ok) {
      throw new Error((body && body.error) || `${res.status} ${res.statusText}`);
    }
    return body;
  }

  // ---- HSV <-> hex, mapped through each device's own h/s/v ranges ----

  function hexToHsv(hex) {
    const r = parseInt(hex.slice(1, 3), 16) / 255;
    const g = parseInt(hex.slice(3, 5), 16) / 255;
    const b = parseInt(hex.slice(5, 7), 16) / 255;
    const max = Math.max(r, g, b), min = Math.min(r, g, b);
    const d = max - min;
    let h = 0;
    if (d !== 0) {
      if (max === r) h = ((g - b) / d) % 6;
      else if (max === g) h = (b - r) / d + 2;
      else h = (r - g) / d + 4;
      h *= 60;
      if (h < 0) h += 360;
    }
    const s = max === 0 ? 0 : d / max;
    const v = max;
    return { h, s, v };
  }

  function hsvToHex(h, s, v) {
    const c = v * s;
    const x = c * (1 - Math.abs(((h / 60) % 2) - 1));
    const m = v - c;
    let r = 0, g = 0, b = 0;
    if (h < 60) [r, g, b] = [c, x, 0];
    else if (h < 120) [r, g, b] = [x, c, 0];
    else if (h < 180) [r, g, b] = [0, c, x];
    else if (h < 240) [r, g, b] = [0, x, c];
    else if (h < 300) [r, g, b] = [x, 0, c];
    else [r, g, b] = [c, 0, x];
    const toHex = (n) => Math.round((n + m) * 255).toString(16).padStart(2, '0');
    return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
  }

  // ---- card creation ----

  function buildCard(device) {
    const el = document.createElement('section');
    el.className = 'card';
    el.dataset.id = device.id;

    const refs = {};

    const top = document.createElement('div');
    top.className = 'card-top';

    const nameWrap = document.createElement('div');
    const name = document.createElement('div');
    name.className = 'card-name';
    name.textContent = device.name;
    const sub = document.createElement('div');
    sub.className = 'card-sub';
    sub.textContent = device.online ? 'Online' : 'Offline';
    nameWrap.append(name, sub);
    refs.sub = sub;

    const switchLabel = document.createElement('label');
    switchLabel.className = 'switch';
    const switchInput = document.createElement('input');
    switchInput.type = 'checkbox';
    switchInput.disabled = true;
    const track = document.createElement('span');
    track.className = 'track';
    const thumb = document.createElement('span');
    thumb.className = 'thumb';
    switchLabel.append(switchInput, track, thumb);
    refs.switchInput = switchInput;

    top.append(nameWrap, switchLabel);

    const controls = document.createElement('div');
    controls.className = 'card-controls';

    el.append(top, controls);
    if (!device.online) el.classList.add('offline');

    grid.appendChild(el);
    cards.set(device.id, { el, refs, controls, device });
  }

  function addSlider(deviceId, controls, opts) {
    const { key, label, min, max, className, valueToText } = opts;
    const row = document.createElement('div');
    row.className = 'control-row';
    const labelRow = document.createElement('div');
    labelRow.className = 'control-label';
    const labelText = document.createElement('span');
    labelText.textContent = label;
    const valueText = document.createElement('span');
    labelRow.append(labelText, valueText);

    const input = document.createElement('input');
    input.type = 'range';
    input.className = className;
    input.min = String(min);
    input.max = String(max);
    input.disabled = true;

    const update = (v) => { valueText.textContent = valueToText ? valueToText(v) : v; };

    input.addEventListener('input', () => update(Number(input.value)));
    input.addEventListener('change', () => {
      sendCommand(deviceId, [{ code: key, value: Number(input.value) }]);
    });

    row.append(labelRow, input);
    controls.appendChild(row);
    return { input, update };
  }

  function addColorPicker(deviceId, controls) {
    const row = document.createElement('div');
    row.className = 'control-row color-row';
    const label = document.createElement('span');
    label.className = 'control-label';
    label.textContent = 'Color';
    const input = document.createElement('input');
    input.type = 'color';
    input.disabled = true;
    input.addEventListener('change', () => {
      const card = cards.get(deviceId);
      const spec = card.spec;
      const hsv = hexToHsv(input.value);
      const commands = [];
      if (spec.mode_code && spec.mode_colour) {
        commands.push({ code: spec.mode_code, value: spec.mode_colour });
      }
      commands.push({
        code: spec.colour_code,
        value: {
          h: Math.round(hsv.h),
          s: Math.round(hsv.s * spec.colour_s_max),
          v: Math.round(hsv.v * spec.colour_v_max),
        },
      });
      sendCommand(deviceId, commands);
    });
    row.append(label, input);
    controls.appendChild(row);
    return { input };
  }

  async function sendCommand(deviceId, commands) {
    try {
      await api(`/api/devices/${encodeURIComponent(deviceId)}/command`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ commands }),
      });
    } catch (err) {
      showBanner(`Couldn't update ${cards.get(deviceId)?.device.name || deviceId}: ${err.message}`);
      loadStatus(deviceId); // resync UI with reality
    }
  }

  // ---- status loading ----

  async function loadStatus(deviceId) {
    const card = cards.get(deviceId);
    if (!card) return;
    card.el.classList.add('loading');
    try {
      const state = await api(`/api/devices/${encodeURIComponent(deviceId)}/status`);
      applyState(deviceId, state);
    } catch (err) {
      showBanner(`Couldn't load ${card.device.name}: ${err.message}`);
    } finally {
      card.el.classList.remove('loading');
    }
  }

  function applyState(deviceId, state) {
    const card = cards.get(deviceId);
    if (!card) return;
    const spec = state.spec;
    card.spec = spec;

    // Build controls once, on first successful status load.
    if (!card.built) {
      card.built = true;
      const { switchInput } = card.refs;
      if (spec.switch_code) {
        switchInput.disabled = false;
        switchInput.addEventListener('change', () => {
          sendCommand(deviceId, [{ code: spec.switch_code, value: switchInput.checked }]);
        });
      }

      if (spec.bright_code) {
        card.brightness = addSlider(deviceId, card.controls, {
          key: spec.bright_code,
          label: 'Brightness',
          min: spec.bright_min,
          max: spec.bright_max,
          className: 'brightness',
          valueToText: (v) => `${Math.round(((v - spec.bright_min) / (spec.bright_max - spec.bright_min)) * 100)}%`,
        });
      }

      if (spec.temp_code) {
        card.temp = addSlider(deviceId, card.controls, {
          key: spec.temp_code,
          label: 'Warm ↔ Cool',
          min: spec.temp_min,
          max: spec.temp_max,
          className: 'warmcool',
          valueToText: () => '',
        });
      }

      if (spec.colour_code) {
        card.color = addColorPicker(deviceId, card.controls);
      }
    }

    if (spec.switch_code && typeof state.on === 'boolean') {
      card.refs.switchInput.checked = state.on;
    }
    if (card.brightness && typeof state.brightness === 'number') {
      card.brightness.input.value = state.brightness;
      card.brightness.input.disabled = false;
      card.brightness.update(state.brightness);
    }
    if (card.temp && typeof state.temperature === 'number') {
      card.temp.input.value = state.temperature;
      card.temp.input.disabled = false;
      card.temp.update(state.temperature);
    }
    if (card.color && state.colour) {
      // Some firmware reports colour_data(_v2) as a JSON string instead of an object.
      let colour = state.colour;
      if (typeof colour === 'string') {
        try { colour = JSON.parse(colour); } catch (_) { colour = null; }
      }
      if (colour && typeof colour === 'object') {
        const h = colour.h || 0;
        const s = (colour.s || 0) / (spec.colour_s_max || 1000);
        const v = (colour.v || 0) / (spec.colour_v_max || 1000);
        card.color.input.value = hsvToHex(h, s, v);
        card.color.input.disabled = false;
      }
    }
  }

  // ---- top-level load / refresh ----

  async function loadAll() {
    refreshBtn.classList.add('spinning');
    try {
      const devices = await api('/api/devices');
      grid.querySelectorAll('.card').forEach((el) => el.remove());
      cards.clear();
      emptyState.hidden = devices.length > 0;

      devices.forEach(buildCard);
      await Promise.all(devices.map((d) => (d.online ? loadStatus(d.id) : Promise.resolve())));
    } catch (err) {
      showBanner(`Couldn't load lights: ${err.message}`);
    } finally {
      refreshBtn.classList.remove('spinning');
    }
  }

  async function allSwitch(on) {
    try {
      await api('/api/all', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ on }),
      });
      cards.forEach((card) => {
        if (card.refs.switchInput && !card.refs.switchInput.disabled) {
          card.refs.switchInput.checked = on;
        }
      });
    } catch (err) {
      showBanner(`Couldn't update all lights: ${err.message}`);
    }
  }

  refreshBtn.addEventListener('click', loadAll);
  allOnBtn.addEventListener('click', () => allSwitch(true));
  allOffBtn.addEventListener('click', () => allSwitch(false));

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') loadAll();
  });

  loadAll();
})();
