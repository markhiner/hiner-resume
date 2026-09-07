(() => {
  'use strict';

  const grid = document.getElementById('lights');
  const emptyState = document.getElementById('emptyState');
  const banner = document.getElementById('banner');
  const refreshBtn = document.getElementById('refreshBtn');
  const allOnBtn = document.getElementById('allOnBtn');
  const allOffBtn = document.getElementById('allOffBtn');
  const automationLabel = document.getElementById('automationLabel');
  const automationToggle = document.getElementById('automationToggle');

  const cards = new Map(); // device id -> { el, refs, spec, controls, state }

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

  // A device's "brightness" lives in one of two places depending on which
  // mode it's in: bright_code (white mode) or colour_code's v component
  // (color mode). Bulbs with only colour_data and no white mode always use
  // the color path.
  function brightnessGoesThroughColour(spec, mode) {
    if (!spec.colour_code) return false;
    if (!spec.bright_code) return true;
    if (spec.mode_code && spec.mode_colour) return mode === spec.mode_colour;
    return false;
  }

  function parseColour(raw) {
    if (!raw) return null;
    if (typeof raw === 'string') {
      try { return JSON.parse(raw); } catch (_) { return null; }
    }
    return typeof raw === 'object' ? raw : null;
  }

  // Hue/saturation/brightness sliders each need the others' current value to
  // build a full colour_data command — the sliders themselves are the single
  // source of truth (no separate state object to drift out of sync).
  function currentHue(card) {
    return card.hue ? Number(card.hue.input.value) : 0;
  }

  function currentSaturationValue(card) {
    const pct = card.saturation ? Number(card.saturation.input.value) : 100;
    return Math.round((pct / 100) * (card.spec.colour_s_max || 1000));
  }

  function currentVValue(card, pctOverride) {
    const pct = pctOverride !== undefined
      ? pctOverride
      : (card.brightness ? Number(card.brightness.input.value) : 100);
    return Math.round((pct / 100) * (card.spec.colour_v_max || 1000));
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
    const status = device.online ? 'Online' : 'Offline';
    sub.textContent = device.group_size ? `${status} · ${device.group_size} lights` : status;
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
    cards.set(device.id, { el, refs, controls, device, state: {} });
  }

  function addSlider(controls, opts) {
    const { label, min, max, className, valueToText, onInput, onCommit } = opts;
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

    input.addEventListener('input', () => {
      const v = Number(input.value);
      update(v);
      if (onInput) onInput(v);
    });
    input.addEventListener('change', () => onCommit(Number(input.value)));

    row.append(labelRow, input);
    controls.appendChild(row);
    return { input, update };
  }

  function addHueSlider(deviceId, controls) {
    const row = document.createElement('div');
    row.className = 'control-row color-row';
    const labelRow = document.createElement('div');
    labelRow.className = 'control-label';
    const labelText = document.createElement('span');
    labelText.textContent = 'Color';
    const swatch = document.createElement('span');
    swatch.className = 'hue-swatch';
    labelRow.append(labelText, swatch);

    const input = document.createElement('input');
    input.type = 'range';
    input.className = 'hue';
    input.min = '0';
    input.max = '360';
    input.disabled = true;

    const paint = (hue) => {
      swatch.style.background = `hsl(${hue}, 100%, 50%)`;
      const card = cards.get(deviceId);
      if (card && card.saturation) {
        card.saturation.input.style.setProperty('--hue', hue);
      }
    };

    input.addEventListener('input', () => paint(Number(input.value)));
    input.addEventListener('change', () => commitHue(deviceId, Number(input.value)));

    row.append(labelRow, input);
    controls.appendChild(row);
    return { input, paint };
  }

  // ---- commit handlers (mode-aware) ----

  function commitSwitch(deviceId, on) {
    const card = cards.get(deviceId);
    sendCommand(deviceId, [{ code: card.spec.switch_code, value: on }]);
  }

  function commitBrightnessPct(deviceId, pct) {
    const card = cards.get(deviceId);
    const spec = card.spec;
    if (brightnessGoesThroughColour(spec, card.state.mode)) {
      const h = currentHue(card);
      const s = currentSaturationValue(card);
      const v = currentVValue(card, pct);
      sendCommand(deviceId, [{ code: spec.colour_code, value: { h, s, v } }]);
    } else {
      const value = Math.round(spec.bright_min + (spec.bright_max - spec.bright_min) * pct / 100);
      sendCommand(deviceId, [{ code: spec.bright_code, value }]);
    }
  }

  function commitTemp(deviceId, value) {
    const card = cards.get(deviceId);
    const spec = card.spec;
    const commands = [];
    if (spec.mode_code && spec.mode_white) {
      commands.push({ code: spec.mode_code, value: spec.mode_white });
      card.state.mode = spec.mode_white; // optimistic
    }
    commands.push({ code: spec.temp_code, value });
    sendCommand(deviceId, commands);
  }

  function commitHue(deviceId, hue) {
    const card = cards.get(deviceId);
    const spec = card.spec;
    const commands = [];
    if (spec.mode_code && spec.mode_colour) {
      commands.push({ code: spec.mode_code, value: spec.mode_colour });
      card.state.mode = spec.mode_colour; // optimistic
    }
    const s = currentSaturationValue(card);
    const v = currentVValue(card);
    commands.push({ code: spec.colour_code, value: { h: hue, s, v } });
    sendCommand(deviceId, commands);
  }

  function commitSaturation(deviceId, pct) {
    const card = cards.get(deviceId);
    const spec = card.spec;
    const commands = [];
    if (spec.mode_code && spec.mode_colour) {
      commands.push({ code: spec.mode_code, value: spec.mode_colour });
      card.state.mode = spec.mode_colour; // optimistic
    }
    const h = currentHue(card);
    const s = Math.round((pct / 100) * (spec.colour_s_max || 1000));
    const v = currentVValue(card);
    commands.push({ code: spec.colour_code, value: { h, s, v } });
    sendCommand(deviceId, commands);
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
    card.state.mode = state.mode;

    const colour = parseColour(state.colour);

    // Build controls once, on first successful status load.
    if (!card.built) {
      card.built = true;
      const { switchInput } = card.refs;
      if (spec.switch_code) {
        switchInput.disabled = false;
        switchInput.addEventListener('change', () => commitSwitch(deviceId, switchInput.checked));
      }

      if (spec.bright_code || spec.colour_code) {
        card.brightness = addSlider(card.controls, {
          label: 'Brightness',
          min: 0,
          max: 100,
          className: 'brightness',
          valueToText: (v) => `${v}%`,
          onCommit: (pct) => commitBrightnessPct(deviceId, pct),
        });
      }

      if (spec.temp_code) {
        card.temp = addSlider(card.controls, {
          label: 'Warm ↔ Cool',
          min: spec.temp_min,
          max: spec.temp_max,
          className: 'warmcool',
          valueToText: () => '',
          onCommit: (v) => commitTemp(deviceId, v),
        });
      }

      if (spec.colour_code) {
        card.saturation = addSlider(card.controls, {
          label: 'Saturation',
          min: 0,
          max: 100,
          className: 'saturation',
          valueToText: (v) => `${v}%`,
          onCommit: (pct) => commitSaturation(deviceId, pct),
        });
        // Built after saturation so its initial paint can colorize the
        // saturation track's gray-to-vivid gradient right away.
        card.hue = addHueSlider(deviceId, card.controls);
      }
    }

    if (spec.switch_code && typeof state.on === 'boolean') {
      card.refs.switchInput.checked = state.on;
    }

    if (card.brightness) {
      const usesColour = brightnessGoesThroughColour(spec, state.mode);
      let pct = null;
      if (usesColour && colour) {
        pct = Math.round(((colour.v || 0) / (spec.colour_v_max || 1000)) * 100);
      } else if (typeof state.brightness === 'number') {
        pct = Math.round(((state.brightness - spec.bright_min) / (spec.bright_max - spec.bright_min)) * 100);
      }
      if (pct !== null) {
        card.brightness.input.value = pct;
        card.brightness.input.disabled = false;
        card.brightness.update(pct);
      }
    }

    if (card.temp && typeof state.temperature === 'number') {
      card.temp.input.value = state.temperature;
      card.temp.input.disabled = false;
      card.temp.update(state.temperature);
    }

    if (card.saturation && colour) {
      const satPct = Math.round(((colour.s || 0) / (spec.colour_s_max || 1000)) * 100);
      card.saturation.input.value = satPct;
      card.saturation.input.disabled = false;
      card.saturation.update(satPct);
    }

    if (card.hue && colour) {
      const hue = colour.h || 0;
      card.hue.input.value = hue;
      card.hue.input.disabled = false;
      card.hue.paint(hue);
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

  // ---- automation status/toggle ----

  function renderAutomation(state) {
    const ruleWord = state.rule_count === 1 ? 'rule' : 'rules';
    automationLabel.textContent = state.enabled
      ? `Automation: ON · ${state.rule_count} ${ruleWord}`
      : `Automation: OFF (${state.rule_count} ${ruleWord} configured)`;
    automationToggle.checked = state.enabled;
    automationToggle.disabled = false;
  }

  async function loadAutomation() {
    try {
      renderAutomation(await api('/api/automation'));
    } catch (err) {
      automationLabel.textContent = `Automation: couldn't load (${err.message})`;
    }
  }

  automationToggle.addEventListener('change', async () => {
    const enabled = automationToggle.checked;
    automationToggle.disabled = true;
    try {
      renderAutomation(await api('/api/automation', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled }),
      }));
    } catch (err) {
      showBanner(`Couldn't update automation: ${err.message}`);
      automationToggle.checked = !enabled;
      automationToggle.disabled = false;
    }
  });

  refreshBtn.addEventListener('click', loadAll);
  allOnBtn.addEventListener('click', () => allSwitch(true));
  allOffBtn.addEventListener('click', () => allSwitch(false));

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      loadAll();
      loadAutomation();
    }
  });

  loadAll();
  loadAutomation();
})();
