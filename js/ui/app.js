/* ui/app.js: the browser front end for all three labs. Engines live in js/shuffle, js/stream, js/grant. */
(function () {
  'use strict';
  const SSG = window.SSG;
  const $ = s => document.querySelector(s);
  const h = (tag, attrs, ...kids) => {
    const el = document.createElement(tag);
    Object.entries(attrs || {}).forEach(([k, v]) => {
      if (v == null || v === false) return;
      if (k === 'class') el.className = v; else if (k === 'text') el.textContent = v; else if (k === 'html') el.innerHTML = v;
      else if (k.startsWith('on')) el.addEventListener(k.slice(2), v); else el.setAttribute(k, v === true ? '' : v);
    });
    kids.flat().forEach(c => { if (c != null && c !== false) el.append(c.nodeType ? c : document.createTextNode(c)); });
    return el;
  };
  const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  function highlight(code) {
    return code.split('\n').map(line => {
      const ci = line.search(/(^|\s)(#|--)/);
      const body = ci >= 0 ? line.slice(0, ci) : line, comment = ci >= 0 ? line.slice(ci) : '';
      const b = esc(body).replace(/("[^"]*"|'[^']*')/g, '<span class="s">$1</span>').replace(/\b(spark|F|readStream|writeStream|MERGE|AUTO CDC|CREATE|FLOW|STREAMING TABLE|SEQUENCE BY|KEYS|STORED AS|APPLY AS DELETE WHEN|broadcast)\b/g, '<span class="k">$1</span>');
      return b + (comment ? `<span class="c">${esc(comment)}</span>` : '');
    }).join('\n');
  }

  /* ---------- progress ---------- */
  const KEY = 'shuffle-stream-grant:v1';
  const store = (() => {
    let d; try { d = JSON.parse(localStorage.getItem(KEY)) || {}; } catch (e) { d = {}; }
    d = Object.assign({ solved: {}, cfg: {}, text: {}, hints: {} }, d);
    return { d, save() { try { localStorage.setItem(KEY, JSON.stringify(d)); } catch (e) { /* private mode */ } } };
  })();

  const LABS = {
    shuffle: { name: 'Shuffle · Spark performance', mod: SSG.shuffleLab },
    stream: { name: 'Stream · Structured Streaming & Lakeflow', mod: SSG.streamLab },
    grant: { name: 'Grant · Unity Catalog & Asset Bundles', mod: SSG.grantLab },
  };
  const ALL = Object.keys(LABS).flatMap(l => LABS[l].mod.CHALLENGES.map(c => Object.assign(c, { lab: l })));
  let lab = 'shuffle', current = null, justSolved = false;

  /* ---------- shell ---------- */
  function renderList() {
    const list = $('#list');
    const items = [];
    let group = null;
    LABS[lab].mod.CHALLENGES.forEach(c => {
      const g = c.lab === 'grant' ? (c.kind === 'bundle' ? 'Asset Bundles' : 'Unity Catalog') : null;
      if (g && g !== group) { group = g; items.push(h('li', { class: 'group-h', text: g })); }
      items.push(h('li', null, h('button', { type: 'button', class: 'item' + (store.d.solved[c.id] ? ' done' : ''), 'aria-current': current && current.id === c.id ? 'true' : 'false', onclick: () => open(c.id) },
        h('span', { class: 'dot', 'aria-label': store.d.solved[c.id] ? 'solved' : 'not solved' }), h('span', { text: c.title }), h('span', { class: 'lv', text: '●'.repeat(c.level) + '○'.repeat(3 - c.level) }))));
    });
    list.replaceChildren(...items);
    const n = ALL.filter(c => store.d.solved[c.id]).length;
    $('#progress-text').textContent = `${n} of ${ALL.length} solved`;
    $('#progress-bar').style.width = (100 * n / ALL.length) + '%';
    document.querySelectorAll('.lab-tab').forEach(t => {
      t.setAttribute('aria-pressed', String(t.dataset.lab === lab));
      const cs = LABS[t.dataset.lab].mod.CHALLENGES; t.title = `${cs.filter(c => store.d.solved[c.id]).length} of ${cs.length} solved`;
    });
  }
  function open(id) {
    const c = ALL.find(x => x.id === id) || LABS[lab].mod.CHALLENGES[0];
    lab = c.lab; current = c; justSolved = false;
    document.body.dataset.lab = lab;
    if (location.hash !== `#${lab}/${c.id}`) history.replaceState(null, '', `#${lab}/${c.id}`);
    document.title = `${c.title} · Shuffle, Stream & Grant`;
    $('#lab-name').textContent = LABS[lab].name;
    $('#level').textContent = ['', 'Warm-up', 'Core', 'Hard'][c.level];
    $('#title').textContent = c.title;
    $('#scenario').innerHTML = c.scenario;
    $('#task').innerHTML = c.task;
    renderHelp();
    $('#verdict').hidden = true;
    ({ shuffle: renderShuffle, stream: renderStream, grant: c.kind === 'bundle' ? renderBundle : renderUc })[lab](c);
    renderList(); renderStatus();
    window.scrollTo({ top: 0 });
  }
  function renderStatus() { const s = $('#status'); const done = store.d.solved[current.id]; s.textContent = done ? 'Solved' : 'Not solved yet'; s.className = 'pill' + (done ? ' ok' : ''); }
  function renderHelp() {
    const c = current, shown = store.d.hints[c.id] || 0;
    const kids = c.hints.slice(0, shown).map((t, i) => h('p', { class: 'hint', html: `<b>Hint ${i + 1}.</b> ${esc(t)}` }));
    if (shown < c.hints.length) kids.push(h('button', { type: 'button', class: 'btn', text: shown ? `Another hint (${shown + 1} of ${c.hints.length})` : `Hint (${c.hints.length} available)`, onclick: () => { store.d.hints[c.id] = shown + 1; store.save(); renderHelp(); } }));
    kids.push(h('button', { type: 'button', class: 'btn quiet', text: 'Show the solution', onclick: showSolution }));
    $('#help').replaceChildren(...kids);
  }
  function showSolution() {
    const c = current;
    if (!confirm('Show the reference solution? Try a few changes first: the feedback tells you what is still wrong.')) return;
    if (c.lab === 'grant') { const ta = document.querySelector('#editor'); ta.value = c.solution; ta.dispatchEvent(new Event('input')); if (c.kind !== 'bundle') runUc(); }
    else { store.d.cfg[c.id] = Object.assign({}, c.solution); store.save(); open(c.id); }
  }
  /* Called by every lab after a check. */
  function report(ok) {
    const c = current;
    if (!ok) { $('#verdict').hidden = true; return; }
    const first = !store.d.solved[c.id];
    if (first) { store.d.solved[c.id] = Date.now(); store.save(); renderList(); renderStatus(); }
    const next = ALL.slice(ALL.indexOf(c) + 1).find(x => !store.d.solved[x.id]) || ALL.find(x => !store.d.solved[x.id]);
    const v = $('#verdict');
    v.replaceChildren(h('h2', { text: first ? 'All goals met' : 'All goals met (solved before)' }), h('p', { html: c.explain }),
      next ? h('div', { class: 'next' }, h('button', { type: 'button', class: 'btn primary', text: `Next: ${next.title} →`, onclick: () => open(next.id) })) : h('p', { text: 'Every challenge in all three labs is solved. Well done.' }));
    v.hidden = false;
  }
  function goalList(goals) {
    return h('ul', { class: 'goals' }, goals.map(g => h('li', { class: 'goal ' + (g.met ? 'met' : 'missed') }, h('span', { text: g.text }), h('span', { class: 'v', text: g.value == null ? '' : String(g.value) }))));
  }
  function knobSelect(id, label, options, value, show, onchange, changed) {
    const idx = Math.max(0, options.findIndex(o => o === value));
    return h('div', { class: 'knob' + (changed ? ' changed' : '') }, h('label', { for: `k-${id}`, text: label }),
      h('select', { id: `k-${id}`, onchange: e => onchange(options[Number(e.target.value)]) }, options.map((o, i) => h('option', { value: String(i), selected: i === idx, text: show ? show(o) : String(o) }))));
  }

  /* ================= Shuffle ================= */
  const S = SSG.shuffle;
  function renderShuffle(c) {
    const L = SSG.shuffleLab;
    const cfg = Object.assign({}, c.baseline, store.d.cfg[c.id] || {});
    const ws = $('#workspace');
    const goalsBox = h('div'), codeBox = h('pre', { class: 'code' }), runBox = h('div');
    const update = () => {
      store.d.cfg[c.id] = cfg; store.save();
      const r = L.check(c, cfg);
      goalsBox.replaceChildren(goalList(r.goals.map(g => ({ met: g.met, text: goalText(g), value: goalValue(g) }))));
      codeBox.innerHTML = highlight(L.code(c, cfg));
      runBox.replaceChildren(...sparkRun(r.result));
      report(r.ok);
    };
    const knobs = c.knobs.map(k => {
      const K = L.KNOBS[k];
      const opts = (c.options && c.options[k]) || K.options || [null];
      const val = cfg[k] !== undefined ? cfg[k] : S.DEFAULTS[k];
      return knobSelect(k, K.label, opts, val, K.show, v => { cfg[k] = v; renderShuffle(c); }, c.baseline[k] !== undefined ? c.baseline[k] !== val : S.DEFAULTS[k] !== val);
    });
    ws.replaceChildren(h('div', { class: 'cols' },
      h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Configuration' }), h('button', { type: 'button', class: 'btn quiet', text: 'Reset', onclick: () => { delete store.d.cfg[c.id]; store.save(); renderShuffle(c); } })), h('div', { class: 'knobs' }, knobs),
        h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Goals' })), goalsBox),
      h('div', { style: 'display:grid;gap:16px;min-width:0' },
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'The job' })), codeBox),
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Run · Spark UI' })), runBox))));
    update();
  }
  function goalText(g) {
    switch (g.metric) {
      case 'runtime': return `Finishes within ${S.fmt(g.max)}`;
      case 'cost': return `Costs at most $${g.max.toFixed(2)} per run`;
      case 'spillMB': return 'No spill to disk';
      case 'outputFiles': return `Writes at most ${g.max} files`;
      case 'readMB': return `Reads at most ${S.fmtMB(g.max)}`;
      default: return 'The job succeeds';
    }
  }
  function goalValue(g) {
    if (g.metric === 'failed') return g.value ? 'failed' : 'succeeded';
    if (g.value == null) return 'job failed';
    switch (g.metric) {
      case 'runtime': return S.fmt(g.value);
      case 'cost': return `$${g.value.toFixed(2)}`;
      case 'outputFiles': return `${g.value} files`;
      default: return S.fmtMB(g.value);
    }
  }
  function sparkRun(r) {
    if (r.failed) return [h('div', { class: 'failure' }, h('b', { text: `Job aborted: ${r.failed.cls}` }), h('p', { text: r.failed.message }))];
    const longest = Math.max(...r.stages.map(s => s.max)), total = r.stages.reduce((a, s) => a + s.time, 0);
    return [
      h('div', { class: 'metrics' },
        h('div', { class: 'metric' }, h('b', { text: S.fmt(r.runtime) }), h('span', { text: 'runtime' })),
        h('div', { class: 'metric' }, h('b', { text: `$${r.cost.toFixed(2)}` }), h('span', { text: `${r.cfg.workers} workers × ${r.cfg.coresPerWorker} cores` })),
        h('div', { class: 'metric' + (r.spillMB ? ' bad' : '') }, h('b', { text: S.fmtMB(r.spillMB) }), h('span', { text: 'spilled to disk' })),
        h('div', { class: 'metric' }, h('b', { text: S.fmtMB(r.readMB) }), h('span', { text: 'read from storage' })),
        h('div', { class: 'metric' }, h('b', { text: r.outputFiles.toLocaleString() }), h('span', { text: 'output files' }))),
      h('div', { class: 'stages' }, r.stages.map((s, i) => {
        const skew = s.tasks > 4 && s.max > 5 * s.median && s.max > 20;
        const min = Math.min(...s.durations);
        const pct = v => (100 * v / longest).toFixed(2) + '%';
        const parts = [`${s.tasks.toLocaleString()} task${s.tasks === 1 ? '' : 's'}`];
        if (s.inputMB) parts.push(`read ${S.fmtMB(s.inputMB)}`);
        if (s.shuffleReadMB) parts.push(`shuffle read ${S.fmtMB(s.shuffleReadMB)}`);
        if (s.shuffleWriteMB) parts.push(`shuffle write ${S.fmtMB(s.shuffleWriteMB)}`);
        if (s.broadcastMB) parts.push(`broadcast ${S.fmtMB(s.broadcastMB)}`);
        if (s.spillMB) parts.push(`spill ${S.fmtMB(s.spillMB)}`);
        if (s.outputFiles) parts.push(`${s.outputFiles} file${s.outputFiles === 1 ? '' : 's'}`);
        return h('div', { class: 'stage' },
          h('div', null, h('div', { class: 'name', text: `Stage ${i + 1} · ${s.name}` }), h('div', { class: 'sub', text: parts.join(' · ') }),
            h('div', { class: 'spread' + (skew ? ' skew' : ''), title: `task durations: min ${S.fmt(min)}, median ${S.fmt(s.median)}, max ${S.fmt(s.max)}` }, h('i', { style: `left:${pct(min)};width:max(3px, calc(${pct(s.max)} - ${pct(min)}))` }), h('span', { class: 'med', style: `left:${pct(s.median)}` })),
            (s.note || []).map(n => h('div', { class: 'note', text: n }))),
          h('div', { class: 't', html: `${S.fmt(s.max)}<small>slowest task</small>` }),
          h('div', { class: 't', html: `${S.fmt(s.time)}<small>${Math.round(100 * s.time / total)}% of job</small>` }));
      })),
      r.findings.length ? h('ul', { class: 'findings' }, r.findings.map(f => h('li', { class: f.level, text: f.text }))) : h('ul', { class: 'findings' }, h('li', { class: '', text: 'No problems detected.' })),
    ];
  }

  /* ================= Stream ================= */
  const MIN = 60000;
  function renderStream(c) {
    const L = SSG.streamLab;
    const cfg = Object.assign({}, c.baseline, store.d.cfg[c.id] || {});
    const goalsBox = h('div'), codeBox = h('pre', { class: 'code' }), runBox = h('div');
    const knobs = c.knobs.map(k => {
      const K = L.KNOBS[k];
      const opts = (c.options && c.options[k]) || K.options;
      return knobSelect(k, K.label, opts, cfg[k], K.show, v => { cfg[k] = v; renderStream(c); }, c.baseline[k] !== cfg[k]);
    });
    $('#workspace').replaceChildren(h('div', { class: 'cols' },
      h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Stream settings' }), h('button', { type: 'button', class: 'btn quiet', text: 'Reset', onclick: () => { delete store.d.cfg[c.id]; store.save(); renderStream(c); } })), h('div', { class: 'knobs' }, knobs),
        h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Goals' })), goalsBox),
      h('div', { style: 'display:grid;gap:16px;min-width:0' },
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'The stream' })), codeBox),
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Replay' })), runBox))));
    store.d.cfg[c.id] = cfg; store.save();
    const r = L.check(c, cfg);
    goalsBox.replaceChildren(goalList(r.goals));
    codeBox.innerHTML = highlight(L.code(c, cfg));
    runBox.replaceChildren(...streamRun(c, r));
    report(r.ok);
  }
  const t = ms => { const m = Math.round(ms / MIN); return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`; };
  function streamRun(c, r) {
    if (r.error) return [h('div', { class: 'failure' }, h('b', { text: `Stream failed to start: ${r.error.cls || 'error'}` }), h('p', { text: r.error.message.replace(/^\[[^\]]+\]\s*/, '') }))];
    const res = r.result, out = [];
    const kind = c.scenarioData.kind || r.cfg.query;
    if (kind === 'window_count' || kind === 'dedupe') {
      const dropped = res.dropped.length;
      out.push(h('div', { class: 'metrics' },
        h('div', { class: 'metric' }, h('b', { text: String(res.batches) }), h('span', { text: 'micro-batches' })),
        h('div', { class: 'metric' + (dropped ? ' bad' : '') }, h('b', { text: String(dropped) }), h('span', { text: 'late rows dropped' })),
        h('div', { class: 'metric' }, h('b', { text: String(res.maxState) }), h('span', { text: kind === 'dedupe' ? 'max keys in state' : 'max windows in state' })),
        h('div', { class: 'metric' }, h('b', { text: String(res.emitted.length) }), h('span', { text: 'rows written' }))));
      out.push(chart(res.log), h('div', { class: 'legend' }, h('span', { style: `--c:var(--stream)`, text: 'watermark' }), h('span', { style: '--c:rgba(60,198,216,.35)', text: 'state size' }), h('span', { style: '--c:var(--bad)', text: 'batches that dropped late rows' })));
      const rows = res.log.slice(0, 400).map(l => h('tr', null, h('td', { class: 'n', text: String(l.batch) }), h('td', { text: t(l.time) }), h('td', { class: 'dim', text: l.kind }), h('td', { class: 'n', text: String(l.input) }),
        h('td', { class: 'n' + (l.late ? ' bad' : ''), text: String(l.late || 0) }), kind === 'dedupe' ? h('td', { class: 'n', text: String(l.duplicates || 0) }) : null,
        h('td', { text: l.watermark === -Infinity || l.watermark == null ? '—' : t(l.watermark) }), h('td', { class: 'n', text: String(l.state) }), h('td', { class: 'n', text: String(l.emitted) })));
      out.push(h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['batch', 'trigger', 'kind', 'input', 'late'].concat(kind === 'dedupe' ? ['dupes'] : [], ['watermark', 'state', 'written']).map(x => h('th', { text: x })))), h('tbody', null, rows))));
      if (kind === 'window_count') out.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Rows written to the sink (first 60)' })), h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['at', 'window', 'count', 'final'].map(x => h('th', { text: x })))),
        h('tbody', null, res.emitted.slice(0, 60).map(e => h('tr', null, h('td', { text: t(e.time) }), h('td', { text: `${t(e.start)}–${t(e.end)}` }), h('td', { class: 'n', text: String(e.count) }), h('td', { class: e.final ? 'ok' : 'dim', text: e.final ? 'final' : 'update' })))))));
    } else if (kind === 'cdc') {
      const truth = new Map(); c.scenarioData.events.slice().sort((a, b) => a.seq - b.seq).forEach(e => { if (e.op === 'DELETE') truth.delete(e.key); else truth.set(e.key, e.value); });
      const keys = Array.from(new Set([...truth.keys(), ...res.target.keys()])).sort();
      out.push(h('div', { class: 'metrics' }, h('div', { class: 'metric' }, h('b', { text: String(c.scenarioData.events.length) }), h('span', { text: 'changes' })), h('div', { class: 'metric' }, h('b', { text: String(res.outOfOrder) }), h('span', { text: 'arrived out of order' })), h('div', { class: 'metric' }, h('b', { text: String(res.target.size) }), h('span', { text: 'rows in the target' }))));
      out.push(h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['customer', 'target table', 'CRM (source of truth)', ''].map(x => h('th', { text: x })))),
        h('tbody', null, keys.map(k => { const got = res.target.has(k) ? res.target.get(k).value : '(deleted)', want = truth.has(k) ? truth.get(k) : '(deleted)'; return h('tr', { class: got === want ? '' : 'err' }, h('td', { text: k }), h('td', { text: got }), h('td', { text: want }), h('td', { text: got === want ? '✓' : '✗' })); })))));
      if (r.cfg.scd === 2) out.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'History (SCD Type 2)' })), h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['customer', 'city', '__START_AT', '__END_AT'].map(x => h('th', { text: x })))), h('tbody', null, res.history.map(v => h('tr', null, h('td', { text: v.key }), h('td', { text: v.value }), h('td', { class: 'n', text: `seq ${v.start}` }), h('td', { class: 'n', text: v.end == null ? 'open' : `seq ${v.end}` })))))));
    } else {
      out.push(h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['at', 'event'].map(x => h('th', { text: x })))), h('tbody', null, res.log.map(l => h('tr', { class: l.kind === 'failure' ? 'err' : '' }, h('td', { text: t(l.time) }), h('td', { class: 'detail', text: l.message })))))));
      const rows = res.rows, cols = rows.length ? Object.keys(rows[0]) : [];
      out.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: `Target table: ${rows.length} rows${res.schema ? ` · columns ${res.schema.join(', ')}` : ''}` })), h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, cols.map(x => h('th', { text: x })))), h('tbody', null, rows.map(rw => h('tr', null, cols.map(k => h('td', { class: rw[k] == null ? 'dim' : '', text: rw[k] == null ? 'NULL' : String(rw[k]) }))))))));
    }
    return out;
  }
  function chart(log) {
    const pts = log.filter(l => l.kind === 'data' || l.kind === 'no-data');
    const W = 800, H = 150, P = 26;
    if (!pts.length) return h('div');
    const t0 = pts[0].time, t1 = pts[pts.length - 1].time || t0 + 1;
    const wms = pts.map(p => (p.nextWatermark != null ? p.nextWatermark : p.watermark)).filter(v => v != null && v !== -Infinity);
    const lo = Math.min(t0, ...(wms.length ? wms : [t0])), hi = t1;
    const maxState = Math.max(1, ...pts.map(p => p.state));
    const x = v => P + (W - 2 * P) * (v - t0) / Math.max(1, t1 - t0);
    const yW = v => H - P - (H - 2 * P) * (v - lo) / Math.max(1, hi - lo);
    const yS = v => H - P - (H - 2 * P) * v / maxState;
    let area = `M${x(pts[0].time)},${H - P}`; pts.forEach(p => { area += ` L${x(p.time)},${yS(p.state)}`; }); area += ` L${x(pts[pts.length - 1].time)},${H - P} Z`;
    let line = ''; pts.forEach(p => { const w = p.nextWatermark != null ? p.nextWatermark : p.watermark; if (w == null || w === -Infinity) return; line += `${line ? ' L' : 'M'}${x(p.time)},${yW(w)}`; });
    const late = pts.filter(p => p.late).map(p => `<circle class="late" cx="${x(p.time)}" cy="${H - P + 8}" r="3"/>`).join('');
    const svg = `<svg class="chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none" role="img" aria-label="Watermark and state size over the run"><line class="ax" x1="${P}" y1="${H - P}" x2="${W - P}" y2="${H - P}"/><path class="st" d="${area}"/>${line ? `<path class="wm" d="${line}"/>` : ''}${late}<text x="${P}" y="${H - 8}">${t(t0)}</text><text x="${W - P}" y="${H - 8}" text-anchor="end">${t(t1)}</text><text x="${P}" y="14">state max ${maxState}</text></svg>`;
    return h('div', { html: svg });
  }

  /* ================= Grant: Unity Catalog ================= */
  function renderUc(c) {
    const L = SSG.grantLab;
    const ta = h('textarea', { id: 'editor', spellcheck: 'false', 'aria-label': 'SQL', oninput: () => { store.d.text[c.id] = ta.value; store.save(); }, onkeydown: e => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runUc(); } } });
    ta.value = store.d.text[c.id] != null ? store.d.text[c.id] : c.starter;
    const u = L.world(); if (c.setupWorld) c.setupWorld(u);
    $('#workspace').replaceChildren(
      h('div', { class: 'cols' },
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Account' })), accountTree(u)),
        h('div', { class: 'card editor' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'SQL · run as a platform admin' }), h('button', { type: 'button', class: 'btn quiet', text: 'Reset', onclick: () => { delete store.d.text[c.id]; store.save(); renderUc(c); } }), h('button', { type: 'button', class: 'btn primary', text: 'Run checks', onclick: runUc })), ta)),
      h('div', { class: 'card', id: 'uc-out' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Checks' })), h('p', { style: 'margin:0;padding:14px;color:var(--ink-3)', text: 'Press Run checks (or Ctrl+Enter) to apply your SQL and run the access tests.' })));
    if (store.d.solved[c.id] || store.d.text[c.id] != null) runUc();
  }
  function accountTree(u) {
    const groups = Array.from(u.principals.entries()).filter(([, p]) => p.kind === 'group' && !p.builtin).map(([n]) => n);
    const members = g => Array.from(u.principals.entries()).filter(([, p]) => p.groups.includes(g)).map(([n]) => n.split('@')[0]);
    const objs = Array.from(u.objects.values()).sort((a, b) => (a.name < b.name ? -1 : 1));
    return h('div', null,
      h('ul', { class: 'tree' }, objs.map(o => h('li', { style: `padding-left:${(o.name.split('.').length - 1) * 14}px`, html: `<span class="o">${o.type.toLowerCase()}</span> <span class="k">${esc(o.name)}</span>${o.type === 'VIEW' ? ` <span class="o">owner ${esc(o.owner)}</span>` : ''}` }))),
      h('ul', { class: 'tree' }, groups.map(g => h('li', { html: `<span class="o">group</span> <span class="k">${esc(g)}</span> <span class="o">${esc(members(g).join(', ') || '—')}</span>` })), h('li', { html: '<span class="o">service principal</span> <span class="k">etl-prod</span>' })));
  }
  function runUc() {
    const c = current, L = SSG.grantLab;
    const r = L.check(c, document.querySelector('#editor').value);
    const out = document.querySelector('#uc-out');
    const verb = { select: 'SELECT', modify: 'INSERT INTO', create_table: 'CREATE TABLE IN', read_volume: 'READ VOLUME', write_volume: 'WRITE VOLUME' };
    const kids = [h('div', { class: 'panel-h' }, h('span', { class: 'label', text: `Access tests · ${r.tests.filter(x => x.met).length} of ${r.tests.length} pass` }))];
    if (r.error) kids.push(h('div', { class: 'failure' }, h('b', { text: `Statement ${r.error.index + 1} failed` }), h('p', { text: r.error.message })));
    kids.push(h('div', { class: 'table-wrap', style: 'max-height:none' }, h('table', { class: 'tbl matrix' }, h('thead', null, h('tr', null, ['as', 'action', 'expected', 'result', 'detail'].map(x => h('th', { text: x })))),
      h('tbody', null, r.tests.map(x => h('tr', null, h('td', { text: x.as }), h('td', { text: `${verb[x.action] || x.action} ${x.object}${x.future ? ' (created after your grants)' : ''}` }), h('td', { text: x.expect + (x.rows != null ? `, ${x.rows} rows` : x.column ? `, ${x.column} = ${x.equals}` : '') }), h('td', { class: 'res ' + (x.met ? 'ok' : 'bad'), text: x.met ? '✓ pass' : '✗ fail' }), h('td', { class: 'detail', text: x.detail })))))));
    kids.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Policies' })), goalList(r.policies.map(p => ({ met: p.met, text: p.text, value: p.met ? 'holds' : p.violations.slice(0, 2).join('; ') + (p.violations.length > 2 ? ` (+${p.violations.length - 2})` : '') }))));
    const shown = r.outputs.filter(o => o.rows);
    shown.forEach(o => {
      const body = o.rows.length
        ? o.rows.map(g => h('tr', null, h('td', { text: g.principal }), h('td', { text: g.privilege }), h('td', { text: g.object_type }), h('td', { text: g.object_key })))
        : [h('tr', null, h('td', { class: 'dim', text: 'no grants' }))];
      kids.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: o.sql })));
      kids.push(h('div', { class: 'table-wrap' }, h('table', { class: 'tbl' }, h('thead', null, h('tr', null, ['principal', 'privilege', 'object_type', 'object_key'].map(x => h('th', { text: x })))), h('tbody', null, body))));
    });
    out.replaceChildren(...kids);
    report(r.ok);
  }

  /* ================= Grant: Asset Bundles ================= */
  function renderBundle(c) {
    const targets = Array.from(new Set(c.checks.map(x => x.target).filter(Boolean)));
    let target = targets[0] || null;
    const ta = h('textarea', { id: 'editor', spellcheck: 'false', 'aria-label': 'databricks.yml', style: 'min-height:440px' });
    ta.value = store.d.text[c.id] != null ? store.d.text[c.id] : c.starter;
    const outBox = h('div'), checksBox = h('div');
    const seg = h('div', { class: 'seg', role: 'group', 'aria-label': 'Target' }, (targets.length ? targets : ['default']).map(tg => h('button', { type: 'button', 'aria-pressed': String(tg === (target || 'default')), text: `-t ${tg}`, onclick: e => { target = tg === 'default' ? null : tg; seg.querySelectorAll('button').forEach(b => b.setAttribute('aria-pressed', String(b === e.target))); update(); } })));
    let timer;
    const update = () => {
      store.d.text[c.id] = ta.value; store.save();
      const r = SSG.grantLab.check(c, ta.value);
      checksBox.replaceChildren(goalList(r.checks.map(x => ({ met: x.met, text: x.text, value: x.target ? `-t ${x.target}` : '' }))));
      const v = SSG.bundles.validate(ta.value, { target });
      outBox.replaceChildren(...bundleOut(v));
      report(r.ok);
    };
    ta.addEventListener('input', () => { clearTimeout(timer); timer = setTimeout(update, 250); });
    ta.addEventListener('keydown', e => { if (e.key === 'Tab') { e.preventDefault(); ta.setRangeText('  ', ta.selectionStart, ta.selectionEnd, 'end'); } });
    $('#workspace').replaceChildren(h('div', { class: 'cols', style: 'grid-template-columns:minmax(0,1.1fr) minmax(0,1fr)' },
      h('div', { class: 'card editor' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'databricks.yml' }), h('button', { type: 'button', class: 'btn quiet', text: 'Reset', onclick: () => { delete store.d.text[c.id]; store.save(); renderBundle(c); } })), ta),
      h('div', { style: 'display:grid;gap:16px;min-width:0' },
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'Checks' })), checksBox),
        h('div', { class: 'card' }, h('div', { class: 'panel-h' }, h('span', { class: 'label', text: 'databricks bundle validate' }), seg), outBox))));
    update();
  }
  function bundleOut(v) {
    const kids = [];
    const issues = v.errors.map(e => ({ level: 'error', e })).concat(v.warnings.map(e => ({ level: 'warning', e })));
    kids.push(issues.length ? h('ul', { class: 'issues' }, issues.map(({ level, e }) => h('li', { class: level }, h('span', { html: `<b>${level === 'error' ? 'Error' : 'Warning'}:</b> ${esc(e.msg)}` }), e.path ? h('code', { text: e.path }) : null))) : h('ul', { class: 'issues' }, h('li', { class: '', style: 'background:var(--ok-bg)', text: 'Validation OK!' })));
    if (v.plan) {
      const p = v.plan;
      kids.push(h('div', { class: 'panel-h' }, h('span', { class: 'label', text: `Deploy plan · target ${p.target || 'default'}${p.mode ? ` · ${p.mode} mode` : ''}` })),
        h('div', { class: 'plan' }, h('p', { style: 'margin:0;color:var(--ink-2);font-size:13px', html: `Files deploy to <code>${esc(p.rootPath)}</code>. Jobs run as <b>${esc(p.runAs)}</b>.` }),
          p.resources.map(rs => h('div', { class: 'res-card' }, h('h4', null, rs.name, h('small', { text: `${rs.type} · ${rs.key}` })),
            rs.type === 'pipeline' ? h('p', { html: `Publishes to <code>${esc(rs.catalog || 'hive_metastore')}.${esc(rs.schema || '?')}</code>${rs.development ? ' · development mode' : ''}${rs.serverless ? ' · serverless' : ''}` }) : null,
            rs.type === 'job' && rs.schedule ? h('p', { html: `Schedule <code>${esc(rs.schedule.quartz_cron_expression || '')}</code> ${rs.schedule.pause_status === 'PAUSED' ? '· <b>paused</b>' : '· active'}` }) : null,
            rs.type === 'job' ? h('div', { class: 'dag' }, rs.tasks.map(tk => h('span', { html: `${esc(tk.key)}${tk.dependsOn.length ? ` <em>after ${esc(tk.dependsOn.join(', '))}</em>` : ''}` }))) : null))));
    }
    return kids;
  }

  /* ---------- wiring ---------- */
  document.querySelectorAll('.lab-tab').forEach(t => t.addEventListener('click', () => {
    const l = t.dataset.lab, cs = LABS[l].mod.CHALLENGES;
    open((cs.find(c => !store.d.solved[c.id]) || cs[0]).id);
  }));
  window.addEventListener('hashchange', () => { const id = location.hash.split('/')[1]; if (id && (!current || id !== current.id)) open(id); });
  const startId = location.hash.split('/')[1];
  open(ALL.some(c => c.id === startId) ? startId : (ALL.find(c => !store.d.solved[c.id]) || ALL[0]).id);
  window.ssg = { open, store };
})();
