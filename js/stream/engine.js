/* stream/engine.js: a micro-batch simulator that follows Spark Structured Streaming semantics.
   Events have an event time (when it happened) and an arrival time (when the stream sees it).
   Triggers group arrivals into micro-batches. The watermark is max(event time seen) − delay,
   computed at the end of each batch and used from the next one; it never moves backwards.
   When the watermark advances past state that can be finalized, a no-data batch runs at the
   same trigger time (spark.sql.streaming.noDataMicroBatches.enabled, on by default). */
(function (root) {
  'use strict';
  const MIN = 60000;

  class StreamError extends Error { constructor(cls, msg) { super(`[${cls}] ${msg}`); this.cls = cls; } }

  /* Windows a timestamp belongs to: [start, start + size), every `slide`. */
  function windowsFor(t, size, slide) {
    const out = [];
    const last = Math.floor(t / slide) * slide;
    for (let s = last; s > t - size; s -= slide) out.push({ start: s, end: s + size });
    return out.reverse();
  }

  function validate(cfg) {
    if (cfg.query === 'window_count') {
      if (cfg.outputMode === 'append' && cfg.watermarkMs == null)
        throw new StreamError('STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION', 'Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark. Add withWatermark(), or use update or complete mode.');
      if (!(cfg.window && cfg.window.size > 0)) throw new StreamError('INVALID_WINDOW', 'The window duration must be positive.');
      if (cfg.window.slide && cfg.window.slide > cfg.window.size) throw new StreamError('INVALID_WINDOW', 'The slide duration must be less than or equal to the window duration.');
    }
    if (cfg.query === 'dedupe' && cfg.outputMode === 'complete')
      throw new StreamError('STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION', 'Complete output mode not supported when there are no streaming aggregations on streaming DataFrames/Datasets.');
    if (cfg.dedupe === 'withinWatermark' && cfg.watermarkMs == null)
      throw new StreamError('STREAMING_DEDUPLICATION', 'dropDuplicatesWithinWatermark() requires a watermark: add withWatermark() first.');
  }

  /* Batches: fixed processing-time triggers, or availableNow (everything that has arrived, then stop). */
  function plan(events, cfg) {
    const sorted = events.slice().sort((a, b) => a.arrival - b.arrival || a.seq - b.seq);
    if (cfg.trigger === 'availableNow') {
      const at = cfg.runAt != null ? cfg.runAt : sorted.length ? sorted[sorted.length - 1].arrival : 0;
      const avail = sorted.filter(e => e.arrival <= at);
      const per = cfg.maxPerTrigger || Infinity;
      const batches = [];
      for (let i = 0; i < avail.length; i += per) batches.push({ time: at, rows: avail.slice(i, i + per) });
      return { batches, stoppedAt: at };
    }
    const step = cfg.trigger;
    if (!(step > 0)) throw new StreamError('INVALID_TRIGGER', 'The processing-time trigger interval must be positive.');
    const until = cfg.until != null ? cfg.until : sorted.length ? sorted[sorted.length - 1].arrival + step : step;
    const batches = [];
    let i = 0;
    for (let t = step; t <= until + step - 1; t += step) {
      const rows = [];
      while (i < sorted.length && sorted[i].arrival <= t) rows.push(sorted[i++]);
      batches.push({ time: t, rows });
    }
    return { batches };
  }

  /* ---------- windowed aggregation ---------- */
  function runWindowCount(events, cfg) {
    const size = cfg.window.size, slide = cfg.window.slide || size;
    const wm = cfg.watermarkMs;
    const state = new Map(); // window start -> {start, end, count, firstSeen, changed}
    const emitted = [];      // {batch, time, window, count, final}
    const sink = new Map();  // window start -> count, as the table downstream sees it
    const log = [];
    let watermark = -Infinity, maxEt = -Infinity, dropped = [], maxState = 0, batchNo = 0;
    const doEmit = (time, mode, extra) => {
      if (mode === 'append') {
        const done = Array.from(state.values()).filter(w => w.end <= watermark).sort((a, b) => a.start - b.start);
        done.forEach(w => { emitted.push({ batch: batchNo, time, start: w.start, end: w.end, count: w.count, final: true }); sink.set(w.start, w.count); state.delete(w.start); });
        return done.length;
      }
      if (mode === 'update') {
        const ch = Array.from(state.values()).filter(w => w.changed).sort((a, b) => a.start - b.start);
        ch.forEach(w => { emitted.push({ batch: batchNo, time, start: w.start, end: w.end, count: w.count, final: wm != null && w.end <= watermark }); sink.set(w.start, w.count); w.changed = false; });
        if (wm != null) Array.from(state.values()).filter(w => w.end <= watermark).forEach(w => state.delete(w.start));
        return ch.length + (extra || 0);
      }
      const all = Array.from(state.values()).sort((a, b) => a.start - b.start);
      sink.clear();
      all.forEach(w => { emitted.push({ batch: batchNo, time, start: w.start, end: w.end, count: w.count, final: false }); sink.set(w.start, w.count); w.changed = false; });
      return all.length;
    };
    const { batches } = plan(events, cfg);
    for (const b of batches) {
      if (!b.rows.length) {
        // no-data batch only if the watermark can finalize something
        if (wm != null && cfg.outputMode !== 'complete' && Array.from(state.values()).some(w => w.end <= watermark)) {
          batchNo++;
          const n = doEmit(b.time, cfg.outputMode);
          log.push({ batch: batchNo, time: b.time, kind: 'no-data', input: 0, late: 0, watermark, state: state.size, emitted: n });
        }
        continue;
      }
      batchNo++;
      let late = 0;
      for (const e of b.rows) {
        maxEt = Math.max(maxEt, e.eventTime);
        const wins = windowsFor(e.eventTime, size, slide);
        let used = 0;
        for (const w of wins) {
          if (wm != null && cfg.outputMode !== 'complete' && w.end <= watermark) continue; // window already finalized
          let s = state.get(w.start);
          if (!s) { s = { start: w.start, end: w.end, count: 0, changed: false }; state.set(w.start, s); }
          s.count++; s.changed = true; used++;
        }
        if (!used) { late++; dropped.push(e); }
      }
      maxState = Math.max(maxState, state.size);
      const n = doEmit(b.time, cfg.outputMode);
      const before = watermark;
      if (wm != null) watermark = Math.max(watermark, maxEt - wm);
      log.push({ batch: batchNo, time: b.time, kind: 'data', input: b.rows.length, late, watermark: before, nextWatermark: watermark, state: state.size, emitted: n });
      // the watermark moved: a no-data batch finalizes immediately (same trigger time)
      if (wm != null && watermark > before && cfg.outputMode === 'append' && Array.from(state.values()).some(w => w.end <= watermark)) {
        batchNo++;
        const m = doEmit(b.time, 'append');
        log.push({ batch: batchNo, time: b.time, kind: 'no-data', input: 0, late: 0, watermark, state: state.size, emitted: m });
      } else if (wm != null && watermark > before && cfg.outputMode === 'update') {
        Array.from(state.values()).filter(w => w.end <= watermark && !w.changed).forEach(w => state.delete(w.start));
      }
    }
    return { emitted, sink, log, dropped, maxState, finalState: state.size, batches: batchNo };
  }

  /* ---------- deduplication ---------- */
  function runDedupe(events, cfg) {
    const wm = cfg.watermarkMs;
    const seen = new Map(); // key -> expiry event time (or Infinity)
    const out = [], log = [], dropped = [];
    let watermark = -Infinity, maxEt = -Infinity, maxState = 0, batchNo = 0;
    const { batches } = plan(events, cfg);
    for (const b of batches) {
      if (!b.rows.length) continue;
      batchNo++;
      let late = 0, dups = 0, passed = 0;
      for (const e of b.rows) {
        maxEt = Math.max(maxEt, e.eventTime);
        if (wm != null && cfg.dedupe !== 'none' && e.eventTime <= watermark) { late++; dropped.push(e); continue; }
        let key = null;
        if (cfg.dedupe === 'dropDuplicates') key = cfg.dedupeKeys === 'id+time' ? `${e.id}|${e.eventTime}` : String(e.id);
        else if (cfg.dedupe === 'withinWatermark') key = String(e.id);
        if (key != null) {
          const exp = seen.get(key);
          if (exp != null) { dups++; continue; }
          const expiry = cfg.dedupe === 'withinWatermark' ? e.eventTime + wm : cfg.dedupeKeys === 'id+time' && wm != null ? e.eventTime : Infinity;
          seen.set(key, expiry);
        }
        out.push({ batch: batchNo, time: b.time, id: e.id, eventTime: e.eventTime, value: e.value });
        passed++;
      }
      const before = watermark;
      if (wm != null) watermark = Math.max(watermark, maxEt - wm);
      for (const [k, exp] of seen) if (exp !== Infinity && exp < watermark) seen.delete(k);
      maxState = Math.max(maxState, seen.size);
      log.push({ batch: batchNo, time: b.time, kind: 'data', input: b.rows.length, late, duplicates: dups, emitted: passed, watermark: before, nextWatermark: watermark, state: seen.size });
    }
    return { emitted: out, log, dropped, maxState, finalState: seen.size, batches: batchNo };
  }

  /* ---------- change data capture into a target table (what AUTO CDC does) ---------- */
  /* With sequenceBy 'seq', changes are ordered by the source sequence column, so the result is
     the same whatever order they arrive in (AUTO CDC ... SEQUENCE BY). With 'arrival', the last
     change to arrive wins: what a naive MERGE per batch does. */
  function runCdc(events, cfg) {
    const { batches } = plan(events, cfg);
    const seen = [], log = [];
    let batchNo = 0, outOfOrder = 0;
    const maxSeq = new Map();
    for (const b of batches) {
      if (!b.rows.length) continue;
      batchNo++;
      b.rows.forEach(r => { if (r.seq < (maxSeq.get(r.key) || -Infinity)) outOfOrder++; maxSeq.set(r.key, Math.max(maxSeq.get(r.key) || -Infinity, r.seq)); seen.push(r); });
      log.push({ batch: batchNo, time: b.time, kind: 'data', input: b.rows.length });
    }
    const byKey = new Map();
    seen.forEach(r => { if (!byKey.has(r.key)) byKey.set(r.key, []); byKey.get(r.key).push(r); });
    const target = new Map(), history = [];
    for (const [key, list] of byKey) {
      const changes = cfg.sequenceBy === 'seq' ? list.slice().sort((a, b) => a.seq - b.seq) : list;
      let cur = null, open = null;
      for (const r of changes) {
        if (r.op === 'DELETE') {
          if (!cfg.applyDeletes) continue;
          cur = null;
          if (open) { open.end = r.seq; open = null; }
          continue;
        }
        cur = { value: r.value, seq: r.seq };
        if (!open || open.value !== r.value) {
          if (open) open.end = r.seq;
          open = { key, value: r.value, start: r.seq, end: null };
          history.push(open);
        }
      }
      if (cur) target.set(key, cur);
    }
    history.sort((x, y) => (x.key < y.key ? -1 : x.key > y.key ? 1 : x.start - y.start));
    return { target, history: cfg.scd === 2 ? history : [], log, outOfOrder, batches: batchNo };
  }

  /* ---------- Auto Loader schema evolution, with job retries ---------- */
  /* files: [{arrival, rows, columns:[...]}]. Modes follow cloudFiles.schemaEvolutionMode. */
  function runAutoLoader(files, cfg) {
    let schema = cfg.initialColumns.slice();
    const out = [], log = [];
    let attempts = 0, failures = 0, i = 0, stopped = null;
    const sorted = files.slice().sort((a, b) => a.arrival - b.arrival);
    while (i < sorted.length) {
      const f = sorted[i];
      const extra = f.columns.filter(c => !schema.includes(c));
      if (extra.length && (cfg.mode === 'addNewColumns' || cfg.mode === 'failOnNewColumns')) {
        failures++;
        log.push({ time: f.arrival, kind: 'failure', message: `[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE] Encountered unknown fields during parsing: ${extra.join(', ')}. The stream stops; ${cfg.mode === 'addNewColumns' ? 'restarting it picks up the new schema.' : 'the schema must be updated manually before it can continue.'}` });
        if (cfg.mode === 'addNewColumns') schema = schema.concat(extra);
        if (cfg.mode === 'failOnNewColumns' || attempts >= (cfg.retries || 0)) { stopped = f.arrival; break; }
        attempts++;
        log.push({ time: f.arrival, kind: 'restart', message: `The job retried (attempt ${attempts} of ${cfg.retries}).` });
        continue; // same file is processed again with the evolved schema
      }
      f.rows.forEach(r => {
        const row = {};
        schema.forEach(c => { row[c] = r[c] === undefined ? null : r[c]; });
        if (cfg.mode === 'rescue') { const rescued = {}; Object.keys(r).forEach(k => { if (!schema.includes(k)) rescued[k] = r[k]; }); row._rescued_data = Object.keys(rescued).length ? JSON.stringify(rescued) : null; }
        out.push(row);
      });
      log.push({ time: f.arrival, kind: 'batch', message: `Ingested ${f.rows.length} rows from ${f.name}.`, rows: f.rows.length });
      i++;
    }
    return { rows: out, schema: cfg.mode === 'rescue' ? schema.concat('_rescued_data') : schema, log, failures, stopped, remaining: sorted.length - i };
  }

  /* ---------- checkpoints and restarts into a sink ---------- */
  /* An append-only sink receives each file's rows. A restart with the same checkpoint resumes;
     a new (or deleted) checkpoint reprocesses every file. MERGE into the sink by key is idempotent. */
  function runRestart(files, cfg) {
    const processed = new Set();
    const sink = [];
    const merged = new Map();
    const log = [];
    const timeline = files.map(f => ({ t: f.arrival, kind: 'file', f })).concat((cfg.restarts || []).map(t => ({ t, kind: 'restart' }))).sort((a, b) => a.t - b.t || (a.kind === 'restart' ? 1 : -1));
    const arrived = [];
    const process = (f, t) => {
      f.rows.forEach(r => { if (cfg.sinkWrite === 'merge') merged.set(r.id, r); else sink.push(r); });
      processed.add(f.name);
      log.push({ time: t, kind: 'batch', message: `Processed ${f.name} (${f.rows.length} rows).` });
    };
    for (const ev of timeline) {
      if (ev.kind === 'file') { arrived.push(ev.f); process(ev.f, ev.t); continue; }
      if (cfg.checkpoint === 'new') {
        processed.clear();
        log.push({ time: ev.t, kind: 'restart', message: 'Restarted with a new checkpoint location: the stream has no memory of what it processed.' });
        arrived.forEach(f => process(f, ev.t));
      } else log.push({ time: ev.t, kind: 'restart', message: 'Restarted from the existing checkpoint: resumes after the last committed batch.' });
    }
    const rows = cfg.sinkWrite === 'merge' ? Array.from(merged.values()) : sink;
    return { rows, log, duplicates: rows.length - new Set(rows.map(r => r.id)).size };
  }

  function run(scenario, cfg) {
    const c = Object.assign({}, scenario.base || {}, cfg);
    if (scenario.kind === 'autoloader') return runAutoLoader(scenario.files, c);
    if (scenario.kind === 'restart') return runRestart(scenario.files, c);
    validate(c);
    if (c.query === 'window_count') return runWindowCount(scenario.events, c);
    if (c.query === 'dedupe') return runDedupe(scenario.events, c);
    if (c.query === 'cdc') return runCdc(scenario.events, c);
    throw new StreamError('UNKNOWN_QUERY', c.query);
  }

  root.SSG = root.SSG || {};
  root.SSG.stream = { run, windowsFor, StreamError, MIN, runWindowCount, runDedupe, runCdc, runAutoLoader, runRestart };
})(typeof window !== 'undefined' ? window : global);
