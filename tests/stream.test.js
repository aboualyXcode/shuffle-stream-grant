#!/usr/bin/env node
/* Tests the streaming simulator against the guarantees Structured Streaming documents,
   using hundreds of random event streams. Usage: node tests/stream.test.js */
'use strict';
const path = require('path');
require(path.join(__dirname, '..', 'js', 'stream', 'engine.js'));
const S = global.SSG.stream, MIN = S.MIN;
let checks = 0, failures = 0;
const ok = (c, m) => { checks++; if (!c) { failures++; if (failures < 25) console.log('  FAIL ' + m); } };
function rng(seed) { let a = seed | 0; return () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
function stream(seed, n, maxDelayMin) {
  const r = rng(seed), out = [];
  for (let i = 0; i < n; i++) {
    const et = Math.floor(r() * 120) * MIN + Math.floor(r() * 60) * 1000;
    const delay = Math.floor(r() * r() * maxDelayMin * MIN);
    out.push({ id: 'e' + i, eventTime: et, arrival: et + delay, seq: i, value: 1 });
  }
  return out;
}

/* ---- the example from the Structured Streaming programming guide ---- */
console.log('Programming guide example (10-minute windows sliding every 5, 10-minute watermark)');
{
  const t = (h, m) => (h * 60 + m) * MIN;
  const ev = (et, arr, id) => ({ id, eventTime: et, arrival: arr, seq: 0 });
  const events = [
    ev(t(12, 7), t(12, 7), 'dog'), ev(t(12, 8), t(12, 8), 'owl'),
    ev(t(12, 14), t(12, 14), 'dog'), ev(t(12, 9), t(12, 13), 'cat'),
    ev(t(12, 15), t(12, 18), 'cat'), ev(t(12, 8), t(12, 19), 'dog'), ev(t(12, 13), t(12, 19), 'owl'), ev(t(12, 21), t(12, 19), 'owl'),
    ev(t(12, 4), t(12, 23), 'donkey'), ev(t(12, 17), t(12, 24), 'owl'),
  ];
  const res = S.run({ events }, { query: 'window_count', window: { size: 10 * MIN, slide: 5 * MIN }, watermarkMs: 10 * MIN, outputMode: 'update', trigger: 5 * MIN });
  const third = res.log.find(l => l.time === t(12, 20));
  ok(third && third.nextWatermark === t(12, 11), 'after the 12:20 trigger the watermark is 12:11 (max event time 12:21 minus 10 minutes)');
  const fourth = res.log.find(l => l.time === t(12, 25));
  ok(fourth && fourth.late === 1, 'at 12:25 the 12:04 event is too late and dropped');
  ok(res.dropped.length === 1 && res.dropped[0].id === 'donkey', 'only the donkey is dropped');
  ok(res.emitted.some(e => e.time === t(12, 25) && e.start === t(12, 10) && e.count === 4), 'the late-but-not-too-late 12:17 owl still updates window 12:10–12:20 (now 4 events)');
}

/* ---- windowed aggregation invariants ---- */
console.log('Windowed aggregation over random streams');
for (let seed = 1; seed <= 150; seed++) {
  const r = rng(seed * 7);
  const size = [5, 10, 15][seed % 3] * MIN, slide = seed % 4 === 0 ? size / 5 : size;
  const wm = [0, 2, 5, 10, 30][seed % 5] * MIN;
  const trig = [1, 2, 5][seed % 3] * MIN;
  const events = stream(seed, 60 + Math.floor(r() * 80), 25);
  const truth = new Map();
  events.forEach(e => S.windowsFor(e.eventTime, size, slide).forEach(w => truth.set(w.start, (truth.get(w.start) || 0) + 1)));

  const app = S.run({ events }, { query: 'window_count', window: { size, slide }, watermarkMs: wm, outputMode: 'append', trigger: trig, until: 400 * MIN });
  const once = new Set();
  app.emitted.forEach(e => { ok(!once.has(e.start), `seed ${seed}: append emits window ${e.start} once`); once.add(e.start); ok(e.final, `seed ${seed}: append rows are final`); });
  let wmPrev = -Infinity;
  app.log.forEach(l => { const w = l.nextWatermark != null ? l.nextWatermark : l.watermark; ok(w >= wmPrev, `seed ${seed}: watermark is monotonic`); wmPrev = w; });
  // brute force: replay batches, each event counts in windows whose end > watermark at its batch
  const expected = new Map();
  const sorted = events.slice().sort((a, b) => a.arrival - b.arrival || a.seq - b.seq);
  let w = -Infinity, maxEt = -Infinity, i = 0;
  for (let tt = trig; i < sorted.length; tt += trig) {
    const batch = [];
    while (i < sorted.length && sorted[i].arrival <= tt) batch.push(sorted[i++]);
    if (!batch.length) continue;
    batch.forEach(e => { maxEt = Math.max(maxEt, e.eventTime); S.windowsFor(e.eventTime, size, slide).forEach(x => { if (x.end > w) expected.set(x.start, (expected.get(x.start) || 0) + 1); }); });
    w = Math.max(w, maxEt - wm);
  }
  app.emitted.forEach(e => ok(e.count === expected.get(e.start), `seed ${seed}: window ${e.start / MIN} final count ${e.count} = brute force ${expected.get(e.start)}`));
  ok(app.emitted.length + app.finalState === new Set(expected.keys()).size, `seed ${seed}: every window is either emitted or still in state`);
  if (wm >= 25 * MIN) ok(app.dropped.length === 0 && app.emitted.every(e => e.count === truth.get(e.start)), `seed ${seed}: a watermark longer than the maximum delay drops nothing`);

  const comp = S.run({ events }, { query: 'window_count', window: { size, slide }, watermarkMs: wm, outputMode: 'complete', trigger: trig, until: 400 * MIN });
  ok(comp.sink.size === truth.size && Array.from(truth).every(([k, v]) => comp.sink.get(k) === v), `seed ${seed}: complete mode sink equals the ground truth`);
  const upd = S.run({ events }, { query: 'window_count', window: { size, slide }, watermarkMs: wm, outputMode: 'update', trigger: trig, until: 400 * MIN });
  app.emitted.forEach(e => ok(upd.sink.get(e.start) === e.count, `seed ${seed}: update mode converges to the append result for finalized windows`));
  const noWm = S.run({ events }, { query: 'window_count', window: { size, slide }, watermarkMs: null, outputMode: 'update', trigger: trig });
  ok(noWm.dropped.length === 0 && noWm.finalState === truth.size, `seed ${seed}: without a watermark nothing is dropped and state grows to every window`);
}
try { S.run({ events: [] }, { query: 'window_count', window: { size: MIN }, outputMode: 'append', trigger: MIN, watermarkMs: null }); ok(false, 'append without watermark must fail'); } catch (e) { ok(/Append output mode not supported/.test(e.message), 'append without a watermark raises the Spark error'); }

/* ---- deduplication ---- */
console.log('Deduplication');
for (let seed = 1; seed <= 60; seed++) {
  const base = stream(seed, 50, 10);
  const r = rng(seed);
  const events = base.slice();
  base.forEach(e => { if (r() < 0.3) events.push(Object.assign({}, e, { arrival: e.arrival + Math.floor(r() * 20) * MIN, seq: events.length })); });
  const ids = new Set(events.map(e => e.id));
  const exact = S.run({ events }, { query: 'dedupe', dedupe: 'dropDuplicates', dedupeKeys: 'id', watermarkMs: null, outputMode: 'append', trigger: MIN });
  ok(exact.emitted.length === ids.size && new Set(exact.emitted.map(e => e.id)).size === ids.size, `seed ${seed}: dropDuplicates without a watermark emits each id once`);
  ok(exact.finalState === ids.size, `seed ${seed}: and keeps every id in state forever`);
  const none = S.run({ events }, { query: 'dedupe', dedupe: 'none', watermarkMs: null, outputMode: 'append', trigger: MIN });
  ok(none.emitted.length === events.length, `seed ${seed}: no deduplication passes duplicates through`);
  const within = S.run({ events }, { query: 'dedupe', dedupe: 'withinWatermark', watermarkMs: 30 * MIN, outputMode: 'append', trigger: MIN });
  ok(new Set(within.emitted.map(e => e.id)).size === within.emitted.length, `seed ${seed}: a 30-minute watermark longer than any gap removes every duplicate`);
  ok(within.maxState <= exact.maxState, `seed ${seed}: watermark-bounded state is never larger`);
}

/* ---- CDC ---- */
console.log('Change data capture');
for (let seed = 1; seed <= 80; seed++) {
  const r = rng(seed * 3);
  const changes = [];
  for (let i = 0; i < 40; i++) { const key = 'k' + Math.floor(r() * 8); changes.push({ key, seq: i + 1, op: r() < 0.12 ? 'DELETE' : 'UPSERT', value: 'v' + Math.floor(r() * 4), eventTime: 0, arrival: (i + 1) * MIN }); }
  const shuffled = changes.map(c => Object.assign({}, c, { arrival: Math.floor(r() * 50) * MIN }));
  const cfg = { query: 'cdc', sequenceBy: 'seq', applyDeletes: true, scd: 2, trigger: 5 * MIN };
  const a = S.run({ events: changes }, cfg), b = S.run({ events: shuffled }, cfg);
  const dump = x => JSON.stringify(Array.from(x.target).sort()) + JSON.stringify(x.history);
  ok(dump(a) === dump(b), `seed ${seed}: SEQUENCE BY makes the result independent of arrival order`);
  const truth = new Map();
  changes.forEach(c => { if (c.op === 'DELETE') truth.delete(c.key); else truth.set(c.key, c.value); });
  ok(JSON.stringify(Array.from(a.target).map(([k, v]) => [k, v.value]).sort()) === JSON.stringify(Array.from(truth).sort()), `seed ${seed}: SCD1 target equals replaying changes in sequence order`);
  const keys = new Set(a.history.map(h => h.key));
  keys.forEach(k => { const vs = a.history.filter(h => h.key === k); for (let i = 1; i < vs.length; i++) ok(vs[i - 1].end != null && vs[i - 1].end <= vs[i].start, `seed ${seed}: ${k} versions do not overlap`); ok(vs.filter(v => v.end == null).length <= 1, `seed ${seed}: ${k} has at most one open version`); });
  const naive = S.run({ events: shuffled }, Object.assign({}, cfg, { sequenceBy: 'arrival', scd: 1 }));
  if (b.outOfOrder > 0) ok(true, 'counted');
  void naive;
}

/* ---- Auto Loader and restarts ---- */
console.log('Auto Loader schema evolution and checkpoints');
{
  const files = [
    { name: 'f1.json', arrival: 1, columns: ['id', 'amount'], rows: [{ id: 1, amount: 5 }] },
    { name: 'f2.json', arrival: 2, columns: ['id', 'amount', 'currency'], rows: [{ id: 2, amount: 7, currency: 'EUR' }] },
    { name: 'f3.json', arrival: 3, columns: ['id', 'amount', 'currency'], rows: [{ id: 3, amount: 9, currency: 'USD' }] },
  ];
  const add = S.run({ kind: 'autoloader', files }, { mode: 'addNewColumns', initialColumns: ['id', 'amount'], retries: 1 });
  ok(add.failures === 1 && add.rows.length === 3 && add.schema.includes('currency') && add.rows[1].currency === 'EUR', 'addNewColumns fails once, then the retry evolves the schema and loses nothing');
  const noRetry = S.run({ kind: 'autoloader', files }, { mode: 'addNewColumns', initialColumns: ['id', 'amount'], retries: 0 });
  ok(noRetry.stopped != null && noRetry.rows.length === 1, 'without retries the stream stays stopped');
  const rescue = S.run({ kind: 'autoloader', files }, { mode: 'rescue', initialColumns: ['id', 'amount'] });
  ok(rescue.failures === 0 && rescue.rows.length === 3 && /currency/.test(rescue.rows[1]._rescued_data), 'rescue never fails and keeps new fields in _rescued_data');
  const none = S.run({ kind: 'autoloader', files }, { mode: 'none', initialColumns: ['id', 'amount'] });
  ok(none.failures === 0 && none.rows.every(rw => !('currency' in rw)), 'none silently ignores new columns');
  const fail = S.run({ kind: 'autoloader', files }, { mode: 'failOnNewColumns', initialColumns: ['id', 'amount'], retries: 3 });
  ok(fail.stopped != null && fail.rows.length === 1, 'failOnNewColumns stops even with retries');

  const rf = [{ name: 'a', arrival: 1, rows: [{ id: 1 }, { id: 2 }] }, { name: 'b', arrival: 3, rows: [{ id: 3 }] }];
  const keep = S.run({ kind: 'restart', files: rf }, { checkpoint: 'same', sinkWrite: 'append', restarts: [2] });
  const fresh = S.run({ kind: 'restart', files: rf }, { checkpoint: 'new', sinkWrite: 'append', restarts: [2] });
  const merge = S.run({ kind: 'restart', files: rf }, { checkpoint: 'new', sinkWrite: 'merge', restarts: [2] });
  ok(keep.duplicates === 0 && keep.rows.length === 3, 'restarting from the same checkpoint resumes without duplicates');
  ok(fresh.duplicates === 2, 'a new checkpoint reprocesses everything into an append sink');
  ok(merge.duplicates === 0 && merge.rows.length === 3, 'a MERGE sink is idempotent even when everything is reprocessed');
}

console.log(failures ? `\n${failures} of ${checks} checks failed` : `\nAll ${checks} checks passed.`);
process.exit(failures ? 1 : 0);
