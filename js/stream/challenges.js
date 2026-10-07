/* stream/challenges.js: streaming challenges. Each generates a deterministic event stream,
   exposes the stream settings you may change, and checks goals against ground truth computed
   independently of the simulator. */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};
  const MIN = 60000;
  function rng(seed) { let a = seed | 0; return () => { a = (a + 0x6D2B79F5) | 0; let t = Math.imul(a ^ (a >>> 15), 1 | a); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }

  /* events spread over `minutes`; delay(r) returns the arrival delay in minutes */
  function events(seed, n, minutes, delay) {
    const r = rng(seed), out = [];
    for (let i = 0; i < n; i++) {
      const et = Math.floor(r() * minutes * 60) * 1000;
      out.push({ id: `e${i}`, eventTime: et, arrival: et + Math.round(delay(r) * MIN), seq: i, value: 1 });
    }
    return out;
  }
  const truthCounts = (evs, size) => { const m = new Map(); evs.forEach(e => { const s = Math.floor(e.eventTime / size) * size; m.set(s, (m.get(s) || 0) + 1); }); return m; };

  const KNOBS = {
    watermarkMs: { label: 'Watermark delay', code: v => (v == null ? '' : `.withWatermark("event_time", "${v / MIN} minutes")`), show: v => (v == null ? 'none' : `${v / MIN} min`) },
    outputMode: { label: 'Output mode', options: ['append', 'update', 'complete'], code: v => `.outputMode("${v}")` },
    dedupe: { label: 'Deduplication', options: ['none', 'dropDuplicates', 'withinWatermark'], show: v => ({ none: 'none', dropDuplicates: 'dropDuplicates("payment_id")', withinWatermark: 'dropDuplicatesWithinWatermark("payment_id")' })[v] },
    sequenceBy: { label: 'Order changes by', options: ['arrival', 'seq'], show: v => (v === 'seq' ? 'SEQUENCE BY change_seq' : 'arrival (last write wins)') },
    applyDeletes: { label: 'Deletes', options: [false, true], show: v => (v ? "APPLY AS DELETE WHEN op = 'DELETE'" : 'ignored') },
    scd: { label: 'Stored as', options: [1, 2], show: v => `SCD TYPE ${v}` },
    mode: { label: 'cloudFiles.schemaEvolutionMode', options: ['addNewColumns', 'rescue', 'failOnNewColumns', 'none'] },
    retries: { label: 'Job retries on failure', options: [0, 1, 3], show: v => String(v) },
    checkpoint: { label: 'Checkpoint on redeploy', options: ['new', 'same'], show: v => (v === 'new' ? 'new path per release' : 'same path') },
    sinkWrite: { label: 'Sink write', options: ['append', 'merge'], show: v => (v === 'merge' ? 'foreachBatch MERGE on id' : 'append') },
  };

  const CH = [];
  const add = c => CH.push(Object.assign({ lab: 'stream', hints: [] }, c));

  /* ---- windowed counts ---- */
  const kiosk = events(11, 400, 120, r => r() * 2);
  add({
    id: 'append-needs-watermark', level: 1, title: 'Append mode will not start',
    scenario: 'Kiosk scans are counted per 5-minute window into an append-only Delta table that finance reads. The stream fails at startup. Scans reach the stream at most 2 minutes after they happen.',
    task: 'Start the stream with <b>append</b> output, write each window <b>once with its final count</b>, and drop <b>no</b> scans.',
    scenarioData: { events: kiosk }, base: { query: 'window_count', window: { size: 5 * MIN }, trigger: MIN, until: 180 * MIN },
    baseline: { outputMode: 'append', watermarkMs: null }, knobs: ['watermarkMs', 'outputMode'],
    options: { watermarkMs: [null, MIN / 2, 2 * MIN, 5 * MIN, 30 * MIN] },
    goals: [{ id: 'runs', text: 'The stream starts' }, { id: 'appendOnly', text: 'Each window is written once, as a final count' }, { id: 'noDrops', text: 'No scans are dropped as late' }, { id: 'accurate', text: 'Every written count is correct' }],
    solution: { outputMode: 'append', watermarkMs: 5 * MIN },
    hints: ['Read the error: append mode can only write a window once it knows the window is complete.', 'The watermark tells Spark how late data can be. Too short, and on-time-ish data is dropped.'],
    explain: 'In append mode a window is written exactly once, so Spark must know when no more data can arrive for it: that is the watermark, <code>max(event_time) − delay</code>. Without one, Spark refuses to start. Choose the delay from how late your data really is, plus some margin.',
  });
  const mobile = events(23, 500, 180, r => (r() < 0.12 ? 3 + r() * 9 : r() * 1.5));
  add({
    id: 'late-mobile-events', level: 2, title: 'Phones that go offline',
    scenario: 'App events are counted per 5-minute window. Phones buffer events while offline, so about 1 in 8 arrives up to 12 minutes late. With a 2-minute watermark, those are silently dropped and the counts come out low.',
    task: 'Count every event that is <b>at most 15 minutes late</b>, and publish each final window count <b>within 25 minutes</b> of the window closing.',
    scenarioData: { events: mobile }, base: { query: 'window_count', window: { size: 5 * MIN }, trigger: MIN, outputMode: 'append', until: 260 * MIN },
    baseline: { watermarkMs: 2 * MIN }, knobs: ['watermarkMs'],
    options: { watermarkMs: [2 * MIN, 5 * MIN, 10 * MIN, 15 * MIN, 20 * MIN, 30 * MIN, 60 * MIN] },
    goals: [{ id: 'runs', text: 'The stream runs' }, { id: 'noDropsWithin', within: 15 * MIN, text: 'No event at most 15 minutes late is dropped' }, { id: 'latency', max: 25 * MIN, text: 'Final counts appear within 25 minutes of the window end' }, { id: 'accurate', text: 'Every written count is correct' }],
    solution: { watermarkMs: 15 * MIN },
    hints: ['Check the batch log: how many events are dropped as late, and how late were they?', 'A longer watermark keeps windows open longer, which delays when they are written.'],
    explain: 'The watermark is a trade-off between completeness and latency. A window is finalized when the watermark passes its end, so a 15-minute watermark adds about 15 minutes of delay to every result, and anything later than that is dropped. Measure your lateness distribution and choose deliberately.',
  });
  const day = events(37, 900, 24 * 60, r => r() * 8);
  add({
    id: 'state-explosion', level: 2, title: 'State that never shrinks',
    scenario: 'A 24-hour stream counts events per 5-minute window in update mode with no watermark. The state store keeps every window ever seen, and the cluster runs out of memory every few days. Events arrive at most 8 minutes late.',
    task: 'Keep the state store to <b>at most 12 windows</b> while dropping <b>no event at most 10 minutes late</b>, with update output.',
    scenarioData: { events: day }, base: { query: 'window_count', window: { size: 5 * MIN }, trigger: MIN, outputMode: 'update', until: 24 * 60 * MIN + 20 * MIN },
    baseline: { watermarkMs: null }, knobs: ['watermarkMs'],
    options: { watermarkMs: [null, 2 * MIN, 10 * MIN, 60 * MIN, 180 * MIN] },
    goals: [{ id: 'runs', text: 'The stream runs' }, { id: 'state', max: 12, text: 'State never holds more than 12 windows' }, { id: 'noDropsWithin', within: 10 * MIN, text: 'No event at most 10 minutes late is dropped' }],
    solution: { watermarkMs: 10 * MIN },
    hints: ['Without a watermark, Spark can never know a window is finished, so it keeps all of them.', 'The state holds roughly (watermark delay ÷ window size) + 1 windows.'],
    explain: 'Every stateful operation needs a bound. The watermark lets Spark evict windows it will never update again; without it, state grows forever, even in update mode. State size ≈ windows still open = watermark ÷ slide, which is why a generous watermark costs memory.',
  });
  const store = events(41, 300, 120, r => r() * 3);
  add({
    id: 'live-dashboard', level: 3, title: 'A live dashboard, not a report',
    scenario: 'Store managers watch per-window sales on a live dashboard backed by a Delta table. Today the stream uses append mode with a 30-minute watermark, so every number appears half an hour late. Data arrives at most 3 minutes late.',
    task: 'Show each sale on the dashboard <b>within 2 minutes</b> of arrival, keep state to <b>at most 5 windows</b>, and have the table converge to <b>correct counts</b>.',
    scenarioData: { events: store }, base: { query: 'window_count', window: { size: 5 * MIN }, trigger: MIN, until: 170 * MIN },
    baseline: { outputMode: 'append', watermarkMs: 30 * MIN }, knobs: ['outputMode', 'watermarkMs'],
    options: { watermarkMs: [null, 5 * MIN, 30 * MIN] },
    goals: [{ id: 'runs', text: 'The stream runs' }, { id: 'reflect', max: 2 * MIN, text: 'Every sale is visible within 2 minutes of arrival' }, { id: 'state', max: 5, text: 'State never holds more than 5 windows' }, { id: 'converges', text: 'The dashboard table ends with correct counts' }],
    solution: { outputMode: 'update', watermarkMs: 5 * MIN },
    hints: ['Append mode only writes a window when it is final. Which mode writes changes every batch?', 'Complete mode is also immediate, but it never evicts state.'],
    explain: 'Update mode writes every changed window in every micro-batch, so dashboards see data within one trigger interval, and a MERGE sink keeps the table at the latest value. A watermark still matters: it bounds state. Complete mode rewrites everything every batch and never forgets, which is only viable for small, bounded key sets.',
  });

  /* ---- deduplication ---- */
  const pay = (() => { const base = events(53, 220, 120, r => (r() < 0.1 ? 2 + r() * 4 : r() * 0.8)); const r = rng(54); const out = base.slice(); base.forEach(e => { if (r() < 0.25) out.push(Object.assign({}, e, { arrival: e.arrival + Math.round((0.5 + r() * 3.5) * MIN), seq: out.length })); }); return out; })();
  add({
    id: 'payment-retries', level: 2, title: 'Charged twice by a retry',
    scenario: 'The payment gateway retries webhooks when it does not get a fast reply, so about a quarter of payments arrive twice, up to 4 minutes apart. During gateway backlogs, a payment can also arrive up to 6 minutes after it happened. Duplicates in the payments table mean customers are charged twice.',
    task: 'Write each payment <b>exactly once</b>, keep the deduplication state <b>under 100 keys</b>, and drop <b>no</b> legitimate payment.',
    scenarioData: { events: pay }, base: { query: 'dedupe', outputMode: 'append', trigger: MIN, dedupeKeys: 'id', until: 140 * MIN },
    baseline: { dedupe: 'none', watermarkMs: null }, knobs: ['dedupe', 'watermarkMs'],
    options: { watermarkMs: [null, MIN, 10 * MIN, 120 * MIN] },
    goals: [{ id: 'runs', text: 'The stream runs' }, { id: 'unique', text: 'Each payment is written once' }, { id: 'complete', text: 'Every payment is written' }, { id: 'dedupeState', max: 100, text: 'Deduplication state stays under 100 keys' }],
    solution: { dedupe: 'withinWatermark', watermarkMs: 10 * MIN },
    alt: null,
    hints: ['dropDuplicates without a watermark remembers every key forever.', 'dropDuplicatesWithinWatermark forgets a key once the watermark passes its event time plus the delay.', 'Rows older than the watermark are dropped as late, so the delay must cover how late a payment can legitimately arrive, not just the retry gap.'],
    explain: '<code>dropDuplicatesWithinWatermark</code> (Spark 3.5+, Databricks Runtime 13.3+) keeps each key only as long as duplicates can still arrive, so state stays bounded. The watermark has two jobs here: it expires deduplication state, and it drops rows older than itself as late. Too short, and legitimate late payments vanish; too long, and state grows. Size it from your real lateness.',
  });

  /* ---- CDC ---- */
  function cdcFeed(seed) {
    const r = rng(seed), out = [];
    const keys = ['C-1001', 'C-1002', 'C-1003', 'C-1004', 'C-1005', 'C-1006'];
    const cities = ['Berlin', 'Lisbon', 'Osaka', 'Cairo', 'Austin', 'Madrid'];
    let seq = 0;
    keys.forEach((k, i) => out.push({ key: k, op: 'UPSERT', value: cities[i], seq: ++seq }));
    for (let i = 0; i < 30; i++) { const k = keys[Math.floor(r() * keys.length)]; out.push({ key: k, op: r() < 0.12 ? 'DELETE' : 'UPSERT', value: cities[Math.floor(r() * cities.length)], seq: ++seq }); }
    // delivery reorders changes: some arrive several batches late
    return out.map((c, i) => Object.assign({}, c, { eventTime: 0, arrival: (i + (r() < 0.3 ? 6 + Math.floor(r() * 8) : 0)) * MIN }));
  }
  const feed = cdcFeed(61);
  const truthTarget = f => { const m = new Map(); f.slice().sort((a, b) => a.seq - b.seq).forEach(c => { if (c.op === 'DELETE') m.delete(c.key); else m.set(c.key, c.value); }); return m; };
  add({
    id: 'cdc-out-of-order', level: 2, title: 'The customer table is wrong',
    scenario: 'Customer changes arrive from the CRM\'s change feed, sometimes out of order, and include deletes. The pipeline applies them in arrival order and ignores deletes, so the customers table disagrees with the CRM.',
    task: 'Make the target table <b>match the CRM exactly</b>: latest value per customer, deleted customers gone.',
    scenarioData: { events: feed }, base: { query: 'cdc', trigger: 5 * MIN, scd: 1 },
    baseline: { sequenceBy: 'arrival', applyDeletes: false }, knobs: ['sequenceBy', 'applyDeletes'],
    goals: [{ id: 'runs', text: 'The pipeline runs' }, { id: 'targetMatches', text: 'The table matches the source' }],
    solution: { sequenceBy: 'seq', applyDeletes: true },
    hints: ['Look at a customer whose final value is wrong: which change arrived last, and which happened last?', 'AUTO CDC orders changes by SEQUENCE BY, not by arrival.'],
    explain: '<code>AUTO CDC … KEYS (customer_id) SEQUENCE BY change_seq APPLY AS DELETE WHEN op = \'DELETE\'</code> applies changes in source order whatever order they arrive in, and handles deletes. Without a sequence column, the last change to arrive wins, which breaks as soon as delivery reorders anything.',
  });
  add({
    id: 'cdc-history', level: 3, title: 'Where did they live in March?',
    scenario: 'Finance needs every customer\'s city history to attribute past revenue, from the same out-of-order change feed. Deletes must close a customer\'s history.',
    task: 'Store the <b>full history</b>: one version per change of city, in the right order, with deletes closing the last version.',
    scenarioData: { events: feed }, base: { query: 'cdc', trigger: 5 * MIN },
    baseline: { scd: 1, sequenceBy: 'arrival', applyDeletes: false }, knobs: ['scd', 'sequenceBy', 'applyDeletes'],
    goals: [{ id: 'runs', text: 'The pipeline runs' }, { id: 'historyMatches', text: 'History matches the source, version by version' }],
    solution: { scd: 2, sequenceBy: 'seq', applyDeletes: true },
    hints: ['SCD Type 1 keeps only the latest value.', 'History is only right if versions are ordered by when changes happened.'],
    explain: '<code>STORED AS SCD TYPE 2</code> keeps a version per change with <code>__START_AT</code> and <code>__END_AT</code>, and with <code>SEQUENCE BY</code> it inserts late changes into the middle of history instead of appending them out of order.',
  });

  /* ---- Auto Loader and checkpoints ---- */
  const partnerFiles = [1, 2, 3, 4, 5, 6].map(i => ({ name: `orders_${i}.json`, arrival: i * 10 * MIN, columns: i >= 4 ? ['order_id', 'amount', 'currency'] : ['order_id', 'amount'], rows: [1, 2, 3].map(k => Object.assign({ order_id: i * 10 + k, amount: 10 * k }, i >= 4 ? { currency: k % 2 ? 'EUR' : 'USD' } : {})) }));
  add({
    id: 'schema-evolution', level: 2, title: 'A partner added a column',
    scenario: 'A partner started sending a <code>currency</code> field mid-day. The ingestion stream stopped, and the on-call engineer was paged at 3 a.m.',
    task: 'Ingest <b>every file</b> without manual intervention, and make <code>currency</code> available <b>as a column</b>.',
    scenarioData: { kind: 'autoloader', files: partnerFiles }, base: { initialColumns: ['order_id', 'amount'] },
    baseline: { mode: 'addNewColumns', retries: 0 }, knobs: ['mode', 'retries'],
    goals: [{ id: 'allFiles', text: 'Every file is ingested' }, { id: 'column', column: 'currency', text: 'currency is a column with its values' }],
    solution: { mode: 'addNewColumns', retries: 1 },
    hints: ['addNewColumns is the default: the stream stops on purpose when it sees a new column, and the restart picks it up.', 'rescue never stops, but where do the new values end up?'],
    explain: 'With <code>addNewColumns</code>, Auto Loader fails the stream once with <code>UNKNOWN_FIELD_EXCEPTION</code>, records the new schema, and continues after a restart, so production jobs should always have retries. <code>rescue</code> keeps unexpected fields in <code>_rescued_data</code> without stopping, which is right when you do not want the schema to change automatically.',
  });
  const relFiles = [1, 2, 3, 4].map(i => ({ name: `batch_${i}.json`, arrival: i * 10 * MIN, rows: [1, 2, 3].map(k => ({ id: `${i}-${k}` })) }));
  add({
    id: 'checkpoint-rotation', level: 2, title: 'Duplicates after every release',
    scenario: 'The deploy script gives the stream a new checkpoint path on each release. After each deploy, the target table contains every row again.',
    task: 'Survive <b>two redeploys</b> with <b>no duplicate rows</b> in the target.',
    scenarioData: { kind: 'restart', files: relFiles }, base: { restarts: [15 * MIN, 35 * MIN] },
    baseline: { checkpoint: 'new', sinkWrite: 'append' }, knobs: ['checkpoint', 'sinkWrite'],
    goals: [{ id: 'noDuplicates', text: 'No duplicate rows in the target' }, { id: 'allRows', count: 12, text: 'All 12 rows are present' }],
    solution: { checkpoint: 'same', sinkWrite: 'append' },
    alt: { checkpoint: 'new', sinkWrite: 'merge' },
    hints: ['The checkpoint is the stream\'s memory of what it has processed.', 'Even with a stable checkpoint, an idempotent sink protects you when a checkpoint is lost.'],
    explain: 'The checkpoint records which files and offsets were committed. Change it and the stream starts from scratch. Keep checkpoints stable across deploys, next to the table they serve. Writing with <code>foreachBatch</code> + MERGE on a key makes the sink idempotent too, which is the belt-and-braces production setup.',
  });

  /* ---------- checking ---------- */
  function lateness(e) { return e.arrival - e.eventTime; }
  function check(ch, cfg) {
    const c = Object.assign({}, ch.base, ch.baseline, cfg);
    let res, error = null;
    try { res = SSG.stream.run(Object.assign({}, ch.scenarioData, { base: {} }), c); }
    catch (e) { error = e; }
    const g = ch.goals.map(goal => {
      if (error) return Object.assign({ met: goal.id === 'runs' ? false : false, value: error.message }, goal);
      let met = true, value = '';
      const evs = ch.scenarioData.events;
      switch (goal.id) {
        case 'runs': met = true; value = 'running'; break;
        case 'appendOnly': { const seen = new Set(); met = c.outputMode === 'append' && res.emitted.every(x => { const ok = !seen.has(x.start) && x.final; seen.add(x.start); return ok; }); value = c.outputMode === 'append' ? `${res.emitted.length} windows written` : `${c.outputMode} mode rewrites windows`; break; }
        case 'noDrops': met = res.dropped.length === 0; value = `${res.dropped.length} dropped`; break;
        case 'noDropsWithin': { const bad = res.dropped.filter(e => lateness(e) <= goal.within); met = !bad.length; value = `${bad.length} of ${res.dropped.length} dropped events were within ${goal.within / MIN} min`; break; }
        case 'accurate': { const truth = truthCounts(evs, c.window.size); const wrong = res.emitted.filter(x => x.final && x.count !== truth.get(x.start)); met = !wrong.length; value = wrong.length ? `${wrong.length} final counts are low` : 'all final counts correct'; break; }
        case 'latency': { const lat = res.emitted.filter(x => x.final).map(x => x.time - x.end); const max = lat.length ? Math.max(...lat) : Infinity; met = max <= goal.max; value = `slowest window written ${Math.round(max / MIN)} min after closing`; break; }
        case 'state': met = res.maxState <= goal.max; value = `up to ${res.maxState} windows in state`; break;
        case 'dedupeState': met = res.maxState < goal.max; value = `up to ${res.maxState} keys in state`; break;
        case 'reflect': {
          const size = c.window.size, step = c.trigger;
          const dropped = new Set(res.dropped.map(e => e.id));
          let worst = 0;
          for (const e of evs) {
            if (dropped.has(e.id)) continue;
            const bt = Math.ceil(e.arrival / step) * step, start = Math.floor(e.eventTime / size) * size;
            const em = res.emitted.find(x => x.start === start && x.time >= bt);
            worst = Math.max(worst, em ? em.time - e.arrival : Infinity);
          }
          met = worst <= goal.max; value = worst === Infinity ? 'some sales are still not visible when the run ends' : `slowest sale visible after ${(worst / MIN).toFixed(1)} min`; break;
        }
        case 'converges': { const truth = truthCounts(evs, c.window.size); const bad = Array.from(truth).filter(([k, v]) => res.sink.get(k) !== v); met = !bad.length; value = bad.length ? `${bad.length} windows wrong in the table` : 'table matches'; break; }
        case 'unique': { const n = res.emitted.length, u = new Set(res.emitted.map(x => x.id)).size; met = n === u; value = `${n - u} duplicates written`; break; }
        case 'complete': { const ids = new Set(evs.map(e => e.id)); const got = new Set(res.emitted.map(x => x.id)); const miss = Array.from(ids).filter(i => !got.has(i)).length; met = !miss; value = `${miss} payments missing`; break; }
        case 'targetMatches': { const truth = truthTarget(evs); const t = res.target; const bad = Array.from(new Set([...truth.keys(), ...t.keys()])).filter(k => (truth.get(k) || null) !== (t.has(k) ? t.get(k).value : null)); met = !bad.length; value = bad.length ? `${bad.length} customers differ: ${bad.slice(0, 3).join(', ')}` : 'matches the source'; break; }
        case 'historyMatches': {
          const want = SSG.stream.runCdc(evs.map(e => Object.assign({}, e, { arrival: e.seq * MIN })), { sequenceBy: 'seq', applyDeletes: true, scd: 2, trigger: MIN }).history;
          met = c.scd === 2 && JSON.stringify(res.history) === JSON.stringify(want); value = c.scd !== 2 ? 'no history is kept' : met ? `${want.length} versions, all correct` : 'versions differ from the source'; break;
        }
        case 'allFiles': met = res.remaining === 0 && res.stopped == null; value = res.stopped != null ? `stopped with ${res.remaining} files waiting` : 'all files ingested'; break;
        case 'column': { met = res.schema.includes(goal.column) && res.rows.some(rw => rw[goal.column] != null); value = res.schema.includes(goal.column) ? (met ? `${goal.column} has values` : `${goal.column} exists but is empty`) : `${goal.column} is not a column`; break; }
        case 'noDuplicates': met = res.duplicates === 0; value = `${res.duplicates} duplicate rows`; break;
        case 'allRows': met = new Set(res.rows.map(r => r.id)).size === goal.count; value = `${new Set(res.rows.map(r => r.id)).size} distinct rows`; break;
        default: met = false;
      }
      return Object.assign({ met, value }, goal);
    });
    return { result: res, error, goals: g, ok: !error && g.every(x => x.met), cfg: c };
  }
  /* The stream as code, reflecting the current settings. */
  function code(ch, cfg) {
    const c = Object.assign({}, ch.base, ch.baseline, cfg);
    if (c.query === 'window_count') return `(spark.readStream.table("raw_events")\n   ${KNOBS.watermarkMs.code(c.watermarkMs) || '# no watermark'}\n   .groupBy(F.window("event_time", "${c.window.size / MIN} minutes"))\n   .count()\n .writeStream\n   ${KNOBS.outputMode.code(c.outputMode)}\n   .trigger(processingTime="${c.trigger / MIN} minute")\n   .option("checkpointLocation", "/Volumes/ops/streams/_checkpoints/counts")\n   .toTable("window_counts"))`;
    if (c.query === 'dedupe') return `(spark.readStream.table("payment_webhooks")\n   ${KNOBS.watermarkMs.code(c.watermarkMs) || '# no watermark'}\n   ${c.dedupe === 'none' ? '# no deduplication' : c.dedupe === 'dropDuplicates' ? '.dropDuplicates(["payment_id"])' : '.dropDuplicatesWithinWatermark(["payment_id"])'}\n .writeStream\n   .outputMode("append")\n   .option("checkpointLocation", "/Volumes/ops/streams/_checkpoints/payments")\n   .toTable("payments"))`;
    if (c.query === 'cdc') return `CREATE OR REFRESH STREAMING TABLE customers;\n\nCREATE FLOW customers_cdc AS AUTO CDC INTO customers\nFROM STREAM(crm_changes)\nKEYS (customer_id)\n${c.sequenceBy === 'seq' ? 'SEQUENCE BY change_seq' : '-- no SEQUENCE BY: changes apply in arrival order (what a hand-written MERGE per batch does)'}\n${c.applyDeletes ? "APPLY AS DELETE WHEN op = 'DELETE'\n" : ''}COLUMNS * EXCEPT (op, change_seq)\nSTORED AS SCD TYPE ${c.scd || 1};`;
    if (ch.scenarioData.kind === 'autoloader') return `(spark.readStream.format("cloudFiles")\n   .option("cloudFiles.format", "json")\n   .option("cloudFiles.schemaLocation", "/Volumes/ops/ingest/_schemas/partner_orders")\n   .option("cloudFiles.schemaEvolutionMode", "${c.mode}")\n   .load("/Volumes/ops/ingest/partner_orders")\n .writeStream\n   .option("checkpointLocation", "/Volumes/ops/ingest/_checkpoints/partner_orders")\n   .toTable("partner_orders"))\n\n# job settings: max_retries = ${c.retries}`;
    return `checkpoint = "/Volumes/ops/streams/_checkpoints/orders${c.checkpoint === 'new' ? '_" + release_id' : '"'}\n\n${c.sinkWrite === 'merge' ? 'def upsert(batch, batch_id):\n    (DeltaTable.forName(spark, "orders").alias("t")\n       .merge(batch.alias("s"), "t.id = s.id")\n       .whenNotMatchedInsertAll().execute())\n\n' : ''}(spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")\n   .load("/Volumes/ops/ingest/orders")\n .writeStream\n   .option("checkpointLocation", checkpoint)\n   ${c.sinkWrite === 'merge' ? '.foreachBatch(upsert)\n   .start())' : '.toTable("orders"))'}`;
  }

  SSG.streamLab = { KNOBS, CHALLENGES: CH, check, code, MIN };
})(typeof window !== 'undefined' ? window : global);
