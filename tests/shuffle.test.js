#!/usr/bin/env node
/* The Spark model's planning rules and cause-and-effect relationships. Usage: node tests/shuffle.test.js */
'use strict';
const path = require('path');
require(path.join(__dirname, '..', 'js', 'shuffle', 'model.js'));
const S = global.SSG.shuffle, GB = S.GB;
let checks = 0, failures = 0;
const ok = (c, m) => { checks++; if (!c) { failures++; console.log('  FAIL ' + m); } };

console.log('File splitting (FilePartition rules)');
{
  const cfg = Object.assign({}, S.DEFAULTS);
  let r = S.scanTasks(1, 1024, cfg, 4);
  ok(r.tasks.length === 8 && Math.abs(r.tasks[0].mb - 128) < 1e-9, 'a 1 GB file splits into 8 × 128 MB tasks');
  r = S.scanTasks(1000, 1000, cfg, 64);
  ok(Math.abs(r.maxSplit - (1000 + 4000) / 64) < 1e-9 && r.tasks.length === Math.ceil(1000 / Math.floor(r.maxSplit / 5)), 'small files are packed using the 4 MB open cost and bytes-per-core');
  r = S.scanTasks(10, 10 * 1024, cfg, 400);
  ok(r.maxSplit < 128 && r.tasks.length >= 400 * 0.9, 'with many cores, splits shrink below 128 MB to use them');
}

const base = {
  fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'customers', sizeMB: 40, files: 1 },
  aggReduction: 0.002, output: 'daily_revenue', projection: 0.6,
};
const r0 = S.run(base, {});
console.log('Joins');
ok(r0.strategy === 'sortMerge' && r0.stages.length === 4, 'a 40 MB dimension is above the 10 MB default threshold: sort-merge join');
const rb = S.run(base, { broadcastHint: true });
ok(rb.strategy === 'broadcast' && rb.stages.length === 2 && rb.runtime < r0.runtime * 0.8, 'a broadcast hint removes the dimension scan and join shuffle stages and is faster');
ok(S.run(base, { broadcastThreshold: 64 }).strategy === 'broadcast', 'raising the threshold above the table size broadcasts it');
ok(S.run(base, { broadcastThreshold: -1, broadcastHint: false }).strategy === 'sortMerge', '-1 disables automatic broadcasting');
const big = Object.assign({}, base, { dim: { name: 'events', sizeMB: 12 * GB, files: 100 } });
let f = S.run(big, { broadcastHint: true });
ok(f.failed && /larger than 8GB/.test(f.failed.message), 'broadcasting more than 8 GB fails with Spark\'s hard limit');
f = S.run(Object.assign({}, base, { dim: { name: 'events', sizeMB: 7 * GB, files: 60 } }), { broadcastHint: true, memPerWorkerGB: 32 });
ok(f.failed && /Not enough memory to build and broadcast/.test(f.failed.message), 'a broadcast too large for executor memory fails with an OOM');
const filteredDim = Object.assign({}, base, { dim: { name: 'customers', sizeMB: 900, files: 8, selectivity: 0.005 } });
ok(S.run(filteredDim, { aqe: true }).strategy === 'broadcast (AQE)' && S.run(filteredDim, { aqe: false }).strategy === 'sortMerge', 'AQE converts to a broadcast join when the runtime size is small');

console.log('Skew');
const skew = Object.assign({}, base, { hotKeyShare: 0.3 });
const sNo = S.run(skew, { aqe: false }), sAqe = S.run(skew, { aqe: true }), sSalt = S.run(skew, { aqe: false, salt: 16 });
const joinStage = r => r.stages.find(s => /Sort-merge/.test(s.name));
ok(joinStage(sNo).max > 20 * joinStage(sNo).median && sNo.findings.some(x => /Skew/.test(x.text)), 'a 30% hot key creates a straggler task, and it is flagged');
ok(joinStage(sAqe).max < joinStage(sNo).max / 4 && sAqe.runtime < sNo.runtime, 'AQE skew handling splits the hot partition');
ok(joinStage(sSalt).max < joinStage(sNo).max / 4 && sSalt.runtime < sNo.runtime, 'salting spreads the hot key');

console.log('Shuffle partitions and spill');
const huge = { fact: { name: 'clicks', sizeMB: 3000 * GB, files: 24000 }, dim: { name: 'pages', sizeMB: 300, files: 4 }, projection: 1, aggReduction: 0.001 };
const h200 = S.run(huge, { aqe: false, shufflePartitions: 200 }), h8k = S.run(huge, { aqe: false, shufflePartitions: 8000 });
ok(h200.spillMB > 0 && h200.findings.some(x => /spilled/.test(x.text)), '3 TB into 200 partitions spills');
ok(h8k.spillMB === 0 && h8k.runtime < h200.runtime, '8000 partitions fit in memory and are faster');
const tiny = { fact: { name: 'refunds', sizeMB: 2 * GB, files: 16 }, aggReduction: 0.5, output: 'refund_summary' };
const tOff = S.run(tiny, { aqe: false }), tOn = S.run(tiny, { aqe: true });
ok(tOff.outputFiles === 200 && tOn.outputFiles < 20, 'without AQE, 200 shuffle partitions write 200 small files; AQE coalesces them');
ok(S.run(tiny, { aqe: false, write: 'coalesce', coalesceTo: 4 }).outputFiles === 4, 'coalesce(4) writes 4 files');
ok(S.run(tiny, { aqe: false, write: 'optimized' }).outputFiles === Math.ceil(1024 / 128), 'optimized writes target ~128 MB files');

console.log('Scans, clustering, compaction, Photon, UDFs, cluster size');
const dated = { fact: { name: 'orders', sizeMB: 1000 * GB, files: 8000, filterColumn: 'order_date', selectivity: 0.007 }, aggReduction: 0.01 };
const d0 = S.run(dated, {}), d1 = S.run(dated, { clusteredBy: 'order_date' });
ok(d1.readMB < d0.readMB * 0.05 && d1.runtime < d0.runtime / 5 && d0.findings.some(x => /not clustered/.test(x.text)), 'clustering by the filter column skips almost every file');
const small = { fact: { name: 'events_raw', sizeMB: 200 * GB, files: 120000 }, aggReduction: 0.01 };
const c0 = S.run(small, {}), c1 = S.run(small, { compacted: true });
ok(c0.findings.some(x => /files averaging/.test(x.text)) && c1.runtime < c0.runtime, 'small files are flagged, and compaction speeds up the scan');
ok(S.run(base, { photon: true }).runtime < r0.runtime, 'Photon is faster on built-in operations');
const udf = Object.assign({}, base, { udf: true });
ok(S.run(udf, { udf: 'python' }).runtime > 2 * S.run(udf, { udf: 'builtin' }).runtime, 'a Python UDF is much slower than a built-in');
let prev = Infinity;
for (const wk of [2, 4, 8, 16, 32]) { const r = S.run(base, { workers: wk }); ok(r.runtime <= prev + 1e-9, `more workers is never slower (${wk})`); prev = r.runtime; }
console.log(failures ? `\n${failures} of ${checks} checks failed` : `\nAll ${checks} checks passed.`);
process.exit(failures ? 1 : 0);
