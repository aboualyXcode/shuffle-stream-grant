#!/usr/bin/env node
/* Every challenge in all three labs must be sound: the starting point misses the goals, the reference
   solution (and any alternative) meets them, and known wrong answers are rejected.
   Usage: node tests/challenges.test.js */
'use strict';
const path = require('path');
['yaml.js', 'stream/engine.js', 'stream/challenges.js', 'grant/uc.js', 'grant/bundles.js', 'grant/challenges.js', 'shuffle/model.js', 'shuffle/challenges.js'].forEach(f => require(path.join(__dirname, '..', 'js', f)));
const { streamLab, grantLab, shuffleLab } = global.SSG;
let checks = 0, failures = 0;
const ok = (c, m) => { checks++; if (!c) { failures++; console.log('  FAIL ' + m); } };
const ids = new Set();
const unique = id => { ok(!ids.has(id), `duplicate challenge id ${id}`); ids.add(id); };

console.log('Shuffle (Spark tuning)');
for (const c of shuffleLab.CHALLENGES) {
  unique(c.id);
  ok(!shuffleLab.check(c, {}).ok, `${c.id}: the current configuration misses the goals`);
  ok(shuffleLab.check(c, c.solution).ok, `${c.id}: the reference solution meets the goals`);
  if (c.alt) ok(shuffleLab.check(c, c.alt).ok, `${c.id}: the alternative meets the goals`);
  ok(c.knobs.every(k => shuffleLab.KNOBS[k]), `${c.id}: every knob is defined`);
  ok(Object.keys(c.solution).every(k => c.knobs.includes(k)), `${c.id}: the solution only uses available knobs`);
  ok(/spark/.test(shuffleLab.code(c, c.solution)), `${c.id}: code renders`);
}
const sh = id => shuffleLab.CHALLENGES.find(c => c.id === id);
ok(!shuffleLab.check(sh('skewed-join'), { aqe: false, workers: 64 }).ok, 'skewed-join: more workers does not fix skew');
ok(!shuffleLab.check(sh('broadcast-too-big'), { broadcastHint: true, broadcastThreshold: 10 }).ok, 'broadcast-too-big: hinting the 12 GB table still fails');
ok(!shuffleLab.check(sh('data-skipping'), { clusteredBy: 'customer_id' }).ok, 'data-skipping: clustering by the wrong column does not help');

console.log('Stream (Structured Streaming and Lakeflow)');
const MIN = streamLab.MIN;
for (const c of streamLab.CHALLENGES) {
  unique(c.id);
  ok(!streamLab.check(c, {}).ok, `${c.id}: the current configuration misses the goals`);
  ok(streamLab.check(c, c.solution).ok, `${c.id}: the reference solution meets the goals`);
  if (c.alt) ok(streamLab.check(c, c.alt).ok, `${c.id}: the alternative meets the goals`);
  ok(streamLab.code(c, c.solution).length > 20, `${c.id}: code renders`);
}
const st = id => streamLab.CHALLENGES.find(c => c.id === id);
ok(!streamLab.check(st('late-mobile-events'), { watermarkMs: 60 * MIN }).ok, 'late-mobile-events: an hour-long watermark is too slow');
ok(!streamLab.check(st('state-explosion'), { watermarkMs: 2 * MIN }).ok, 'state-explosion: a 2-minute watermark drops data');
ok(!streamLab.check(st('live-dashboard'), { outputMode: 'complete', watermarkMs: 5 * MIN }).ok, 'live-dashboard: complete mode never evicts state');
ok(!streamLab.check(st('payment-retries'), { dedupe: 'dropDuplicates', watermarkMs: null }).ok, 'payment-retries: unbounded dropDuplicates state is rejected');
ok(!streamLab.check(st('payment-retries'), { dedupe: 'withinWatermark', watermarkMs: MIN }).ok, 'payment-retries: a 1-minute watermark drops late payments');
ok(!streamLab.check(st('schema-evolution'), { mode: 'rescue', retries: 0 }).ok, 'schema-evolution: rescue keeps running but currency is not a column');
ok(!streamLab.check(st('cdc-out-of-order'), { sequenceBy: 'seq', applyDeletes: false }).ok, 'cdc-out-of-order: ignoring deletes leaves deleted customers');

console.log('Grant (Unity Catalog and Asset Bundles)');
for (const c of grantLab.CHALLENGES) {
  unique(c.id);
  ok(!grantLab.check(c, c.starter).ok, `${c.id}: the starting point fails`);
  ok(grantLab.check(c, c.solution).ok, `${c.id}: the reference solution passes`);
}
const gr = id => grantLab.CHALLENGES.find(c => c.id === id);
ok(!grantLab.check(gr('uc-first-grant'), 'GRANT USE CATALOG ON CATALOG prod TO `analysts`; GRANT USE SCHEMA ON SCHEMA prod.gold TO `analysts`; GRANT SELECT ON TABLE prod.gold.daily_revenue TO `analysts`; GRANT SELECT ON TABLE prod.gold.customer_ltv TO `analysts`').ok, 'uc-first-grant: per-table grants miss future tables');
ok(!grantLab.check(gr('uc-first-grant'), 'GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG prod TO `analysts`').ok, 'uc-first-grant: catalog-wide SELECT exposes silver');
ok(!grantLab.check(gr('uc-first-grant'), 'GRANT USE CATALOG ON CATALOG prod TO `alice@stagedoor.io`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `alice@stagedoor.io`').ok, 'uc-first-grant: grants to a person break the groups-only policy');
ok(!grantLab.check(gr('uc-medallion-roles'), gr('uc-medallion-roles').solution.replace('GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.silver TO `data-engineers`', 'GRANT ALL PRIVILEGES ON SCHEMA prod.silver TO `data-engineers`')).ok, 'uc-medallion-roles: ALL PRIVILEGES breaks policy');
ok(!grantLab.check(gr('uc-partner-view'), 'GRANT USE CATALOG ON CATALOG prod TO `acme-partners`; GRANT USE SCHEMA ON SCHEMA prod.gold TO `acme-partners`; GRANT SELECT ON VIEW prod.gold.acme_orders TO `acme-partners`').ok, 'uc-partner-view: the view fails while its owner lacks access');
ok(!grantLab.check(gr('uc-partner-view'), gr('uc-partner-view').solution + '\nGRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `acme-partners`;').ok, 'uc-partner-view: direct table access is rejected');
const fixRun = grantLab.check(gr('uc-access-incident'), 'GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`');
ok(!fixRun.ok && fixRun.policies.some(p => !p.met), 'uc-access-incident: adding grants without revoking the broad one still violates policy');
ok(!grantLab.check(gr('dab-targets'), gr('dab-targets').solution.replace('    run_as:\n      service_principal_name: etl-prod\n', '')).ok, 'dab-targets: prod must run as the service principal');

console.log(`\n${ids.size} challenges.`);
console.log(failures ? `${failures} of ${checks} checks failed` : `All ${checks} checks passed.`);
process.exit(failures ? 1 : 0);
