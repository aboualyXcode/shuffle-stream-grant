#!/usr/bin/env node
/* Unity Catalog access rules and Asset Bundle validation. Usage: node tests/grant.test.js */
'use strict';
const path = require('path');
['yaml.js', 'grant/uc.js', 'grant/bundles.js'].forEach(f => require(path.join(__dirname, '..', 'js', f)));
const { uc, bundles } = global.SSG;
let checks = 0, failures = 0;
const ok = (c, m) => { checks++; if (!c) { failures++; console.log('  FAIL ' + m); } };

function world() {
  const u = new uc.UnityCatalog();
  u.addGroup('analysts'); u.addGroup('bi-team', ['analysts']); u.addGroup('data-engineers'); u.addGroup('hr');
  u.addUser('alice@stagedoor.io', ['bi-team']); u.addUser('bob@stagedoor.io', ['data-engineers']); u.addUser('hana@stagedoor.io', ['hr']);
  u.addUser('admin'); u.addServicePrincipal('etl-prod');
  u.add('CATALOG', 'prod', { owner: 'admin' }); u.add('SCHEMA', 'prod.gold', { owner: 'admin' }); u.add('SCHEMA', 'prod.silver', { owner: 'admin' });
  u.add('TABLE', 'prod.gold.orders', { owner: 'admin', cols: ['order_id', 'region', 'amount'], rows: [{ order_id: 1, region: 'EU', amount: 10 }, { order_id: 2, region: 'US', amount: 20 }, { order_id: 3, region: 'EU', amount: 5 }] });
  u.add('TABLE', 'prod.silver.customers', { owner: 'admin', cols: ['customer_id', 'email', 'region'], rows: [{ customer_id: 1, email: 'amara@example.com', region: 'EU' }, { customer_id: 2, email: 'diego@example.com', region: 'US' }] });
  return u;
}
console.log('Unity Catalog');
{
  const u = world(), A = 'alice@stagedoor.io';
  let r = u.can(A, 'select', 'prod.gold.orders');
  ok(!r.ok && /USE CATALOG on Catalog 'prod'/.test(r.error), 'no grants: USE CATALOG is the first thing missing');
  u.execute('GRANT USE CATALOG ON CATALOG prod TO `analysts`');
  r = u.can(A, 'select', 'prod.gold.orders'); ok(!r.ok && /USE SCHEMA/.test(r.error), 'then USE SCHEMA (granted to a parent group of alice\'s group)');
  u.execute('GRANT USE SCHEMA ON SCHEMA prod.gold TO `analysts`');
  r = u.can(A, 'select', 'prod.gold.orders'); ok(!r.ok && /SELECT on Table/.test(r.error), 'USE SCHEMA is required but not sufficient');
  u.execute('GRANT SELECT ON SCHEMA prod.gold TO `analysts`');
  ok(u.can(A, 'select', 'prod.gold.orders').ok, 'SELECT on the schema is inherited by its tables');
  ok(!u.can(A, 'select', 'prod.silver.customers').ok, 'but not by tables in other schemas');
  ok(!u.can(A, 'modify', 'prod.gold.orders').ok, 'SELECT does not allow writes');
  u.execute('REVOKE SELECT ON SCHEMA prod.gold FROM `analysts`');
  ok(!u.can(A, 'select', 'prod.gold.orders').ok, 'REVOKE removes access');
  u.execute('GRANT USE SCHEMA, SELECT ON CATALOG prod TO `analysts`');
  ok(u.can(A, 'select', 'prod.silver.customers').ok && u.can(A, 'select', 'prod.gold.orders').ok, 'privileges granted on a catalog reach every schema and table');
  const u2 = world();
  u2.execute('ALTER TABLE prod.gold.orders OWNER TO `bob@stagedoor.io`');
  r = u2.can('bob@stagedoor.io', 'select', 'prod.gold.orders'); ok(!r.ok && /USE CATALOG/.test(r.error), 'a table owner still needs USE CATALOG and USE SCHEMA');
  u2.execute('GRANT USE CATALOG ON CATALOG prod TO `data-engineers`; GRANT USE SCHEMA ON SCHEMA prod.gold TO `data-engineers`');
  ok(u2.can('bob@stagedoor.io', 'select', 'prod.gold.orders').ok && u2.can('bob@stagedoor.io', 'modify', 'prod.gold.orders').ok, 'with them, the owner can read and write');
  ok(!u2.can('bob@stagedoor.io', 'create_table', 'prod.gold').ok, 'owning a table does not allow creating tables in its schema');
  const u3 = world();
  u3.execute('GRANT ALL PRIVILEGES ON SCHEMA prod.silver TO `data-engineers`; GRANT USE CATALOG ON CATALOG prod TO `data-engineers`');
  ok(u3.can('bob@stagedoor.io', 'create_table', 'prod.silver').ok && u3.can('bob@stagedoor.io', 'modify', 'prod.silver.customers').ok, 'ALL PRIVILEGES on a schema covers its tables');
  let threw = null; try { u3.execute('GRANT MODIFY ON CATALOG prod TO `nobody`'); } catch (e) { threw = e; } ok(threw && threw.cls === 'PRINCIPAL_DOES_NOT_EXIST', 'unknown principals are rejected');
  threw = null; try { u3.execute('GRANT READ VOLUME ON TABLE prod.gold.orders TO `analysts`'); } catch (e) { threw = e; } ok(threw && threw.cls === 'INVALID_PRIVILEGE', 'privileges must apply to the securable');
  threw = null; try { u3.execute('GRANT SELECT ON SCHEMA prod.gold.orders TO `analysts`'); } catch (e) { threw = e; } ok(threw && threw.cls === 'INVALID_SECURABLE_TYPE', 'the securable type must match');

  // views use the owner's rights
  const v = world();
  v.add('VIEW', 'prod.gold.orders_eu', { owner: 'bob@stagedoor.io', from: 'prod.gold.orders', where: uc.parseExpr("region = 'EU'") });
  v.execute('GRANT USE CATALOG ON CATALOG prod TO `analysts`; GRANT USE SCHEMA ON SCHEMA prod.gold TO `analysts`; GRANT SELECT ON VIEW prod.gold.orders_eu TO `analysts`');
  r = v.can(A, 'select', 'prod.gold.orders_eu'); ok(!r.ok && /owner \(bob@stagedoor.io\)/.test(r.error), 'a view fails when its owner cannot read the base table');
  v.execute('GRANT USE CATALOG ON CATALOG prod TO `data-engineers`; GRANT USE SCHEMA ON SCHEMA prod.gold TO `data-engineers`; GRANT SELECT ON TABLE prod.gold.orders TO `data-engineers`');
  ok(v.select(A, 'prod.gold.orders_eu').length === 2, 'with owner access, analysts read the view without any grant on the table');
  ok(!v.can(A, 'select', 'prod.gold.orders').ok, 'and still cannot read the table directly');

  // row filters and masks see the caller
  const f = world();
  f.execute(`GRANT USE CATALOG ON CATALOG prod TO \`account users\`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO \`account users\`;
    CREATE FUNCTION prod.silver.eu_only(region STRING) RETURN is_account_group_member('hr') OR region = 'EU';
    ALTER TABLE prod.silver.customers SET ROW FILTER prod.silver.eu_only ON (region);
    CREATE FUNCTION prod.silver.mask_email(email STRING) RETURN CASE WHEN is_account_group_member('hr') THEN email ELSE '***@' || split_part(email, '@', 2) END;
    ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.silver.mask_email`);
  const ra = f.select(A, 'prod.silver.customers'), rh = f.select('hana@stagedoor.io', 'prod.silver.customers');
  ok(ra.length === 1 && ra[0].email === '***@example.com', 'non-HR users see EU rows only, with masked emails');
  ok(rh.length === 2 && rh[0].email === 'amara@example.com', 'HR sees every row unmasked');
  ok(f.execute('SHOW GRANTS ON SCHEMA prod.silver')[0].rows.length === 2, 'SHOW GRANTS lists grants');
}

console.log('Asset Bundles');
{
  const base = `bundle:
  name: orders
variables:
  catalog:
    description: Target catalog
resources:
  pipelines:
    orders_pipeline:
      name: orders-pipeline
      catalog: \${var.catalog}
      schema: orders
      libraries:
        - file:
            path: ./pipelines/orders.sql
  jobs:
    nightly:
      name: nightly-orders
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: UTC
      tasks:
        - task_key: refresh
          pipeline_task:
            pipeline_id: \${resources.pipelines.orders_pipeline.id}
        - task_key: report
          depends_on:
            - task_key: refresh
          notebook_task:
            notebook_path: ./notebooks/report.py
targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: dev
  prod:
    mode: production
    variables:
      catalog: prod
    run_as:
      service_principal_name: etl-prod
`;
  let r = bundles.validate(base, { target: 'dev' });
  ok(r.ok, 'a well-formed bundle validates: ' + JSON.stringify(r.errors));
  const job = r.plan.resources.find(x => x.type === 'job'), pipe = r.plan.resources.find(x => x.type === 'pipeline');
  ok(job.name === '[dev alice] nightly-orders' && job.schedule.pause_status === 'PAUSED', 'development mode prefixes names and pauses schedules');
  ok(pipe.catalog === 'dev' && pipe.development === true, 'target variables resolve, and dev pipelines run in development mode');
  r = bundles.validate(base, { target: 'prod' });
  ok(r.ok && r.plan.resources.find(x => x.type === 'pipeline').catalog === 'prod' && r.plan.resources.find(x => x.type === 'job').name === 'nightly-orders' && /etl-prod/.test(r.plan.runAs), 'production keeps names, resolves prod values and runs as the service principal');
  r = bundles.validate(base.replace('    variables:\n      catalog: dev\n', ''), { target: 'dev' });
  ok(!r.ok && r.errors.some(e => /no value assigned to required variable catalog/.test(e.msg)), 'a variable without a value is an error');
  r = bundles.validate(base.replace('schema: orders', 'schemaa: orders'), { target: 'dev' });
  ok(r.warnings.some(w => /unknown field: schemaa \(did you mean schema\?\)/.test(w.msg)), 'unknown fields are warned about, with a suggestion');
  r = bundles.validate(base.replace('            - task_key: refresh', '            - task_key: refrsh'), { target: 'dev' });
  ok(!r.ok && r.errors.some(e => /depends on unknown task "refrsh"/.test(e.msg)), 'depends_on must name an existing task');
  r = bundles.validate(base.replace('        - task_key: refresh\n          pipeline_task:', '        - task_key: refresh\n          depends_on:\n            - task_key: report\n          pipeline_task:'), { target: 'dev' });
  ok(!r.ok && r.errors.some(e => /cycle: (refresh → report → refresh|report → refresh → report)/.test(e.msg)), 'task cycles are detected');
  r = bundles.validate(base.replace('orders_pipeline.id', 'order_pipeline.id'), { target: 'dev' });
  ok(!r.ok && r.errors.some(e => /reference does not exist: \$\{resources.pipelines.order_pipeline.id\}/.test(e.msg)), 'references to undefined resources are errors');
  r = bundles.validate(base.replace('          notebook_task:\n            notebook_path: ./notebooks/report.py', '          notebook_task:\n            notebook_path: ./notebooks/report.py\n          spark_python_task:\n            python_file: ./x.py'), { target: 'dev' });
  ok(!r.ok && r.errors.some(e => /only one task type/.test(e.msg)), 'a task has exactly one task type');
  r = bundles.validate('bundle:\n  name: x\n bad indent', {});
  ok(!r.ok && /YAML/.test(r.errors[0].msg), 'YAML syntax errors are reported');
  r = bundles.validate(base, { target: 'staging' });
  ok(!r.ok && r.errors.some(e => /no such target: staging/.test(e.msg)), 'unknown targets are errors');
}
console.log(failures ? `\n${failures} of ${checks} checks failed` : `\nAll ${checks} checks passed.`);
process.exit(failures ? 1 : 0);
