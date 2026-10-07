/* grant/challenges.js: Unity Catalog access challenges (write SQL against a test matrix and
   least-privilege policies) and Asset Bundle challenges (write databricks.yml). */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};

  /* ---------- the Stagedoor account ---------- */
  function world() {
    const u = new SSG.uc.UnityCatalog();
    ['platform-admins', 'data-engineers', 'analysts', 'finance', 'support', 'hr', 'acme-partners', 'regional-eu', 'regional-us', 'on-call'].forEach(g => u.addGroup(g));
    u.addUser('root@stagedoor.io', ['platform-admins']);
    u.addUser('alice@stagedoor.io', ['analysts']);
    u.addUser('bob@stagedoor.io', ['data-engineers']);
    u.addUser('carla@stagedoor.io', ['finance']);
    u.addUser('dan@stagedoor.io', ['support']);
    u.addUser('hana@stagedoor.io', ['hr']);
    u.addUser('priya@stagedoor.io', ['analysts', 'regional-eu']);
    u.addUser('sam@stagedoor.io', ['analysts', 'regional-us']);
    u.addUser('ops@acme.example', ['acme-partners']);
    u.addServicePrincipal('etl-prod');
    const own = 'platform-admins';
    u.add('CATALOG', 'prod', { owner: own });
    ['bronze', 'silver', 'gold', 'finance'].forEach(s => u.add('SCHEMA', `prod.${s}`, { owner: own }));
    u.add('TABLE', 'prod.bronze.raw_orders', { owner: own, cols: ['payload', 'ingested_at'], rows: [{ payload: '{}', ingested_at: '2026-06-30' }] });
    u.add('VOLUME', 'prod.bronze.landing', { owner: own });
    const orders = [['O-1', 'C-1', 'EU', 'acme', 120], ['O-2', 'C-2', 'US', 'direct', 80], ['O-3', 'C-3', 'EU', 'direct', 45], ['O-4', 'C-1', 'EU', 'acme', 60], ['O-5', 'C-4', 'US', 'acme', 200], ['O-6', 'C-5', 'US', 'direct', 30]];
    u.add('TABLE', 'prod.silver.orders', { owner: own, cols: ['order_id', 'customer_id', 'region', 'partner', 'amount'], rows: orders.map(([order_id, customer_id, region, partner, amount]) => ({ order_id, customer_id, region, partner, amount })) });
    u.add('TABLE', 'prod.silver.customers', { owner: own, cols: ['customer_id', 'full_name', 'email', 'region'], rows: [['C-1', 'Amara Okafor', 'amara@example.com', 'EU'], ['C-2', 'Diego Ramos', 'diego@example.org', 'US'], ['C-3', 'Hana Sato', 'hana@example.jp', 'EU'], ['C-4', 'Layla Hassan', 'layla@example.com', 'US'], ['C-5', 'Noah Fischer', 'noah@example.de', 'US']].map(([customer_id, full_name, email, region]) => ({ customer_id, full_name, email, region })) });
    u.add('TABLE', 'prod.gold.daily_revenue', { owner: own, cols: ['day', 'region', 'revenue'], rows: [{ day: '2026-06-30', region: 'EU', revenue: 225 }, { day: '2026-06-30', region: 'US', revenue: 310 }] });
    u.add('TABLE', 'prod.gold.customer_ltv', { owner: own, cols: ['customer_id', 'ltv'], rows: [{ customer_id: 'C-1', ltv: 180 }] });
    u.add('TABLE', 'prod.finance.payouts', { owner: own, cols: ['partner', 'amount'], rows: [{ partner: 'acme', amount: 380 }] });
    return u;
  }

  /* least-privilege policies */
  const POLICY = {
    groupsOnly: { text: 'Grants go to groups or service principals, never to individual users', check: u => u.grantList().filter(g => (u.principals.get(g.principal) || {}).kind === 'user').map(g => `${g.privilege} on ${g.object} to ${g.principal}`) },
    noAll: { text: 'No ALL PRIVILEGES', check: u => u.grantList().filter(g => g.privilege === 'ALL PRIVILEGES').map(g => `ALL PRIVILEGES on ${g.object} to ${g.principal}`) },
    noCatalogData: { text: 'No SELECT or MODIFY at catalog level (grant per schema)', check: u => u.grantList().filter(g => g.type === 'CATALOG' && ['SELECT', 'MODIFY'].includes(g.privilege)).map(g => `${g.privilege} on catalog ${g.object} to ${g.principal}`) },
    noAccountUsersData: { text: 'Nothing beyond USE CATALOG is granted to account users', check: u => u.grantList().filter(g => g.principal === 'account users' && g.privilege !== 'USE CATALOG').map(g => `${g.privilege} on ${g.object} to account users`) },
  };

  const CH = [];
  const add = c => CH.push(Object.assign({ lab: 'grant', kind: 'uc', setup: '', policies: [], hints: [] }, c));
  const T = (as, action, object, expect, extra) => Object.assign({ as, action, object, expect }, extra || {});

  add({
    id: 'uc-first-grant', level: 1, title: 'Analysts need the gold layer',
    scenario: 'The analytics team (group <code>analysts</code>) needs to read every table in <code>prod.gold</code>, including tables that will be created next month, and nothing else.',
    task: 'Write the GRANT statements.',
    tests: [T('alice@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'allow'), T('alice@stagedoor.io', 'select', 'prod.gold.customer_ltv', 'allow'), T('alice@stagedoor.io', 'select', 'prod.gold.churn_scores', 'allow', { future: true }), T('alice@stagedoor.io', 'select', 'prod.silver.orders', 'deny'), T('alice@stagedoor.io', 'modify', 'prod.gold.daily_revenue', 'deny'), T('dan@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'deny')],
    policies: ['groupsOnly', 'noAll'],
    starter: '-- Grant the analysts group read access to everything in prod.gold\n',
    solution: 'GRANT USE CATALOG ON CATALOG prod TO `analysts`;\nGRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO `analysts`;',
    hints: ['Reading a table needs three privileges: USE CATALOG on the catalog, USE SCHEMA on the schema, SELECT on the table.', 'Privileges granted on a schema are inherited by every current and future table in it.'],
    explain: 'Granting at schema level is how access survives new tables: no one has to remember a GRANT for each one. <code>USE CATALOG</code> and <code>USE SCHEMA</code> let a principal reach objects; they reveal nothing on their own.',
  });
  add({
    id: 'uc-medallion-roles', level: 2, title: 'Roles for the medallion layers',
    scenario: 'Stagedoor wants least-privilege roles. Data engineers read bronze and build silver. The production ETL service principal reads silver and publishes gold. Analysts read gold. Nobody else gets anything.',
    task: 'Grant exactly what the test matrix needs, and respect every policy.',
    tests: [
      T('bob@stagedoor.io', 'select', 'prod.bronze.raw_orders', 'allow'), T('bob@stagedoor.io', 'read_volume', 'prod.bronze.landing', 'allow'), T('bob@stagedoor.io', 'select', 'prod.silver.orders', 'allow'), T('bob@stagedoor.io', 'modify', 'prod.silver.orders', 'allow'), T('bob@stagedoor.io', 'create_table', 'prod.silver', 'allow'), T('bob@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'allow'), T('bob@stagedoor.io', 'modify', 'prod.gold.daily_revenue', 'deny'),
      T('etl-prod', 'select', 'prod.silver.orders', 'allow'), T('etl-prod', 'modify', 'prod.gold.daily_revenue', 'allow'), T('etl-prod', 'create_table', 'prod.gold', 'allow'), T('etl-prod', 'modify', 'prod.silver.orders', 'deny'),
      T('alice@stagedoor.io', 'select', 'prod.gold.customer_ltv', 'allow'), T('alice@stagedoor.io', 'select', 'prod.silver.customers', 'deny'), T('alice@stagedoor.io', 'select', 'prod.bronze.raw_orders', 'deny'),
      T('carla@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'deny'), T('bob@stagedoor.io', 'select', 'prod.finance.payouts', 'deny'),
    ],
    policies: ['groupsOnly', 'noAll', 'noCatalogData'],
    starter: '-- data-engineers: read bronze (tables and the landing volume), build silver, read gold\n-- etl-prod: read silver, publish gold\n-- analysts: read gold\n',
    solution: `GRANT USE CATALOG ON CATALOG prod TO \`data-engineers\`;
GRANT USE CATALOG ON CATALOG prod TO \`etl-prod\`;
GRANT USE CATALOG ON CATALOG prod TO \`analysts\`;
GRANT USE SCHEMA, SELECT, READ VOLUME ON SCHEMA prod.bronze TO \`data-engineers\`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.silver TO \`data-engineers\`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO \`data-engineers\`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO \`etl-prod\`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.gold TO \`etl-prod\`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO \`analysts\`;`,
    hints: ['Work schema by schema. Each role needs USE CATALOG once, then privileges per schema.', 'Writing to a table needs MODIFY; creating one needs CREATE TABLE on the schema. Reading files in a volume needs READ VOLUME.'],
    explain: 'Schema-level grants per layer and per role keep access predictable and auditable. Production writes belong to a service principal, not to people: engineers can read what the ETL produced, but only the pipeline identity can change it.',
  });
  add({
    id: 'uc-partner-view', level: 2, title: 'Share only the partner\'s rows',
    scenario: 'The partner Acme (group <code>acme-partners</code>) may see its own orders and nothing else. The data team created the view <code>prod.gold.acme_orders</code> (orders where partner = \'acme\'), owned by the service principal <code>etl-prod</code>.',
    task: 'Give Acme access to the view, and only the view. The view must actually work.',
    setupWorld: u => { u.add('VIEW', 'prod.gold.acme_orders', { owner: 'etl-prod', from: 'prod.silver.orders', where: SSG.uc.parseExpr("partner = 'acme'") }); },
    tests: [T('ops@acme.example', 'select', 'prod.gold.acme_orders', 'allow', { rows: 3 }), T('ops@acme.example', 'select', 'prod.silver.orders', 'deny'), T('ops@acme.example', 'select', 'prod.gold.daily_revenue', 'deny')],
    policies: ['groupsOnly', 'noAll'],
    starter: '-- Acme reads prod.gold.acme_orders. Who else needs what for the view to work?\n',
    solution: `GRANT USE CATALOG ON CATALOG prod TO \`acme-partners\`;
GRANT USE SCHEMA ON SCHEMA prod.gold TO \`acme-partners\`;
GRANT SELECT ON VIEW prod.gold.acme_orders TO \`acme-partners\`;
GRANT USE CATALOG ON CATALOG prod TO \`etl-prod\`;
GRANT USE SCHEMA ON SCHEMA prod.silver TO \`etl-prod\`;
GRANT SELECT ON TABLE prod.silver.orders TO \`etl-prod\`;`,
    hints: ['Granting SELECT on the view is necessary but, here, not sufficient. Run the tests and read the error.', 'A view reads its base table with the view owner\'s privileges.'],
    explain: 'Views are Unity Catalog\'s classic way to share a subset: the reader needs SELECT on the view only, and the view owner\'s access is what reaches the base table. If the owner loses access, the view breaks for everyone. For per-user filtering on the table itself, see row filters next.',
  });
  add({
    id: 'uc-row-filter', level: 3, title: 'Each region sees its own orders',
    scenario: 'All analysts can already query <code>prod.silver.orders</code>. Regional managers in <code>regional-eu</code> and <code>regional-us</code> must see only their region; <code>finance</code> sees everything; anyone else sees nothing.',
    task: 'Create a filter function and attach it as a row filter on <code>prod.silver.orders</code>.',
    setup: 'GRANT USE CATALOG ON CATALOG prod TO `account users`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `analysts`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `finance`',
    tests: [T('priya@stagedoor.io', 'select', 'prod.silver.orders', 'allow', { rows: 3 }), T('sam@stagedoor.io', 'select', 'prod.silver.orders', 'allow', { rows: 3 }), T('carla@stagedoor.io', 'select', 'prod.silver.orders', 'allow', { rows: 6 }), T('alice@stagedoor.io', 'select', 'prod.silver.orders', 'allow', { rows: 0 })],
    policies: ['groupsOnly'],
    starter: `CREATE OR REPLACE FUNCTION prod.silver.region_filter(region STRING)
RETURN false;  -- replace with the rule

ALTER TABLE prod.silver.orders SET ROW FILTER prod.silver.region_filter ON (region);`,
    solution: `CREATE OR REPLACE FUNCTION prod.silver.region_filter(region STRING)
RETURN is_account_group_member('finance')
    OR (is_account_group_member('regional-eu') AND region = 'EU')
    OR (is_account_group_member('regional-us') AND region = 'US');

ALTER TABLE prod.silver.orders SET ROW FILTER prod.silver.region_filter ON (region);`,
    hints: ['A row filter is a SQL function that returns true for rows the current user may see.', "is_account_group_member('group') checks the user running the query."],
    explain: 'Row filters apply to every query on the table, whatever tool runs it, so you don\'t need one view per audience. The function runs as the querying user, so <code>is_account_group_member</code> and <code>current_user()</code> see who is asking.',
  });
  add({
    id: 'uc-column-mask', level: 2, title: 'Mask customer emails',
    scenario: 'Analysts and support can query <code>prod.silver.customers</code>. Support needs full emails to contact customers; HR may see them too; everyone else should see only the domain, as <code>***@example.com</code>.',
    task: 'Create a mask function and set it on the <code>email</code> column.',
    setup: 'GRANT USE CATALOG ON CATALOG prod TO `account users`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `analysts`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `support`; GRANT USE SCHEMA, SELECT ON SCHEMA prod.silver TO `hr`',
    tests: [T('dan@stagedoor.io', 'select', 'prod.silver.customers', 'allow', { column: 'email', equals: 'amara@example.com' }), T('hana@stagedoor.io', 'select', 'prod.silver.customers', 'allow', { column: 'email', equals: 'amara@example.com' }), T('alice@stagedoor.io', 'select', 'prod.silver.customers', 'allow', { column: 'email', equals: '***@example.com' })],
    policies: ['groupsOnly'],
    starter: `CREATE OR REPLACE FUNCTION prod.silver.mask_email(email STRING)
RETURN email;  -- replace with the rule

ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.silver.mask_email;`,
    solution: `CREATE OR REPLACE FUNCTION prod.silver.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('support') OR is_account_group_member('hr') THEN email
            ELSE '***@' || split_part(email, '@', 2) END;

ALTER TABLE prod.silver.customers ALTER COLUMN email SET MASK prod.silver.mask_email;`,
    hints: ['A mask function receives the column value and returns what the current user may see.', "split_part(email, '@', 2) returns the domain."],
    explain: 'Column masks keep one table for every audience: the value each user sees depends on who they are. Combine them with row filters for row- and column-level security without copies or views.',
  });
  add({
    id: 'uc-access-incident', level: 3, title: 'Incident: "I still can\'t read daily_revenue"',
    scenario: 'Alice (analysts) has been blocked for a day. Someone tried to fix it with a few grants, and someone else "solved" it for another team by granting ALL PRIVILEGES on the whole catalog to every user. Security noticed. The current grants are already applied; run the tests to see them fail.',
    task: 'Make the tests pass and the policies hold. Use <code>SHOW GRANTS ON …</code> to investigate, and REVOKE what should not be there.',
    setup: 'GRANT USE CATALOG ON CATALOG prod TO `analysts`; GRANT USE SCHEMA ON SCHEMA prod.silver TO `analysts`; GRANT SELECT ON TABLE prod.gold.daily_revenue TO `alice@stagedoor.io`; GRANT ALL PRIVILEGES ON CATALOG prod TO `account users`',
    tests: [T('alice@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'allow'), T('alice@stagedoor.io', 'select', 'prod.gold.customer_ltv', 'allow'), T('dan@stagedoor.io', 'select', 'prod.gold.daily_revenue', 'deny'), T('dan@stagedoor.io', 'select', 'prod.finance.payouts', 'deny'), T('alice@stagedoor.io', 'select', 'prod.silver.orders', 'deny')],
    policies: ['groupsOnly', 'noAll', 'noAccountUsersData'],
    starter: 'SHOW GRANTS ON CATALOG prod;\n',
    solution: `REVOKE ALL PRIVILEGES ON CATALOG prod FROM \`account users\`;
REVOKE SELECT ON TABLE prod.gold.daily_revenue FROM \`alice@stagedoor.io\`;
REVOKE USE SCHEMA ON SCHEMA prod.silver FROM \`analysts\`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.gold TO \`analysts\`;`,
    hints: ['Before revoking, the catalog-wide grant to account users makes everything pass for everyone: that is the problem.', 'The first fix granted USE SCHEMA on the wrong schema, and SELECT to a person instead of a group.'],
    explain: 'Access incidents are usually a missing USE privilege or a grant on the wrong object, and the "quick fix" of granting broadly to everyone is how data leaks. <code>SHOW GRANTS</code> on each level of the path (catalog, schema, table) finds the gap; grants to groups at the right level close it.',
  });

  /* ---------- Asset Bundles ---------- */
  const addB = c => CH.push(Object.assign({ lab: 'grant', kind: 'bundle', hints: [] }, c));
  const brokenYaml = `bundle:
  name: orders

variables:
  catalog:
    description: Catalog to publish to

resources:
  pipelines:
    orders_pipeline:
      name: orders-pipeline
      catalog: \${var.catalog}
      schema: orders
      serverles: true
      libraries:
        - file:
            path: ./pipelines/orders.sql

  jobs:
    nightly:
      name: nightly-orders
      tasks:
        - task_key: refresh
          pipeline_task:
            pipeline_id: \${resources.pipelines.order_pipeline.id}
        - task_key: report
          depends_on:
            - task_key: refrsh
          notebook_task:
            notebook_path: ./notebooks/report.py

targets:
  dev:
    mode: development
    default: true
`;
  addB({
    id: 'dab-fix', level: 1, title: 'databricks bundle validate fails',
    scenario: 'A colleague\'s first bundle fails validation, and CI blocks the merge. It defines a pipeline and a nightly job that refreshes it and then runs a report.',
    task: 'Make <code>databricks bundle validate -t dev</code> pass with <b>no errors and no warnings</b>. The dev target should publish to the <code>dev</code> catalog.',
    starter: brokenYaml,
    checks: [
      { text: 'Validates for dev with no errors', target: 'dev', fn: r => r.ok },
      { text: 'No warnings', target: 'dev', fn: r => r.ok && !r.warnings.length },
      { text: 'The pipeline publishes to the dev catalog', target: 'dev', fn: r => r.ok && r.plan.resources.some(x => x.type === 'pipeline' && x.catalog === 'dev') },
      { text: 'report runs after refresh, which runs the pipeline', target: 'dev', fn: r => r.ok && (() => { const j = r.plan.resources.find(x => x.type === 'job'); const t = k => j.tasks.find(x => x.key === k); return t('refresh') && t('report') && t('report').dependsOn.includes('refresh') && /pipeline orders_pipeline/.test(t('refresh').detail); })() },
    ],
    solution: brokenYaml.replace('      serverles: true', '      serverless: true').replace('order_pipeline.id', 'orders_pipeline.id').replace('task_key: refrsh', 'task_key: refresh').replace('    default: true\n', '    default: true\n    variables:\n      catalog: dev\n'),
    hints: ['Fix errors from the top: each message names the path.', 'A variable with no default needs a value from the target (targets.dev.variables) or --var.'],
    explain: '<code>bundle validate</code> catches typos in field names, missing variable values, and references to resources that do not exist, before anything is deployed. Run it in CI on every pull request.',
  });
  const twoTargets = `bundle:
  name: orders

variables:
  catalog:
    description: Catalog to publish to

resources:
  pipelines:
    orders_pipeline:
      name: orders-pipeline
      catalog: \${var.catalog}
      schema: orders
      serverless: true
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

targets:
  dev:
    default: true
    variables:
      catalog: dev
`;
  addB({
    id: 'dab-targets', level: 2, title: 'Development and production targets',
    scenario: 'The bundle deploys only to dev, and each engineer\'s deploy overwrites the others\'. Production is deployed by hand from a laptop.',
    task: 'Make <code>dev</code> a <b>development</b> target (per-user, schedules paused) and add a <code>prod</code> target in <b>production</b> mode that publishes to the <code>prod</code> catalog and runs as the service principal <code>etl-prod</code>.',
    starter: twoTargets,
    checks: [
      { text: 'dev validates', target: 'dev', fn: r => r.ok },
      { text: 'dev is per-user: names get the [dev …] prefix', target: 'dev', fn: r => r.ok && r.plan.resources.every(x => x.type === 'schema' || /^\[dev /.test(x.name)) },
      { text: 'dev schedules are paused and the pipeline runs in development mode', target: 'dev', fn: r => r.ok && r.plan.resources.filter(x => x.type === 'job').every(x => !x.schedule || x.schedule.pause_status === 'PAUSED') && r.plan.resources.filter(x => x.type === 'pipeline').every(x => x.development) },
      { text: 'prod validates in production mode', target: 'prod', fn: r => r.ok && r.plan.mode === 'production' },
      { text: 'prod publishes to the prod catalog', target: 'prod', fn: r => r.ok && r.plan.resources.some(x => x.type === 'pipeline' && x.catalog === 'prod') },
      { text: 'prod runs as the service principal etl-prod', target: 'prod', fn: r => r.ok && /service principal etl-prod/.test(r.plan.runAs) },
      { text: 'prod keeps its schedule active', target: 'prod', fn: r => r.ok && r.plan.resources.filter(x => x.type === 'job').every(x => x.schedule && x.schedule.pause_status !== 'PAUSED') },
    ],
    solution: twoTargets.replace('  dev:\n    default: true\n', '  dev:\n    mode: development\n    default: true\n') + `  prod:
    mode: production
    variables:
      catalog: prod
    run_as:
      service_principal_name: etl-prod
`,
    hints: ['mode: development prefixes resource names with [dev <user>], pauses schedules and puts pipelines in development mode.', 'run_as sets the identity production jobs run as.'],
    explain: 'Targets turn one definition into many deployments. Development mode isolates each engineer\'s copy; production mode deploys the shared copy, which should run as a service principal so it does not depend on (or act as) any person.',
  });
  const dagStart = `bundle:
  name: orders

resources:
  pipelines:
    ingest:
      name: orders-ingest
      catalog: prod
      schema: bronze
      libraries:
        - file:
            path: ./pipelines/ingest.sql
  jobs:
    daily:
      name: daily-orders
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: \${resources.pipelines.ingest.id}
`;
  addB({
    id: 'dab-dag', level: 2, title: 'Build the task graph',
    scenario: 'The daily job runs only the ingest pipeline. The rest still runs from a cron script on a VM.',
    task: 'Add tasks so that <code>silver_orders</code> and <code>silver_customers</code> run in parallel after <code>ingest</code>; <code>gold</code> runs after both; <code>report</code> runs after <code>gold</code>. Each new task runs the notebook <code>./notebooks/&lt;task_key&gt;.py</code>.',
    starter: dagStart,
    checks: [
      { text: 'Validates', fn: r => r.ok },
      { text: 'silver_orders and silver_customers depend only on ingest', fn: r => r.ok && ['silver_orders', 'silver_customers'].every(k => { const t = task(r, k); return t && t.dependsOn.join() === 'ingest'; }) },
      { text: 'gold depends on both silver tasks', fn: r => r.ok && (t => t && t.dependsOn.slice().sort().join() === 'silver_customers,silver_orders')(task(r, 'gold')) },
      { text: 'report depends on gold', fn: r => r.ok && (t => t && t.dependsOn.join() === 'gold')(task(r, 'report')) },
      { text: 'Each new task runs its notebook', fn: r => r.ok && ['silver_orders', 'silver_customers', 'gold', 'report'].every(k => { const t = task(r, k); return t && t.type === 'notebook_task' && t.detail === `./notebooks/${k}.py`; }) },
    ],
    solution: dagStart + ['silver_orders', 'silver_customers'].map(k => `        - task_key: ${k}\n          depends_on:\n            - task_key: ingest\n          notebook_task:\n            notebook_path: ./notebooks/${k}.py\n`).join('') + `        - task_key: gold\n          depends_on:\n            - task_key: silver_orders\n            - task_key: silver_customers\n          notebook_task:\n            notebook_path: ./notebooks/gold.py\n        - task_key: report\n          depends_on:\n            - task_key: gold\n          notebook_task:\n            notebook_path: ./notebooks/report.py\n`,
    hints: ['depends_on is a list of mappings: - task_key: ingest', 'Tasks with no dependency between them run in parallel.'],
    explain: 'Lakeflow Jobs run each task as soon as its dependencies succeed, so expressing the real graph, rather than a sequence, shortens the critical path. The validator also rejects cycles and dependencies on tasks that do not exist.',
  });
  const permStart = `bundle:
  name: orders

resources:
  jobs:
    nightly:
      name: nightly-orders
      tasks:
        - task_key: report
          notebook_task:
            notebook_path: ./notebooks/report.py

targets:
  dev:
    mode: development
    default: true
  prod:
    mode: production
    run_as:
      service_principal_name: etl-prod
    permissions:
      - level: CAN_MANAGE
        user_name: bob@stagedoor.io
`;
  addB({
    id: 'dab-permissions', level: 2, title: 'Who can touch production?',
    scenario: 'In production, Bob is the only person who can manage the job. When he is on holiday, nobody can fix it, and the analysts cannot see run results.',
    task: 'In the <code>prod</code> target: <code>data-engineers</code> can manage, <code>on-call</code> can manage runs (start and cancel), <code>analysts</code> can view, and no permission is granted to an individual user.',
    starter: permStart,
    checks: [
      { text: 'prod validates', target: 'prod', fn: r => r.ok },
      { text: 'data-engineers: CAN_MANAGE', target: 'prod', fn: r => r.ok && hasPerm(r, 'group_name', 'data-engineers', 'CAN_MANAGE') },
      { text: 'on-call: CAN_MANAGE_RUN', target: 'prod', fn: r => r.ok && hasPerm(r, 'group_name', 'on-call', 'CAN_MANAGE_RUN') },
      { text: 'analysts: CAN_VIEW', target: 'prod', fn: r => r.ok && hasPerm(r, 'group_name', 'analysts', 'CAN_VIEW') },
      { text: 'No permissions for individual users', target: 'prod', fn: r => r.ok && !perms(r).some(p => p.user_name) },
      { text: 'dev still validates', target: 'dev', fn: r => r.ok },
    ],
    solution: permStart.replace(`    permissions:
      - level: CAN_MANAGE
        user_name: bob@stagedoor.io
`, `    permissions:
      - level: CAN_MANAGE
        group_name: data-engineers
      - level: CAN_MANAGE_RUN
        group_name: on-call
      - level: CAN_VIEW
        group_name: analysts
`),
    hints: ['Each permission has a level and exactly one of user_name, group_name or service_principal_name.', 'Levels: CAN_VIEW, CAN_MANAGE_RUN, CAN_MANAGE (and IS_OWNER).'],
    explain: 'Bundle permissions are applied to every resource the bundle deploys, so access is reviewed in code alongside the job itself. Granting to groups means people can join and leave without anyone editing the bundle.',
  });
  function task(r, k) { const j = r.plan && r.plan.resources.find(x => x.type === 'job'); return j && j.tasks.find(t => t.key === k); }
  function perms(r) { const d = r.doc || {}; const t = (d.targets || {})[r.targetName] || {}; return [].concat(d.permissions || [], t.permissions || []); }
  function hasPerm(r, kind, name, level) { return perms(r).some(p => p && p[kind] === name && p.level === level); }

  /* ---------- checking ---------- */
  function checkUc(ch, sql) {
    const u = world();
    if (ch.setupWorld) ch.setupWorld(u);
    if (ch.setup) u.execute(ch.setup, 'root@stagedoor.io');
    const outputs = [];
    let error = null;
    const stmts = SSG.uc.splitStatements(sql);
    for (let i = 0; i < stmts.length; i++) {
      try { const r = u.statement(stmts[i], 'root@stagedoor.io'); outputs.push({ sql: stmts[i], ok: true, message: r.message, rows: r.rows }); }
      catch (e) { error = { index: i, sql: stmts[i], message: e.message }; outputs.push({ sql: stmts[i], ok: false, message: e.message }); break; }
    }
    const tests = ch.tests.map(t => {
      if (t.future && !u.get(t.object)) u.add('TABLE', t.object, { owner: 'platform-admins', cols: ['x'], rows: [{ x: 1 }] });
      const r = u.can(t.as, t.action, t.object);
      let met = (t.expect === 'allow') === r.ok, detail = r.ok ? 'allowed' : r.error;
      if (met && r.ok && t.action === 'select' && (t.rows != null || t.column)) {
        const rows = u.select(t.as, t.object);
        if (t.rows != null && rows.length !== t.rows) { met = false; detail = `allowed, but sees ${rows.length} row${rows.length === 1 ? '' : 's'} (expected ${t.rows})`; }
        else if (t.column) { const v = rows[0] && rows[0][t.column]; if (v !== t.equals) { met = false; detail = `sees ${t.column} = ${v} (expected ${t.equals})`; } else detail = `sees ${t.column} = ${v}`; }
        else detail = `allowed, ${rows.length} row${rows.length === 1 ? '' : 's'}`;
      }
      return Object.assign({ met, detail }, t);
    });
    const policies = ch.policies.map(id => { const v = POLICY[id].check(u); return { id, text: POLICY[id].text, met: !v.length, violations: v }; });
    return { ok: !error && tests.every(t => t.met) && policies.every(p => p.met), tests, policies, outputs, error, uc: u };
  }
  function checkBundle(ch, yamlText) {
    const runs = {};
    const res = ch.checks.map(c => {
      const target = c.target || null;
      const key = target || '(default)';
      if (!runs[key]) runs[key] = SSG.bundles.validate(yamlText, { target });
      let met = false; try { met = !!c.fn(runs[key]); } catch (e) { met = false; }
      return Object.assign({ met }, c);
    });
    return { ok: res.every(x => x.met), checks: res, runs };
  }
  const check = (ch, input) => (ch.kind === 'bundle' ? checkBundle(ch, input) : checkUc(ch, input));

  SSG.grantLab = { CHALLENGES: CH, check, world, POLICY };
})(typeof window !== 'undefined' ? window : global);
