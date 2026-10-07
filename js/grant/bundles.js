/* grant/bundles.js: validates a databricks.yml (a subset of Databricks Asset Bundles) and computes
   what `databricks bundle deploy -t <target>` would deploy. It checks structure, variables and
   references, the task graph, and applies development/production mode presets. */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};

  const TOP = ['bundle', 'include', 'variables', 'workspace', 'artifacts', 'resources', 'targets', 'permissions', 'run_as', 'sync', 'presets', 'experimental'];
  const BUNDLE = ['name', 'databricks_cli_version', 'git', 'cluster_id', 'compute_id', 'uuid'];
  const VARIABLE = ['description', 'default', 'lookup', 'type'];
  const WORKSPACE = ['host', 'root_path', 'profile', 'file_path', 'artifact_path', 'state_path'];
  const TARGET = ['mode', 'default', 'workspace', 'variables', 'resources', 'run_as', 'permissions', 'presets', 'artifacts', 'bundle', 'sync', 'git', 'cluster_id'];
  const RUN_AS = ['service_principal_name', 'user_name'];
  const PERMISSION = ['level', 'user_name', 'group_name', 'service_principal_name'];
  const JOB = ['name', 'tasks', 'job_clusters', 'schedule', 'trigger', 'email_notifications', 'permissions', 'max_concurrent_runs', 'parameters', 'tags', 'timeout_seconds', 'environments', 'queue', 'continuous', 'health', 'description', 'run_as', 'notification_settings'];
  const TASK = ['task_key', 'depends_on', 'notebook_task', 'spark_python_task', 'python_wheel_task', 'pipeline_task', 'sql_task', 'run_job_task', 'dbt_task', 'condition_task', 'for_each_task', 'job_cluster_key', 'existing_cluster_id', 'new_cluster', 'environment_key', 'libraries', 'timeout_seconds', 'max_retries', 'min_retry_interval_millis', 'retry_on_timeout', 'run_if', 'email_notifications', 'description', 'health'];
  const TASK_TYPES = ['notebook_task', 'spark_python_task', 'python_wheel_task', 'pipeline_task', 'sql_task', 'run_job_task', 'dbt_task', 'condition_task', 'for_each_task'];
  const PIPELINE = ['name', 'catalog', 'schema', 'target', 'libraries', 'serverless', 'continuous', 'development', 'configuration', 'channel', 'edition', 'photon', 'clusters', 'permissions', 'notifications', 'root_path', 'tags', 'event_log'];
  const SCHEDULE = ['quartz_cron_expression', 'timezone_id', 'pause_status'];
  const PERMISSION_LEVELS = ['CAN_VIEW', 'CAN_RUN', 'CAN_MANAGE', 'IS_OWNER', 'CAN_MANAGE_RUN'];

  function validate(text, opts) {
    const o = Object.assign({ target: null, user: 'alice@stagedoor.io', vars: {} }, opts);
    const errors = [], warnings = [];
    const err = (path, msg) => errors.push({ path, msg });
    const warn = (path, msg) => warnings.push({ path, msg });
    let doc;
    try { const docs = SSG.yaml.parseAll(text); doc = docs[0]; if (docs.length > 1) warn('', 'Only the first YAML document is used.'); }
    catch (e) { return { ok: false, errors: [{ path: '', msg: 'YAML syntax error: ' + e.message }], warnings, plan: null }; }
    if (!doc || typeof doc !== 'object' || Array.isArray(doc)) return { ok: false, errors: [{ path: '', msg: 'databricks.yml must be a mapping with a top-level `bundle` key.' }], warnings, plan: null };

    const known = (obj, keys, path) => { if (obj && typeof obj === 'object' && !Array.isArray(obj)) Object.keys(obj).forEach(k => { if (!keys.includes(k)) warn(path ? `${path}.${k}` : k, `unknown field: ${k}${suggest(k, keys)}`); }); };
    const isMap = v => v && typeof v === 'object' && !Array.isArray(v);
    known(doc, TOP, '');
    if (!isMap(doc.bundle) || !doc.bundle.name) err('bundle.name', 'bundle.name is required.');
    else known(doc.bundle, BUNDLE, 'bundle');
    const vars = isMap(doc.variables) ? doc.variables : {};
    Object.entries(vars).forEach(([k, v]) => { if (isMap(v)) known(v, VARIABLE, `variables.${k}`); else err(`variables.${k}`, `variable ${k} must be a mapping with a default and/or description, for example:\n  ${k}:\n    default: ${v == null ? '…' : v}`); });

    // pick the target
    const targets = isMap(doc.targets) ? doc.targets : {};
    Object.entries(targets).forEach(([k, t]) => {
      if (!isMap(t)) { err(`targets.${k}`, 'a target must be a mapping.'); return; }
      known(t, TARGET, `targets.${k}`);
      if (t.mode != null && !['development', 'production'].includes(t.mode)) err(`targets.${k}.mode`, `unsupported value of 'mode': ${t.mode}. Use development or production.`);
      if (isMap(t.workspace)) known(t.workspace, WORKSPACE, `targets.${k}.workspace`);
      if (isMap(t.run_as)) { known(t.run_as, RUN_AS, `targets.${k}.run_as`); if (t.run_as.service_principal_name && t.run_as.user_name) err(`targets.${k}.run_as`, 'run_as must set either service_principal_name or user_name, not both.'); }
      if (isMap(t.variables)) Object.keys(t.variables).forEach(v => { if (!(v in vars)) err(`targets.${k}.variables.${v}`, `variable ${v} is not defined in the top-level variables section.`); });
    });
    const defaults = Object.entries(targets).filter(([, t]) => isMap(t) && t.default === true).map(([k]) => k);
    if (defaults.length > 1) err('targets', `multiple targets are marked default: ${defaults.join(', ')}.`);
    let targetName = o.target || defaults[0] || (Object.keys(targets).length === 1 ? Object.keys(targets)[0] : null);
    if (!targetName && Object.keys(targets).length) err('targets', 'please specify a target with -t: none is marked default: true.');
    if (targetName && Object.keys(targets).length && !targets[targetName]) { err('targets', `no such target: ${targetName}. Available targets: ${Object.keys(targets).join(', ')}.`); targetName = null; }
    const target = targetName ? targets[targetName] || {} : {};
    const mode = target.mode || null;

    // resolve variables
    const values = {};
    Object.entries(vars).forEach(([k, v]) => {
      const tv = isMap(target.variables) ? target.variables[k] : undefined;
      const val = o.vars[k] !== undefined ? o.vars[k] : tv !== undefined ? tv : isMap(v) ? v.default : undefined;
      if (val === undefined || val === null) err(`variables.${k}`, `no value assigned to required variable ${k}. Assignment can be done using "--var", by setting the BUNDLE_VAR_${k} environment variable, or with a default or a target override in databricks.yml.`);
      else values[k] = val;
    });
    const shortName = o.user.split('@')[0].replace(/[^a-z0-9]+/gi, '_');
    const ctxVals = {
      'bundle.name': doc.bundle && doc.bundle.name, 'bundle.target': targetName || 'default',
      'workspace.current_user.userName': o.user, 'workspace.current_user.short_name': shortName,
    };
    const ws = Object.assign({}, isMap(doc.workspace) ? doc.workspace : {}, isMap(target.workspace) ? target.workspace : {});
    if (isMap(doc.workspace)) known(doc.workspace, WORKSPACE, 'workspace');
    ctxVals['workspace.root_path'] = ws.root_path || `/Workspace/Users/${o.user}/.bundle/${ctxVals['bundle.name']}/${ctxVals['bundle.target']}`;
    const resolve = (v, path, resKeys) => {
      if (typeof v === 'string') return v.replace(/\$\{([^}]+)\}/g, (m, ref) => {
        if (ref.startsWith('var.')) { const k = ref.slice(4); if (!(k in vars)) { err(path, `reference does not exist: \${${ref}}. Define it under variables.`); return m; } return k in values ? String(values[k]) : m; }
        if (ref in ctxVals) return String(ctxVals[ref]);
        const rm = /^resources\.(jobs|pipelines|schemas)\.([^.]+)\.(id|name)$/.exec(ref);
        if (rm) { if (!resKeys[rm[1]] || !resKeys[rm[1]].has(rm[2])) { err(path, `reference does not exist: \${${ref}}.`); return m; } return `<${rm[1].slice(0, -1)} ${rm[2]} ${rm[3]}>`; }
        err(path, `reference does not exist: \${${ref}}.`); return m;
      });
      if (Array.isArray(v)) return v.map((x, i) => resolve(x, `${path}[${i}]`, resKeys));
      if (isMap(v)) { const out = {}; Object.keys(v).forEach(k => { out[k] = resolve(v[k], `${path}.${k}`, resKeys); }); return out; }
      return v;
    };

    // merge resources: target overrides are merged into the top-level definitions
    const merge = (a, b) => { if (!isMap(a) || !isMap(b)) return b === undefined ? a : b; const out = Object.assign({}, a); Object.keys(b).forEach(k => { out[k] = merge(a[k], b[k]); }); return out; };
    const resources = merge(isMap(doc.resources) ? doc.resources : {}, isMap(target.resources) ? target.resources : {});
    const resKeys = { jobs: new Set(Object.keys(resources.jobs || {})), pipelines: new Set(Object.keys(resources.pipelines || {})), schemas: new Set(Object.keys(resources.schemas || {})) };
    Object.keys(resources).forEach(k => { if (!['jobs', 'pipelines', 'schemas', 'volumes', 'experiments', 'models', 'registered_models', 'model_serving_endpoints', 'quality_monitors', 'clusters', 'dashboards', 'apps'].includes(k)) warn(`resources.${k}`, `unknown resource type: ${k}`); });

    const plan = { target: targetName, mode, rootPath: resolve(ctxVals['workspace.root_path'], 'workspace.root_path', resKeys), runAs: null, resources: [] };
    const runAs = resolve(target.run_as || doc.run_as || null, 'run_as', resKeys);
    plan.runAs = runAs ? (runAs.service_principal_name ? `service principal ${runAs.service_principal_name}` : `user ${runAs.user_name}`) : `${o.user} (the deploying user)`;
    const prefix = mode === 'development' ? `[dev ${shortName}] ` : '';
    const checkPerms = (perms, path) => (Array.isArray(perms) ? perms : []).forEach((p, i) => {
      if (!isMap(p)) { err(`${path}[${i}]`, 'a permission must be a mapping with level and a principal.'); return; }
      known(p, PERMISSION, `${path}[${i}]`);
      if (!PERMISSION_LEVELS.includes(p.level)) err(`${path}[${i}].level`, `invalid permission level ${p.level}. Use one of ${PERMISSION_LEVELS.join(', ')}.`);
      if (['user_name', 'group_name', 'service_principal_name'].filter(k => p[k]).length !== 1) err(`${path}[${i}]`, 'a permission needs exactly one of user_name, group_name or service_principal_name.');
    });
    checkPerms(doc.permissions, 'permissions'); checkPerms(target.permissions, `targets.${targetName}.permissions`);

    Object.entries(resources.jobs || {}).forEach(([key, job]) => {
      const path = `resources.jobs.${key}`;
      if (!isMap(job)) { err(path, 'a job must be a mapping.'); return; }
      known(job, JOB, path);
      const j = resolve(job, path, resKeys);
      const tasks = Array.isArray(j.tasks) ? j.tasks : [];
      if (!tasks.length) err(`${path}.tasks`, 'a job needs at least one task.');
      const keys = new Set();
      tasks.forEach((t, i) => {
        const tp = `${path}.tasks[${i}]`;
        if (!isMap(t)) { err(tp, 'a task must be a mapping.'); return; }
        known(t, TASK, tp);
        if (!t.task_key) err(`${tp}.task_key`, 'task_key is required.');
        else if (keys.has(t.task_key)) err(`${tp}.task_key`, `duplicate task_key "${t.task_key}" in job ${key}.`);
        else keys.add(t.task_key);
        const types = TASK_TYPES.filter(x => t[x]);
        if (types.length !== 1) err(tp, types.length ? `a task can have only one task type, found ${types.join(' and ')}.` : `task "${t.task_key || i}" has no task type: add one of ${TASK_TYPES.slice(0, 4).join(', ')}, ….`);
        if (t.job_cluster_key && !(j.job_clusters || []).some(c => c.job_cluster_key === t.job_cluster_key)) err(`${tp}.job_cluster_key`, `job_cluster_key "${t.job_cluster_key}" is not defined in job_clusters.`);
        if (t.pipeline_task && !(typeof t.pipeline_task.pipeline_id === 'string' && t.pipeline_task.pipeline_id)) err(`${tp}.pipeline_task.pipeline_id`, 'pipeline_task needs a pipeline_id, usually ${resources.pipelines.<key>.id}.');
      });
      tasks.forEach((t, i) => (Array.isArray(t.depends_on) ? t.depends_on : []).forEach((d, k) => { if (!isMap(d) || !keys.has(d.task_key)) err(`${path}.tasks[${i}].depends_on[${k}]`, `task "${t.task_key}" depends on unknown task "${isMap(d) ? d.task_key : d}". depends_on entries look like - task_key: <key>.`); }));
      const cyc = findCycle(tasks);
      if (cyc) err(`${path}.tasks`, `the task graph has a cycle: ${cyc.join(' → ')}.`);
      if (j.schedule) { known(j.schedule, SCHEDULE, `${path}.schedule`); if (!j.schedule.quartz_cron_expression) err(`${path}.schedule.quartz_cron_expression`, 'quartz_cron_expression is required in a schedule.'); }
      checkPerms(j.permissions, `${path}.permissions`);
      const schedule = j.schedule ? Object.assign({}, j.schedule, mode === 'development' ? { pause_status: 'PAUSED' } : {}) : null;
      plan.resources.push({ type: 'job', key, name: prefix + (j.name || key), tasks: tasks.map(t => ({ key: t.task_key, type: TASK_TYPES.find(x => t[x]), dependsOn: (t.depends_on || []).map(d => d.task_key), detail: t.notebook_task ? t.notebook_task.notebook_path : t.pipeline_task ? t.pipeline_task.pipeline_id : t.spark_python_task ? t.spark_python_task.python_file : '' })), schedule, permissions: j.permissions || [], parameters: j.parameters || [] });
    });
    Object.entries(resources.pipelines || {}).forEach(([key, p]) => {
      const path = `resources.pipelines.${key}`;
      if (!isMap(p)) { err(path, 'a pipeline must be a mapping.'); return; }
      known(p, PIPELINE, path);
      const r = resolve(p, path, resKeys);
      if (r.schema && r.target) err(path, 'set either schema or target, not both (target is the legacy name).');
      if (!r.catalog) warn(`${path}.catalog`, 'no catalog set: the pipeline publishes to the Hive metastore. Set catalog to publish to Unity Catalog.');
      if (!Array.isArray(r.libraries) || !r.libraries.length) err(`${path}.libraries`, 'a pipeline needs at least one library (a notebook, file or glob).');
      plan.resources.push({ type: 'pipeline', key, name: prefix + (r.name || key), catalog: r.catalog || null, schema: r.schema || r.target || null, development: mode === 'development' ? true : !!r.development, serverless: !!r.serverless, continuous: !!r.continuous, configuration: r.configuration || {} });
    });
    Object.entries(resources.schemas || {}).forEach(([key, s]) => {
      const r = resolve(s, `resources.schemas.${key}`, resKeys);
      if (!r.catalog_name || !r.name) err(`resources.schemas.${key}`, 'a schema needs catalog_name and name.');
      plan.resources.push({ type: 'schema', key, name: `${r.catalog_name}.${mode === 'development' ? `dev_${shortName}_` : ''}${r.name}` });
    });
    return { ok: !errors.length, errors, warnings, plan: errors.length ? null : plan, values, targetName, doc };
  }
  function findCycle(tasks) {
    const deps = new Map(tasks.filter(t => t && t.task_key).map(t => [t.task_key, (t.depends_on || []).map(d => d && d.task_key).filter(Boolean)]));
    const state = new Map(), stack = [];
    const visit = k => {
      if (state.get(k) === 2) return null;
      if (state.get(k) === 1) return stack.slice(stack.indexOf(k)).concat(k);
      state.set(k, 1); stack.push(k);
      for (const d of deps.get(k) || []) { if (!deps.has(d)) continue; const c = visit(d); if (c) return c; }
      stack.pop(); state.set(k, 2); return null;
    };
    for (const k of deps.keys()) { const c = visit(k); if (c) return c.reverse(); }
    return null;
  }
  function suggest(k, keys) {
    const lev = (a, b) => { const d = Array.from({ length: a.length + 1 }, (_, i) => [i]); for (let j = 1; j <= b.length; j++) d[0][j] = j; for (let i = 1; i <= a.length; i++) for (let j = 1; j <= b.length; j++) d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1)); return d[a.length][b.length]; };
    const best = keys.map(x => [x, lev(k, x)]).sort((a, b) => a[1] - b[1])[0];
    return best && best[1] <= 3 ? ` (did you mean ${best[0]}?)` : '';
  }

  SSG.bundles = { validate, findCycle };
})(typeof window !== 'undefined' ? window : global);
