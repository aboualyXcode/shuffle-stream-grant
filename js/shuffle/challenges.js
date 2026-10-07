/* shuffle/challenges.js: Spark tuning challenges. Each has a workload, the configuration the job
   runs with today, the knobs you may change, and goals. Tests require the baseline to miss the goals
   and the reference solution to meet them. */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};
  const GB = 1024;
  const mb = v => (v < 0 ? '-1' : v >= GB ? `${v / GB}g` : `${v}m`);

  /* Every knob: how it is shown, its options, and the code it corresponds to. */
  const KNOBS = {
    shufflePartitions: { label: 'Shuffle partitions', options: [50, 200, 800, 2000, 4000, 8000], code: v => `spark.conf.set("spark.sql.shuffle.partitions", ${v})` },
    aqe: { label: 'Adaptive query execution', options: [true, false], show: v => (v ? 'on' : 'off'), code: v => `spark.conf.set("spark.sql.adaptive.enabled", ${v})` },
    broadcastThreshold: { label: 'Auto-broadcast threshold', options: [-1, 10, 64, 256, 1024, 16 * GB], show: v => (v < 0 ? 'disabled (-1)' : v >= GB ? `${v / GB} GB` : `${v} MB`), code: v => `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "${mb(v)}")` },
    broadcastHint: { label: 'Broadcast hint on the dimension', options: [false, true], show: v => (v ? 'broadcast(dim)' : 'none'), code: (v, w) => (v ? `orders.join(broadcast(${w.dim ? w.dim.name : 'dim'}), "key")` : '') },
    salt: { label: 'Salt the hot key', options: [1, 4, 8, 16, 32, 64], show: v => (v === 1 ? 'no salting' : `${v} salts`), code: v => (v > 1 ? `# add salt = floor(rand() * ${v}) to the fact side, explode 0..${v - 1} on the dimension side, join on (key, salt)` : '') },
    compacted: { label: 'Compact the input first', options: [false, true], show: v => (v ? 'OPTIMIZE before the job' : 'no'), code: (v, w) => (v ? `spark.sql("OPTIMIZE ${w.fact.name}")` : '') },
    clusteredBy: { label: 'Liquid clustering on the input', options: null, show: v => v || 'none', code: (v, w) => (v ? `spark.sql("ALTER TABLE ${w.fact.name} CLUSTER BY (${v})"); spark.sql("OPTIMIZE ${w.fact.name}")` : '') },
    udf: { label: 'Transformation', options: ['python', 'builtin'], show: v => (v === 'python' ? 'Python UDF' : 'built-in functions'), code: v => (v === 'python' ? '@udf("string")\ndef normalize(s): return s.strip().lower()' : 'F.lower(F.trim("email"))') },
    photon: { label: 'Photon', options: [false, true], show: v => (v ? 'on' : 'off'), code: v => (v ? '# cluster: runtime_engine = PHOTON' : '') },
    write: { label: 'Output files', options: ['default', 'coalesce', 'optimized'], show: v => ({ default: 'one per task', coalesce: 'coalesce(n)', optimized: 'optimized writes' })[v], code: v => (v === 'optimized' ? 'spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", true)' : '') },
    coalesceTo: { label: 'coalesce(n)', options: [1, 4, 16, 64], show: v => String(v), code: () => '' },
    workers: { label: 'Workers (8 cores, 64 GB each)', options: [2, 4, 8, 16, 32, 64], show: v => String(v), code: v => `# cluster: num_workers = ${v}` },
  };

  const CH = [];
  const add = c => CH.push(Object.assign({ lab: 'shuffle', goals: [], hints: [] }, c));

  add({
    id: 'small-files', level: 1, title: '120,000 tiny files',
    scenario: 'An hourly export lands thousands of small JSON-converted Parquet files in <code>events_raw</code>. The daily rollup scans the whole table and has crept up to several minutes.',
    task: 'Bring the job under <b>2.5 minutes</b> without adding workers.',
    workload: { fact: { name: 'events_raw', sizeMB: 200 * GB, files: 120000 }, aggReduction: 0.01, output: 'daily_events' },
    baseline: { workers: 8 }, knobs: ['compacted', 'shufflePartitions', 'aqe'],
    goals: [{ metric: 'runtime', max: 150 }],
    solution: { compacted: true },
    hints: ['Look at the scan stage: how many files does each task open, and what does opening a file cost?', 'OPTIMIZE rewrites small files into large ones (about 1 GB by default).'],
    explain: 'Opening a file in cloud storage costs tens of milliseconds regardless of its size, so 120,000 small files spend more time opening than reading. Compacting with <code>OPTIMIZE</code> (or letting predictive optimization do it) fixes the table for every reader, not just this job. Upstream, Auto Loader with optimized writes avoids creating the small files in the first place.',
  });
  add({
    id: 'broadcast-small-dim', level: 1, title: 'Shuffling 400 GB to join 40 MB',
    scenario: 'Daily revenue joins 400 GB of <code>orders</code> to a 40 MB <code>customers</code> table. Because 40 MB is above the default broadcast threshold, Spark shuffles both sides.',
    task: 'Get the job under <b>2.5 minutes</b>.',
    workload: { fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'customers', sizeMB: 40, files: 1 }, projection: 0.6, aggReduction: 0.002, output: 'daily_revenue' },
    baseline: {}, knobs: ['broadcastHint', 'broadcastThreshold', 'shufflePartitions', 'aqe'],
    goals: [{ metric: 'runtime', max: 150 }],
    solution: { broadcastHint: true },
    hints: ['Which join strategy did Spark pick, and why? Compare the dimension size with spark.sql.autoBroadcastJoinThreshold.', 'A broadcast join sends the small table to every executor, so the large table is never shuffled.'],
    explain: 'Below <code>spark.sql.autoBroadcastJoinThreshold</code> (10 MB by default) Spark broadcasts automatically. Above it, a <code>broadcast()</code> hint or a higher threshold does the same. A shuffle of the large side is usually the most expensive thing in a join, so avoiding it is often the biggest single win.',
  });
  add({
    id: 'broadcast-too-big', level: 2, title: 'The job that crashed after a "quick fix"',
    scenario: 'After the last incident, someone raised the broadcast threshold to 16 GB "so joins are always fast". Tonight the job joins <code>orders</code> to the 12 GB <code>sessions</code> table and fails.',
    task: 'Make the job <b>succeed</b> and finish under <b>12 minutes</b>.',
    workload: { fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'sessions', sizeMB: 12 * GB, files: 96 }, projection: 0.6, aggReduction: 0.002, output: 'session_revenue' },
    baseline: { broadcastThreshold: 16 * GB }, knobs: ['broadcastThreshold', 'broadcastHint', 'shufflePartitions', 'aqe'],
    goals: [{ metric: 'failed', equals: false }, { metric: 'runtime', max: 720 }],
    solution: { broadcastThreshold: 10 },
    hints: ['Read the error: Spark has a hard limit on broadcast size.', 'Broadcasting only makes sense for tables that fit comfortably in every executor\'s memory.'],
    explain: 'Broadcast tables are collected to the driver and copied to every executor, and Spark refuses tables over 8 GB outright. Well below that, they can still exhaust executor memory. Keep the threshold modest, broadcast deliberately with hints, and let AQE convert joins at runtime when a side turns out to be small.',
  });
  add({
    id: 'skewed-join', level: 2, title: 'One customer, 30% of the rows',
    scenario: 'A marketplace partner places 30% of all orders under a single customer ID. The join stage now has 199 tasks that finish in seconds and one that runs for an hour. AQE was disabled years ago "for predictability".',
    task: 'Get the job under <b>6 minutes</b> with <b>no spill</b>.',
    workload: { fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'customers', sizeMB: 40 * GB, files: 320 }, hotKeyShare: 0.3, projection: 0.6, aggReduction: 0.002, output: 'customer_revenue' },
    baseline: { aqe: false }, knobs: ['aqe', 'salt', 'shufflePartitions', 'workers'],
    goals: [{ metric: 'runtime', max: 360 }, { metric: 'spillMB', max: 0 }],
    solution: { aqe: true },
    alt: { aqe: true, shufflePartitions: 800 },
    hints: ['In the join stage, compare the slowest task with the median.', 'AQE\'s skew join handling splits oversized partitions automatically. Salting is the manual alternative, but it replicates the whole dimension once per salt: look at what that costs with a 40 GB table.', 'Adding workers does not help: one task can only run on one core.'],
    explain: 'Skew shows up as one straggler task: max task time far above the median. AQE splits partitions larger than 5× the median and 256 MB into smaller tasks. Salting spreads the hot key by hand when AQE cannot see the skew, for example in aggregations or when it is disabled. More hardware never fixes skew.',
  });
  add({
    id: 'too-few-partitions', level: 2, title: '3 TB through 200 partitions',
    scenario: 'The clickstream sessionization job joins 3 TB of clicks. With the default 200 shuffle partitions, each join task holds about 15 GB and spills heavily.',
    task: 'Eliminate the <b>spill</b> and get under <b>45 minutes</b>. AQE stays off: it is disabled on this legacy cluster.',
    workload: { fact: { name: 'clicks', sizeMB: 3000 * GB, files: 24000 }, dim: { name: 'pages', sizeMB: 2 * GB, files: 16 }, projection: 1, aggReduction: 0.001, output: 'sessions' },
    baseline: { aqe: false, broadcastThreshold: 10 }, knobs: ['shufflePartitions', 'workers'], fixed: { aqe: false },
    goals: [{ metric: 'spillMB', max: 0 }, { metric: 'runtime', max: 2700 }],
    solution: { shufflePartitions: 8000 },
    hints: ['Divide the shuffled data by the number of partitions: how big is each task?', 'A common target is 100–200 MB per shuffle partition.'],
    explain: 'Shuffle partitions should be sized by data, not left at 200: 3 TB ÷ 200 is 15 GB per task, far beyond task memory, so every task spills to disk. Aim for roughly 100–200 MB per partition. With AQE on, set a generous number and let it coalesce.',
  });
  add({
    id: 'too-many-partitions', level: 1, title: '200 files for 1 GB of output',
    scenario: 'A small refunds rollup writes its result with the default 200 shuffle partitions, on a cluster where AQE is off. Every run leaves 200 tiny files that the BI tool then has to open.',
    task: 'Write the result as <b>at most 20 files</b>, with the job under <b>1 minute</b>.',
    workload: { fact: { name: 'refunds', sizeMB: 2 * GB, files: 16 }, aggReduction: 0.5, output: 'refund_summary' },
    baseline: { aqe: false }, knobs: ['aqe', 'shufflePartitions', 'write', 'coalesceTo'],
    goals: [{ metric: 'outputFiles', max: 20 }, { metric: 'runtime', max: 60 }],
    solution: { aqe: true },
    alt: { aqe: false, write: 'optimized' },
    hints: ['Each task of the final stage writes at least one file.', 'AQE coalesces small shuffle partitions; optimized writes target ~128 MB files.'],
    explain: 'Small output files make every downstream read slower. AQE coalesces small partitions to the advisory size (64 MB), and Delta optimized writes add a shuffle so files land near 128 MB. <code>coalesce(n)</code> also works, but it reduces the parallelism of the whole final stage.',
  });
  add({
    id: 'data-skipping', level: 2, title: 'Reading three years to get one week',
    scenario: 'The weekly report filters <code>orders</code> to the last 7 days, but the 1 TB table is not organized by date, so every file is scanned.',
    task: 'Read <b>less than 30 GB</b> and finish under <b>1 minute</b>.',
    workload: { fact: { name: 'orders', sizeMB: 1000 * GB, files: 8000, filterColumn: 'order_date', selectivity: 0.007 }, aggReduction: 0.01, output: 'weekly_report' },
    baseline: {}, knobs: ['clusteredBy', 'compacted', 'workers', 'photon'], options: { clusteredBy: [null, 'customer_id', 'order_date'] },
    goals: [{ metric: 'readMB', max: 30 * GB }, { metric: 'runtime', max: 60 }],
    solution: { clusteredBy: 'order_date' },
    hints: ['A filter only skips files when the file statistics can exclude them.', 'Liquid clustering co-locates rows with similar values of the clustering keys.'],
    explain: 'Delta keeps min/max statistics per file. When a table is clustered by the filter column, most files can be excluded without being opened. Clustering by the wrong column, or not at all, leaves every file overlapping every date range. Cluster by the columns your queries filter and join on.',
  });
  add({
    id: 'python-udf', level: 2, title: 'The UDF that ate the cluster',
    scenario: 'An email-normalization step was written as a Python UDF. On 400 GB of orders it now dominates the run, and turning Photon on did not help.',
    task: 'Get the job under <b>4 minutes</b> on the same cluster.',
    workload: { fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'customers', sizeMB: 8, files: 1 }, udf: true, projection: 0.6, aggReduction: 0.002, output: 'clean_orders' },
    baseline: { udf: 'python', photon: true }, knobs: ['udf', 'photon', 'workers'],
    goals: [{ metric: 'runtime', max: 240 }],
    solution: { udf: 'builtin', photon: true },
    hints: ['Where does the time go in the stage with the UDF?', 'lower(trim(email)) does the same thing natively.'],
    explain: 'Python UDFs serialize every row to a Python worker and back, and Photon cannot execute them. Built-in functions run in the engine, vectorized. When no built-in exists, a pandas (vectorized) UDF is much cheaper than a row-by-row one.',
  });
  add({
    id: 'right-size', level: 3, title: 'Meet the SLA for the least money',
    scenario: 'The nightly job must finish within 10 minutes. Today it runs on 64 workers, and finance wants to know why it costs so much.',
    task: 'Finish within <b>10 minutes</b> for <b>at most $0.60</b> per run.',
    workload: { fact: { name: 'orders', sizeMB: 400 * GB, files: 3200 }, dim: { name: 'customers', sizeMB: 40, files: 1 }, projection: 0.6, aggReduction: 0.002, output: 'nightly_revenue' },
    baseline: { workers: 64 }, knobs: ['workers', 'broadcastHint', 'photon', 'shufflePartitions'],
    goals: [{ metric: 'runtime', max: 600 }, { metric: 'cost', max: 0.6 }],
    solution: { workers: 4, broadcastHint: true },
    hints: ['Double the workers and the time roughly halves, but stage overheads and the driver do not shrink. Cost = workers × price × time.', 'Fixing the plan first makes every worker count go further.'],
    explain: 'Scaling out is close to linear only while the work parallelizes; fixed overheads mean the last doublings cost more than they save. Fix the plan first (here, the broadcast join), then pick the smallest cluster that meets the SLA. Serverless jobs and autoscaling move part of this decision to the platform.',
  });
  add({
    id: 'capstone', level: 3, title: 'Capstone: everything at once',
    scenario: 'The partner settlement job has every problem in this track: 90,000 small files, a 40 MB dimension that is shuffled, a 25% hot key, a Python UDF, and 200 tiny output files. It runs on a legacy cluster with AQE off.',
    task: 'Get it under <b>4 minutes</b>, with <b>no spill</b> and <b>at most 16 output files</b>, on at most 8 workers.',
    workload: { fact: { name: 'settlements', sizeMB: 300 * GB, files: 90000 }, dim: { name: 'partners', sizeMB: 40, files: 1 }, hotKeyShare: 0.25, udf: true, projection: 0.7, aggReduction: 0.0005, output: 'partner_settlements' },
    baseline: { aqe: false, udf: 'python' }, knobs: ['compacted', 'broadcastHint', 'aqe', 'salt', 'udf', 'photon', 'write', 'shufflePartitions', 'workers'], options: { workers: [2, 4, 8] },
    goals: [{ metric: 'runtime', max: 240 }, { metric: 'spillMB', max: 0 }, { metric: 'outputFiles', max: 16 }],
    solution: { compacted: true, broadcastHint: true, aqe: true, udf: 'builtin', photon: true },
    hints: ['Fix one finding at a time, starting with the largest stage.', 'A broadcast join also removes the skew problem: the hot key never has to be shuffled.'],
    explain: 'Real tuning is a loop: read the stages, fix the dominant cost, rerun. Notice that the broadcast join made salting unnecessary: removing a shuffle removes its skew too. Order matters: plan fixes (join strategy, data layout, built-ins) before configuration, and configuration before hardware.',
  });

  function measure(result, metric) {
    if (metric === 'failed') return !!result.failed;
    if (result.failed) return null;
    return { runtime: result.runtime, cost: result.cost, spillMB: result.spillMB, outputFiles: result.outputFiles, readMB: result.readMB }[metric];
  }
  function check(ch, cfg) {
    const r = SSG.shuffle.run(ch.workload, Object.assign({}, ch.baseline, cfg, ch.fixed || {}));
    const goals = ch.goals.map(g => {
      const v = measure(r, g.metric);
      const met = g.metric === 'failed' ? v === g.equals : v != null && (g.max == null || v <= g.max + 1e-9) && (g.min == null || v >= g.min);
      return Object.assign({ value: v, met }, g);
    });
    return { result: r, goals, ok: goals.every(g => g.met) };
  }
  /* The job as code, reflecting the current knob settings. */
  function code(ch, cfg) {
    const c = Object.assign({}, SSG.shuffle.DEFAULTS, ch.baseline, cfg, ch.fixed || {});
    const w = ch.workload, lines = [];
    ch.knobs.forEach(k => { const line = KNOBS[k].code(c[k], w); if (line && !['broadcastHint', 'udf', 'salt', 'coalesceTo'].includes(k)) lines.push(line); });
    lines.push('');
    lines.push(`orders = spark.table("${w.fact.name}")${w.fact.filterColumn ? `.where("${w.fact.filterColumn} >= current_date() - INTERVAL 7 DAYS")` : ''}`);
    if (w.udf) lines.push(`orders = orders.withColumn("email", ${c.udf === 'python' ? 'normalize("email")   # Python UDF' : 'F.lower(F.trim("email"))'})`);
    if (w.dim) {
      if (c.salt > 1) lines.push(KNOBS.salt.code(c.salt));
      lines.push(`joined = orders.join(${c.broadcastHint ? `broadcast(spark.table("${w.dim.name}"))` : `spark.table("${w.dim.name}")`}, "key")`);
    } else lines.push('joined = orders');
    lines.push('result = joined.groupBy("day", "key").agg(F.sum("amount").alias("amount"))');
    lines.push(c.write === 'coalesce' ? `result.coalesce(${c.coalesceTo || 1}).write.mode("overwrite").saveAsTable("${w.output}")` : `result.write.mode("overwrite").saveAsTable("${w.output}")`);
    return lines.join('\n');
  }

  SSG.shuffleLab = { KNOBS, CHALLENGES: CH, check, code };
})(typeof window !== 'undefined' ? window : global);
