/* shuffle/model.js: a simplified Spark execution model for tuning exercises.
   It uses Spark's real planning rules and defaults (file splitting, broadcast thresholds,
   shuffle partitions, AQE coalescing and skew handling, spill) and turns them into stages and
   tasks with estimated durations, scheduled onto the cluster's cores. Absolute times are estimates
   from throughput constants; the relationships (what gets faster, what spills, what fails) are the
   point. The constants are documented in docs/ and in THROUGHPUT below. */
(function (root) {
  'use strict';
  const SSG = root.SSG = root.SSG || {};
  const MB = 1, GB = 1024;

  const DEFAULTS = {
    maxPartitionBytes: 128 * MB,       // spark.sql.files.maxPartitionBytes
    openCostInBytes: 4 * MB,           // spark.sql.files.openCostInBytes
    shufflePartitions: 200,            // spark.sql.shuffle.partitions
    broadcastThreshold: 10 * MB,       // spark.sql.autoBroadcastJoinThreshold (-1 disables)
    aqe: true,                         // spark.sql.adaptive.enabled
    advisoryPartitionSize: 64 * MB,    // spark.sql.adaptive.advisoryPartitionSizeInBytes
    skewFactor: 5,                     // spark.sql.adaptive.skewJoin.skewedPartitionFactor
    skewThreshold: 256 * MB,           // spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes
    broadcastHint: false, salt: 1, compacted: false, clusteredBy: null, udf: 'builtin', photon: false,
    write: 'default', coalesceTo: null, workers: 8, coresPerWorker: 8, memPerWorkerGB: 64, pricePerWorkerHour: 1.2,
  };
  /* Per-core throughput in MB/s and fixed overheads in seconds. */
  const THROUGHPUT = {
    scan: 120, scanPhoton: 260, shuffleWrite: 90, shuffleRead: 110, join: 160, joinPhoton: 380,
    agg: 200, aggPhoton: 480, pythonUdf: 9, write: 80, spill: 70, taskOverhead: 0.06, fileOpen: 0.06,
    stageOverhead: 1.5, broadcastNet: 400,
  };
  const BROADCAST_HARD_LIMIT = 8 * GB;
  const EXPANSION = 3; // in-memory size of deserialized rows vs bytes on disk

  class SparkFailure extends Error { constructor(cls, msg) { super(msg); this.cls = cls; } }

  /* Split files into read tasks the way FilePartition does. */
  function scanTasks(files, totalMB, cfg, cores) {
    const openCost = cfg.openCostInBytes;
    const bytesPerCore = (totalMB + files * openCost) / cores;
    const maxSplit = Math.min(cfg.maxPartitionBytes, Math.max(openCost, bytesPerCore));
    const fileMB = totalMB / Math.max(1, files);
    const splitsPerFile = Math.max(1, Math.ceil(fileMB / maxSplit));
    const pieceMB = fileMB / splitsPerFile;
    const filesPerTask = Math.max(1, Math.floor(maxSplit / (pieceMB + openCost)));
    const pieces = files * splitsPerFile;
    const n = Math.ceil(pieces / filesPerTask);
    const tasks = [];
    let left = pieces;
    for (let i = 0; i < n; i++) { const k = Math.min(filesPerTask, left); left -= k; tasks.push({ mb: k * pieceMB, files: k }); }
    return { tasks, maxSplit };
  }
  /* Assign tasks to cores in partition order (Spark launches tasks in order), return the makespan. */
  function schedule(durations, cores) {
    const free = new Array(Math.min(cores, durations.length)).fill(0);
    if (!free.length) return 0;
    for (const d of durations) { let k = 0; for (let i = 1; i < free.length; i++) if (free[i] < free[k]) k = i; free[k] += d; }
    return Math.max(...free);
  }
  const median = a => { const s = a.slice().sort((x, y) => x - y); return s.length ? s[Math.floor((s.length - 1) / 2)] : 0; };

  /* Shuffle partition sizes for `mb` bytes hashed into `p` partitions, with a share `hot` on one key. */
  function shuffleSizes(mb, p, hot, salt) {
    const sizes = new Array(p).fill(mb * (1 - hot) / p);
    if (hot > 0) { const s = Math.max(1, Math.min(salt, p)); for (let i = 0; i < s; i++) sizes[(7 + i * Math.floor(p / s)) % p] += mb * hot / s; }
    return sizes;
  }
  function coalesce(sizes, target) {
    const out = []; let cur = 0;
    for (const s of sizes) { if (cur > 0 && cur + s > target) { out.push(cur); cur = 0; } cur += s; }
    if (cur > 0 || !out.length) out.push(cur);
    return out;
  }
  function splitSkew(sizes, cfg) {
    const med = median(sizes);
    const limit = Math.max(cfg.skewThreshold, cfg.skewFactor * med);
    const out = []; let split = 0;
    sizes.forEach(s => { if (s > limit) { const k = Math.ceil(s / cfg.advisoryPartitionSize); for (let i = 0; i < k; i++) out.push(s / k); split++; } else out.push(s); });
    return { sizes: out, split };
  }

  function simulate(w, userCfg) {
    const cfg = Object.assign({}, DEFAULTS, w.defaults || {}, userCfg || {});
    const T = THROUGHPUT;
    const cores = cfg.workers * cfg.coresPerWorker;
    const memPerTaskMB = (cfg.memPerWorkerGB * GB / cfg.coresPerWorker) * 0.5;
    const stages = [], findings = [];
    const taskTime = (mbIn, mbOut, opts) => {
      const o = opts || {};
      let t = T.taskOverhead + (o.files || 0) * T.fileOpen;
      if (o.scan) t += mbIn / (cfg.photon ? T.scanPhoton : T.scan);
      if (o.read) t += mbIn / T.shuffleRead;
      if (o.join) t += mbIn / (cfg.photon ? T.joinPhoton : T.join);
      if (o.agg) t += mbIn / (cfg.photon ? T.aggPhoton : T.agg);
      if (o.udf) t += mbIn * o.udf / T.pythonUdf;
      if (o.write) t += mbOut / T.write;
      if (o.shuffle) t += mbOut / T.shuffleWrite;
      let spill = 0;
      if (o.memMB && o.memMB * EXPANSION > memPerTaskMB) { spill = o.memMB - memPerTaskMB / EXPANSION; t += 2 * spill / T.spill; }
      return { t, spill };
    };
    const addStage = (name, tasks, extra) => {
      const d = tasks.map(x => x.t);
      const time = schedule(d, cores) + T.stageOverhead;
      const st = Object.assign({ name, tasks: d.length, time, durations: d, median: median(d), max: d.length ? Math.max(...d) : 0, spillMB: tasks.reduce((a, x) => a + (x.spill || 0), 0) }, extra || {});
      stages.push(st); return st;
    };

    /* 1. scan the fact table, with data skipping when clustered on the filter column */
    const f = w.fact;
    let files = cfg.compacted ? Math.max(1, Math.ceil(f.sizeMB / (f.compactTargetMB || 1024))) : f.files;
    const skipping = f.filterColumn && cfg.clusteredBy === f.filterColumn;
    const readFraction = skipping ? Math.min(1, f.selectivity * 1.15 + 0.002) : 1;
    const readMB = f.sizeMB * readFraction;
    files = Math.max(1, Math.round(files * readFraction));
    const afterFilterMB = f.sizeMB * (f.selectivity == null ? 1 : f.selectivity);
    const scan = scanTasks(files, readMB, cfg, cores);
    const keep = afterFilterMB / readMB;

    /* 2. the join strategy */
    const d = w.dim;
    let strategy = 'none', converted = false;
    if (d) {
      const staticDim = d.sizeMB, runtimeDim = d.sizeMB * (d.selectivity == null ? 1 : d.selectivity);
      const canBroadcast = cfg.broadcastThreshold >= 0 && staticDim <= cfg.broadcastThreshold;
      strategy = cfg.broadcastHint || canBroadcast ? 'broadcast' : 'sortMerge';
      if (strategy === 'sortMerge' && cfg.aqe && cfg.broadcastThreshold >= 0 && runtimeDim <= cfg.broadcastThreshold) converted = true;
      if (strategy === 'broadcast') {
        const bMB = runtimeDim;
        if (bMB > BROADCAST_HARD_LIMIT) throw new SparkFailure('BROADCAST_TOO_LARGE', `Cannot broadcast the table that is larger than 8GB: ${(bMB / GB).toFixed(1)} GB.`);
        if (bMB * EXPANSION > cfg.memPerWorkerGB * GB * 0.3) throw new SparkFailure('SparkOutOfMemoryError', `Not enough memory to build and broadcast the table to all worker nodes. As a workaround, you can either disable broadcast by setting spark.sql.autoBroadcastJoinThreshold to -1 or increase the spark driver memory by setting spark.driver.memory to a higher value. (Broadcasting ${(bMB / GB).toFixed(1)} GB, about ${(bMB * EXPANSION / GB).toFixed(0)} GB in memory, on ${cfg.memPerWorkerGB} GB workers.)`);
      }
    }
    const udfFactor = w.udf && cfg.udf === 'python' ? 1 : 0;
    if (w.udf && cfg.udf === 'python' && cfg.photon) findings.push({ level: 'warn', text: 'Photon cannot run Python UDFs: the UDF part of the plan falls back to the JVM and Python workers.' });

    // stage: scan fact (+ broadcast join and partial aggregation, or shuffle write)
    const outFact = afterFilterMB * (w.projection || 1);
    if (strategy === 'broadcast') {
      const bMB = d.sizeMB * (d.selectivity == null ? 1 : d.selectivity);
      const bcast = bMB / T.broadcastNet * Math.log2(cfg.workers + 1);
      const tasks = scan.tasks.map(x => { const mbIn = x.mb * keep * (w.projection || 1); const r = taskTime(x.mb, mbIn * (w.aggReduction || 0.01), { scan: true, files: x.files, join: true, agg: true, udf: udfFactor, shuffle: true }); return { t: r.t + bcast / Math.max(1, scan.tasks.length / cores), spill: 0 }; });
      addStage(`Scan ${f.name} + broadcast join with ${d.name}`, tasks, { inputMB: readMB, shuffleWriteMB: outFact * (w.aggReduction || 0.01), broadcastMB: bMB });
    } else {
      const tasks = scan.tasks.map(x => taskTime(x.mb, x.mb * keep * (w.projection || 1), { scan: true, files: x.files, udf: d ? 0 : udfFactor, agg: !d, shuffle: true }));
      addStage(`Scan ${f.name}${d ? '' : ' + partial aggregate'}`, tasks, { inputMB: readMB, shuffleWriteMB: d ? outFact : outFact * (w.aggReduction || 0.01) });
    }
    if (strategy === 'sortMerge') {
      const dimMB = d.sizeMB * (d.selectivity == null ? 1 : d.selectivity);
      const ds = scanTasks(d.files || Math.ceil(d.sizeMB / 128), d.sizeMB, cfg, cores);
      addStage(`Scan ${d.name}`, ds.tasks.map(x => taskTime(x.mb, x.mb * (d.selectivity == null ? 1 : d.selectivity), { scan: true, files: x.files, shuffle: true })), { inputMB: d.sizeMB, shuffleWriteMB: dimMB });
      // the join stage
      const p = cfg.shufflePartitions;
      let sizes = shuffleSizes(outFact, p, w.hotKeyShare || 0, cfg.salt);
      // simple salting replicates the whole dimension once per salt value
      const dimPer = dimMB * Math.max(1, cfg.salt) / p;
      let note = [];
      if (converted) {
        note.push(`AQE converted the sort-merge join to a broadcast join at runtime: ${d.name} is only ${dimMB.toFixed(0)} MB after filtering.`);
        sizes = coalesce(sizes, cfg.advisoryPartitionSize * 4).length ? sizes : sizes;
        const tasks = sizes.map(s => taskTime(s, s * (w.aggReduction || 0.01), { read: true, join: true, agg: true, udf: udfFactor, shuffle: true }));
        addStage(`Join with ${d.name} (broadcast, converted by AQE)`, tasks, { shuffleReadMB: outFact, shuffleWriteMB: outFact * (w.aggReduction || 0.01), note });
      } else {
        let splitCount = 0;
        if (cfg.aqe) {
          const r = splitSkew(sizes, cfg); splitCount = r.split;
          if (splitCount) note.push(`AQE split ${splitCount} skewed partition${splitCount > 1 ? 's' : ''} into ${r.sizes.length - sizes.length + splitCount} tasks.`);
          sizes = r.sizes;
          const before = sizes.length;
          if (!splitCount) { sizes = coalesce(sizes, cfg.advisoryPartitionSize); if (sizes.length < before) note.push(`AQE coalesced ${before} shuffle partitions into ${sizes.length}.`); }
        }
        if (cfg.salt > 1) note.push(`Salting spreads the hot key over ${cfg.salt} partitions, at the cost of replicating ${d.name} ${cfg.salt}× (${fmtMB(dimMB * cfg.salt)} shuffled instead of ${fmtMB(dimMB)}).`);
        const tasks = sizes.map(s => taskTime(s + dimPer, s * (w.aggReduction || 0.01), { read: true, join: true, agg: true, udf: udfFactor, shuffle: true, memMB: s + dimPer }));
        addStage(`Sort-merge join with ${d.name}`, tasks, { shuffleReadMB: outFact + dimMB, shuffleWriteMB: outFact * (w.aggReduction || 0.01), note });
      }
    }
    /* final stage: aggregate (if any) and write */
    const resultMB = w.resultMB != null ? w.resultMB : outFact * (w.aggReduction || 0.01);
    const lastIn = stages[stages.length - 1].shuffleWriteMB;
    let p = cfg.shufflePartitions;
    let finalSizes = shuffleSizes(lastIn, p, 0, 1);
    const finalNote = [];
    if (cfg.aqe) { const before = finalSizes.length; finalSizes = coalesce(finalSizes, cfg.advisoryPartitionSize); if (finalSizes.length < before) finalNote.push(`AQE coalesced ${before} shuffle partitions into ${finalSizes.length}.`); }
    let outFiles = finalSizes.length;
    let writeTasks = finalSizes.map(s => taskTime(s, resultMB / finalSizes.length, { read: true, agg: true, write: true, memMB: s }));
    if (cfg.write === 'coalesce' && cfg.coalesceTo) {
      const n = Math.max(1, Math.min(cfg.coalesceTo, finalSizes.length));
      writeTasks = Array.from({ length: n }, () => taskTime(lastIn / n, resultMB / n, { read: true, agg: true, write: true, memMB: lastIn / n }));
      outFiles = n; finalNote.push(`coalesce(${n}) writes ${n} file${n > 1 ? 's' : ''} from ${n} task${n > 1 ? 's' : ''}.`);
    } else if (cfg.write === 'optimized') {
      outFiles = Math.max(1, Math.ceil(resultMB / 128));
      writeTasks = finalSizes.map(s => taskTime(s, resultMB / finalSizes.length, { read: true, agg: true, write: true, memMB: s }));
      writeTasks.forEach(x => { x.t += 0.4; });
      finalNote.push(`Optimized writes add a shuffle so each file is about 128 MB: ${outFiles} file${outFiles > 1 ? 's' : ''}.`);
    }
    addStage(`Aggregate and write ${w.output || 'result'}`, writeTasks, { shuffleReadMB: lastIn, outputMB: resultMB, outputFiles: outFiles, note: finalNote });

    /* totals and diagnostics */
    const runtime = stages.reduce((a, s) => a + s.time, 0) + (w.driverOverhead || 6);
    const cost = cfg.workers * cfg.pricePerWorkerHour * runtime / 3600;
    const spillMB = stages.reduce((a, s) => a + s.spillMB, 0);
    stages.forEach(s => {
      if (s.tasks > 4 && s.max > 5 * s.median && s.max > 20) findings.push({ level: 'bad', stage: s.name, text: `Skew in "${s.name}": the slowest task takes ${fmt(s.max)}, ${Math.round(s.max / s.median)}× the median (${fmt(s.median)}).` });
      if (s.spillMB > 0) findings.push({ level: 'bad', stage: s.name, text: `"${s.name}" spilled ${fmtMB(s.spillMB)} to disk: partitions are larger than task memory (about ${fmtMB(memPerTaskMB / EXPANSION)} of data per task).` });
      if (s.tasks >= 1000 && s.median < 0.5) findings.push({ level: 'warn', stage: s.name, text: `"${s.name}" runs ${s.tasks.toLocaleString()} tiny tasks (median ${fmt(s.median)}): scheduling overhead dominates.` });
      if (s.tasks < cores / 2 && s.time > 60) findings.push({ level: 'warn', stage: s.name, text: `"${s.name}" uses ${s.tasks} task${s.tasks === 1 ? '' : 's'} on ${cores} cores: most of the cluster is idle.` });
    });
    if (!cfg.compacted && f.files > 5000 && f.sizeMB / f.files < 16) findings.push({ level: 'bad', text: `${f.name} has ${f.files.toLocaleString()} files averaging ${fmtMB(f.sizeMB / f.files)}: opening files costs more than reading them. Compact it (OPTIMIZE).` });
    if (f.filterColumn && !skipping) findings.push({ level: 'warn', text: `The filter on ${f.filterColumn} keeps ${(100 * f.selectivity).toFixed(1)}% of rows but every file of ${f.name} is read: it is not clustered by ${f.filterColumn}, so no files can be skipped.` });
    if (strategy === 'sortMerge' && !converted && d.sizeMB * (d.selectivity == null ? 1 : d.selectivity) < 1024) findings.push({ level: 'warn', text: `${d.name} is small (${fmtMB(d.sizeMB * (d.selectivity == null ? 1 : d.selectivity))}) but is joined with a sort-merge join that shuffles ${fmtMB(outFact)} of ${f.name}. A broadcast join would avoid that shuffle.` });
    if (udfFactor) findings.push({ level: 'warn', text: 'A Python UDF processes every row: rows are serialized to Python workers and back. A built-in function does the same work natively.' });
    if (outFiles > 200 && resultMB / outFiles < 16) findings.push({ level: 'warn', text: `The output is ${outFiles.toLocaleString()} files averaging ${fmtMB(resultMB / outFiles)}: the next reader inherits a small-files problem.` });
    return { cfg, stages, runtime, cost, spillMB, outputFiles: outFiles, readMB, strategy: converted ? 'broadcast (AQE)' : strategy, findings, cores, failed: null };
  }
  function run(w, cfg) {
    try { return simulate(w, cfg); }
    catch (e) { if (e instanceof SparkFailure) return { failed: { cls: e.cls, message: e.message }, stages: [], findings: [], cfg: Object.assign({}, DEFAULTS, w.defaults || {}, cfg) }; throw e; }
  }
  const fmt = s => (s >= 3600 ? `${(s / 3600).toFixed(1)} h` : s >= 60 ? `${(s / 60).toFixed(1)} min` : s >= 1 ? `${s.toFixed(1)} s` : `${Math.round(s * 1000)} ms`);
  const fmtMB = mb => (mb >= GB * 1024 ? `${(mb / GB / 1024).toFixed(1)} TB` : mb >= GB ? `${(mb / GB).toFixed(1)} GB` : mb >= 1 ? `${mb.toFixed(0)} MB` : `${(mb * 1024).toFixed(0)} KB`);

  SSG.shuffle = { run, DEFAULTS, THROUGHPUT, scanTasks, schedule, shuffleSizes, coalesce, splitSkew, fmt, fmtMB, GB, MB };
})(typeof window !== 'undefined' ? window : global);
