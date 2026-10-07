# 1. Spark performance

Most slow Spark jobs are slow for one of a handful of reasons, and the Spark UI tells you which. This chapter is the tuning loop the **Shuffle** lab trains: read the stages, find the dominant cost, fix it at the right level, rerun.

## Read the job as stages and tasks

Spark splits a job into **stages** at every shuffle. Each stage runs as **tasks**, one per partition, spread over the cluster's cores. Three questions find most problems:

1. **Which stage takes the time?** Fix that one first.
2. **How are its task durations spread?** If the slowest task takes many times the median, the problem is skew, and adding hardware will not help.
3. **What did it read, shuffle, spill and write?** Bytes explain time.

The lab's run view shows exactly this: each stage's task-duration spread (min to max, with the median marked), its share of the job, shuffle and spill volumes, and findings.

## Fix in this order

| Level | Examples | Why first |
|---|---|---|
| **1. Data layout** | Compact small files, cluster by filter columns | Benefits every reader, forever |
| **2. Query plan** | Broadcast small tables, built-ins instead of UDFs, filter early | Removes work instead of speeding it up |
| **3. Configuration** | AQE, shuffle partitions, optimized writes | Tunes the work that remains |
| **4. Hardware** | Workers, instance types, Photon | Costs money every run |

## Small files

Opening a file in cloud storage costs tens of milliseconds regardless of its size. A table of 120,000 files of 2 MB spends more time opening than reading.

- Spark packs small files into tasks using `spark.sql.files.maxPartitionBytes` (128 MB) and `spark.sql.files.openCostInBytes` (4 MB, the assumed cost of opening a file), but packing cannot remove the per-file cost.
- **Fix the table:** `OPTIMIZE` compacts files (to about 1 GB by default). Predictive optimization runs it automatically on Unity Catalog managed tables.
- **Prevent it upstream:** optimized writes and auto compaction, or Auto Loader with sensible triggers.

Practise: `small-files`.

## Joins

| Strategy | When | Cost |
|---|---|---|
| **Broadcast hash join** | One side is small | Small side copied to every executor; the large side is never shuffled |
| **Sort-merge join** | Both sides large | Both sides shuffled by key and sorted |

- Spark broadcasts automatically below `spark.sql.autoBroadcastJoinThreshold` (10 MB by default; `-1` disables it). Above it, use a hint: `big.join(broadcast(small), "key")` or `/*+ BROADCAST(small) */`.
- **Broadcasting has limits.** Spark refuses tables over 8 GB ("Cannot broadcast the table that is larger than 8GB"), and well below that a broadcast can exhaust executor memory. Raising the threshold to "make everything fast" is how jobs start crashing.
- **AQE converts joins at runtime:** when a side turns out to be small after filtering, adaptive execution switches to a broadcast join.

Practise: `broadcast-small-dim`, `broadcast-too-big`.

## Shuffle partitions

`spark.sql.shuffle.partitions` defaults to 200 whatever the data size.

- **Too few:** 3 TB ÷ 200 = 15 GB per task, which spills to disk and runs for an hour.
- **Too many:** 2 GB ÷ 200 = 10 MB per task: scheduling overhead dominates, and every task writes a small output file.
- **Rule of thumb:** aim for roughly 100–200 MB per shuffle partition.
- **With AQE on** (the default on Databricks), set a generous number and let it **coalesce** partitions to `spark.sql.adaptive.advisoryPartitionSizeInBytes` (64 MB).

Practise: `too-few-partitions`, `too-many-partitions`.

## Skew

When one key holds a large share of the rows, its partition is processed by a single task while the others finish in seconds.

- **Symptom:** in one stage, the slowest task is far above the median.
- **AQE skew join:** splits partitions larger than `skewedPartitionFactor` (5) × the median and `skewedPartitionThresholdInBytes` (256 MB) into smaller tasks.
- **Salting:** add a random salt to the hot key on the large side and replicate the other side once per salt. It works where AQE cannot see the skew (aggregations, or with AQE off), but simple salting multiplies the other side's shuffle by the salt count, so it suits small and medium tables.
- **More workers never fix skew:** one task runs on one core.

Practise: `skewed-join`.

## Spill

When a task's partition does not fit in its share of executor memory, Spark spills to local disk and reads it back. Spill is a symptom: of partitions that are too large (raise the partition count), of skew (see above), or of memory-hungry operations. Fix the cause rather than buying memory.

## Data skipping

Delta stores min/max statistics per file. A filter on a column the table is **clustered** by excludes most files without opening them; on an unclustered column, every file overlaps every range and must be read.

- **Liquid clustering:** `ALTER TABLE orders CLUSTER BY (order_date)`, then `OPTIMIZE`. `CLUSTER BY AUTO` lets Databricks choose the keys.
- **Filter on the column directly** (`order_date >= …`), not through a function.

Practise: `data-skipping`.

## Python UDFs and Photon

Row-by-row Python UDFs serialize every row to a Python process and back, and **Photon** (the vectorized engine) cannot execute them. Built-in functions cover almost everything (`lower`, `trim`, `regexp_extract`, `from_json`, higher-order functions); pandas UDFs are the fallback when they do not.

Practise: `python-udf`.

## Output files

Every task of the final stage writes at least one file. Ways to control the count:
- AQE coalescing;
- Delta **optimized writes** (`spark.databricks.delta.optimizeWrite.enabled`), which aim for ~128 MB files;
- `coalesce(n)`, which also cuts the final stage's parallelism to `n`.

## Sizing the cluster

Doubling workers roughly halves the parallel part of a job, but stage overheads and the driver do not shrink, so the last doublings cost more than they save. Fix the plan first, then choose the smallest cluster that meets the SLA. Serverless compute moves much of this decision to the platform.

Practise: `right-size`, then the `capstone`.

## How the lab models Spark

The lab estimates task times from per-core throughput constants and schedules tasks onto cores. Its rules (file splitting, broadcast thresholds and limits, AQE coalescing and skew splitting, spill) follow Spark's documented behaviour and defaults, and its numbers are plausible magnitudes, not benchmarks. See [chapter 5](05-about-the-simulators.md).
