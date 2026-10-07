# 5. About the simulators

Everything in the playground runs in your browser. Each lab is backed by a simulator written for teaching. This chapter says exactly what each one models faithfully, what it simplifies, and how it is tested.

## Shuffle: the Spark model (`js/shuffle/model.js`)

**Faithful rules and defaults**
- File splitting the way Spark's `FilePartition` packs files: `maxPartitionBytes` (128 MB), `openCostInBytes` (4 MB) and bytes per core.
- Broadcast decisions: `autoBroadcastJoinThreshold` (10 MB, `-1` disables), hints, AQE runtime conversion, the 8 GB hard limit and Spark's out-of-memory message.
- AQE coalescing to the advisory size (64 MB), and skew splitting above 5 × median and 256 MB.
- Default `shuffle.partitions` (200), and data skipping when a table is clustered by the filter column.
- Output files per final-stage task, `coalesce(n)`, and optimized writes (~128 MB files).
- Tasks scheduled onto cores in partition order, so stragglers dominate a stage as they do on a real cluster.

**Estimates**
- Task durations come from per-core throughput constants (scan, shuffle, join, aggregation, Python UDF, spill) plus per-task and per-file overheads, listed in `THROUGHPUT`. Photon is modelled as higher throughput for built-in operations.
- Spill begins when a partition's in-memory size (3× its bytes) exceeds half of a core's share of worker memory.
- Cost is workers × price × runtime, ignoring the driver, DBU types and autoscaling.

Absolute times are plausible magnitudes, not predictions for your cluster. The relationships are what the model is for: what gets faster, what spills, what fails, and why.

## Stream: the Structured Streaming simulator (`js/stream/engine.js`)

**Faithful rules**
- **Batching:** processing-time and `availableNow` triggers group events by arrival.
- **Watermark:** max event time minus the delay, computed at the end of a batch, used from the next, never decreasing; plus the no-data micro-batch that finalizes state immediately.
- **Late rows:** a row is dropped when every window it belongs to has already been finalized.
- **Output modes:** append, update and complete, with the state eviction each one implies, and the error for append mode on an aggregation without a watermark.
- **Deduplication:** `dropDuplicates` with and without a watermark, and `dropDuplicatesWithinWatermark`.
- **AUTO CDC semantics:** ordering by `SEQUENCE BY` regardless of arrival, deletes, SCD Types 1 and 2.
- **Auto Loader:** the four schema evolution modes, with job retries.
- **Checkpoints:** restart semantics with stable and new checkpoint locations, into append and MERGE sinks.

**Simplified**
- Only tumbling and sliding window counts, plus deduplication. No stream-stream joins, `transformWithState`, multiple watermarks or source rate limits.
- Batches take no time to run, so latency means trigger interval plus watermark delay, not processing time.

## Grant: Unity Catalog and bundles (`js/grant/uc.js`, `js/grant/bundles.js`)

**Faithful rules**
- Privilege model 1.0: downward inheritance, the `USE CATALOG`/`USE SCHEMA` requirement, privileges valid per securable type, and `ALL PRIVILEGES`.
- Nested groups and the built-in `account users` group.
- Owners hold all privileges on what they own but still need `USE` on its parents.
- Views read base tables with their owner's rights.
- Row filters and column masks are evaluated for the querying user, with `is_account_group_member` and `current_user`.
- `GRANT`, `REVOKE`, `CREATE FUNCTION … RETURN`, `ALTER TABLE … SET ROW FILTER` and `ALTER COLUMN … SET MASK`, `OWNER TO` and `SHOW GRANTS`, with errors close to Databricks' wording.

**Simplified**
- No metastore admins, `MANAGE` semantics beyond holding the privilege, `BROWSE`, tags or ABAC policies, external locations and storage credentials, Delta Sharing, or workspace bindings.
- Filter and mask functions support a small expression language: comparisons, `AND`/`OR`/`NOT`, `IN`, `CASE`, `||` and a few string functions.

**The bundle validator** covers the common top-level sections, targets and modes, variables and substitutions, `run_as`, permissions, jobs (tasks, dependencies, schedules, job clusters), pipelines and schemas. It does not deploy, call workspace APIs, resolve `lookup` variables or build artifacts, and the real CLI checks more fields.

## How it is tested

| Suite | What it checks |
|---|---|
| `tests/stream.test.js` | 28,000 checks over hundreds of random event streams. Every finalized window equals a brute-force replay; the watermark never decreases; complete mode equals ground truth; CDC results are independent of arrival order; SCD Type 2 versions never overlap. Also the Structured Streaming programming guide's worked example. |
| `tests/grant.test.js` | Inheritance, `USE` requirements, nested groups, owners, views with and without owner access, row filters and masks for different users, invalid grants; bundle validation errors, warnings, modes and plans. |
| `tests/shuffle.test.js` | File splitting rules, broadcast choices and failures, AQE conversion, skew handling, spill, coalescing, clustering, compaction, Photon, UDFs, and that more workers are never slower. |
| `tests/challenges.test.js` | Every challenge's starting point fails, its reference solution (and alternative) passes, and known wrong answers are rejected. |

Streaming challenges were also checked exhaustively: every combination of each challenge's settings was run, and each challenge accepts exactly the settings its lesson teaches.

If you find behaviour that differs from Databricks, please open an issue with the scenario and the real result.
