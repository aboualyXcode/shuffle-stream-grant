# 2. Streaming and Lakeflow pipelines

Streaming adds two dimensions batch jobs never face: **time** (data arrives late and out of order) and **memory** (state accumulates until something tells Spark it can forget). The **Stream** lab replays messy event streams through Structured Streaming's rules so you can see both.

## Two clocks

- **Event time:** when something happened (the `event_time` column).
- **Processing time:** when the stream sees it.

They differ whenever a phone is offline, a gateway retries, or a CDC connector reorders changes. Aggregate by event time; reason about latency in processing time.

## Micro-batches and triggers

Structured Streaming processes data in micro-batches:

| Trigger | Behaviour |
|---|---|
| `processingTime="1 minute"` | A batch every minute, with whatever has arrived |
| `availableNow=True` | Process everything available (possibly in several batches), then stop: incremental batch jobs |
| Default (no trigger) | Next batch as soon as the previous finishes |

## Watermarks

```python
events.withWatermark("event_time", "15 minutes").groupBy(F.window("event_time", "5 minutes")).count()
```

The watermark is **max(event time seen) − delay**. Spark computes it at the end of each batch and uses it from the next; it never moves backwards. It does two jobs:

1. **Finalizes state.** Once the watermark passes a window's end, that window can never change again: append mode writes it, and its state is dropped.
2. **Drops late data.** A row whose windows have all been finalized is dropped as too late.

The delay is a trade-off: longer means more complete results but later output and more state. Measure how late your data really is and add a margin. Practise: `late-mobile-events`.

When the watermark advances and state can be finalized, Spark runs a **no-data micro-batch** immediately, so results do not wait for the next event.

## Output modes

| Mode | Writes | Needs | Good for |
|---|---|---|---|
| **append** | Each row once, when final | A watermark (for aggregations) | Append-only tables, files, downstream streams |
| **update** | Changed rows every batch | Nothing, but a watermark bounds state | Dashboards, MERGE into a table |
| **complete** | The whole result every batch | An aggregation | Small, bounded result sets only |

Append mode with an aggregation and no watermark fails at startup: "Append output mode not supported when there are streaming aggregations … without watermark". Practise: `append-needs-watermark`, `live-dashboard`.

## State

Aggregations, deduplication and stream-stream joins keep state in the state store. Without a watermark it is never evicted and grows until the job runs out of memory, even in update mode. With one, it holds roughly (watermark delay ÷ window slide) + 1 windows. Watch state metrics in the streaming query progress. Practise: `state-explosion`.

## Deduplication

| Method | State | Notes |
|---|---|---|
| `dropDuplicates(["id"])` | Every key, forever | Unbounded: fine only for small key spaces |
| `withWatermark(...).dropDuplicates(["id", "event_time"])` | Bounded by the watermark | Duplicates must share the event time |
| `withWatermark(...).dropDuplicatesWithinWatermark(["id"])` | Each key for the delay | Spark 3.5+ / Databricks Runtime 13.3+; duplicates may differ in event time |

The watermark both expires deduplication state and drops rows older than itself, so it must cover how late legitimate rows can arrive, not just the gap between duplicates. Practise: `payment-retries`.

## Checkpoints and exactly-once

The **checkpoint** records which offsets or files each batch processed and the state. With a replayable source and an idempotent or transactional sink (Delta), restarting from the same checkpoint gives exactly-once results.

- **Never** change or delete a checkpoint casually: a new checkpoint starts from scratch and reprocesses everything.
- Keep checkpoints next to the table they serve, stable across deploys.
- `foreachBatch` with a `MERGE` on a key makes the sink idempotent too, which protects against a lost checkpoint.

Practise: `checkpoint-rotation`.

## Auto Loader schema evolution

`cloudFiles.schemaEvolutionMode` decides what happens when files bring new columns:

| Mode | On a new column |
|---|---|
| `addNewColumns` (default without a schema) | The stream **fails** once with `UNKNOWN_FIELD_EXCEPTION`, records the new schema, and continues after a restart |
| `rescue` | Never fails; unexpected fields go to `_rescued_data`; the schema does not change |
| `failOnNewColumns` | Fails, and stays failed until the schema is updated |
| `none` | Ignores new columns silently |

With `addNewColumns`, the failure is by design: production jobs need **retries**. Practise: `schema-evolution`.

## CDC with AUTO CDC

Change feeds arrive out of order and include deletes. Applying them in arrival order (a hand-written MERGE per batch) lets a stale change overwrite a newer one. Lakeflow pipelines handle this declaratively:

```sql
CREATE OR REFRESH STREAMING TABLE customers;

CREATE FLOW customers_cdc AS AUTO CDC INTO customers
FROM STREAM(crm_changes)
KEYS (customer_id)
SEQUENCE BY change_seq
APPLY AS DELETE WHEN op = 'DELETE'
COLUMNS * EXCEPT (op, change_seq)
STORED AS SCD TYPE 2;
```

- `SEQUENCE BY` orders changes by the source's sequence, whatever order they arrive in.
- `STORED AS SCD TYPE 1` keeps the latest value; `SCD TYPE 2` keeps every version with `__START_AT` and `__END_AT`, inserting late changes into the middle of history.
- `APPLY AS DELETE WHEN` turns delete records into deletes (and closes history in Type 2).

Practise: `cdc-out-of-order`, `cdc-history`.

## Lakeflow pipelines in one paragraph

Lakeflow pipelines (formerly Delta Live Tables) let you declare **streaming tables** (incremental, append-mostly), **materialized views** (results of a query, refreshed incrementally where possible) and **flows** such as AUTO CDC. The pipeline manages checkpoints, retries, dependencies and data-quality **expectations** for you. Everything in this chapter still applies: watermarks, state and sequencing are the same concepts underneath.
