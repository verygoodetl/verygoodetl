# Architecture

VeryGoodETL is a data-processing runtime. Scheduling and orchestration are intentionally outside the core library.

## Execution model

A `Pipeline` is a directed acyclic graph of stages connected by bounded edges. Every stage executes independently and communicates using immutable `Batch` values.

There are three stage roles:

- `Source` produces batches.
- `Processor` consumes batches and may produce batches.
- `Sink` consumes batches.

The graph supports fan-out by connecting one stream to multiple downstream stages and fan-in with `Pipeline.Merge`.

## Lifecycle

The lifecycle contract is deliberately strong:

1. A stage receives zero or more batches.
2. All upstream edges close successfully.
3. The stage's `Finish` method runs exactly once.
4. The stage closes its downstream edges.

If any stage fails, the pipeline context is canceled before that stage's downstream edges are closed. Downstream stages therefore must not interpret an upstream failure as a successful end-of-stream and run `Finish` on partial data.

This preserves a useful distinction between processing data as it arrives and finalizing work only when all data is known to have arrived.

## Batch ownership

The runtime treats batches as immutable.

When a batch is sent downstream, the runtime retains one reference for each outgoing edge. A receiving processor or sink owns that retained reference for the duration of its callback; the runtime releases it when the callback returns.

Immutability makes fan-out inexpensive with Arrow because branches can share the same underlying buffers. A transform that changes data should produce a new batch rather than mutate an input batch.

## Backpressure

Every graph edge is a bounded channel. `Output.Send` blocks when a downstream edge is full. `WithBufferSize` controls edge capacity for the pipeline.

The initial runtime favors predictable bounded memory over unbounded queues.

## Failure and cancellation

The first stage error cancels the pipeline. External context cancellation does the same. Stages should respect their supplied context.

On cancellation, unread retained batches are drained and released so failed pipelines do not leak Arrow references.

## Raw archival and replay

A reporting database is considered a derived artifact, not the durable source of truth.

The core runtime provides `Stream.CopyTo` as the primitive needed to branch unmodified batches to a side sink:

```text
                     +--> durable raw archive
                     |
source --> batches --+--> transforms --> reporting database
```

The `filesink` package (`filesink.Sink`) implements this side sink today: it writes batches to a Parquet, Arrow IPC, or CSV file via `gocloud.dev/blob`, so the same code targets S3, GCS, or local disk. `filesink.Sink` is a general-purpose file writer, not an archive-specific type — by default, writing to an existing key overwrites it, matching ordinary file-writing expectations (e.g. a staging file rewritten each run ahead of a database load, or a regenerated report). The immutable-writes semantic this section gestures at is available but is an explicit opt-in, not the default: pass `WithWriterOptions(&blob.WriterOptions{IfNotExist: true})` to make writing to an existing key fail instead of overwrite. Parquet also stores the exact Arrow schema in file metadata (not just Parquet's own lossier physical-type inference), independent of which write mode is used. Source/run metadata conventions, partitioning, completion manifests, and replay tooling are still deliberately out of scope — `filesink.Sink` is a low-level, single-object primitive, not the full manifest/replay system; those remain layered concerns for later.

The long-term goal is that the transform portion of a production pipeline can be run unchanged against archived source data to rebuild derived outputs.

## SQL sources: explicit schema over driver inference

The `sqlsource` package (`sqlsource.Source`) runs a SQL query via `database/sql` and emits batches. It deliberately requires a caller-supplied `*arrow.Schema` rather than inferring Arrow types from driver metadata.

`database/sql` exposes an optional `driver.RowsColumnTypeScanType` interface that a library could use to infer types automatically, but its implementation is inconsistent across drivers — common SQLite drivers don't implement it at all and fall back to a generic `interface{}`. Automatic inference would therefore be silently unreliable depending on which driver and version happens to be in use: the same column could land as a different Arrow type on different setups. Requiring an explicit schema up front trades a little convenience for behavior that's predictable and auditable regardless of driver, which is the same trade-off this runtime already makes elsewhere (e.g. the strong `Finish` lifecycle contract over a more permissive one).

## Scheduling boundary

VeryGoodETL does not decide when jobs run.

A pipeline should be easy to embed in a normal Go executable. That executable may be invoked by Sidekiq, cron, Airflow, Kubernetes, a CI system, or another scheduler. If scheduling support is ever added, it should be layered above the processing runtime rather than coupled to it.

## Future multi-input semantics

`Merge` currently combines upstream streams and preserves the guarantee that `Finish` waits for every input. A future advanced multi-input interface may expose input identity and per-input completion events for joins and other asymmetric operators. The common `Processor` interface should remain simple.
