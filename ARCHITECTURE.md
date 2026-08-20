# Architecture

Very Good ETL is a data-processing runtime. Scheduling and orchestration are intentionally outside the core library.

## Execution model

A `Pipeline` is a directed acyclic graph of stages connected by bounded edges. Every stage executes independently and communicates using immutable `Batch` values.

There are three stage roles:

- `Source` produces batches.
- `Processor` consumes batches and may produce batches.
- `Sink` consumes batches.

The graph supports fan-out by connecting one stream to multiple downstream stages and fan-in with `Pipeline.Merge`.

## Lifecycle

The lifecycle contract is deliberately strong:

1. A `Source` stage receives no batches and only produces them; a `Processor` or `Sink` stage receives zero or more.
2. For a `Processor` or `Sink`, all upstream edges close successfully before its `Finish` runs.
3. A `Processor`'s or `Sink`'s `Finish` method runs exactly once, but only along the success path: if every upstream input completes successfully, `Finish` runs exactly once. A `Source` has no `Finish` — its lifecycle ends when `Run` returns.
4. The runtime closes the stage's downstream edges once it returns, whether or not `Finish` ever ran: for a `Source`, that's when `Run` returns; for a `Processor` or `Sink`, that's when `Finish` returns on the success path described in point 3, or immediately — with `Finish` skipped entirely — if that stage's own inputs failed, its own processing failed, or the context was canceled first.

If any stage fails, the pipeline context is canceled before that stage's downstream edges are closed. Downstream stages therefore must not interpret an upstream failure as a successful end-of-stream and run `Finish` on partial data — instead, a stage whose inputs failed or whose context was canceled first skips `Finish` entirely, and may have `Abort` called on it once instead (see `Aborter`). A stage should not treat `Finish` as its only chance to run required cleanup or commit logic; that logic must be reachable from `Abort` too, since a failed or canceled run may never reach `Finish` at all.

This preserves a useful distinction between processing data as it arrives and finalizing work only when all data is known to have arrived.

## Batch ownership

The runtime treats batches as immutable.

When a batch is sent downstream, the runtime retains one reference for each outgoing edge. A receiving processor or sink owns that retained reference for the duration of its callback; the runtime releases it when the callback returns.

`Output.Send` transfers one retained reference to the runtime; the source or processor that created the batch may release its own reference once `Send` returns. `sqlsource.Source` follows this pattern: its scan loop defers `record.Release()` on the record it just built, immediately after handing it to `Send`.

Immutability makes fan-out inexpensive with Arrow because branches can share the same underlying buffers. A transform that changes data should produce a new batch rather than mutate an input batch.

## Backpressure

Every graph edge is a bounded channel. `Output.Send` blocks when a downstream edge is full. `WithBufferSize` controls edge capacity for the pipeline.

The initial runtime favors predictable bounded memory over unbounded queues.

## Failure and cancellation

The first stage error cancels the pipeline. External context cancellation does the same. Stages should respect their supplied context.

Once `Run` has actually started a pipeline's stages, it always waits for every one of them to finish unwinding, then reports an error for each node that failed to complete cleanly — never the bare fact that ctx was canceled. When a stage's own callback (`Process`, `Consume`, `Finish`) respects its context and returns an error once canceled, that becomes the node's result, typically `ctx.Err()`. But a `Processor` or `Sink` can also be canceled while no callback of its own is running at all — for instance, while the runtime is blocking on its behalf waiting for the next input batch — and in that case there is no stage-returned error to report, so the runtime synthesizes `ctx.Err()` itself as that node's result. But if every stage completes without an error, `Run` returns `nil` even when ctx was also canceled: the pipeline's work finished before the cancellation could have had any effect on it, so nothing was actually canceled.

This "always waits" guarantee applies only once stages have actually been started. If a builder call — `From`, `Process`, `Merge`, or `To` — was given a nil stage, the pipeline records that as a validation error at build time, and `Run` returns it immediately without starting any stage goroutine: there is nothing to unwind.

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

Very Good ETL does not decide when jobs run.

A pipeline should be easy to embed in a normal Go executable. That executable may be invoked by Sidekiq, cron, Airflow, Kubernetes, a CI system, or another scheduler. If scheduling support is ever added, it should be layered above the processing runtime rather than coupled to it.

## Future multi-input semantics

`Merge` currently combines upstream streams and preserves the guarantee that `Finish` waits for every input. A future advanced multi-input interface may expose input identity and per-input completion events for joins and other asymmetric operators. The common `Processor` interface should remain simple.
