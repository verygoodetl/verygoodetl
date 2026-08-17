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

This preserves the useful distinction from goetl between processing data as it arrives and finalizing work only when all data is known to have arrived.

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

The core runtime provides `Stream.Tap` as the primitive needed to branch unmodified batches to a side sink:

```text
                     +--> durable raw archive
                     |
source --> batches --+--> transforms --> reporting database
```

A future archive package should add stronger semantics such as immutable writes, source/run metadata, schema metadata, partitioning, completion manifests, and replay. Those semantics do not belong in the generic graph runtime.

The long-term goal is that the transform portion of a production pipeline can be run unchanged against archived source data to rebuild derived outputs.

## Scheduling boundary

VeryGoodETL does not decide when jobs run.

A pipeline should be easy to embed in a normal Go executable. That executable may be invoked by Sidekiq, cron, Airflow, Kubernetes, a CI system, or another scheduler. If scheduling support is ever added, it should be layered above the processing runtime rather than coupled to it.

## Future multi-input semantics

`Merge` currently combines upstream streams and preserves the guarantee that `Finish` waits for every input. A future advanced multi-input interface may expose input identity and per-input completion events for joins and other asymmetric operators. The common `Processor` interface should remain simple.
