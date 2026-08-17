# VeryGoodETL

> A very good vectorized ETL and data processing framework for Go.

VeryGoodETL is an early-stage Go library for building concurrent, batch-oriented data pipelines. Apache Arrow records are the initial execution representation, while the runtime provides the graph, lifecycle, backpressure, cancellation, and ownership semantics around them.

Use our software, or do not. We are not beggars.

## Status

VeryGoodETL is under active design. The initial runtime is intentionally small while its execution semantics are proven.

## Design goals

- Process typed data in batches rather than row-at-a-time payloads.
- Build pipelines as DAGs with first-class fan-out and fan-in.
- Preserve the useful `Process` / `Finish` lifecycle: `Finish` runs only after every upstream input has completed successfully.
- Apply bounded backpressure between stages.
- Cancel the entire graph on the first error or context cancellation.
- Make raw archival a natural side output so derived reporting data can be rebuilt from durable source data.
- Remain a processing library, not a scheduler. A VeryGoodETL pipeline should be easy to compile into a normal Go binary and invoke from Sidekiq, cron, Airflow, a Kubernetes Job, or whatever already schedules work.

## Installation

```sh
go get github.com/verygoodetl/verygoodetl
```

The package name is `etl`. Because the import path's last segment
(`verygoodetl`) doesn't match, `goimports` will add an explicit alias:

```go
import etl "github.com/verygoodetl/verygoodetl"
```

## Core model

A pipeline is a directed graph of sources, processors, and sinks:

```text
                     +--> raw archive
                     |
source --> batches --+--> validate --> transform --> reporting database
                     |
                     +--> another processor --> another sink
```

The unit moving between stages is a `Batch`. The initial `Batch` implementation wraps an Apache Arrow record.

```go
type Processor interface {
    Process(context.Context, etl.Batch, etl.Output) error
    Finish(context.Context, etl.Output) error
}
```

`Process` may emit zero or more batches. `Finish` is called exactly once after all upstream inputs have closed successfully and no future call to `Process` can occur.

## Example

```go
pipeline := etl.New(etl.WithBufferSize(8))

orders := pipeline.From(source)

// CopyTo preserves the stream while sending the same immutable batches to a
// side sink. This is a logical copy; Arrow buffers may be shared in memory.
orders.CopyTo(rawArchive)

clean := orders.Process(cleanOrders)
clean.To(reportingDatabase)

if err := pipeline.Run(ctx); err != nil {
    return err
}
```

Fan-in is explicit:

```go
allOrders := pipeline.Merge(mergeProcessor, webOrders, storeOrders)
allOrders.To(sink)
```

`mergeProcessor.Finish` will not run until both `webOrders` and `storeOrders` have finished successfully.

## Batch ownership

Batches are immutable from the runtime's point of view. This allows a batch to fan out without copying its Arrow buffers.

`Output.Send` retains one reference for each downstream edge. The downstream runtime releases that reference after the receiving processor or sink returns. Code that creates an Arrow record remains responsible for its own original reference.

## What is deliberately not here yet

- S3 / data-lake archive implementation
- SQL sources and sinks
- CSV / Parquet packages
- expression or vector-compute DSL
- joins, aggregation, sorting, and other higher-level processors
- scheduling or orchestration

Those should be built on top of a runtime whose semantics are boring and correct.

## Philosophy

Raw source data should be durable. Reporting databases should be rebuildable derived artifacts.

Extract once. Preserve the source. Process in batches. Compose freely. Transform deterministically. Rebuild anything.
