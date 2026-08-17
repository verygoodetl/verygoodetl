// Package etl provides a concurrent, batch-oriented dataflow runtime for Go.
//
// Pipelines are directed acyclic graphs of Source, Processor, and Sink stages.
// Streams may fan out to multiple stages, and Pipeline.Merge may fan multiple
// streams into a single Processor. Edges are bounded to provide backpressure.
//
// Processor.Finish and Sink.Finish are lifecycle barriers: they are called only
// after every upstream input has completed successfully and no additional batch
// can arrive. A stage error or context cancellation cancels the graph instead
// of allowing downstream stages to finalize partial work as if it were complete.
//
// Batch is intentionally a framework-level abstraction. ArrowBatch is the
// initial implementation and wraps an Apache Arrow record, giving the runtime
// typed, columnar batches without requiring every public API to be an Arrow API.
package etl
