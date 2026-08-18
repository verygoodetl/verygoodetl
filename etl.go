// Package etl provides a batch-oriented dataflow runtime for building ETL
// pipelines in Go.
package etl

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
)

// Batch is the unit of data that moves through a Pipeline. Implementations are
// immutable from the runtime's point of view so the same batch may safely fan
// out to multiple downstream stages.
type Batch interface {
	Schema() *arrow.Schema
	NumRows() int64
	Record() arrow.Record
	Retain()
	Release()
}

// ArrowBatch adapts an Arrow record to Batch.
type ArrowBatch struct {
	record arrow.Record
}

// NewBatch wraps record. Ownership remains with the caller; the caller must
// keep its reference alive for as long as the returned batch is in use.
func NewBatch(record arrow.Record) *ArrowBatch {
	return &ArrowBatch{record: record}
}

func (b *ArrowBatch) Schema() *arrow.Schema { return b.record.Schema() }
func (b *ArrowBatch) NumRows() int64        { return b.record.NumRows() }
func (b *ArrowBatch) Record() arrow.Record  { return b.record }
func (b *ArrowBatch) Retain()               { b.record.Retain() }
func (b *ArrowBatch) Release()              { b.record.Release() }

// Output is supplied to sources and processors. Send transfers one retained
// reference to the runtime; callers may release their own reference after Send
// returns.
type Output interface {
	Send(context.Context, Batch) error
}

// Source produces batches for a pipeline. Run must return any error from
// Output.Send rather than discarding it; a swallowed Send error can prevent
// the pipeline from unwinding on cancellation or failure.
type Source interface {
	Run(context.Context, Output) error
}

// Processor consumes batches and may emit zero or more batches. Finish is
// called exactly once after every upstream input has completed successfully.
// No further call to Process can occur after Finish begins. As with Source,
// Process and Finish must return any error from Output.Send rather than
// discarding it.
type Processor interface {
	Process(context.Context, Batch, Output) error
	Finish(context.Context, Output) error
}

// ProcessorFuncs makes small processors easy to define without a new type.
type ProcessorFuncs struct {
	ProcessFunc func(context.Context, Batch, Output) error
	FinishFunc  func(context.Context, Output) error
}

func (p ProcessorFuncs) Process(ctx context.Context, b Batch, out Output) error {
	if p.ProcessFunc == nil {
		return out.Send(ctx, b)
	}
	return p.ProcessFunc(ctx, b, out)
}

func (p ProcessorFuncs) Finish(ctx context.Context, out Output) error {
	if p.FinishFunc == nil {
		return nil
	}
	return p.FinishFunc(ctx, out)
}

// Sink consumes batches. Finish has the same all-inputs-complete guarantee as
// Processor.Finish.
type Sink interface {
	Consume(context.Context, Batch) error
	Finish(context.Context) error
}

// Aborter is optionally implemented by a Processor or Sink that holds
// resources needing best-effort cleanup when a pipeline fails or is
// canceled before that stage's Finish is reached — for example, canceling
// an in-flight blob upload rather than leaking it. The runtime calls Abort
// at most once for a given stage, and only when Finish will never be
// called for it.
type Aborter interface {
	Abort()
}

// SinkFuncs makes small sinks easy to define.
type SinkFuncs struct {
	ConsumeFunc func(context.Context, Batch) error
	FinishFunc  func(context.Context) error
}

func (s SinkFuncs) Consume(ctx context.Context, b Batch) error {
	if s.ConsumeFunc == nil {
		return nil
	}
	return s.ConsumeFunc(ctx, b)
}

func (s SinkFuncs) Finish(ctx context.Context) error {
	if s.FinishFunc == nil {
		return nil
	}
	return s.FinishFunc(ctx)
}
