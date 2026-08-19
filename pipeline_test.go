package etl

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type batchesSource struct {
	batches []Batch
}

func (s batchesSource) Run(ctx context.Context, out Output) error {
	for _, b := range s.batches {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func intBatch(t *testing.T, values ...int64) Batch {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(values, nil)
	a := b.NewArray()
	defer a.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(schema, []arrow.Array{a}, int64(len(values)))
	t.Cleanup(record.Release)
	return NewBatch(record)
}

func TestFinishWaitsForAllInputs(t *testing.T) {
	p := New()
	left := p.From(batchesSource{batches: []Batch{intBatch(t, 1), intBatch(t, 2)}})
	right := p.From(batchesSource{batches: []Batch{intBatch(t, 3)}})

	var mu sync.Mutex
	processed := 0
	finishedAt := -1
	merged := p.Merge(ProcessorFuncs{
		ProcessFunc: func(ctx context.Context, b Batch, out Output) error {
			mu.Lock()
			processed++
			mu.Unlock()
			return out.Send(ctx, b)
		},
		FinishFunc: func(context.Context, Output) error {
			mu.Lock()
			finishedAt = processed
			mu.Unlock()
			return nil
		},
	}, left, right)

	consumed := 0
	merged.To(SinkFuncs{ConsumeFunc: func(context.Context, Batch) error {
		consumed++
		return nil
	}})

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if processed != 3 || consumed != 3 {
		t.Fatalf("processed=%d consumed=%d, want 3/3", processed, consumed)
	}
	if finishedAt != 3 {
		t.Fatalf("Finish observed %d processed batches, want 3", finishedAt)
	}
}

func TestFanOutDeliversEveryBatch(t *testing.T) {
	p := New()
	stream := p.From(batchesSource{batches: []Batch{intBatch(t, 1, 2, 3)}})

	counts := []int{0, 0}
	for i := range counts {
		i := i
		stream.To(SinkFuncs{ConsumeFunc: func(_ context.Context, b Batch) error {
			counts[i] += int(b.NumRows())
			return nil
		}})
	}

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if counts[0] != 3 || counts[1] != 3 {
		t.Fatalf("fan-out counts=%v, want [3 3]", counts)
	}
}

func TestProcessorErrorCancelsPipelineAndSkipsFinish(t *testing.T) {
	p := New()
	stream := p.From(batchesSource{batches: []Batch{intBatch(t, 1)}})

	want := errors.New("bad transform")
	finished := false
	stream.Process(ProcessorFuncs{
		ProcessFunc: func(context.Context, Batch, Output) error { return want },
		FinishFunc: func(context.Context, Output) error {
			finished = true
			return nil
		},
	}).To(SinkFuncs{})

	err := p.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run error=%v, want %v", err, want)
	}
	if finished {
		t.Fatal("Finish called after Process returned an error")
	}
}

// abortableSink records whether the runtime called Abort on it, to verify
// the runtime invokes Aborter in place of a skipped Finish.
type abortableSink struct {
	consumed int
	finished bool
	aborted  bool
}

func (s *abortableSink) Consume(context.Context, Batch) error {
	s.consumed++
	return nil
}

func (s *abortableSink) Finish(context.Context) error {
	s.finished = true
	return nil
}

func (s *abortableSink) Abort() {
	s.aborted = true
}

var _ Aborter = (*abortableSink)(nil)

func TestRunTwiceReturnsErrorInsteadOfReusingClosedEdges(t *testing.T) {
	p := New()
	p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(SinkFuncs{})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error running an already-run pipeline a second time, got nil")
	}
}

func TestGraphMutationAfterRunPanics(t *testing.T) {
	p := New()
	p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(SinkFuncs{})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	defer func() {
		if recover() == nil {
			t.Fatal("want From to panic once the pipeline has been run")
		}
	}()
	p.From(batchesSource{})
}

func TestUpstreamFailureCallsAbortOnDownstreamSink(t *testing.T) {
	want := errors.New("upstream boom")
	p := New()
	sink := &abortableSink{}
	p.From(errorSource{err: want}).To(sink)

	err := p.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run error=%v, want %v", err, want)
	}
	if sink.finished {
		t.Fatal("Finish called after upstream failure")
	}
	if !sink.aborted {
		t.Fatal("want Abort called after upstream failure skipped Finish")
	}
}

// nilPtrSource, nilPtrProcessor, and nilPtrSink are concrete pointer types
// implementing Source, Processor, and Sink respectively, with methods that
// dereference the receiver. A nil *T of any of these, once wrapped in its
// interface, is not == nil (the interface carries the concrete type
// descriptor and only a nil value pointer), so they exercise the typed-nil
// detection in isNilStage: without it, From/Process/Merge/To would let a nil
// *T through and its method would panic on a nil-pointer dereference the
// first time the runtime actually invokes it.
type nilPtrSource struct{ batches []Batch }

func (s *nilPtrSource) Run(ctx context.Context, out Output) error {
	for _, b := range s.batches {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

type nilPtrProcessor struct{ n int }

func (p *nilPtrProcessor) Process(ctx context.Context, b Batch, out Output) error {
	p.n++
	return out.Send(ctx, b)
}

func (p *nilPtrProcessor) Finish(context.Context, Output) error { return nil }

type nilPtrSink struct{ n int }

func (s *nilPtrSink) Consume(context.Context, Batch) error {
	s.n++
	return nil
}

func (s *nilPtrSink) Finish(context.Context) error { return nil }

func TestTypedNilStagesReturnErrorInsteadOfPanicking(t *testing.T) {
	t.Run("From", func(t *testing.T) {
		p := New()
		var src *nilPtrSource
		p.From(src).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for typed-nil Source, got nil")
		}
	})

	t.Run("Process", func(t *testing.T) {
		p := New()
		var proc *nilPtrProcessor
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).Process(proc).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for typed-nil Processor, got nil")
		}
	})

	t.Run("Merge", func(t *testing.T) {
		p := New()
		left := p.From(batchesSource{batches: []Batch{intBatch(t, 1)}})
		right := p.From(batchesSource{batches: []Batch{intBatch(t, 2)}})
		var proc *nilPtrProcessor
		p.Merge(proc, left, right).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for typed-nil Processor passed to Merge, got nil")
		}
	})

	t.Run("To", func(t *testing.T) {
		p := New()
		var sink *nilPtrSink
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(sink)

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for typed-nil Sink, got nil")
		}
	})
}

// nilSliceSource, nilSliceProcessor, and nilSliceSink are named slice types
// implementing Source, Processor, and Sink respectively, with value-receiver
// methods that never touch the receiver — the same shape as, say, a
// http.HandlerFunc-style adapter or a named slice type used purely as a
// marker. A nil value of any of these is a legal, safe stage: unlike
// nilPtrSource/nilPtrProcessor/nilPtrSink above, invoking their methods on a
// nil receiver never panics, so isNilStage must not reject them.
type nilSliceSource []string

func (nilSliceSource) Run(context.Context, Output) error { return nil }

type nilSliceProcessor []string

func (nilSliceProcessor) Process(ctx context.Context, b Batch, out Output) error {
	return out.Send(ctx, b)
}

func (nilSliceProcessor) Finish(context.Context, Output) error { return nil }

type nilSliceSink []string

func (nilSliceSink) Consume(context.Context, Batch) error { return nil }

func (nilSliceSink) Finish(context.Context) error { return nil }

func TestNilNamedSliceStagesAreAcceptedNotRejected(t *testing.T) {
	t.Run("From", func(t *testing.T) {
		p := New()
		var src nilSliceSource
		p.From(src).To(SinkFuncs{})

		if err := p.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v, want nil slice Source to be accepted", err)
		}
	})

	t.Run("Process", func(t *testing.T) {
		p := New()
		var proc nilSliceProcessor
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).Process(proc).To(SinkFuncs{})

		if err := p.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v, want nil slice Processor to be accepted", err)
		}
	})

	t.Run("Merge", func(t *testing.T) {
		p := New()
		left := p.From(batchesSource{batches: []Batch{intBatch(t, 1)}})
		right := p.From(batchesSource{batches: []Batch{intBatch(t, 2)}})
		var proc nilSliceProcessor
		p.Merge(proc, left, right).To(SinkFuncs{})

		if err := p.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v, want nil slice Processor passed to Merge to be accepted", err)
		}
	})

	t.Run("To", func(t *testing.T) {
		p := New()
		var sink nilSliceSink
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(sink)

		if err := p.Run(context.Background()); err != nil {
			t.Fatalf("Run: %v, want nil slice Sink to be accepted", err)
		}
	})
}

// TestPipelineFrozenAfterFailedValidationRun verifies that a Run which fails
// because a builder call was given a nil stage still marks the pipeline as
// started, consistent with the single-use contract: a later builder call
// panics instead of silently mutating an already-"run" pipeline, and a
// second Run reports "already run" rather than re-returning the original
// validation error.
func TestPipelineFrozenAfterFailedValidationRun(t *testing.T) {
	p := New()
	p.From(nil).To(SinkFuncs{})

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for nil Source, got nil")
	}

	t.Run("further builder call panics", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("want From to panic after a failed Run, since the pipeline is still considered started")
			}
		}()
		p.From(batchesSource{})
	})

	t.Run("second Run reports already run", func(t *testing.T) {
		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error running an already-run pipeline a second time, got nil")
		}
		const want = "etl: pipeline already run; a Pipeline may be run at most once"
		if err.Error() != want {
			t.Fatalf("second Run error = %q, want %q", err.Error(), want)
		}
	})
}

func TestNilStagesReturnErrorInsteadOfPanicking(t *testing.T) {
	t.Run("From", func(t *testing.T) {
		p := New()
		p.From(nil).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil Source, got nil")
		}
	})

	t.Run("Process", func(t *testing.T) {
		p := New()
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).Process(nil).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil Processor, got nil")
		}
	})

	t.Run("Merge", func(t *testing.T) {
		p := New()
		left := p.From(batchesSource{batches: []Batch{intBatch(t, 1)}})
		right := p.From(batchesSource{batches: []Batch{intBatch(t, 2)}})
		p.Merge(nil, left, right).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil Processor passed to Merge, got nil")
		}
	})

	t.Run("To", func(t *testing.T) {
		p := New()
		p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(nil)

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil Sink, got nil")
		}
	})
}

func TestSuccessfulRunDoesNotCallAbort(t *testing.T) {
	p := New()
	sink := &abortableSink{}
	p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(sink)

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !sink.finished {
		t.Fatal("want Finish called on successful run")
	}
	if sink.aborted {
		t.Fatal("Abort called after a successful run")
	}
}
