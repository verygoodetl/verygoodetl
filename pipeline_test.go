package etl

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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

// countingBatch is a minimal Batch with no real Arrow record behind it —
// just enough to exercise consumeInputs' handling of a batch it never
// actually needs to inspect. released, if non-nil, records whether Release
// was called.
type countingBatch struct {
	released *bool
}

func (countingBatch) Schema() *arrow.Schema { return nil }
func (countingBatch) NumRows() int64        { return 0 }
func (countingBatch) Record() arrow.Record  { return nil }
func (countingBatch) Retain()               {}
func (b countingBatch) Release() {
	if b.released != nil {
		*b.released = true
	}
}

// TestConsumeInputsWaitsForReadersAfterConsumeError is a regression test:
// consumeInputs used to return as soon as consume failed, without waiting
// for its per-edge reader goroutines, which could keep draining and
// releasing queued batches after the caller had already moved on.
func TestConsumeInputsWaitsForReadersAfterConsumeError(t *testing.T) {
	released := false
	e := &edge{ch: make(chan envelope, 2)}
	e.ch <- envelope{batch: countingBatch{}}
	e.ch <- envelope{batch: countingBatch{released: &released}}
	close(e.ch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wantErr := errors.New("boom")
	err := consumeInputs(ctx, cancel, []*edge{e}, func(Batch) error {
		return wantErr
	})

	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}
	if !released {
		t.Fatal("want the still-queued second batch drained and released by the time consumeInputs returns, not left for its detached reader goroutine to release later, unobserved, after the caller has already moved on")
	}
}

// foreverSource sends batches until ctx is done, never returning on its own.
// It stands in for a real unbounded source (e.g. a Kafka consumer) that only
// stops in response to cancellation.
type foreverSource struct{}

func (foreverSource) Run(ctx context.Context, out Output) error {
	for {
		if err := out.Send(ctx, countingBatch{}); err != nil {
			return err
		}
	}
}

// failingSink fails on its first Consume.
type failingSink struct{ err error }

func (s failingSink) Consume(context.Context, Batch) error { return s.err }
func (failingSink) Finish(context.Context) error           { return nil }

// TestRunCancelsUnboundedSourceOnConsumeFailure is a regression test: a
// failing Consume must cancel the pipeline-wide context, not just
// consumeInputs' own derived context, or an unbounded source never learns
// to stop and Run hangs forever.
func TestRunCancelsUnboundedSourceOnConsumeFailure(t *testing.T) {
	wantErr := errors.New("boom")
	p := New()
	p.From(foreverSource{}).To(failingSink{err: wantErr})

	done := make(chan error, 1)
	go func() {
		done <- p.Run(context.Background())
	}()

	select {
	case err := <-done:
		if !errors.Is(err, wantErr) {
			t.Fatalf("err=%v, want %v", err, wantErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return; unbounded source was not canceled")
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

// TestRunNilContextReturnsErrorInsteadOfPanicking is a regression test:
// context.WithCancel(nil) panics, so Run must reject a nil ctx itself before
// reaching that call rather than letting the panic surface to the caller.
func TestRunNilContextReturnsErrorInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(SinkFuncs{})

	err := p.Run(nil)
	if err == nil {
		t.Fatal("want error for a nil context.Context, got nil")
	}
	const want = "etl: Run called with a nil context.Context"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
	}
}

// nilPtrContext lets a test construct a typed-nil context.Context: a
// *nilPtrContext(nil) satisfies the interface via its embedded field, so it
// compares != nil, but calling any method on it panics on the nil receiver.
type nilPtrContext struct{ context.Context }

// TestRunTypedNilContextReturnsErrorInsteadOfPanicking is a regression test:
// a typed-nil context.Context is != nil by interface comparison, so a plain
// `ctx == nil` check lets it through and context.WithCancel(ctx) panics on
// the nil receiver (see isNilValue).
func TestRunTypedNilContextReturnsErrorInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(batchesSource{batches: []Batch{intBatch(t, 1)}}).To(SinkFuncs{})

	var typedNil *nilPtrContext
	err := p.Run(typedNil)
	if err == nil {
		t.Fatal("want error for a typed-nil context.Context, got nil")
	}
	const want = "etl: Run called with a nil context.Context"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
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

// nilPtrSource, nilPtrProcessor, and nilPtrSink dereference their receiver,
// so a nil *T wrapped in the interface is != nil but panics on first use —
// exercising isNilValue's typed-nil detection in From/Process/Merge/To.
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
// with value-receiver methods that never touch the receiver (mirroring
// http.HandlerFunc), so a nil value is a legal stage — unlike
// nilPtrSource/nilPtrProcessor/nilPtrSink above, isNilValue must not reject
// them.
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

// nilBatchSource sends a typed-nil *ArrowBatch through Output.Send, the
// Batch-side analogue of the nilPtr* stages above — exercising Send's
// typed-nil detection instead of a builder method's.
type nilBatchSource struct{}

func (nilBatchSource) Run(ctx context.Context, out Output) error {
	var b *ArrowBatch
	return out.Send(ctx, b)
}

func TestSendTypedNilBatchReturnsErrorInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(nilBatchSource{}).To(SinkFuncs{})

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("want error for typed-nil Batch passed to Send, got nil")
	}
	const want = "etl: cannot send a nil batch"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
	}
}

// nilRecordBatchSource wraps a nil arrow.Record in a non-nil *ArrowBatch
// (e.g. via NewBatch(nil)), exercising nilBatch's check of the wrapped
// record rather than the ArrowBatch pointer itself.
type nilRecordBatchSource struct{}

func (nilRecordBatchSource) Run(ctx context.Context, out Output) error {
	return out.Send(ctx, NewBatch(nil))
}

func TestSendArrowBatchWrappingNilRecordReturnsErrorInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(nilRecordBatchSource{}).To(SinkFuncs{})

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("want error for a non-nil *ArrowBatch wrapping a nil arrow.Record, got nil")
	}
	const want = "etl: cannot send a nil batch"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
	}
}

// customNilBatch is a Batch implementation other than *ArrowBatch, so a nil
// value of it exercises nilBatch's reflection fallback rather than its
// *ArrowBatch fast path.
type customNilBatch struct{ *ArrowBatch }

type customNilBatchSource struct{}

func (customNilBatchSource) Run(ctx context.Context, out Output) error {
	var b *customNilBatch
	return out.Send(ctx, Batch(b))
}

// typedNilCtxSource sends a well-formed batch but passes a typed-nil
// context.Context to Send, exercising Send's own ctx == nil fallback check
// rather than nilBatch.
type typedNilCtxSource struct {
	t *testing.T
}

func (s typedNilCtxSource) Run(ctx context.Context, out Output) error {
	var typedNil *nilPtrContext
	return out.Send(typedNil, intBatch(s.t, 1))
}

// TestSendTypedNilContextFallsBackToNodeContextInsteadOfPanicking is a
// regression test: Send's `ctx == nil` check missed a typed-nil
// context.Context, so it was kept instead of falling back to the node's own
// context, and ctx.Done() panicked on the nil receiver. Send now uses
// isNilValue, matching Run's typed-nil validation.
func TestSendTypedNilContextFallsBackToNodeContextInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(typedNilCtxSource{t: t}).To(SinkFuncs{})

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestSendTypedNilCustomBatchReturnsErrorInsteadOfPanicking(t *testing.T) {
	p := New()
	p.From(customNilBatchSource{}).To(SinkFuncs{})

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("want error for typed-nil custom Batch passed to Send, got nil")
	}
	const want = "etl: cannot send a nil batch"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
	}
}

// nilMapBatch and nilSliceBatch use the same nil-value-receiver-adapter
// shape as nilSliceSource above, but nilBatch must reject them: unlike a
// stage, Batch's methods must return real data, which a nil map/slice
// receiver can't supply.
type nilMapBatch map[string]int

func (nilMapBatch) Schema() *arrow.Schema { return nil }
func (nilMapBatch) NumRows() int64        { return 0 }
func (nilMapBatch) Record() arrow.Record  { return nil }
func (nilMapBatch) Retain()               {}
func (nilMapBatch) Release()              {}

type nilSliceBatch []int

func (nilSliceBatch) Schema() *arrow.Schema { return nil }
func (nilSliceBatch) NumRows() int64        { return 0 }
func (nilSliceBatch) Record() arrow.Record  { return nil }
func (nilSliceBatch) Retain()               {}
func (nilSliceBatch) Release()              {}

type nilMapBatchSource struct{}

func (nilMapBatchSource) Run(ctx context.Context, out Output) error {
	var b nilMapBatch
	return out.Send(ctx, b)
}

type nilSliceBatchSource struct{}

func (nilSliceBatchSource) Run(ctx context.Context, out Output) error {
	var b nilSliceBatch
	return out.Send(ctx, b)
}

func TestSendNilMapOrSliceBatchReturnsErrorInsteadOfPanicking(t *testing.T) {
	const want = "etl: cannot send a nil batch"

	t.Run("map", func(t *testing.T) {
		p := New()
		p.From(nilMapBatchSource{}).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil map-backed Batch passed to Send, got nil")
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
		}
	})

	t.Run("slice", func(t *testing.T) {
		p := New()
		p.From(nilSliceBatchSource{}).To(SinkFuncs{})

		err := p.Run(context.Background())
		if err == nil {
			t.Fatal("want error for nil slice-backed Batch passed to Send, got nil")
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Run error = %q, want it to contain %q", err.Error(), want)
		}
	})
}

// TestPipelineFrozenAfterFailedValidationRun verifies a Run that fails from
// a nil-stage validation error still marks the pipeline started: a later
// builder call panics, and a second Run reports "already run" rather than
// re-returning the validation error.
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
