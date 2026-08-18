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
