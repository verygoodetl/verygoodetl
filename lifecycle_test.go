package etl

import (
	"context"
	"errors"
	"testing"
)

type errorSource struct {
	err error
}

func (s errorSource) Run(context.Context, Output) error { return s.err }

type cancelSource struct{}

func (cancelSource) Run(ctx context.Context, _ Output) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestUpstreamFailureSkipsDownstreamFinish(t *testing.T) {
	want := errors.New("extract failed")
	p := New()
	stream := p.From(errorSource{err: want})

	processorFinished := false
	sinkFinished := false
	stream.Process(ProcessorFuncs{
		FinishFunc: func(context.Context, Output) error {
			processorFinished = true
			return nil
		},
	}).To(SinkFuncs{
		FinishFunc: func(context.Context) error {
			sinkFinished = true
			return nil
		},
	})

	err := p.Run(context.Background())
	if !errors.Is(err, want) {
		t.Fatalf("Run error=%v, want %v", err, want)
	}
	if processorFinished {
		t.Fatal("processor Finish called after upstream failure")
	}
	if sinkFinished {
		t.Fatal("sink Finish called after upstream failure")
	}
}

func TestContextCancellationCancelsGraphAndSkipsFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := New()
	stream := p.From(cancelSource{})

	finished := false
	stream.To(SinkFuncs{FinishFunc: func(context.Context) error {
		finished = true
		return nil
	}})

	cancel()
	err := p.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v, want context.Canceled", err)
	}
	if finished {
		t.Fatal("Finish called after context cancellation")
	}
}

func TestCopyToPreservesStream(t *testing.T) {
	p := New()
	stream := p.From(batchesSource{batches: []Batch{intBatch(t, 1, 2)}})

	rawRows := 0
	cleanRows := 0
	stream.CopyTo(SinkFuncs{ConsumeFunc: func(_ context.Context, b Batch) error {
		rawRows += int(b.NumRows())
		return nil
	}}).Process(ProcessorFuncs{}).To(SinkFuncs{ConsumeFunc: func(_ context.Context, b Batch) error {
		cleanRows += int(b.NumRows())
		return nil
	}})

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rawRows != 2 || cleanRows != 2 {
		t.Fatalf("rawRows=%d cleanRows=%d, want 2/2", rawRows, cleanRows)
	}
}
