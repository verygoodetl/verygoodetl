package etl

import (
	"context"
	"errors"
	"testing"
	"time"
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

// danglingLoopSource sends batches in an unbounded loop and relies solely on
// Output.Send's returned error to learn about cancellation, exactly the
// pattern etl.Source's doc comment endorses ("Run must return any error from
// Output.Send... a swallowed Send error can prevent the pipeline from
// unwinding on cancellation or failure"). It never checks ctx itself.
type danglingLoopSource struct {
	started chan struct{}
}

func (s *danglingLoopSource) Run(ctx context.Context, out Output) error {
	b := intBatch(&testing.T{}, 1)
	for i := 0; ; i++ {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
		if i == 0 {
			close(s.started)
		}
	}
}

// TestSendReportsCancellationWithNoDownstreamEdges is a regression test:
// Output.Send's cancellation check lived only inside its per-edge select, so
// a stage with zero outgoing edges — a dangling Process() branch, or (as
// here) p.From(src) with nothing ever attached — skipped that loop entirely
// and Send unconditionally returned nil, no matter how long ctx had been
// canceled. A source that (like danglingLoopSource, and like the pattern
// etl.Source's own doc comment endorses) depends entirely on Send's returned
// error to know when to stop would then loop forever, and Run would never
// return. Send must check ctx even when there are no edges to send to.
func TestSendReportsCancellationWithNoDownstreamEdges(t *testing.T) {
	p := New()
	src := &danglingLoopSource{started: make(chan struct{})}
	p.From(src)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	<-src.started
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error=%v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation: a zero-edge Send never reported it, so the source's unbounded loop never stopped")
	}
}

func TestCompletedWorkIsNotReportedAsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// errorSource{err: nil}'s Run returns immediately without ever touching
	// ctx or Output, so it reliably returns nil here regardless of ctx's
	// state. That isolates the invariant under test: Run reports success
	// whenever every stage's own Run/Process/Finish returns nil, regardless
	// of whether the caller separately canceled ctx. A stage that actually
	// depends on ctx to do its work (e.g. cancelSource above) still reports
	// its own error when canceled, and that error — not the mere fact that
	// ctx was canceled — is what Run returns; see
	// TestContextCancellationCancelsGraphAndSkipsFinish. (A dangling source
	// with no downstream stage does not qualify for this: Output.Send still
	// checks ctx even with zero edges, precisely so a source that only
	// learns about cancellation via Send's returned error can still stop.)
	p := New()
	p.From(errorSource{err: nil})

	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run error=%v, want nil: no stage reported an error, so the work isn't \"canceled\" even though ctx was", err)
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
