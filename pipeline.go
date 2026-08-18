package etl

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const defaultBufferSize = 4

type nodeKind uint8

const (
	sourceNode nodeKind = iota
	processorNode
	sinkNode
)

type envelope struct {
	batch Batch
}

type edge struct {
	ch chan envelope
}

type node struct {
	id        int
	kind      nodeKind
	source    Source
	processor Processor
	sink      Sink
	incoming  []*edge
	outgoing  []*edge
}

// Pipeline is a directed acyclic graph of sources, processors, and sinks.
// A Pipeline is built with From/Process/Merge/To and then run at most once:
// Run marks it started, after which any graph-building call panics and a
// second call to Run returns an error rather than reusing closed edges.
type Pipeline struct {
	mu         sync.Mutex
	nodes      []*node
	bufferSize int
	started    bool
}

// Option configures a Pipeline.
type Option func(*Pipeline)

// WithBufferSize sets the capacity of each graph edge. Bounded edges provide
// backpressure when a downstream stage cannot keep up.
func WithBufferSize(size int) Option {
	return func(p *Pipeline) {
		if size >= 0 {
			p.bufferSize = size
		}
	}
}

// New creates an empty pipeline.
func New(opts ...Option) *Pipeline {
	p := &Pipeline{bufferSize: defaultBufferSize}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Stream identifies the output of a stage and is used to construct the graph.
type Stream struct {
	pipeline *Pipeline
	node     *node
}

// From adds a source to the graph.
func (p *Pipeline) From(src Source) Stream {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.panicIfStartedLocked()
	n := &node{id: len(p.nodes), kind: sourceNode, source: src}
	p.nodes = append(p.nodes, n)
	return Stream{pipeline: p, node: n}
}

// Process adds a processor downstream of this stream.
func (s Stream) Process(processor Processor) Stream {
	p := s.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	p.panicIfStartedLocked()
	n := &node{id: len(p.nodes), kind: processorNode, processor: processor}
	p.nodes = append(p.nodes, n)
	p.connect(s.node, n)
	return Stream{pipeline: p, node: n}
}

// Merge creates a fan-in stage. Batches are processed as they arrive from any
// input. Finish is not invoked until every input stream has completed.
func (p *Pipeline) Merge(processor Processor, inputs ...Stream) Stream {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.panicIfStartedLocked()
	n := &node{id: len(p.nodes), kind: processorNode, processor: processor}
	p.nodes = append(p.nodes, n)
	for _, input := range inputs {
		if input.pipeline != p {
			panic("etl: cannot merge streams from different pipelines")
		}
		p.connect(input.node, n)
	}
	return Stream{pipeline: p, node: n}
}

// To attaches a sink to this stream.
func (s Stream) To(sink Sink) {
	p := s.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	p.panicIfStartedLocked()
	n := &node{id: len(p.nodes), kind: sinkNode, sink: sink}
	p.nodes = append(p.nodes, n)
	p.connect(s.node, n)
}

// panicIfStartedLocked panics if Run has already been called on p. Callers
// must hold p.mu. Graph edges are plain channels created once by connect and
// closed exactly once by Run (see closeEdges); allowing graph mutation after
// Run has started would race with the running goroutines over those same
// node and edge structures, and a node added after the initial snapshot in
// Run would never be scheduled at all.
func (p *Pipeline) panicIfStartedLocked() {
	if p.started {
		panic("etl: cannot modify a pipeline's graph after Run has been called")
	}
}

// CopyTo sends each batch to sink while preserving this stream for additional
// downstream processing. It is a logical copy: Arrow buffers are retained and
// shared rather than necessarily copied in memory.
func (s Stream) CopyTo(sink Sink) Stream {
	s.To(sink)
	return s
}

func (p *Pipeline) connect(from, to *node) {
	e := &edge{ch: make(chan envelope, p.bufferSize)}
	from.outgoing = append(from.outgoing, e)
	to.incoming = append(to.incoming, e)
}

// Run executes the graph until every stage completes. Run always waits for
// every stage to finish unwinding, even after a failure or a canceled ctx,
// since stages are cooperative: only a stage's own returned error is ever
// reported, not the mere fact that ctx was canceled. In practice this
// distinction only matters when a stage neither checks ctx itself nor has
// any Output.Send call left to notice it (see Output.Send in etl.go) — an
// otherwise well-behaved stage that respects ctx reports its own error when
// canceled, and that error is what Run returns. If every stage completes
// without an error, Run returns nil even if ctx was canceled concurrently:
// the work finished before the cancellation could have any effect, so it
// wasn't actually canceled. The first stage error cancels the pipeline for
// every other stage, and so does an external cancellation of ctx.
//
// A Pipeline may be run at most once: Run closes every graph edge's channel
// as its stages finish, so a second call would send on or close already-
// closed channels. Calling Run again returns an error instead of reusing
// those closed edges.
func (p *Pipeline) Run(ctx context.Context) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("etl: pipeline already run; a Pipeline may be run at most once")
	}
	p.started = true
	nodes := append([]*node(nil), p.nodes...)
	p.mu.Unlock()

	if len(nodes) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	fail := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	for _, n := range nodes {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := runNode(ctx, n)
			if err != nil {
				// Cancel the graph before closing this stage's outputs. This prevents
				// downstream stages from observing a clean EOF and entering Finish
				// after an upstream failure.
				fail(fmt.Errorf("stage %d: %w", n.id, err))
			}
			closeEdges(n.outgoing)
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func runNode(ctx context.Context, n *node) error {
	out := nodeOutput{ctx: ctx, edges: n.outgoing}

	switch n.kind {
	case sourceNode:
		return n.source.Run(ctx, out)
	case processorNode:
		if err := consumeInputs(ctx, n.incoming, func(b Batch) error {
			defer b.Release()
			return n.processor.Process(ctx, b, out)
		}); err != nil {
			abortIfAborter(n.processor)
			return err
		}
		if err := ctx.Err(); err != nil {
			abortIfAborter(n.processor)
			return err
		}
		return n.processor.Finish(ctx, out)
	case sinkNode:
		if err := consumeInputs(ctx, n.incoming, func(b Batch) error {
			defer b.Release()
			return n.sink.Consume(ctx, b)
		}); err != nil {
			abortIfAborter(n.sink)
			return err
		}
		if err := ctx.Err(); err != nil {
			abortIfAborter(n.sink)
			return err
		}
		return n.sink.Finish(ctx)
	default:
		return errors.New("unknown node kind")
	}
}

// abortIfAborter calls Abort on v if it optionally implements Aborter. It is
// called in place of Finish whenever a processor or sink node is about to
// return early because of an upstream failure or cancellation, giving a
// stage holding external resources (e.g. filesink.Sink's open blob writer) a
// chance for best-effort cleanup instead of leaking them.
func abortIfAborter(v any) {
	if a, ok := v.(Aborter); ok {
		a.Abort()
	}
}

func closeEdges(edges []*edge) {
	for _, e := range edges {
		close(e.ch)
	}
}

type nodeOutput struct {
	ctx   context.Context
	edges []*edge
}

func (o nodeOutput) Send(ctx context.Context, b Batch) error {
	if b == nil {
		return errors.New("etl: cannot send a nil batch")
	}
	if ctx == nil {
		ctx = o.ctx
	}

	for _, e := range o.edges {
		b.Retain()
		select {
		case e.ch <- envelope{batch: b}:
		case <-ctx.Done():
			b.Release()
			return ctx.Err()
		case <-o.ctx.Done():
			b.Release()
			return o.ctx.Err()
		}
	}
	return nil
}

func consumeInputs(ctx context.Context, inputs []*edge, consume func(Batch) error) error {
	if len(inputs) == 0 {
		return nil
	}

	merged := make(chan Batch)
	var readers sync.WaitGroup
	readers.Add(len(inputs))

	for _, input := range inputs {
		input := input
		go func() {
			defer readers.Done()
			for {
				select {
				case env, ok := <-input.ch:
					if !ok {
						return
					}
					select {
					case merged <- env.batch:
					case <-ctx.Done():
						env.batch.Release()
						drainEdge(input)
						return
					}
				case <-ctx.Done():
					drainEdge(input)
					return
				}
			}
		}()
	}

	go func() {
		readers.Wait()
		close(merged)
	}()

	for {
		select {
		case b, ok := <-merged:
			if !ok {
				return nil
			}
			if err := consume(b); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func drainEdge(e *edge) {
	for env := range e.ch {
		env.batch.Release()
	}
}
