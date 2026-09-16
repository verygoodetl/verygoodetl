package etl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	err        error
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
	if isNilValue(src) {
		p.setErrLocked(errors.New("etl: From called with a nil Source"))
	}
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
	if isNilValue(processor) {
		p.setErrLocked(errors.New("etl: Process called with a nil Processor"))
	}
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
	if isNilValue(processor) {
		p.setErrLocked(errors.New("etl: Merge called with a nil Processor"))
	}
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
	if isNilValue(sink) {
		p.setErrLocked(errors.New("etl: To called with a nil Sink"))
	}
	n := &node{id: len(p.nodes), kind: sinkNode, sink: sink}
	p.nodes = append(p.nodes, n)
	p.connect(s.node, n)
}

// isNilValue reports whether v is nil, or a typed nil pointer wrapped in a
// non-nil interface (e.g. `var s *mySource = nil` passed to From) — a case
// `v == nil` misses because the interface carries a non-nil type descriptor.
//
// Only reflect.Ptr is treated as possibly nil-and-invalid: a nil map, slice,
// chan, or func can be a legitimate value-receiver adapter (mirroring
// http.HandlerFunc) that never touches its receiver. Kind is checked before
// IsNil because IsNil panics on kinds that don't support it (e.g. a struct
// implementing the interface).
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// nilBatch is isNilValue specialized for Send, which runs on every batch
// rather than once per stage registration: the *ArrowBatch fast path avoids
// reflect.ValueOf for the common case, falling back to reflection only for
// a custom Batch implementation.
//
// Unlike isNilValue, nilBatch rejects every nil-capable kind (map, slice,
// func, chan, pointer), not just pointers — isNilValue's pointer-only
// carve-out assumes a value-receiver adapter that ignores its receiver,
// which doesn't apply here: Schema, NumRows, and Record must return real
// data, so a nil Batch of any kind is never usable.
func nilBatch(b Batch) bool {
	if ab, ok := b.(*ArrowBatch); ok {
		// arrow.Record is an interface: NewBatch(typedNilRecord) can produce a
		// non-nil ab whose record is itself a typed nil — the same trap
		// isNilValue guards against for Source/Processor/Sink.
		return ab == nil || isNilValue(ab.record)
	}
	if b == nil {
		return true
	}
	rv := reflect.ValueOf(b)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// setErrLocked records the first builder-time validation error for p (e.g. a
// nil stage passed to From/Process/Merge/To); later errors are usually just
// downstream noise from the same root cause. Surfaced via Run rather than
// returned directly so the fluent builder chain never needs to check for an
// error mid-chain. Callers must hold p.mu.
func (p *Pipeline) setErrLocked(err error) {
	if p.err == nil {
		p.err = err
	}
}

// panicIfStartedLocked panics if Run has already been called on p. Callers
// must hold p.mu. Graph edges are closed exactly once by Run (see
// closeEdges); mutating the graph after that would race the running
// goroutines over the same node/edge structures.
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

// Run executes the graph until every stage completes. A nil ctx is rejected
// before the pipeline is marked started, since context.WithCancel(ctx) would
// otherwise panic and a caller mistake shouldn't freeze the graph.
//
// If a builder call (From/Process/Merge/To) was given a nil stage, Run
// returns that error without starting any stage, but still marks the
// pipeline started so the single-use contract holds either way: further
// builder calls panic, and a second Run reports "already run".
//
// Run always waits for every stage to unwind, even after a failure or a
// canceled ctx: stages are cooperative, so only a stage's own returned error
// is reported, not the mere fact that ctx was canceled. If every stage
// returns nil, Run returns nil even under a concurrent cancellation — the
// work finished before it could take effect. The first stage error, or an
// external cancellation, stops every other stage.
//
// A Pipeline may be run at most once: Run closes every edge's channel as its
// stages finish, so a second call returns an error instead of operating on
// already-closed channels.
func (p *Pipeline) Run(ctx context.Context) error {
	if isNilValue(ctx) {
		return errors.New("etl: Run called with a nil context.Context")
	}

	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("etl: pipeline already run; a Pipeline may be run at most once")
	}
	p.started = true
	if p.err != nil {
		p.mu.Unlock()
		return p.err
	}
	nodes := append([]*node(nil), p.nodes...)
	p.mu.Unlock()

	if len(nodes) == 0 {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		recorded error
	)
	// cancel() unblocks other stages' consumeInputs (see consumeInputs), which
	// races a genuine failure against sibling stages merely reacting to that
	// same cancellation. Preferring a non-cancellation error keeps the
	// reported error the actual root cause, not whichever "context canceled"
	// cascade lands first.
	fail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if recorded == nil || (isCancellationErr(recorded) && !isCancellationErr(err)) {
			recorded = err
		}
		mu.Unlock()
		cancel()
	}

	for _, n := range nodes {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := runNode(ctx, cancel, n)
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

	return recorded
}

// isCancellationErr reports whether err is exactly the kind of error a stage
// returns solely because ctx was canceled out from under it, as opposed to a
// failure of its own.
func isCancellationErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func runNode(ctx context.Context, cancel context.CancelFunc, n *node) error {
	out := nodeOutput{ctx: ctx, edges: n.outgoing}

	switch n.kind {
	case sourceNode:
		return n.source.Run(ctx, out)
	case processorNode:
		if err := consumeInputs(ctx, cancel, n.incoming, func(b Batch) error {
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
		if err := consumeInputs(ctx, cancel, n.incoming, func(b Batch) error {
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

// abortIfAborter calls Abort in place of Finish when a stage is skipping
// Finish due to upstream failure or cancellation (see Aborter).
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
	// nilBatch also catches a typed-nil *ArrowBatch that would otherwise
	// panic on Retain() below (see nilBatch).
	if nilBatch(b) {
		return errors.New("etl: cannot send a nil batch")
	}
	if isNilValue(ctx) {
		ctx = o.ctx
	}

	// Checked here too (not just in the per-edge loop below): a stage with
	// zero outgoing edges — a dangling Process(), or a From with nothing
	// attached — would otherwise never observe cancellation via Send, and a
	// Source relying solely on Send's error to know when to stop (see
	// Source's doc comment) would loop forever.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-o.ctx.Done():
		return o.ctx.Err()
	default:
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

// consumeInputs merges inputs into a single stream and calls consume for
// each batch. On early return (a failing consume, or ctx already done), it
// cancels its own readers and waits for them to exit before returning —
// otherwise a reader could still be draining and releasing queued batches
// after the caller (and Run) has already moved on. The happy path needs no
// such wait: close(merged) only happens after readers.Wait() already has.
//
// Canceling the derived readCtx only stops this function's own readers, not
// the upstream nodes feeding inputs; without also invoking the pipeline's
// cancel, an upstream that never stops on its own (e.g. an unbounded source)
// would keep sending forever and readers.Wait() below would block forever.
func consumeInputs(ctx context.Context, cancel context.CancelFunc, inputs []*edge, consume func(Batch) error) error {
	if len(inputs) == 0 {
		return nil
	}

	readCtx, cancelReaders := context.WithCancel(ctx)
	defer cancelReaders()

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
					case <-readCtx.Done():
						env.batch.Release()
						drainEdge(input)
						return
					}
				case <-readCtx.Done():
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
				cancel()
				cancelReaders()
				readers.Wait()
				return err
			}
		case <-readCtx.Done():
			err := ctx.Err()
			cancel()
			cancelReaders()
			readers.Wait()
			return err
		}
	}
}

func drainEdge(e *edge) {
	for env := range e.ch {
		env.batch.Release()
	}
}
