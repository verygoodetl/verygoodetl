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

// isNilValue reports whether v is either an untyped nil interface (v == nil)
// or a typed nil pointer wrapped in a non-nil interface, e.g. a caller
// passing a `var s *mySource = nil` to From, or a `var b *ArrowBatch = nil`
// to Send. In Go, an interface value is == nil only when both its type and
// value are nil, so a plain `v == nil` check misses the typed-nil case: the
// interface carries a concrete type descriptor and a nil value pointer, so
// it compares != nil even though calling any method on it dereferences a
// nil receiver.
//
// The check is deliberately narrow: only Kind() == reflect.Ptr is treated as
// possibly nil-and-invalid. A nil map, slice, chan, or func can be a
// perfectly valid, safe value — for example a named slice type with a
// value-receiver method that never touches the receiver (mirroring
// http.HandlerFunc-style adapters), which is legal and idiomatic Go. Only a
// nil pointer is guaranteed to blow up the moment a method with a pointer
// receiver dereferences one of its fields, which is the concrete failure
// mode this validation exists to catch. (Kind() can never be
// reflect.Interface here: v's static type is the empty interface, so
// reflect.ValueOf(v) always unwraps to v's concrete dynamic type or is
// invalid; it is never itself interface-kinded.) Kind is checked before
// IsNil because IsNil panics on kinds that don't support it, e.g. a struct
// value implementing the interface.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// nilBatch is isNilValue specialized for Send, which runs on every batch at
// every stage rather than once per stage registration. *ArrowBatch is the
// only Batch implementation this package ships, so a type assertion to it
// serves the overwhelming majority of calls with only the cheap ab == nil
// check plus one isNilValue call on the wrapped record, reserving the
// heavier reflect.ValueOf(b) fallback below for the rare custom Batch
// implementation.
//
// Unlike isNilValue, nilBatch rejects every nil-capable kind (map, slice,
// func, chan, and pointer), not just nil pointers. isNilValue's pointer-only
// carve-out exists for Source/Processor/Sink, whose single-method shape
// makes a named map/slice/func/chan type with a value-receiver method that
// ignores the receiver a plausible, safe adapter (mirroring
// http.HandlerFunc). Batch has no such use case: Schema, NumRows, and
// Record must return real, usable data, which a nil map, slice, func, or
// chan receiver has no way to carry without the method touching the
// receiver — at which point a nil func or chan panics unconditionally on
// invocation, and a nil map or slice is indistinguishable from a
// legitimately empty one, so treating a nil value of any of these kinds as
// a well-formed Batch offers no real capability. Rejecting them here costs
// nothing valid while catching the same class of bug isNilValue's pointer
// check exists to catch.
func nilBatch(b Batch) bool {
	if ab, ok := b.(*ArrowBatch); ok {
		// ab.record is only read once ab itself is known non-nil: arrow.Record
		// is an interface, so a caller doing NewBatch((*array.RecordBatch)(nil))
		// (or any other typed-nil concrete record) produces a non-nil ab whose
		// record field is != nil by pointer-equality but panics the moment
		// Retain/Release/Schema/NumRows touches it. isNilValue already handles
		// exactly this shape for Source/Processor/Sink.
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

// setErrLocked records the first builder-time validation error for p, such as
// a nil stage passed to From, Process, Merge, or To. Callers must hold p.mu.
// Only the first error is kept: a nil stage recorded early in the chain is
// almost always the root cause, while later calls may themselves be built on
// top of the already-invalid Stream and would just add noise. The error is
// surfaced by Run instead of being returned directly from the builder methods
// so that graph construction can keep using its fluent, chained style without
// every call needing to check for an error before proceeding.
func (p *Pipeline) setErrLocked(err error) {
	if p.err == nil {
		p.err = err
	}
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

// Run executes the graph until every stage completes. A nil ctx is rejected
// immediately with an error: context.WithCancel(ctx) would otherwise panic
// before any stage goroutine starts, and unlike the nil-stage validation
// below this check happens before the pipeline is marked started, since a
// nil ctx is a caller mistake independent of anything already recorded on p.
// If a builder call such as From, Process, Merge, or To was given a nil stage, Run returns that
// error immediately without starting any stage goroutine. The pipeline is
// still marked as run in this case, so the graph is frozen just as it would
// be after a successful Run: further builder calls panic and a second call
// to Run returns the "already run" error rather than re-surfacing the
// validation error. Run always waits for every stage to finish unwinding,
// even after a failure or a canceled ctx,
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
	// fail's cancel below is what lets other stages' consumeInputs unblock a
	// stalled upstream node (see consumeInputs), so a stage failing for a real
	// reason routinely races against sibling stages that are merely reacting
	// to that same cancellation. Preferring a non-cancellation error over one
	// already recorded keeps the reported error the actual root cause instead
	// of whichever "context canceled" cascade happened to land first.
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
	// A typed nil pointer (e.g. `var b *ArrowBatch = nil`) wrapped in a
	// non-nil Batch interface would pass b == nil and then panic on the
	// Retain() call below; nilBatch catches that case the same way
	// isNilValue already does for stages passed to From/Process/Merge/To.
	if nilBatch(b) {
		return errors.New("etl: cannot send a nil batch")
	}
	if ctx == nil {
		ctx = o.ctx
	}

	// A stage with no downstream attachment (a dangling Process() branch, or
	// p.From(src) with nothing ever attached to it) has zero outgoing edges,
	// so the loop below never runs and its per-edge cancellation checks
	// never execute. Checking here too means Send still reports
	// cancellation even with no edges to send to, which a Source or
	// Processor relying solely on Send's returned error to learn about
	// cancellation (a pattern the Source/Processor docs above explicitly
	// permit) depends on to ever stop.
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
// each batch. On either early-return path below (a failing consume, or ctx
// already done), it cancels its own derived context and waits for every
// reader goroutine to observe that and exit before returning itself —
// otherwise those goroutines are left running detached, and a caller of
// Run could observe it return while they're still draining queued batches
// in the background. The happy path (merged closing) needs no such wait:
// close(merged) below only happens after readers.Wait() has already
// returned, so every reader is already done by construction.
//
// Canceling the derived context only stops this function's own reader
// goroutines — it has no effect on the upstream nodes feeding inputs, which
// watch the pipeline-wide ctx instead. Without also invoking cancel (the
// pipeline's CancelFunc) before waiting, an upstream node that never stops
// on its own (e.g. an unbounded source) would keep sending batches forever,
// so drainEdge would never see its edge close and readers.Wait() below
// would block forever.
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
