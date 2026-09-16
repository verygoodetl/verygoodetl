package filesink

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"

	etl "github.com/verygoodetl/verygoodetl"
)

// Sink writes batches to a single destination, encoded with format: either
// an object at key in a bucket (via New), or a caller-supplied io.Writer
// directly (via NewToWriter). A Sink writes to that destination exactly
// once and must be attached to exactly one node in a pipeline; it is not
// safe to reuse across multiple streams.
type Sink struct {
	bucket         *blob.Bucket
	key            string
	w              io.Writer
	writerBacked   bool // set once at construction: true for NewToWriter, false for New
	format         Format
	writerOpts     *blob.WriterOptions
	explicitSchema *arrow.Schema
	constructErr   error // set at construction if bucket/w was nil for the requested mode

	started bool
	bw      *blob.Writer
	rw      RecordWriter
	cancel  context.CancelFunc
	aborted bool
}

var _ etl.Sink = (*Sink)(nil)
var _ etl.Aborter = (*Sink)(nil)

// SinkOption configures a Sink.
type SinkOption func(*Sink)

// WithWriterOptions overrides the blob.WriterOptions used to open the
// destination object. Without it, a Sink only sets ContentType and
// otherwise uses gocloud's defaults, so writing to an existing key
// overwrites it — pass IfNotExist: true for archival writes that must fail
// instead of overwriting.
func WithWriterOptions(o *blob.WriterOptions) SinkOption {
	return func(s *Sink) { s.writerOpts = o }
}

// WithSchema forces Sink to write a valid, empty object if Finish runs
// without any batch ever having been consumed. Without this option, a Sink
// that never receives a batch writes nothing.
func WithSchema(schema *arrow.Schema) SinkOption {
	return func(s *Sink) { s.explicitSchema = schema }
}

// New creates a Sink that writes batches to key in bucket using format. A
// nil bucket is a construction error, surfaced from the first Consume or
// Finish call rather than returned here, so New can keep composing directly
// into a pipeline (e.g. p.To(filesink.New(...))); callers who already have
// an io.Writer instead of a bucket should use NewToWriter.
func New(bucket *blob.Bucket, key string, format Format, opts ...SinkOption) *Sink {
	s := &Sink{bucket: bucket, key: key, format: format}
	switch {
	case bucket == nil:
		s.constructErr = fmt.Errorf("filesink: New: nil bucket (use NewToWriter for a caller-supplied io.Writer)")
	case isNilFormat(format):
		s.constructErr = fmt.Errorf("filesink: New: nil format")
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewToWriter creates a Sink that writes batches to w directly using
// format, for callers who already have a destination io.Writer (stdout, an
// HTTP response writer, a pipe) instead of a blob bucket. w is never closed
// by the Sink; closing it, if needed, is the caller's responsibility.
//
// WithWriterOptions has no effect here: its knobs are blob-object concepts
// with no equivalent for an arbitrary io.Writer.
func NewToWriter(w io.Writer, format Format, opts ...SinkOption) *Sink {
	s := &Sink{w: w, format: format, writerBacked: true}
	switch {
	case isNilWriter(w):
		s.constructErr = fmt.Errorf("filesink: NewToWriter: nil writer")
	case isNilFormat(format):
		s.constructErr = fmt.Errorf("filesink: NewToWriter: nil format")
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// isNilWriter reports whether w is nil, including a typed nil pointer or a
// nil map/slice/func/chan wrapped in a non-nil interface — cases a plain
// w == nil check misses. Broader than isNilFormat/the root etl package's
// isNilValue: io.Writer's single Write method has no safe non-pointer nil
// implementation, unlike the open Source/Processor/Sink/Format interfaces
// (where a value-receiver adapter that ignores the receiver, mirroring
// http.HandlerFunc, remains plausible). Same reasoning as nilBatch
// (pipeline.go) for Batch.
func isNilWriter(w io.Writer) bool {
	if w == nil {
		return true
	}
	rv := reflect.ValueOf(w)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// isNilFormat reports whether format is nil, including a typed nil pointer
// wrapped in a non-nil interface. Format is an open interface like
// Source/Processor/Sink, so unlike isNilWriter this only treats a nil
// pointer as invalid — a nil map/slice/func/chan with a value-receiver
// method remains a plausible, safe adapter here.
func isNilFormat(format Format) bool {
	if format == nil {
		return true
	}
	rv := reflect.ValueOf(format)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// isNilRecord reports whether rec is nil, including a typed nil pointer
// wrapped in a non-nil interface — e.g. a custom etl.Batch whose Record
// method returns a typed-nil concrete arrow.Record. Mirrors nilBatch's
// check (pipeline.go) for the general etl.Batch case filesink must handle.
func isNilRecord(rec arrow.Record) bool {
	if rec == nil {
		return true
	}
	rv := reflect.ValueOf(rec)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// Consume implements etl.Sink.
func (s *Sink) Consume(ctx context.Context, b etl.Batch) error {
	// Checked on every call, not just the first: a nil schema on a later
	// batch would still reach the format writer's unchecked dereference.
	schema := b.Schema()
	if schema == nil {
		return fmt.Errorf("filesink: batch has a nil schema")
	}
	// A non-nil schema doesn't guarantee a usable record: a custom
	// etl.Batch can return nil (or typed-nil) from Record() despite a real
	// Schema(), past nilBatch's own check (pipeline.go, built-ins only).
	record := b.Record()
	if isNilRecord(record) {
		return fmt.Errorf("filesink: batch has a nil record")
	}
	if !s.started {
		if err := s.open(ctx, schema); err != nil {
			return err
		}
	}
	if err := s.rw.Write(record); err != nil {
		s.abort()
		return fmt.Errorf("filesink: write batch: %w", err)
	}
	return nil
}

// Finish implements etl.Sink.
func (s *Sink) Finish(ctx context.Context) error {
	if s.constructErr != nil {
		return s.constructErr
	}
	if !s.started {
		if s.explicitSchema == nil {
			return nil
		}
		if err := s.open(ctx, s.explicitSchema); err != nil {
			return err
		}
	}

	if err := s.rw.Close(); err != nil {
		s.abort()
		return fmt.Errorf("filesink: close format writer: %w", err)
	}
	if s.bw == nil {
		// Writer-path Sink: no blob object to commit; w is never closed here.
		return nil
	}
	if err := s.bw.Close(); err != nil {
		if s.cancel != nil {
			s.cancel()
		}
		return fmt.Errorf("filesink: commit object: %w", err)
	}
	return nil
}

// open lazily creates the format writer for schema, over the blob writer
// (New) or the caller-supplied io.Writer (NewToWriter), per s.writerBacked
// — not inferred from s.bucket == nil, which would misclassify
// New(nil, ...) as writer-backed and hand a nil s.w to the format writer
// instead of failing clearly. writeCtx is derived from ctx so upstream
// cancellation or a writer failure aborts via cancel-then-Close (gocloud's
// documented pattern); a writer-path Sink has no such resource to cancel.
func (s *Sink) open(ctx context.Context, schema *arrow.Schema) error {
	if s.constructErr != nil {
		return s.constructErr
	}

	if s.writerBacked {
		// writeOnly hides Close so w is never closed here — NewToWriter's
		// contract leaves it to the caller.
		rw, err := s.format.NewWriter(schema, writeOnly{s.w})
		if err != nil {
			return fmt.Errorf("filesink: new format writer: %w", err)
		}
		s.started = true
		s.rw = rw
		return nil
	}

	writeCtx, cancel := context.WithCancel(ctx)

	bw, err := s.bucket.NewWriter(writeCtx, s.key, s.writerOptions())
	if err != nil {
		cancel()
		return fmt.Errorf("filesink: open writer: %w", err)
	}

	// writeOnly hides Close so bw is closed exactly once, by Sink itself.
	rw, err := s.format.NewWriter(schema, writeOnly{bw})
	if err != nil {
		cancel()
		_ = bw.Close()
		return fmt.Errorf("filesink: new format writer: %w", err)
	}

	s.started = true
	s.bw = bw
	s.rw = rw
	s.cancel = cancel
	return nil
}

// writeOnly forwards Write but hides Close (and everything else) from w,
// since some Format implementations (e.g. Parquet's writer) close any
// io.Writer they're given that also implements io.Closer.
type writeOnly struct {
	w io.Writer
}

func (w writeOnly) Write(p []byte) (int, error) { return w.w.Write(p) }

func (s *Sink) writerOptions() *blob.WriterOptions {
	if s.writerOpts != nil {
		opts := *s.writerOpts
		if opts.ContentType == "" {
			opts.ContentType = s.format.ContentType()
		}
		return &opts
	}
	return &blob.WriterOptions{ContentType: s.format.ContentType()}
}

// abort cancels the in-flight write and releases the blob writer without
// committing the object. Best-effort — gocloud's cancel-then-Close pattern
// isn't guaranteed atomic across every backend. Idempotent, so it's safe to
// reach from both a Consume/Finish error path and from Abort.
func (s *Sink) abort() {
	if s.aborted {
		return
	}
	s.aborted = true
	if s.cancel != nil {
		s.cancel()
	}
	if s.bw != nil {
		_ = s.bw.Close()
	}
}

// Abort implements etl.Aborter. Called when Finish will never run (upstream
// failure or cancellation), so a writer opened by an earlier Consume isn't
// left dangling.
func (s *Sink) Abort() {
	s.abort()
}
