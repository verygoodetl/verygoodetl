package filesink

import (
	"context"
	"fmt"
	"io"

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
// destination object. Without this option, a Sink sets only ContentType
// from the Format and otherwise uses gocloud's defaults, so writing to an
// existing key overwrites it. For durable, archival-style writes where a
// key must never be silently overwritten, pass
// WithWriterOptions(&blob.WriterOptions{IfNotExist: true}) — this makes
// writing to an existing key fail instead.
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
	if bucket == nil {
		s.constructErr = fmt.Errorf("filesink: New: nil bucket (use NewToWriter for a caller-supplied io.Writer)")
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewToWriter creates a Sink that writes batches to w directly using
// format, for callers who already have a destination io.Writer — stdout,
// an HTTP response writer, a pipe — and don't need a blob bucket at all.
// w is never closed by the Sink, matching Format.NewWriter's own contract
// for the io.Writer it's given; closing w, if it needs closing at all, is
// the caller's responsibility.
//
// WithWriterOptions has no effect on a Sink created this way: its knobs
// (ContentType, IfNotExist, ...) are blob-object concepts with no
// equivalent for an arbitrary io.Writer.
func NewToWriter(w io.Writer, format Format, opts ...SinkOption) *Sink {
	s := &Sink{w: w, format: format, writerBacked: true}
	if w == nil {
		s.constructErr = fmt.Errorf("filesink: NewToWriter: nil writer")
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Consume implements etl.Sink.
func (s *Sink) Consume(ctx context.Context, b etl.Batch) error {
	// Checked unconditionally, not just while opening on the first call:
	// b.Record() is handed straight to the format writer below regardless
	// of s.started, and every current Format implementation dereferences
	// its record argument (e.g. NumRows) with no nil check, so a nil
	// schema arriving on a later batch would panic instead of failing the
	// pipeline with an ordinary error, same as it would on the first.
	schema := b.Schema()
	if schema == nil {
		return fmt.Errorf("filesink: batch has a nil schema")
	}
	if !s.started {
		if err := s.open(ctx, schema); err != nil {
			return err
		}
	}
	if err := s.rw.Write(b.Record()); err != nil {
		s.abort()
		return fmt.Errorf("filesink: write batch: %w", err)
	}
	return nil
}

// Finish implements etl.Sink.
func (s *Sink) Finish(ctx context.Context) error {
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
		// Writer-path Sink (NewToWriter): there's no blob object to commit,
		// and w is never closed by the Sink — see NewToWriter's doc comment.
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

// open lazily creates the format writer for schema, over either the blob
// writer (New) or the caller-supplied io.Writer (NewToWriter). Which one is
// used is decided by s.writerBacked, set once at construction time by New
// vs. NewToWriter — not inferred here from whether s.bucket happens to be
// nil, which would misclassify a Sink constructed via New(nil, ...) as
// writer-backed and hand a nil s.w to the format writer instead of failing
// clearly. The context used to open the blob writer is derived from ctx so
// that failing either writer, or cancellation of the pipeline itself,
// aborts the write via cancel-then-Close per gocloud's documented pattern;
// a writer-path Sink has no analogous context-scoped resource to cancel.
func (s *Sink) open(ctx context.Context, schema *arrow.Schema) error {
	if s.constructErr != nil {
		return s.constructErr
	}

	if s.writerBacked {
		// Some Format implementations (e.g. Parquet's underlying writer)
		// close any io.Writer they're given that also implements io.Closer.
		// writeOnly hides Close so w is never closed here, per NewToWriter's
		// documented contract that it's the caller's own writer to manage.
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

	// Some Format implementations (e.g. Parquet's underlying writer) close
	// any io.Writer they're given that also implements io.Closer. writeOnly
	// hides Close so bw is closed exactly once, by Sink itself.
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

// writeOnly forwards Write but deliberately does not expose Close (or any
// other method) from w, even if w implements io.Closer.
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
// committing the object. Best-effort: gocloud documents this cancel-then-
// Close pattern but does not guarantee identical atomicity across every
// backend. A no-op past the first call, so it's safe to reach both from a
// Consume/Finish error path and from Abort.
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

// Abort implements etl.Aborter. The pipeline runtime calls it when this
// Sink's Finish will never run because of an upstream failure or
// cancellation, so a writer opened by an earlier Consume isn't left
// dangling — e.g. an in-flight blob upload that would otherwise never be
// canceled or closed.
func (s *Sink) Abort() {
	s.abort()
}
