package filesink

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"

	etl "github.com/verygoodetl/verygoodetl"
)

// Sink writes batches to a single object at key in bucket, encoded with
// format. A Sink writes exactly one object and must be attached to exactly
// one node in a pipeline; it is not safe to reuse across multiple streams.
type Sink struct {
	bucket         *blob.Bucket
	key            string
	format         Format
	writerOpts     *blob.WriterOptions
	explicitSchema *arrow.Schema

	started bool
	bw      *blob.Writer
	rw      RecordWriter
	cancel  context.CancelFunc
}

var _ etl.Sink = (*Sink)(nil)

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

// New creates a Sink that writes batches to key in bucket using
// format.
func New(bucket *blob.Bucket, key string, format Format, opts ...SinkOption) *Sink {
	s := &Sink{bucket: bucket, key: key, format: format}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Consume implements etl.Sink.
func (s *Sink) Consume(ctx context.Context, b etl.Batch) error {
	if !s.started {
		if err := s.open(ctx, b.Schema()); err != nil {
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
	if err := s.bw.Close(); err != nil {
		if s.cancel != nil {
			s.cancel()
		}
		return fmt.Errorf("filesink: commit object: %w", err)
	}
	return nil
}

// open lazily creates the blob writer and format writer for schema. The
// context used to open the blob writer is derived from ctx so that failing
// either writer, or cancellation of the pipeline itself, aborts the write
// via cancel-then-Close per gocloud's documented pattern.
func (s *Sink) open(ctx context.Context, schema *arrow.Schema) error {
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
// backend.
func (s *Sink) abort() {
	if s.cancel != nil {
		s.cancel()
	}
	if s.bw != nil {
		_ = s.bw.Close()
	}
}
