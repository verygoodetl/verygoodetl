package archive

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"gocloud.dev/blob"

	etl "github.com/verygoodetl/verygoodetl"
)

// Sink archives batches to a single object at key in bucket, encoded with
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
// destination object. Without this option, a Sink sets ContentType from the
// Format and IfNotExist to true, so writing to a key that already exists
// fails rather than silently overwriting a prior archive. Pass
// WithWriterOptions explicitly (e.g. with a zero-value *blob.WriterOptions)
// to allow overwriting.
func WithWriterOptions(o *blob.WriterOptions) SinkOption {
	return func(s *Sink) { s.writerOpts = o }
}

// WithSchema forces Sink to write a valid, empty archive object if Finish
// runs without any batch ever having been consumed. Without this option, a
// Sink that never receives a batch writes nothing.
func WithSchema(schema *arrow.Schema) SinkOption {
	return func(s *Sink) { s.explicitSchema = schema }
}

// NewSink creates a Sink that archives batches to key in bucket using
// format.
func NewSink(bucket *blob.Bucket, key string, format Format, opts ...SinkOption) *Sink {
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
		return fmt.Errorf("archive: write batch: %w", err)
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
		return fmt.Errorf("archive: close format writer: %w", err)
	}
	if err := s.bw.Close(); err != nil {
		if s.cancel != nil {
			s.cancel()
		}
		return fmt.Errorf("archive: commit object: %w", err)
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
		return fmt.Errorf("archive: open writer: %w", err)
	}

	// Some Format implementations (e.g. Parquet's underlying writer) close
	// any io.Writer they're given that also implements io.Closer. writeOnly
	// hides Close so bw is closed exactly once, by Sink itself.
	rw, err := s.format.NewWriter(schema, writeOnly{bw})
	if err != nil {
		cancel()
		_ = bw.Close()
		return fmt.Errorf("archive: new format writer: %w", err)
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
	return &blob.WriterOptions{
		ContentType: s.format.ContentType(),
		IfNotExist:  true,
	}
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
