package filesink_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"gocloud.dev/blob"
	"gocloud.dev/blob/fileblob"
	"gocloud.dev/blob/memblob"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/filesink"
)

type batchesSource struct {
	batches []etl.Batch
}

func (s batchesSource) Run(ctx context.Context, out etl.Output) error {
	for _, b := range s.batches {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func batch(t *testing.T, schema *arrow.Schema, values ...int64) etl.Batch {
	t.Helper()
	return etl.NewBatch(intRecord(t, schema, values...))
}

// batchThenErrorSource sends one pre-built batch, then fails. It simulates
// an upstream Source (or sibling Processor) failing partway through a
// stream after a downstream Sink has already consumed at least one batch —
// as opposed to errorSource (in the root package's tests), which fails
// before sending anything.
type batchThenErrorSource struct {
	batch etl.Batch
	err   error
}

func (s batchThenErrorSource) Run(ctx context.Context, out etl.Output) error {
	if err := out.Send(ctx, s.batch); err != nil {
		return err
	}
	return s.err
}

func TestSinkHappyPathParquet(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1, 2, 3),
		batch(t, schema, 4),
	}}).To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "orders.parquet")
	if err != nil {
		t.Fatal(err)
	}
	_, table := readParquet(t, data)
	if table.NumRows() != 4 {
		t.Fatalf("rows=%d, want 4", table.NumRows())
	}
	if !schemasMatch(table.Schema(), schema) {
		t.Fatalf("schema mismatch: got %v, want %v", table.Schema(), schema)
	}
}

func TestSinkHappyPathArrowIPC(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1, 2, 3),
	}}).To(filesink.New(bucket, "orders.arrow", filesink.ArrowIPC()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "orders.arrow")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewFileReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !reader.Schema().Equal(schema) {
		t.Fatalf("schema mismatch: got %v, want %v", reader.Schema(), schema)
	}

	var rows int64
	for i := 0; i < reader.NumRecords(); i++ {
		rec, err := reader.Record(i)
		if err != nil {
			t.Fatal(err)
		}
		rows += rec.NumRows()
	}
	if rows != 3 {
		t.Fatalf("rows=%d, want 3", rows)
	}
}

func TestSinkZeroBatchesWritesNothing(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	p := etl.New()
	p.From(batchesSource{}).To(filesink.New(bucket, "empty.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	exists, err := bucket.Exists(context.Background(), "empty.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("want no object written for zero batches")
	}
}

func TestSinkZeroBatchesWithSchemaWritesEmptyFile(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{}).To(filesink.New(bucket, "empty.parquet", filesink.Parquet(), filesink.WithSchema(schema)))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "empty.parquet")
	if err != nil {
		t.Fatal(err)
	}
	_, table := readParquet(t, data)
	if table.NumRows() != 0 {
		t.Fatalf("rows=%d, want 0", table.NumRows())
	}
	if !schemasMatch(table.Schema(), schema) {
		t.Fatalf("schema mismatch: got %v, want %v", table.Schema(), schema)
	}
}

// TestSinkZeroBatchesWithSchemaArrowIPCWritesEmptyFile locks in that
// ArrowIPC + WithSchema + zero batches succeeds and produces a valid,
// readable-back empty IPC file, rather than failing with the IPC writer's
// "could not write empty file" error. That error is returned by
// (*ipc.FileWriter).Close only when the underlying start-of-stream write
// itself fails (e.g. a broken io.Writer) — Close starts the writer lazily if
// it hasn't been started yet, and starting with zero records writes just the
// schema header, which succeeds fine. So a schema-only, zero-record Finish
// (see Finish at sink.go:80, which calls Close without ever calling Write)
// does not hit that error path.
func TestSinkZeroBatchesWithSchemaArrowIPCWritesEmptyFile(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{}).To(filesink.New(bucket, "empty.arrow", filesink.ArrowIPC(), filesink.WithSchema(schema)))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "empty.arrow")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := ipc.NewFileReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !reader.Schema().Equal(schema) {
		t.Fatalf("schema mismatch: got %v, want %v", reader.Schema(), schema)
	}
	if reader.NumRecords() != 0 {
		t.Fatalf("records=%d, want 0", reader.NumRecords())
	}
}

func TestSinkSchemaMismatchAbortsWithoutCommitting(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema1 := fieldSchema("value")
	schema2 := fieldSchema("other")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema1, 1),
		batch(t, schema2, 2),
	}}).To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error on schema mismatch")
	}

	exists, err := bucket.Exists(context.Background(), "orders.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("want no object committed after schema-mismatch abort")
	}
}

// failingFormat lets Write fail on the Nth call, to test that Sink aborts a
// mid-stream failure instead of committing a partial object.
type failingFormat struct {
	failAt int
	calls  int
}

func (f *failingFormat) ContentType() string { return "application/octet-stream" }

func (f *failingFormat) NewWriter(_ *arrow.Schema, _ io.Writer) (filesink.RecordWriter, error) {
	return &failingWriter{parent: f}, nil
}

type failingWriter struct {
	parent *failingFormat
}

func (w *failingWriter) Write(arrow.Record) error {
	w.parent.calls++
	if w.parent.calls >= w.parent.failAt {
		return errors.New("injected write failure")
	}
	return nil
}

func (w *failingWriter) Close() error { return nil }

func TestSinkMidStreamFailureAbortsWithoutCommitting(t *testing.T) {
	dir := t.TempDir()
	bucket, err := fileblob.OpenBucket(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1),
		batch(t, schema, 2),
	}}).To(filesink.New(bucket, "orders.bin", &failingFormat{failAt: 2}))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error from injected write failure")
	}

	if _, err := os.Stat(filepath.Join(dir, "orders.bin")); !os.IsNotExist(err) {
		t.Fatalf("want no file committed after abort, stat err=%v", err)
	}
}

// abortSpySink wraps a real *filesink.Sink, delegating Consume/Finish to it
// unchanged but recording whether the runtime actually called Abort. A bare
// bucket.Exists-after-failure check can't distinguish "Abort ran and
// canceled the in-flight writer" from "Abort was never called, and the
// object simply never got committed because Finish also never ran" — both
// produce an absent object. Overriding just Abort here (Consume/Finish are
// promoted from the embedded *filesink.Sink) lets a test assert the former
// specifically, the same way the root package's abortableSink does for a
// stub sink in TestUpstreamFailureCallsAbortOnDownstreamSink
// (pipeline_test.go).
type abortSpySink struct {
	*filesink.Sink
	aborted bool
}

func (s *abortSpySink) Abort() {
	s.aborted = true
	s.Sink.Abort()
}

// TestSinkAbortCalledOnUpstreamFailureAfterConsumingABatch exercises the
// runtime-invoked Sink.Abort path (sink.go:175), as opposed to
// TestSinkMidStreamFailureAbortsWithoutCommitting and
// TestSinkSchemaMismatchAbortsWithoutCommitting above, which only exercise
// Sink's internal abort() reached from Consume's own error return
// (sink.go:66-77). Here the failure originates upstream, in the Source, so
// the pipeline runtime — not Consume — is what calls Abort() once it
// determines this Sink's Finish will never run (see abortIfAborter at
// pipeline.go:336). The Sink has already opened its blob writer and
// consumed one batch by the time that happens, so this also verifies Abort
// was actually invoked (via abortSpySink) and that it results in no
// committed object, rather than the object simply being absent because
// Finish never ran regardless of whether Abort did anything.
func TestSinkAbortCalledOnUpstreamFailureAfterConsumingABatch(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	wantErr := errors.New("upstream boom")
	sink := &abortSpySink{Sink: filesink.New(bucket, "orders.parquet", filesink.Parquet())}
	p := etl.New()
	p.From(batchThenErrorSource{batch: batch(t, schema, 1), err: wantErr}).To(sink)

	err := p.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run error=%v, want %v", err, wantErr)
	}

	if !sink.aborted {
		t.Fatal("want Abort called after upstream failure skipped Finish")
	}

	exists, err := bucket.Exists(context.Background(), "orders.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("want no object committed after Abort following upstream failure")
	}
}

func TestSinkOverwritesByDefault(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")

	p1 := etl.New()
	p1.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))
	if err := p1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Default: writing to an existing key succeeds and overwrites it.
	p2 := etl.New()
	p2.From(batchesSource{batches: []etl.Batch{batch(t, schema, 2, 3)}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))
	if err := p2.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "orders.parquet")
	if err != nil {
		t.Fatal(err)
	}
	_, table := readParquet(t, data)
	if table.NumRows() != 2 {
		t.Fatalf("rows=%d, want 2 after overwrite", table.NumRows())
	}
}

// closeTrackingWriter wraps a Writer and records whether Close was called
// on it, to verify NewToWriter's documented contract that it never closes
// the writer it's given.
type closeTrackingWriter struct {
	io.Writer
	closed bool
}

func (w *closeTrackingWriter) Close() error {
	w.closed = true
	return nil
}

func TestSinkToWriterHappyPathParquet(t *testing.T) {
	var buf bytes.Buffer
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1, 2, 3),
		batch(t, schema, 4),
	}}).To(filesink.NewToWriter(&buf, filesink.Parquet()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, table := readParquet(t, buf.Bytes())
	if table.NumRows() != 4 {
		t.Fatalf("rows=%d, want 4", table.NumRows())
	}
	if !schemasMatch(table.Schema(), schema) {
		t.Fatalf("schema mismatch: got %v, want %v", table.Schema(), schema)
	}
}

func TestSinkToWriterZeroBatchesWithSchemaWritesEmptyFile(t *testing.T) {
	var buf bytes.Buffer
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{}).To(filesink.NewToWriter(&buf, filesink.Parquet(), filesink.WithSchema(schema)))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	_, table := readParquet(t, buf.Bytes())
	if table.NumRows() != 0 {
		t.Fatalf("rows=%d, want 0", table.NumRows())
	}
	if !schemasMatch(table.Schema(), schema) {
		t.Fatalf("schema mismatch: got %v, want %v", table.Schema(), schema)
	}
}

func TestSinkToWriterZeroBatchesNoSchemaWritesNothing(t *testing.T) {
	var buf bytes.Buffer
	p := etl.New()
	p.From(batchesSource{}).To(filesink.NewToWriter(&buf, filesink.Parquet()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes, want 0 for zero batches with no explicit schema", buf.Len())
	}
}

// TestSinkToWriterNeverClosesWriter locks in NewToWriter's documented
// contract that the destination writer is the caller's own to manage: a
// Sink writing Parquet (whose underlying writer closes any io.Writer it's
// given that also implements io.Closer, per writeOnly's doc comment at
// sink.go:139) must not let that reach the caller-supplied writer.
func TestSinkToWriterNeverClosesWriter(t *testing.T) {
	w := &closeTrackingWriter{Writer: &bytes.Buffer{}}
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.NewToWriter(w, filesink.Parquet()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if w.closed {
		t.Fatal("want the destination writer left open; NewToWriter must never close it")
	}
}

// TestSinkToWriterAbortAfterUpstreamFailureIsSafe exercises the
// runtime-invoked Abort path (see TestSinkAbortCalledOnUpstreamFailureAfterConsumingABatch
// above, its bucket-backed counterpart) for a writer-path Sink, where
// abort()'s bucket-specific work (canceling the write context, closing the
// blob writer) is all nil-guarded and so should be a safe no-op rather than
// a nil-pointer panic.
func TestSinkToWriterAbortAfterUpstreamFailureIsSafe(t *testing.T) {
	var buf bytes.Buffer
	schema := fieldSchema("value")
	wantErr := errors.New("upstream boom")
	sink := &abortSpySink{Sink: filesink.NewToWriter(&buf, filesink.Parquet())}
	p := etl.New()
	p.From(batchThenErrorSource{batch: batch(t, schema, 1), err: wantErr}).To(sink)

	err := p.Run(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run error=%v, want %v", err, wantErr)
	}
	if !sink.aborted {
		t.Fatal("want Abort called after upstream failure skipped Finish")
	}
}

func TestSinkWithWriterOptionsIfNotExistOptsIntoArchivalSemantics(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	archivalOpt := filesink.WithWriterOptions(&blob.WriterOptions{IfNotExist: true})

	p1 := etl.New()
	p1.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet(), archivalOpt))
	if err := p1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// With IfNotExist opted in, writing to the same key fails instead of
	// overwriting — the archival pattern.
	p2 := etl.New()
	p2.From(batchesSource{batches: []etl.Batch{batch(t, schema, 2)}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet(), archivalOpt))
	if err := p2.Run(context.Background()); err == nil {
		t.Fatal("want error writing to an existing key with IfNotExist opted in")
	}
}
