package filesink_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
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

// batchThenErrorSource sends one pre-built batch, then fails — simulating
// an upstream failure after a downstream Sink already consumed a batch,
// unlike errorSource (root package) which fails before sending anything.
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
// ArrowIPC + WithSchema + zero batches produces a valid, readable empty
// file rather than IPC's "could not write empty file" error: Close starts
// the writer lazily, and starting with zero records just writes the schema
// header.
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

// nilSchemaBatch is a plain, non-nil Batch whose Schema() returns nil —
// unlike a nil-backed Batch, which the pipeline runtime itself now
// rejects — so it reaches Consume unfiltered.
type nilSchemaBatch struct{}

func (nilSchemaBatch) Schema() *arrow.Schema { return nil }
func (nilSchemaBatch) NumRows() int64        { return 0 }
func (nilSchemaBatch) Record() arrow.Record  { return nil }
func (nilSchemaBatch) Retain()               {}
func (nilSchemaBatch) Release()              {}

// TestSinkConsumeNilSchemaBatchReturnsError is a regression test: Consume
// used to hand a nil schema straight to Format.NewWriter, which panics
// since no Format checks for one.
func TestSinkConsumeNilSchemaBatchReturnsError(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{nilSchemaBatch{}}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a batch with a nil schema")
	}
}

// TestSinkConsumeSecondBatchNilSchemaReturnsError is a regression test: the
// nil-schema check used to run only inside the `if !s.started` branch, so a
// nil schema on a later batch skipped it and panicked.
func TestSinkConsumeSecondBatchNilSchemaReturnsError(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1),
		nilSchemaBatch{},
	}}).To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a second batch with a nil schema")
	}
}

// TestSinkNewNilBucketReturnsErrorInsteadOfPanicking is a regression test:
// open used to decide bucket- vs. writer-backed from s.bucket == nil, so
// New(nil, ...) was misclassified as writer-backed and panicked on a nil
// s.w.
func TestSinkNewNilBucketReturnsErrorInsteadOfPanicking(t *testing.T) {
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.New(nil, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil bucket passed to New")
	}
}

// TestSinkNewToWriterNilWriterReturnsErrorInsteadOfPanicking mirrors
// TestSinkNewNilBucketReturnsErrorInsteadOfPanicking for NewToWriter.
func TestSinkNewToWriterNilWriterReturnsErrorInsteadOfPanicking(t *testing.T) {
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.NewToWriter(nil, filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil writer passed to NewToWriter")
	}
}

// TestSinkNewNilBucketZeroBatchesReturnsErrorInsteadOfSucceeding is a
// regression test: Finish used to return nil before checking constructErr
// when no batch arrived, so an invalid Sink could silently "succeed".
func TestSinkNewNilBucketZeroBatchesReturnsErrorInsteadOfSucceeding(t *testing.T) {
	p := etl.New()
	p.From(batchesSource{}).To(filesink.New(nil, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil bucket passed to New, even with zero batches")
	}
}

// nilPtrWriter's zero value is a typed-nil pointer that satisfies io.Writer
// (via the pointer receiver below) and so compares != nil as an interface,
// but Write would panic on the nil receiver.
type nilPtrWriter struct{}

func (w *nilPtrWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestSinkNewToWriterTypedNilWriterReturnsErrorInsteadOfPanicking is a
// regression test: NewToWriter only checked w == nil, missing a typed-nil
// pointer, which used to bypass validation and panic on first Write.
func TestSinkNewToWriterTypedNilWriterReturnsErrorInsteadOfPanicking(t *testing.T) {
	schema := fieldSchema("value")
	var typedNil *nilPtrWriter
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.NewToWriter(typedNil, filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a typed-nil writer passed to NewToWriter")
	}
}

// TestSinkNewNilFormatReturnsErrorInsteadOfPanicking is a regression test:
// New used to store a nil format with no validation, panicking inside open.
func TestSinkNewNilFormatReturnsErrorInsteadOfPanicking(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.New(bucket, "orders.parquet", nil))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil format passed to New")
	}
}

// TestSinkNewToWriterNilFormatReturnsErrorInsteadOfPanicking mirrors
// TestSinkNewNilFormatReturnsErrorInsteadOfPanicking for the other
// construction mode.
func TestSinkNewToWriterNilFormatReturnsErrorInsteadOfPanicking(t *testing.T) {
	var buf bytes.Buffer
	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.NewToWriter(&buf, nil))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil format passed to NewToWriter")
	}
}

// nilPtrFormat's zero value is a typed-nil pointer that satisfies
// filesink.Format via the pointer-receiver methods below, mirroring
// nilPtrWriter for io.Writer.
type nilPtrFormat struct{}

func (f *nilPtrFormat) ContentType() string { return "application/octet-stream" }

func (f *nilPtrFormat) NewWriter(*arrow.Schema, io.Writer) (filesink.RecordWriter, error) {
	return nil, nil
}

// TestSinkNewTypedNilFormatReturnsErrorInsteadOfPanicking is a regression
// test: a plain format == nil check misses a typed-nil pointer wrapped in
// the interface.
func TestSinkNewTypedNilFormatReturnsErrorInsteadOfPanicking(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	var typedNil *nilPtrFormat
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.New(bucket, "orders.parquet", typedNil))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a typed-nil format passed to New")
	}
}

// nilFuncWriter is a func-typed io.Writer; calling a nil func value panics
// unconditionally, so it's the non-pointer nil-capable case isNilWriter
// must also reject.
type nilFuncWriter func([]byte) (int, error)

func (w nilFuncWriter) Write(p []byte) (int, error) { return w(p) }

// TestSinkNewToWriterNilFuncWriterReturnsErrorInsteadOfPanicking is a
// regression test for isNilWriter's broadened check: it used to only check
// Kind() == reflect.Ptr, missing a nil func value.
func TestSinkNewToWriterNilFuncWriterReturnsErrorInsteadOfPanicking(t *testing.T) {
	schema := fieldSchema("value")
	var typedNil nilFuncWriter
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{batch(t, schema, 1)}}).
		To(filesink.NewToWriter(typedNil, filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil func-typed writer passed to NewToWriter")
	}
}

// nilRecordBatch has a valid schema but Record() returns nil directly —
// unlike nilSchemaBatch, whose nil Schema() is caught first — exercising
// the record check on its own.
type nilRecordBatch struct {
	schema *arrow.Schema
}

func (b nilRecordBatch) Schema() *arrow.Schema { return b.schema }
func (b nilRecordBatch) NumRows() int64        { return 0 }
func (b nilRecordBatch) Record() arrow.Record  { return nil }
func (b nilRecordBatch) Retain()               {}
func (b nilRecordBatch) Release()              {}

// TestSinkConsumeNilRecordBatchReturnsError is a regression test: Consume
// checked b.Schema() for nil but not b.Record(), so rw.Write panicked on a
// nil record.
func TestSinkConsumeNilRecordBatchReturnsError(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{nilRecordBatch{schema: schema}}}).
		To(filesink.New(bucket, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a batch with a nil record")
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

// TestSinkMidStreamFailureAbortsWithoutCommitting is a regression test for
// abort's cleanup, not just for "no final file exists": failingWriter never
// touches the underlying blob writer, and fileblob only renames its temp
// file into place on a successful Close — so the final file's absence would
// hold even with abort's body deleted.
//
// NoTempDir puts fileblob's temp file directly in dir (instead of
// os.TempDir()); fileblob's Close removes that temp file but only after
// checking ctx.Err(), so a canceled write context skips the rename. A
// working abort (cancel, then Close) leaves dir empty; a no-op abort leaves
// the never-closed temp file behind, since nothing else cleans it up.
// Asserting dir is empty is what actually distinguishes cleanup running
// from never being invoked.
func TestSinkMidStreamFailureAbortsWithoutCommitting(t *testing.T) {
	dir := t.TempDir()
	bucket, err := fileblob.OpenBucket(dir, &fileblob.Options{NoTempDir: true})
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

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("want dir empty after abort, got leftover entries %v (abort must Close the blob writer so fileblob removes its temp file)", names)
	}
}

// abortSpySink wraps a real *filesink.Sink, recording whether the runtime
// called Abort — a bare bucket.Exists check alone can't distinguish "Abort
// ran and canceled the writer" from "Abort was never called and Finish
// also never ran," both of which leave no object. Mirrors the root
// package's abortableSink (pipeline_test.go).
type abortSpySink struct {
	*filesink.Sink
	aborted bool
}

func (s *abortSpySink) Abort() {
	s.aborted = true
	s.Sink.Abort()
}

// TestSinkAbortCalledOnUpstreamFailureAfterConsumingABatch exercises the
// runtime-invoked Sink.Abort path, as opposed to the Consume-triggered
// abort() exercised by TestSinkMidStreamFailureAbortsWithoutCommitting and
// TestSinkSchemaMismatchAbortsWithoutCommitting: here the failure
// originates upstream, so the pipeline runtime calls Abort() once it
// determines Finish will never run.
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

// TestSinkToWriterNeverClosesWriter locks in NewToWriter's contract that
// the destination writer is the caller's to manage, even though Parquet's
// underlying writer closes any io.Writer that also implements io.Closer.
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

// TestSinkToWriterAbortAfterUpstreamFailureIsSafe mirrors
// TestSinkAbortCalledOnUpstreamFailureAfterConsumingABatch for a
// writer-path Sink, where abort()'s bucket-specific work is nil-guarded.
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
