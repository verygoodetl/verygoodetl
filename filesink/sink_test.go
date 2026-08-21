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
// (see Sink.Finish, which calls Close without ever calling Write)
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

// nilSchemaBatch is an etl.Batch whose Schema method returns nil. It's a
// plain, non-nil struct value — unlike a nil-backed pointer/map/slice/
// func/chan Batch, which the pipeline runtime itself now rejects — so it
// reaches a Sink's Consume unfiltered, exercising this package's own
// handling of a schema-less batch specifically.
type nilSchemaBatch struct{}

func (nilSchemaBatch) Schema() *arrow.Schema { return nil }
func (nilSchemaBatch) NumRows() int64        { return 0 }
func (nilSchemaBatch) Record() arrow.Record  { return nil }
func (nilSchemaBatch) Retain()               {}
func (nilSchemaBatch) Release()              {}

// TestSinkConsumeNilSchemaBatchReturnsError is a regression test: Consume
// used to pass b.Schema() straight into open, which hands it to
// Format.NewWriter — every current Format implementation calls a method on
// the schema (e.g. NumFields) without a nil check, so a nil schema panicked
// instead of failing the pipeline with an ordinary error.
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
// nil-schema check used to live inside Consume's `if !s.started` branch, so
// it only ran while opening the format writer on the first batch. A batch
// with a nil schema arriving on a later call skipped the check entirely and
// went straight to rw.Write(b.Record()), which panicked the same way a nil
// schema on the first batch used to (see TestSinkConsumeNilSchemaBatchReturnsError
// above) since b.Record() is also nil on a nilSchemaBatch.
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
// open used to decide bucket-backed vs. writer-backed by checking
// s.bucket == nil, so New(nil, ...) — a plain caller mistake, not a request
// to go through NewToWriter — was misclassified as writer-backed and handed
// a nil s.w to the format writer, panicking deep inside it instead of
// failing with an ordinary error.
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
// TestSinkNewNilBucketReturnsErrorInsteadOfPanicking for the other
// construction mode: NewToWriter(nil, ...) must fail with an ordinary error
// rather than panicking when the format writer tries to use the nil
// io.Writer.
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
// regression test: Finish used to return nil before ever checking
// constructErr when the sink received no batches and had no explicit
// schema, so an invalid Sink (e.g. New(nil, ...)) silently reported success
// as long as it happened to receive no input.
func TestSinkNewNilBucketZeroBatchesReturnsErrorInsteadOfSucceeding(t *testing.T) {
	p := etl.New()
	p.From(batchesSource{}).To(filesink.New(nil, "orders.parquet", filesink.Parquet()))

	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error for a nil bucket passed to New, even with zero batches")
	}
}

// nilPtrWriter is an io.Writer whose zero value is a typed-nil pointer:
// (*nilPtrWriter)(nil) satisfies io.Writer via the pointer receiver below,
// so it compares != nil as an interface value, but calling Write panics on
// the nil receiver.
type nilPtrWriter struct{}

func (w *nilPtrWriter) Write(p []byte) (int, error) { return len(p), nil }

// TestSinkNewToWriterTypedNilWriterReturnsErrorInsteadOfPanicking is a
// regression test: NewToWriter only checked w == nil, which misses a
// typed-nil pointer wrapped in the io.Writer interface. Such a value is
// != nil by interface comparison and used to bypass the construction-error
// check entirely, panicking once the format writer's first Write call
// reached the nil receiver.
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
// New stored a nil format directly on the Sink with no validation, so the
// first non-empty pipeline run panicked inside open, which calls
// s.format.NewWriter on the nil interface.
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

// nilPtrFormat is a filesink.Format whose zero value is a typed-nil
// pointer: (*nilPtrFormat)(nil) satisfies filesink.Format via the
// pointer-receiver methods below, so it compares != nil as an interface
// value, but format.NewWriter is never actually reached because New's own
// nil-format check (isNilFormat) is expected to catch it first, mirroring
// nilPtrWriter below for io.Writer.
type nilPtrFormat struct{}

func (f *nilPtrFormat) ContentType() string { return "application/octet-stream" }

func (f *nilPtrFormat) NewWriter(*arrow.Schema, io.Writer) (filesink.RecordWriter, error) {
	return nil, nil
}

// TestSinkNewTypedNilFormatReturnsErrorInsteadOfPanicking is a regression
// test: a plain `format == nil` check misses a typed-nil pointer wrapped in
// the Format interface, since the interface carries a concrete type
// descriptor and a nil value pointer and so compares != nil.
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

// nilFuncWriter is a func-typed io.Writer. Calling a nil func value panics
// unconditionally on invocation, so a nil nilFuncWriter is exactly the kind
// of non-pointer nil-capable Writer isNilWriter must also reject — a plain
// Kind() == reflect.Ptr check (mirroring isNilValue in the root etl
// package) would miss it, since a func isn't a pointer.
type nilFuncWriter func([]byte) (int, error)

func (w nilFuncWriter) Write(p []byte) (int, error) { return w(p) }

// TestSinkNewToWriterNilFuncWriterReturnsErrorInsteadOfPanicking is a
// regression test for isNilWriter's broadened nil-capable-kind check: before
// the fix, isNilWriter only checked Kind() == reflect.Ptr, so a nil
// nilFuncWriter sailed past NewToWriter's construction check and panicked
// the moment writeOnly{w}.Write called the nil func value.
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

// nilRecordBatch is an etl.Batch with a valid, non-nil schema but whose
// Record method returns nil directly — unlike nilSchemaBatch above, whose
// nil Schema() is caught by Consume's separate schema check before its
// Record() (also nil) is ever reached. This exercises the record check on
// its own, for a batch that would otherwise sail past the schema check.
type nilRecordBatch struct {
	schema *arrow.Schema
}

func (b nilRecordBatch) Schema() *arrow.Schema { return b.schema }
func (b nilRecordBatch) NumRows() int64        { return 0 }
func (b nilRecordBatch) Record() arrow.Record  { return nil }
func (b nilRecordBatch) Retain()               {}
func (b nilRecordBatch) Release()              {}

// TestSinkConsumeNilRecordBatchReturnsError is a regression test: Consume
// checked b.Schema() for nil but never checked b.Record(), even though the
// same "every Format implementation dereferences it with no nil check"
// reasoning documented for the schema check applies equally to the record —
// rw.Write(b.Record()) panicked on a nil record instead of failing the
// pipeline with an ordinary error.
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
// abort's cancel-then-Close cleanup, not just for "no file was ever
// committed": failingWriter never writes any bytes to the underlying blob
// writer, and fileblob only ever renames its temp file into place inside
// Close, which only a successful Finish reaches — so absence of the final
// "orders.bin" is true regardless of whether abort does anything at all,
// and would pass even with abort's body deleted entirely.
//
// NoTempDir makes fileblob create its temp file (e.g.
// "orders.bin.<ts>.tmp") directly in dir instead of os.TempDir(), and
// fileblob's own Close only removes that temp file — it does not rename it,
// since it checks ctx.Err() and bails out before the rename once the
// context passed to NewWriter was canceled. So a working abort (cancel the
// write context, then Close the blob writer) leaves dir completely empty,
// while a broken abort (e.g. a no-op) leaves the still-open, never-closed
// temp file behind: fileblob has no other mechanism to clean it up once
// Close is never called. Asserting dir is empty, rather than only that the
// final key doesn't exist, is what actually distinguishes cleanup running
// from cleanup never being invoked at all.
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
// runtime-invoked Sink.Abort method, as opposed to
// TestSinkMidStreamFailureAbortsWithoutCommitting and
// TestSinkSchemaMismatchAbortsWithoutCommitting above, which only exercise
// Sink's internal abort() reached from Consume's own error return. Here
// the failure originates upstream, in the Source, so the pipeline runtime
// — not Consume — is what calls Abort() once it determines this Sink's
// Finish will never run (see the pipeline runtime's abortIfAborter
// helper). The Sink has already opened its blob writer and
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
// given that also implements io.Closer, per the writeOnly type's doc
// comment) must not let that reach the caller-supplied writer.
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
