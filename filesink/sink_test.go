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
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(values, nil)
	a := b.NewArray()
	defer a.Release()
	rec := array.NewRecord(schema, []arrow.Array{a}, int64(len(values)))
	t.Cleanup(rec.Release)
	return etl.NewBatch(rec)
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
