package archive_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/verygoodetl/verygoodetl/archive"
)

func fieldSchema(name string) *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil)
}

// schemasMatch compares field names/types/nullability, ignoring metadata:
// Parquet round-trips always attach a PARQUET:field_id metadata entry per
// field, which isn't a meaningful mismatch for these tests.
func schemasMatch(got, want *arrow.Schema) bool {
	if got.NumFields() != want.NumFields() {
		return false
	}
	for i, gf := range got.Fields() {
		wf := want.Field(i)
		if gf.Name != wf.Name || !arrow.TypeEqual(gf.Type, wf.Type) || gf.Nullable != wf.Nullable {
			return false
		}
	}
	return true
}

func intRecord(t *testing.T, schema *arrow.Schema, values ...int64) arrow.Record {
	t.Helper()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(values, nil)
	a := b.NewArray()
	defer a.Release()
	rec := array.NewRecord(schema, []arrow.Array{a}, int64(len(values)))
	t.Cleanup(rec.Release)
	return rec
}

func TestArrowIPCFormatRoundTrip(t *testing.T) {
	schema := fieldSchema("value")
	format := archive.ArrowIPC()

	var buf bytes.Buffer
	w, err := format.NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, schema, 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, schema, 4)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	reader, err := ipc.NewFileReader(bytes.NewReader(buf.Bytes()))
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
	if rows != 4 {
		t.Fatalf("rows=%d, want 4", rows)
	}
}

func TestArrowIPCFormatSchemaMismatch(t *testing.T) {
	schema := fieldSchema("value")
	other := fieldSchema("other")
	format := archive.ArrowIPC()

	var buf bytes.Buffer
	w, err := format.NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, other, 1)); err == nil {
		t.Fatal("want error on schema mismatch")
	}
}

func readParquet(t *testing.T, data []byte) (*file.Reader, arrow.Table) {
	t.Helper()
	pf, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pf.Close() })

	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	table, err := fr.ReadTable(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(table.Release)
	return pf, table
}

func TestParquetFormatRoundTrip(t *testing.T) {
	schema := fieldSchema("value")
	format := archive.Parquet()

	var buf bytes.Buffer
	w, err := format.NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, schema, 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, table := readParquet(t, buf.Bytes())
	if table.NumRows() != 3 {
		t.Fatalf("rows=%d, want 3", table.NumRows())
	}
	if !schemasMatch(table.Schema(), schema) {
		t.Fatalf("schema mismatch: got %v, want %v", table.Schema(), schema)
	}

	cc, err := pf.RowGroup(0).MetaData().ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	if cc.Compression() != compress.Codecs.Snappy {
		t.Fatalf("compression=%v, want Snappy", cc.Compression())
	}
}

func TestParquetFormatWithCompression(t *testing.T) {
	schema := fieldSchema("value")
	format := archive.Parquet(archive.WithCompression(compress.Codecs.Gzip))

	var buf bytes.Buffer
	w, err := format.NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, schema, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	pf, _ := readParquet(t, buf.Bytes())
	cc, err := pf.RowGroup(0).MetaData().ColumnChunk(0)
	if err != nil {
		t.Fatal(err)
	}
	if cc.Compression() != compress.Codecs.Gzip {
		t.Fatalf("compression=%v, want Gzip", cc.Compression())
	}
}

func TestParquetFormatSchemaMismatch(t *testing.T) {
	schema := fieldSchema("value")
	other := fieldSchema("other")
	format := archive.Parquet()

	var buf bytes.Buffer
	w, err := format.NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(intRecord(t, other, 1)); err == nil {
		t.Fatal("want error on schema mismatch")
	}
}
