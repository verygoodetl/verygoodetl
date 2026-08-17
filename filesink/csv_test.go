package filesink_test

import (
	"bytes"
	"context"
	"encoding/csv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob/memblob"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/filesink"
)

func mixedSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "notes", Type: arrow.BinaryTypes.Binary},
		{Name: "created", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
	}, nil)
}

func buildMixedRecord(t *testing.T) arrow.Record {
	t.Helper()
	schema := mixedSchema()

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	idB.AppendValues([]int64{1, 2}, nil)

	nameB := array.NewStringBuilder(memory.DefaultAllocator)
	defer nameB.Release()
	nameB.Append("widget")
	nameB.AppendNull()

	scoreB := array.NewFloat64Builder(memory.DefaultAllocator)
	defer scoreB.Release()
	scoreB.AppendValues([]float64{1.5, 2.25}, nil)

	activeB := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer activeB.Release()
	activeB.AppendValues([]bool{true, false}, nil)

	notesB := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer notesB.Release()
	notesB.Append([]byte{0x00, 0x01, 0xFF})
	notesB.Append([]byte("plain"))

	when := time.Date(2026, 8, 17, 12, 30, 0, 0, time.UTC)
	ts, err := arrow.TimestampFromTime(when, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	createdB := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer createdB.Release()
	createdB.AppendValues([]arrow.Timestamp{ts, ts}, nil)

	cols := []arrow.Array{idB.NewArray(), nameB.NewArray(), scoreB.NewArray(), activeB.NewArray(), notesB.NewArray(), createdB.NewArray()}
	rec := array.NewRecord(schema, cols, 2)
	for _, c := range cols {
		c.Release()
	}
	t.Cleanup(rec.Release)
	return rec
}

func TestCSVRoundTrip(t *testing.T) {
	schema := mixedSchema()
	rec := buildMixedRecord(t)

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(&buf)
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 { // header + 2 rows
		t.Fatalf("rows=%d, want 3: %v", len(rows), rows)
	}

	wantHeader := []string{"id", "name", "score", "active", "notes", "created"}
	if !equalStrings(rows[0], wantHeader) {
		t.Fatalf("header=%v, want %v", rows[0], wantHeader)
	}

	row1 := rows[1]
	if row1[0] != "1" || row1[1] != "widget" || row1[2] != "1.5" || row1[3] != "true" {
		t.Fatalf("row1=%v", row1)
	}
	if row1[4] != "AAH/" { // base64("\x00\x01\xff")
		t.Fatalf("notes=%q, want base64 of raw bytes", row1[4])
	}
	if row1[5] != "2026-08-17T12:30:00Z" {
		t.Fatalf("created=%q", row1[5])
	}

	row2 := rows[2]
	if row2[1] != "" { // NULL name -> empty string
		t.Fatalf("row2 name=%q, want empty (null)", row2[1])
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCSVEscapingIsRFC4180(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	tricky := "a,b\"c\nd"
	b.Append(tricky)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	raw := buf.String()
	if strings.Contains(raw, `\"`) {
		t.Fatalf("raw output uses backslash-escaping, want RFC 4180 doubled quotes: %q", raw)
	}
	if !strings.Contains(raw, `""`) {
		t.Fatalf("raw output missing doubled-quote escaping: %q", raw)
	}

	r := csv.NewReader(strings.NewReader(raw))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if rows[1][0] != tricky {
		t.Fatalf("round-tripped value=%q, want %q", rows[1][0], tricky)
	}
}

// TestCSVEmbeddedBareCRInCRLFMode documents a real, deliberately preserved
// limitation of the standard library's encoding/csv.Writer that csvWriter
// (see csv_writer.go) faithfully carries over: a bare '\r' (one not
// immediately followed by '\n') is silently dropped when UseCRLF is set —
// see the `case '\r': if !w.UseCRLF { ... }` branch. This test exists to
// catch an unintentional behavior change in this fork, not to assert a
// "fixed" outcome.
func TestCSVEmbeddedBareCRInCRLFMode(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	withCR := "before\rafter"
	b.Append(withCR)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithCRLF(true)).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if want := "beforeafter"; rows[1][0] != want {
		t.Fatalf("value=%q, want %q (bare \\r dropped by encoding/csv in CRLF mode)", rows[1][0], want)
	}
}

func TestCSVWithEscapeCharacter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(`a"b`)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithEscapeCharacter(`\`)).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	raw := buf.String()
	if !strings.Contains(raw, `\"`) {
		t.Fatalf("raw output missing backslash-escaped quote: %q", raw)
	}
	if strings.Contains(raw, `""`) {
		t.Fatalf("raw output still uses RFC 4180 doubled-quote escaping: %q", raw)
	}
	// A non-default EscapeCharacter is not RFC 4180-compliant by design, so
	// it will not round-trip through a standard csv.Reader — that's the
	// documented tradeoff of using this option, not asserted here.
}

func TestCSVWithAlwaysEncapsulate(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(123)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithAlwaysEncapsulate(true), filesink.WithHeader(false)).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	if want := "\"123\"\n"; buf.String() != want {
		t.Fatalf("raw output=%q, want %q (every field quoted, even ones that don't need it)", buf.String(), want)
	}

	// Unlike a non-default EscapeCharacter, always-quoting is still
	// RFC 4180-legal and round-trips fine through a standard reader.
	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0][0] != "123" {
		t.Fatalf("round-tripped value=%q, want %q", rows[0][0], "123")
	}
}

func TestCSVNullString(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendNull()
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithNullString(`\N`)).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if rows[1][0] != `\N` {
		t.Fatalf("null value=%q, want %q", rows[1][0], `\N`)
	}
}

func TestCSVHeaderOrderMatchesSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "zebra", Type: arrow.PrimitiveTypes.Int64},
		{Name: "apple", Type: arrow.PrimitiveTypes.Int64},
		{Name: "mango", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b0 := array.NewInt64Builder(memory.DefaultAllocator)
	defer b0.Release()
	b0.Append(1)
	a0 := b0.NewArray()
	defer a0.Release()

	b1 := array.NewInt64Builder(memory.DefaultAllocator)
	defer b1.Release()
	b1.Append(2)
	a1 := b1.NewArray()
	defer a1.Release()

	b2 := array.NewInt64Builder(memory.DefaultAllocator)
	defer b2.Release()
	b2.Append(3)
	a2 := b2.NewArray()
	defer a2.Release()

	rec := array.NewRecord(schema, []arrow.Array{a0, a1, a2}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"zebra", "apple", "mango"}
	if !equalStrings(rows[0], want) {
		t.Fatalf("header=%v, want schema order %v (not alphabetical)", rows[0], want)
	}
}

func TestCSVWithHeaderFalse(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(1)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithHeader(false)).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0][0] != "1" {
		t.Fatalf("rows=%v, want a single data row with no header", rows)
	}
}

func TestCSVWithDelimiter(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	ab := array.NewInt64Builder(memory.DefaultAllocator)
	defer ab.Release()
	ab.Append(1)
	aArr := ab.NewArray()
	defer aArr.Release()
	bb := array.NewInt64Builder(memory.DefaultAllocator)
	defer bb.Release()
	bb.Append(2)
	bArr := bb.NewArray()
	defer bArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{aArr, bArr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithDelimiter(';')).NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	r.Comma = ';'
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if rows[1][0] != "1" || rows[1][1] != "2" {
		t.Fatalf("rows=%v", rows)
	}
}

func TestCSVUnsupportedSchemaType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)

	var buf bytes.Buffer
	if _, err := filesink.CSV().NewWriter(schema, &buf); err == nil {
		t.Fatal("want error for unsupported schema field type")
	}
}

func TestSinkHappyPathCSV(t *testing.T) {
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	schema := fieldSchema("value")
	p := etl.New()
	p.From(batchesSource{batches: []etl.Batch{
		batch(t, schema, 1, 2, 3),
	}}).To(filesink.New(bucket, "orders.csv", filesink.CSV()))

	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	data, err := bucket.ReadAll(context.Background(), "orders.csv")
	if err != nil {
		t.Fatal(err)
	}

	r := csv.NewReader(bytes.NewReader(data))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 { // header + 3 rows
		t.Fatalf("rows=%v", rows)
	}
	if rows[0][0] != "value" {
		t.Fatalf("header=%v", rows[0])
	}
}
