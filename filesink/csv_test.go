package filesink_test

import (
	"bytes"
	"context"
	"encoding/base64"
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

// TestCSVWithEscapeCharacterEscapesLiteralEscapeByte covers a case the
// escape-character path previously got wrong: a field containing a literal
// occurrence of the configured EscapeCharacter, immediately followed by a
// quote character. Without also escaping the literal escape byte itself, an
// escape-based reader can't tell that byte apart from an escape-introducer,
// and misparses the quote that follows (and potentially the rest of the
// record, since the reader believes it's still inside the quoted field).
func TestCSVWithEscapeCharacterEscapesLiteralEscapeByte(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	// Contains a literal backslash (the escape character) immediately
	// followed by a quote — the classic backslash-escaping ambiguity.
	tricky := `x\"y`
	b.Append(tricky)
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
	want := "value\n\"x\\\\\\\"y\"\n" // header, then "x\\\"y" quoted: \\  (escaped backslash) + \" (escaped quote)
	if raw != want {
		t.Fatalf("raw output=%q, want %q", raw, want)
	}

	// Decode by hand using the same backslash-escaping convention the
	// writer was configured with, to confirm the output round-trips
	// unambiguously (a standard csv.Reader doesn't understand backslash
	// escaping, so it can't be used here).
	got := decodeBackslashEscapedField(t, raw)
	if got != tricky {
		t.Fatalf("decoded value=%q, want %q", got, tricky)
	}
}

// decodeBackslashEscapedField extracts and unescapes the single quoted data
// field from a two-line (header + one row) CSV blob written with
// EscapeCharacter set to a backslash, treating \\ and \" as the only
// recognized escape sequences.
func decodeBackslashEscapedField(t *testing.T, raw string) string {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("raw=%q, want exactly 2 lines", raw)
	}
	dataLine := lines[1]
	if len(dataLine) < 2 || dataLine[0] != '"' || dataLine[len(dataLine)-1] != '"' {
		t.Fatalf("data line=%q, want a single quoted field", dataLine)
	}
	inner := dataLine[1 : len(dataLine)-1]

	var out strings.Builder
	for i := 0; i < len(inner); i++ {
		if inner[i] == '\\' && i+1 < len(inner) {
			i++
		}
		out.WriteByte(inner[i])
	}
	return out.String()
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

func TestCSVMultipleRecordsSameSchema(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	newRec := func(v int64) arrow.Record {
		b := array.NewInt64Builder(memory.DefaultAllocator)
		defer b.Release()
		b.Append(v)
		arr := b.NewArray()
		defer arr.Release()
		rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
		t.Cleanup(rec.Release)
		return rec
	}

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(newRec(1)); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(newRec(2)); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(newRec(3)); err != nil {
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
	if len(rows) != 4 { // header + 3 rows
		t.Fatalf("rows=%v, want header + 3 rows", rows)
	}
	for i, want := range []string{"1", "2", "3"} {
		if rows[i+1][0] != want {
			t.Fatalf("row %d=%v, want id=%q", i+1, rows[i+1], want)
		}
	}
}

func TestCSVWriteSchemaMismatchFewerColumnsErrors(t *testing.T) {
	schemaA := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	schemaB := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	idB.Append(1)
	nameB := array.NewStringBuilder(memory.DefaultAllocator)
	defer nameB.Release()
	nameB.Append("widget")
	idArr := idB.NewArray()
	defer idArr.Release()
	nameArr := nameB.NewArray()
	defer nameArr.Release()
	recA := array.NewRecord(schemaA, []arrow.Array{idArr, nameArr}, 1)
	defer recA.Release()

	idB2 := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB2.Release()
	idB2.Append(2)
	idArr2 := idB2.NewArray()
	defer idArr2.Release()
	recB := array.NewRecord(schemaB, []arrow.Array{idArr2}, 1)
	defer recB.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schemaA, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(recA); err != nil {
		t.Fatal(err)
	}

	if err := w.Write(recB); err == nil {
		t.Fatal("want error writing a record with fewer columns than the writer's schema")
	}
}

func TestCSVWriteSchemaMismatchMoreColumnsErrors(t *testing.T) {
	schemaA := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	schemaB := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "extra", Type: arrow.BinaryTypes.String},
	}, nil)

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()
	recA := array.NewRecord(schemaA, []arrow.Array{idArr}, 1)
	defer recA.Release()

	idB2 := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB2.Release()
	idB2.Append(2)
	idArr2 := idB2.NewArray()
	defer idArr2.Release()
	extraB := array.NewStringBuilder(memory.DefaultAllocator)
	defer extraB.Release()
	extraB.Append("surprise")
	extraArr := extraB.NewArray()
	defer extraArr.Release()
	recB := array.NewRecord(schemaB, []arrow.Array{idArr2, extraArr}, 1)
	defer recB.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schemaA, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(recA); err != nil {
		t.Fatal(err)
	}

	if err := w.Write(recB); err == nil {
		t.Fatal("want error writing a record with more columns than the writer's schema")
	}
}

func TestCSVWriteSchemaMismatchDifferentTypeErrors(t *testing.T) {
	schemaA := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	schemaB := arrow.NewSchema([]arrow.Field{
		{Name: "value", Type: arrow.BinaryTypes.String},
	}, nil)

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()
	recA := array.NewRecord(schemaA, []arrow.Array{idArr}, 1)
	defer recA.Release()

	strB := array.NewStringBuilder(memory.DefaultAllocator)
	defer strB.Release()
	strB.Append("oops")
	strArr := strB.NewArray()
	defer strArr.Release()
	recB := array.NewRecord(schemaB, []arrow.Array{strArr}, 1)
	defer recB.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(schemaA, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(recA); err != nil {
		t.Fatal(err)
	}

	if err := w.Write(recB); err == nil {
		t.Fatal("want error writing a record whose column type differs from the writer's schema")
	}
}

func TestCSVFormulasNotEscapedByDefault(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.BinaryTypes.String}}, nil)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	defer b.Release()
	b.Append("=SUM(A1:A2)")
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

	r := csv.NewReader(bytes.NewReader(buf.Bytes()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if want := "=SUM(A1:A2)"; rows[1][0] != want {
		t.Fatalf("value=%q, want %q unescaped by default", rows[1][0], want)
	}
}

func TestCSVWithEscapeFormulasPrefixesTriggerChars(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "formula", Type: arrow.BinaryTypes.String},
		{Name: "plus", Type: arrow.BinaryTypes.String},
		{Name: "minus", Type: arrow.BinaryTypes.String},
		{Name: "at", Type: arrow.BinaryTypes.String},
		{Name: "tab", Type: arrow.BinaryTypes.String},
		{Name: "cr", Type: arrow.BinaryTypes.String},
		{Name: "safe", Type: arrow.BinaryTypes.String},
		{Name: "empty", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	values := []string{
		"=SUM(A1:A2)",
		"+1",
		"-1",
		"@cmd",
		"\tfoo",
		"\rfoo",
		"hello",
		"",
	}
	cols := make([]arrow.Array, len(values))
	for i, v := range values {
		b := array.NewStringBuilder(memory.DefaultAllocator)
		if v == "" {
			b.AppendNull()
		} else {
			b.Append(v)
		}
		cols[i] = b.NewArray()
		b.Release()
		defer cols[i].Release()
	}
	rec := array.NewRecord(schema, cols, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithEscapeFormulas(true)).NewWriter(schema, &buf)
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
	row := rows[1]
	want := []string{"'=SUM(A1:A2)", "'+1", "'-1", "'@cmd", "'\tfoo", "'\rfoo", "hello", ""}
	if !equalStrings(row, want) {
		t.Fatalf("row=%q, want %q", row, want)
	}
}

func TestCSVWithEscapeFormulasAppliesToBinaryField(t *testing.T) {
	// base64 can legitimately produce a leading '+' (it's part of the
	// base64 alphabet), so a binary field rendered as base64 text is just
	// as exposed to formula injection as a string field.
	schema := arrow.NewSchema([]arrow.Field{{Name: "notes", Type: arrow.BinaryTypes.Binary}}, nil)

	// base64("\xfb...") starts with '+'; find bytes whose encoding does.
	var raw []byte
	var encoded string
	for b := byte(0); ; b++ {
		encoded = base64.StdEncoding.EncodeToString([]byte{b, 0, 0})
		if strings.HasPrefix(encoded, "+") {
			raw = []byte{b, 0, 0}
			break
		}
		if b == 255 {
			t.Fatal("could not find a byte sequence whose base64 encoding starts with '+'")
		}
	}

	nb := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer nb.Release()
	nb.Append(raw)
	arr := nb.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithEscapeFormulas(true)).NewWriter(schema, &buf)
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
	if want := "'" + encoded; rows[1][0] != want {
		t.Fatalf("value=%q, want %q", rows[1][0], want)
	}
}

func TestCSVWithEscapeFormulasEscapesHeader(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "=cmd", Type: arrow.PrimitiveTypes.Int64}}, nil)

	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(1)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV(filesink.WithEscapeFormulas(true)).NewWriter(schema, &buf)
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
	if want := "'=cmd"; rows[0][0] != want {
		t.Fatalf("header=%q, want %q escaped like any other formula-triggering cell", rows[0][0], want)
	}
}

func TestCSVWriteSchemaDifferingOnlyInNullabilityAndMetadataSucceeds(t *testing.T) {
	writerSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	recordSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Metadata: arrow.NewMetadata([]string{"source"}, []string{"join"})},
	}, nil)

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()
	nameB := array.NewStringBuilder(memory.DefaultAllocator)
	defer nameB.Release()
	nameB.Append("widget")
	nameArr := nameB.NewArray()
	defer nameArr.Release()
	rec := array.NewRecord(recordSchema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	var buf bytes.Buffer
	w, err := filesink.CSV().NewWriter(writerSchema, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatalf("Write with a schema differing only in nullability/metadata: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
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
