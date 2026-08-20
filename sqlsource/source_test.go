package sqlsource_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

// --- fake database/sql/driver, scoped to this test file ---

type fixture struct {
	columns  []string
	rows     [][]driver.Value
	queryErr error

	mu        sync.Mutex
	lastQuery string
	lastArgs  []driver.NamedValue
}

func (f *fixture) recordQuery(query string, args []driver.NamedValue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastQuery = query
	f.lastArgs = args
}

func (f *fixture) lastCall() (string, []driver.NamedValue) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastQuery, f.lastArgs
}

var (
	fixturesMu sync.Mutex
	fixtures   = map[string]*fixture{}
)

func registerFixture(t *testing.T, f *fixture) string {
	t.Helper()
	dsn := t.Name()
	fixturesMu.Lock()
	fixtures[dsn] = f
	fixturesMu.Unlock()
	t.Cleanup(func() {
		fixturesMu.Lock()
		delete(fixtures, dsn)
		fixturesMu.Unlock()
	})
	return dsn
}

func lookupFixture(dsn string) *fixture {
	fixturesMu.Lock()
	defer fixturesMu.Unlock()
	return fixtures[dsn]
}

type fakeDriver struct{}

func (fakeDriver) Open(dsn string) (driver.Conn, error) {
	f := lookupFixture(dsn)
	if f == nil {
		return nil, errors.New("sqlsourcefake: no fixture registered for dsn " + dsn)
	}
	return &fakeConn{fixture: f}, nil
}

func init() {
	sql.Register("sqlsourcefake", fakeDriver{})
}

type fakeConn struct {
	fixture *fixture
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return nil, errors.New("sqlsourcefake: Prepare not supported, use QueryContext")
}
func (c *fakeConn) Close() error { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("sqlsourcefake: transactions not supported")
}

func (c *fakeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.fixture.recordQuery(query, args)
	if c.fixture.queryErr != nil {
		return nil, c.fixture.queryErr
	}
	return &fakeRows{fixture: c.fixture}, nil
}

var _ driver.QueryerContext = (*fakeConn)(nil)

type fakeRows struct {
	fixture *fixture
	idx     int
}

func (r *fakeRows) Columns() []string { return r.fixture.columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.fixture.rows) {
		return io.EOF
	}
	copy(dest, r.fixture.rows[r.idx])
	r.idx++
	return nil
}

// --- test helpers ---

type countingSink struct {
	mu       sync.Mutex
	batches  []int64
	rows     [][]any
	rowTypes [][]arrow.DataType
}

func (s *countingSink) Consume(_ context.Context, b etl.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, b.NumRows())
	rec := b.Record()
	for r := 0; r < int(rec.NumRows()); r++ {
		row := make([]any, rec.NumCols())
		rowType := make([]arrow.DataType, rec.NumCols())
		for c := 0; c < int(rec.NumCols()); c++ {
			col := rec.Column(c)
			rowType[c] = col.DataType()
			if col.IsNull(r) {
				row[c] = nil
				continue
			}
			row[c] = columnValue(col, r)
		}
		s.rows = append(s.rows, row)
		s.rowTypes = append(s.rowTypes, rowType)
	}
	return nil
}

func (s *countingSink) Finish(context.Context) error { return nil }

func columnValue(col arrow.Array, i int) any {
	switch a := col.(type) {
	case interface{ Value(int) arrow.Timestamp }:
		return a.Value(i)
	case interface{ Value(int) int64 }:
		return a.Value(i)
	case interface{ Value(int) string }:
		return a.Value(i)
	case interface{ Value(int) float64 }:
		return a.Value(i)
	case interface{ Value(int) bool }:
		return a.Value(i)
	default:
		return nil
	}
}

func int64Schema(names ...string) *arrow.Schema {
	fields := make([]arrow.Field, len(names))
	for i, n := range names {
		fields[i] = arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	}
	return arrow.NewSchema(fields, nil)
}

func runSource(t *testing.T, src *sqlsource.Source) *countingSink {
	t.Helper()
	p := etl.New()
	sink := &countingSink{}
	p.From(src).To(sink)
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return sink
}

// --- tests ---

func TestSourceHappyPathMultipleBatches(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "name"},
		rows: [][]driver.Value{
			{int64(1), "a"},
			{int64(2), "b"},
			{int64(3), "c"},
			{int64(4), "d"},
			{int64(5), "e"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, name FROM t", schema, sqlsource.WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if got, want := sink.batches, []int64{2, 2, 1}; !equalInt64s(got, want) {
		t.Fatalf("batches=%v, want %v", got, want)
	}
	if len(sink.rows) != 5 {
		t.Fatalf("rows=%d, want 5", len(sink.rows))
	}
	if sink.rows[0][0] != int64(1) || sink.rows[0][1] != "a" {
		t.Fatalf("row 0 = %v", sink.rows[0])
	}
}

func equalInt64s(a, b []int64) bool {
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

func TestSourceNullHandling(t *testing.T) {
	schema := int64Schema("id", "value")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "value"},
		rows: [][]driver.Value{
			{int64(1), nil},
			{int64(2), int64(42)},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, value FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 2 {
		t.Fatalf("rows=%d, want 2", len(sink.rows))
	}
	if sink.rows[0][1] != nil {
		t.Fatalf("row 0 value=%v, want nil", sink.rows[0][1])
	}
	if sink.rows[1][1] != int64(42) {
		t.Fatalf("row 1 value=%v, want 42", sink.rows[1][1])
	}
}

func TestSourcePermissiveConversionFromBytes(t *testing.T) {
	schema := int64Schema("id", "value")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "value"},
		rows: [][]driver.Value{
			{int64(1), []byte("42")},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, value FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 1 || sink.rows[0][1] != int64(42) {
		t.Fatalf("rows=%v, want [[1 42]]", sink.rows)
	}
}

// TestSourceWithAllocatorNilIgnored is a regression test: WithAllocator used
// to overwrite Source's mem field unconditionally, so passing nil replaced
// the memory.DefaultAllocator set by New with a nil allocator, which panics
// (or otherwise misbehaves) the first time an Arrow builder is constructed
// from it. A nil mem must be silently ignored, keeping the default, the same
// convention WithBatchSize already follows for an invalid n.
func TestSourceWithAllocatorNilIgnored(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id"},
		rows:    [][]driver.Value{{int64(1)}},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id FROM t", schema, sqlsource.WithAllocator(nil))
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 1 || sink.rows[0][0] != int64(1) {
		t.Fatalf("rows=%v, want [[1]] (a nil WithAllocator must not panic and must keep the default allocator)", sink.rows)
	}
}

// TestSourceWithAllocatorTypedNilIgnored is a regression test: unlike
// TestSourceWithAllocatorNilIgnored's untyped nil literal, a typed-nil
// concrete allocator such as (*memory.CheckedAllocator)(nil), wrapped in
// the memory.Allocator interface, is != nil. WithAllocator must still catch
// it and keep the default rather than storing it and panicking the first
// time an Arrow builder is constructed from it.
func TestSourceWithAllocatorTypedNilIgnored(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id"},
		rows:    [][]driver.Value{{int64(1)}},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id FROM t", schema,
		sqlsource.WithAllocator((*memory.CheckedAllocator)(nil)))
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 1 || sink.rows[0][0] != int64(1) {
		t.Fatalf("rows=%v, want [[1]] (a typed-nil WithAllocator must not panic and must keep the default allocator)", sink.rows)
	}
}

// boolSchema returns a schema of two columns, id (Int64) and value (Boolean),
// matching the shape boolConverter tests below scan rows into.
func boolSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
}

// TestSourceBoolFromBinaryByte is a regression test: some drivers (notably
// certain MySQL/MSSQL BIT(1) handling) scan a BIT column as a raw
// single-byte binary value (0x00/0x01) rather than ASCII text ("0"/"1"),
// which strconv.ParseBool doesn't accept, so boolConverter.append used to
// error and abort the whole source on real BIT-column data from such
// drivers.
func TestSourceBoolFromBinaryByte(t *testing.T) {
	schema := boolSchema()
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "value"},
		rows: [][]driver.Value{
			{int64(1), []byte{0x00}},
			{int64(2), []byte{0x01}},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, value FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 2 {
		t.Fatalf("rows=%v, want 2 rows", sink.rows)
	}
	if sink.rows[0][1] != false {
		t.Fatalf("row 0 value=%v, want false", sink.rows[0][1])
	}
	if sink.rows[1][1] != true {
		t.Fatalf("row 1 value=%v, want true", sink.rows[1][1])
	}
}

// TestSourceBoolFromTextDriverValue confirms the existing ASCII-text
// behavior (drivers that send BIT/BOOLEAN as text rather than raw binary)
// still works unchanged alongside the binary-byte handling added above.
func TestSourceBoolFromTextDriverValue(t *testing.T) {
	schema := boolSchema()
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "value"},
		rows: [][]driver.Value{
			{int64(1), []byte("true")},
			{int64(2), []byte("1")},
			{int64(3), []byte("false")},
			{int64(4), "0"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, value FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	want := []bool{true, true, false, false}
	if len(sink.rows) != len(want) {
		t.Fatalf("rows=%v, want %d rows", sink.rows, len(want))
	}
	for i, row := range sink.rows {
		if row[1] != want[i] {
			t.Fatalf("row %d value=%v, want %v", i, row[1], want[i])
		}
	}
}

func TestSourceNilDBAndSchema(t *testing.T) {
	schema := int64Schema("id")
	db, err := sql.Open("sqlsourcefake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := sqlsource.New(nil, "SELECT id FROM t", schema); err == nil {
		t.Fatal("want error constructing Source with a nil db")
	}
	if _, err := sqlsource.New(db, "SELECT id FROM t", nil); err == nil {
		t.Fatal("want error constructing Source with a nil schema")
	}
}

// TestSourceWithArgsCopiesSlice is a regression test: WithArgs used to keep
// a reference to the caller's backing slice, so mutating it after
// construction (or reusing it concurrently) could change the args the next
// query runs with.
func TestSourceWithArgsCopiesSlice(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id"},
		rows:    [][]driver.Value{{int64(1)}},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	args := []any{int64(1)}
	src, err := sqlsource.New(db, "SELECT id FROM t WHERE id = ?", schema, sqlsource.WithArgs(args...))
	if err != nil {
		t.Fatal(err)
	}

	args[0] = int64(999) // mutate the caller's slice after construction
	runSource(t, src)

	_, gotArgs := lookupFixture(dsn).lastCall()
	if len(gotArgs) != 1 || gotArgs[0].Value != int64(1) {
		t.Fatalf("query args=%v, want [1] (mutating the caller's slice after New must not affect the stored args)", gotArgs)
	}
}

func TestSourceUnsupportedSchemaType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)
	db, err := sql.Open("sqlsourcefake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := sqlsource.New(db, "SELECT id FROM t", schema); err == nil {
		t.Fatal("want error constructing Source with an unsupported schema field type")
	}
}

// TestSourceNilTypedSchemaField is a regression test: arrow.NewSchema only
// checks field.Type == nil, so it does not reject a typed-nil DataType like
// (*arrow.TimestampType)(nil) wrapped in the arrow.DataType interface. New
// must catch that case itself instead of panicking later in converterFor.
func TestSourceNilTypedSchemaField(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: (*arrow.TimestampType)(nil)},
	}, nil)
	db, err := sql.Open("sqlsourcefake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := sqlsource.New(db, "SELECT id FROM t", schema); err == nil {
		t.Fatal("want error constructing Source with a typed-nil schema field type")
	}
}

func TestSourceColumnCountMismatch(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "extra"},
		rows:    [][]driver.Value{{int64(1), int64(2)}},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, extra FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	p.From(src).To(&countingSink{})
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error on column count mismatch")
	}
}

func TestSourceQueryError(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns:  []string{"id"},
		queryErr: errors.New("connection refused"),
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	p.From(src).To(&countingSink{})
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error propagated from a failing query")
	}
}

func TestSourceExactBatchBoundary(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id"},
		rows: [][]driver.Value{
			{int64(1)}, {int64(2)}, {int64(3)}, {int64(4)},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id FROM t", schema, sqlsource.WithBatchSize(2))
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if got, want := sink.batches, []int64{2, 2}; !equalInt64s(got, want) {
		t.Fatalf("batches=%v, want %v (no trailing empty batch)", got, want)
	}
}

// TestSourceTimestampWithTimeZone is a regression test: array.NewRecord
// checks a column's Arrow type against its schema field for equality
// (including TimeZone), so a schema field like this one used to panic at
// record-construction time — the builder timestampConverter.newBuilder
// created only carried over Unit, silently dropping TimeZone.
func TestSourceTimestampWithTimeZone(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
	}, nil)

	want := time.Date(2026, 7, 15, 12, 30, 0, 0, time.UTC)
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			{int64(1), want},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 1 {
		t.Fatalf("rows=%v, want 1 row", sink.rows)
	}
	got, ok := sink.rows[0][1].(arrow.Timestamp)
	if !ok {
		t.Fatalf("created_at = %#v (%T), want arrow.Timestamp", sink.rows[0][1], sink.rows[0][1])
	}
	wantTS, err := arrow.TimestampFromTime(want, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != wantTS {
		t.Fatalf("created_at = %v, want %v", got, wantTS)
	}

	wantType := schema.Field(1).Type
	if gotType := sink.rowTypes[0][1]; !arrow.TypeEqual(gotType, wantType) {
		t.Fatalf("created_at column type = %v, want %v (TimeZone must round-trip too)", gotType, wantType)
	}
}

// TestSourceTimestampFromTextDriverValue is a regression test: some
// database/sql drivers scan TIMESTAMP columns as []byte or string rather
// than time.Time (e.g. modernc.org/sqlite for certain column affinities),
// so timestampConverter.append must parse text the same way its sibling
// converters (int64Converter, float64Converter, ...) already do, instead of
// erroring and stopping batch emission.
func TestSourceTimestampFromTextDriverValue(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
	}, nil)

	want, err := arrow.TimestampFromString("2026-07-15 12:30:00", arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			{int64(1), []byte("2026-07-15 12:30:00")},
			{int64(2), "2026-07-15 12:30:00"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 2 {
		t.Fatalf("rows=%v, want 2 rows", sink.rows)
	}
	for i, row := range sink.rows {
		got, ok := row[1].(arrow.Timestamp)
		if !ok {
			t.Fatalf("row %d: created_at = %#v (%T), want arrow.Timestamp", i, row[1], row[1])
		}
		if got != want {
			t.Fatalf("row %d: created_at = %v, want %v", i, got, want)
		}
	}
}

// TestSourceTimestampFromTextDriverValueWithTimeZone is a regression test
// for a bug in the fix TestSourceTimestampFromTextDriverValue covers: when
// the schema's TimestampType declares a non-UTC TimeZone and a driver hands
// back a naive (no offset) text timestamp, timestampConverter.append used
// to parse it as arrow.TimestampFromString does — assuming UTC — which
// silently shifted the resulting instant by the zone's offset instead of
// treating the text as a wall-clock reading in the declared zone. The
// correct instant is what you'd get by parsing the same digits with
// time.ParseInLocation in that zone, which is what a driver returning a
// proper time.Time (the sibling branch, via arrow.TimestampFromTime) would
// have produced for an equivalent value.
func TestSourceTimestampFromTextDriverValueWithTimeZone(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}},
	}, nil)

	// Naive text: no embedded offset, meant to be read as wall-clock time
	// in the schema's declared zone (America/New_York), not UTC.
	wantTime, err := time.ParseInLocation("2006-01-02 15:04:05", "2026-07-15 12:30:00", loc)
	if err != nil {
		t.Fatal(err)
	}
	want, err := arrow.TimestampFromTime(wantTime, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}

	// An explicit-offset (RFC3339-ish) text value must keep its own offset
	// rather than being forced into the schema's declared zone.
	wantOffsetTime, err := time.Parse(time.RFC3339, "2026-07-15T12:30:00-05:00")
	if err != nil {
		t.Fatal(err)
	}
	wantOffset, err := arrow.TimestampFromTime(wantOffsetTime, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}

	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			{int64(1), []byte("2026-07-15 12:30:00")},
			{int64(2), "2026-07-15 12:30:00"},
			{int64(3), "2026-07-15T12:30:00-05:00"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 3 {
		t.Fatalf("rows=%v, want 3 rows", sink.rows)
	}

	wantByRow := []arrow.Timestamp{want, want, wantOffset}
	for i, row := range sink.rows {
		got, ok := row[1].(arrow.Timestamp)
		if !ok {
			t.Fatalf("row %d: created_at = %#v (%T), want arrow.Timestamp", i, row[1], row[1])
		}
		if got != wantByRow[i] {
			t.Fatalf("row %d: created_at = %v, want %v", i, got, wantByRow[i])
		}
	}

	// Sanity check that the naive text case and the UTC-assuming parse
	// actually disagree, so this test would have caught the bug: NY is
	// behind UTC, so treating "12:30:00" as UTC instead of NY wall-clock
	// time yields a different (earlier, by the zone's offset) instant.
	utcAssumed, err := arrow.TimestampFromString("2026-07-15 12:30:00", arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	if utcAssumed == want {
		t.Fatalf("test is not discriminating: UTC-assumed parse %v unexpectedly equals zone-aware parse %v", utcAssumed, want)
	}
}

// TestSourceTimestampUnresolvableTimeZoneDoesNotFailSetup is a regression
// test: converterFor used to resolve the schema field's declared TimeZone
// via time.LoadLocation eagerly, so sqlsource.New failed immediately for any
// schema declaring a TimeZone that doesn't resolve (e.g. because the binary
// wasn't built with time/tzdata and isn't running where the system zoneinfo
// database is available), even for callers whose driver only ever returns
// time.Time — which never needs the resolved zone at all, since
// arrow.TimestampFromTime works from the time.Time's own baked-in offset.
// Resolution must be deferred until a text ([]byte/string) driver value
// actually needs it.
func TestSourceTimestampUnresolvableTimeZoneDoesNotFailSetup(t *testing.T) {
	const badZone = "Not/A_Real_Zone"
	if _, err := time.LoadLocation(badZone); err == nil {
		t.Fatalf("test setup: %q unexpectedly resolved as a real zone", badZone)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: badZone}},
	}, nil)

	want := time.Date(2026, 7, 15, 12, 30, 0, 0, time.UTC)
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			{int64(1), want},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Construction must succeed even though badZone can never be resolved:
	// this is the setup-time regression under test.
	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatalf("sqlsource.New failed eagerly on an unresolvable TimeZone: %v", err)
	}

	// Feeding it a time.Time value (the common, idiomatic driver behavior)
	// must still work fine, since that branch never consults the zone.
	sink := runSource(t, src)
	if len(sink.rows) != 1 {
		t.Fatalf("rows=%v, want 1 row", sink.rows)
	}
	got, ok := sink.rows[0][1].(arrow.Timestamp)
	if !ok {
		t.Fatalf("created_at = %#v (%T), want arrow.Timestamp", sink.rows[0][1], sink.rows[0][1])
	}
	wantTS, err := arrow.TimestampFromTime(want, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	if got != wantTS {
		t.Fatalf("created_at = %v, want %v", got, wantTS)
	}
}

// TestSourceTimestampUnresolvableTimeZoneErrorsOnTextValue complements
// TestSourceTimestampUnresolvableTimeZoneDoesNotFailSetup: once a driver
// actually hands back naive text under a schema whose TimeZone can't be
// resolved, that (and only that) is where the error must surface.
func TestSourceTimestampUnresolvableTimeZoneErrorsOnTextValue(t *testing.T) {
	const badZone = "Not/A_Real_Zone"
	if _, err := time.LoadLocation(badZone); err == nil {
		t.Fatalf("test setup: %q unexpectedly resolved as a real zone", badZone)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: badZone}},
	}, nil)

	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			// Naive text: needs the (unresolvable) declared zone to parse.
			{int64(1), "2026-07-15 12:30:00"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatalf("sqlsource.New failed eagerly on an unresolvable TimeZone: %v", err)
	}

	p := etl.New()
	sink := &countingSink{}
	p.From(src).To(sink)
	err = p.Run(context.Background())
	if err == nil {
		t.Fatal("Run succeeded, want an error resolving the declared time zone for naive text")
	}
	if !strings.Contains(err.Error(), badZone) {
		t.Fatalf("Run error = %v, want it to mention the unresolvable zone %q", err, badZone)
	}
}

// TestSourceTimestampCaseInsensitiveUTCTimeZone is a regression test:
// timestampTextLocation used to compare a schema's declared TimeZone against
// the exact, case-sensitive string "UTC" before falling through to
// time.LoadLocation, but Arrow's own convention (see arrow-go's
// TimestampType.GetZone) treats "UTC" and "utc" as equivalent. Any other
// casing was looked up as if it were a real IANA zone name via
// time.LoadLocation, which fails for a spelling like "utc" that isn't a real
// zone file name, including (unlike a real IANA zone) on builds without
// tzdata linked in.
func TestSourceTimestampCaseInsensitiveUTCTimeZone(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "utc"}},
	}, nil)

	want, err := arrow.TimestampFromString("2026-07-15 12:30:00", arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}
	dsn := registerFixture(t, &fixture{
		columns: []string{"id", "created_at"},
		rows: [][]driver.Value{
			// Naive text: must be parsed as UTC without needing tzdata,
			// exactly like a TimeZone of "" or "UTC" would be.
			{int64(1), []byte("2026-07-15 12:30:00")},
			{int64(2), "2026-07-15 12:30:00"},
		},
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id, created_at FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.rows) != 2 {
		t.Fatalf("rows=%v, want 2 rows", sink.rows)
	}
	for i, row := range sink.rows {
		got, ok := row[1].(arrow.Timestamp)
		if !ok {
			t.Fatalf("row %d: created_at = %#v (%T), want arrow.Timestamp", i, row[1], row[1])
		}
		if got != want {
			t.Fatalf("row %d: created_at = %v, want %v", i, got, want)
		}
	}
}

func TestSourceZeroRows(t *testing.T) {
	schema := int64Schema("id")
	dsn := registerFixture(t, &fixture{
		columns: []string{"id"},
		rows:    nil,
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	src, err := sqlsource.New(db, "SELECT id FROM t", schema)
	if err != nil {
		t.Fatal(err)
	}
	sink := runSource(t, src)

	if len(sink.batches) != 0 {
		t.Fatalf("batches=%v, want none", sink.batches)
	}
}
