package sqlsource_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

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
	mu      sync.Mutex
	batches []int64
	rows    [][]any
}

func (s *countingSink) Consume(_ context.Context, b etl.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, b.NumRows())
	rec := b.Record()
	for r := 0; r < int(rec.NumRows()); r++ {
		row := make([]any, rec.NumCols())
		for c := 0; c < int(rec.NumCols()); c++ {
			col := rec.Column(c)
			if col.IsNull(r) {
				row[c] = nil
				continue
			}
			row[c] = columnValue(col, r)
		}
		s.rows = append(s.rows, row)
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
