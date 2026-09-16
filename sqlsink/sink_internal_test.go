package sqlsink

// Internal package to reach unexported buildInsert: asserting exact SQL text
// is the only way to catch dialect-formatting bugs SQLite would silently accept.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
)

// SQLite would silently tolerate markers restarting at each row (overwriting
// values with the first row's), so this is checked against the query text directly.
func TestBuildInsertDollarPlaceholdersAreSequentialAcrossRows(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.AppendValues([]string{"a", "b", "c"}, nil)
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := &Sink{
		table:       "people",
		columns:     []string{"id", "name"},
		extractors:  []extractor{int64Extractor, stringExtractor},
		placeholder: Dollar,
	}

	query, args := s.buildInsert([]arrow.Array{idArr, nameArr}, 0, 3)

	want := "INSERT INTO people (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
	if len(args) != 6 {
		t.Fatalf("len(args) = %d, want 6", len(args))
	}
}

// buildInsertQuery must produce byte-for-byte the same text as buildInsert
// for the same row count, or a prepared statement's SQL would silently diverge.
func TestBuildInsertQueryMatchesBuildInsertQueryText(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.AppendValues([]int64{1, 2, 3}, nil)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.AppendValues([]string{"a", "b", "c"}, nil)
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := &Sink{
		table:       "people",
		columns:     []string{"id", "name"},
		extractors:  []extractor{int64Extractor, stringExtractor},
		placeholder: Dollar,
	}

	want, _ := s.buildInsert([]arrow.Array{idArr, nameArr}, 0, 3)
	got := s.buildInsertQuery(3)

	if got != want {
		t.Fatalf("buildInsertQuery(3) = %q, want %q (buildInsert's query text for the same row count)", got, want)
	}
}

// SQLite would reject a malformed clause outright rather than reveal a subtle
// spacing/punctuation regression, so the exact query text is asserted directly.
func TestBuildInsertAppendsUpsertClauseVerbatimAfterValues(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.Append("a")
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := &Sink{
		table:        "people",
		columns:      []string{"id", "name"},
		extractors:   []extractor{int64Extractor, stringExtractor},
		placeholder:  Question,
		upsertClause: "ON DUPLICATE KEY UPDATE name = VALUES(name)",
	}

	query, _ := s.buildInsert([]arrow.Array{idArr, nameArr}, 0, 1)

	want := "INSERT INTO people (id, name) VALUES (?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

// placeholder is deliberately left set to Dollar, to prove placeholderFunc wins.
func TestBuildInsertPlaceholderFuncOverridesPlaceholder(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.AppendValues([]int64{1, 2}, nil)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.AppendValues([]string{"a", "b"}, nil)
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := &Sink{
		table:           "people",
		columns:         []string{"id", "name"},
		extractors:      []extractor{int64Extractor, stringExtractor},
		placeholder:     Dollar,
		placeholderFunc: func(n int) string { return fmt.Sprintf("@p%d", n) },
	}

	query, _ := s.buildInsert([]arrow.Array{idArr, nameArr}, 0, 2)

	want := "INSERT INTO people (id, name) VALUES (@p1, @p2), (@p3, @p4)"
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

func TestBuildInsertQuotesTableAndColumnsWhenIdentifierQuoteSet(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()

	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	nameB.Append("a")
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	s := &Sink{
		table:           "order",
		columns:         []string{"id", "select"},
		extractors:      []extractor{int64Extractor, stringExtractor},
		placeholder:     Question,
		identifierQuote: func(name string) string { return `"` + name + `"` },
	}

	query, _ := s.buildInsert([]arrow.Array{idArr, nameArr}, 0, 1)

	want := `INSERT INTO "order" ("id", "select") VALUES (?, ?)`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

// Each dot-separated segment must be quoted independently, or the literal dot
// would end up quoted into the identifier itself.
func TestBuildInsertQuotesEachSegmentOfSchemaQualifiedTableName(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()

	s := &Sink{
		table:           "analytics.orders",
		columns:         []string{"id"},
		extractors:      []extractor{int64Extractor},
		placeholder:     Question,
		identifierQuote: func(name string) string { return `"` + name + `"` },
	}

	query, _ := s.buildInsert([]arrow.Array{idArr}, 0, 1)

	want := `INSERT INTO "analytics"."orders" ("id") VALUES (?)`
	if query != want {
		t.Fatalf("query = %q, want %q", query, want)
	}
}

// A blocking extractor forces genuine overlap: the first call is parked
// inside buildInsert, still holding inUse, when the second's CompareAndSwap runs.
func TestConsumeConcurrentCallsOneFailsWithInUseError(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(schema, []arrow.Array{idArr}, 1)
	defer record.Release()

	entered := make(chan struct{})
	release := make(chan struct{})
	blockingExtractor := func(col arrow.Array, row int) any {
		close(entered)
		<-release
		return int64Extractor(col, row)
	}

	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	s := &Sink{
		db:              db,
		table:           "people",
		maxPlaceholders: defaultMaxPlaceholders,
		started:         true,
		schema:          schema,
		columns:         []string{"id"},
		extractors:      []extractor{blockingExtractor},
	}

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- s.Consume(context.Background(), etl.NewBatch(record))
	}()

	<-entered // the first call is now parked mid-Consume, still holding inUse.

	secondErr := s.Consume(context.Background(), etl.NewBatch(record))
	close(release)

	if err := <-firstErr; err != nil {
		t.Fatalf("first Consume call returned an error: %v", err)
	}
	if secondErr == nil {
		t.Fatal("second, concurrent Consume call returned nil, want an already-in-use error")
	}
}

// Same inUse guard as TestConsumeConcurrentCallsOneFailsWithInUseError, but
// racing Finish against Consume instead of two Consume calls.
func TestFinishConcurrentWithConsumeFailsWithInUseError(t *testing.T) {
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	idB.Append(1)
	idArr := idB.NewArray()
	defer idArr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(schema, []arrow.Array{idArr}, 1)
	defer record.Release()

	entered := make(chan struct{})
	release := make(chan struct{})
	blockingExtractor := func(col arrow.Array, row int) any {
		close(entered)
		<-release
		return int64Extractor(col, row)
	}

	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	s := &Sink{
		db:              db,
		table:           "people",
		maxPlaceholders: defaultMaxPlaceholders,
		started:         true,
		schema:          schema,
		columns:         []string{"id"},
		extractors:      []extractor{blockingExtractor},
	}

	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- s.Consume(context.Background(), etl.NewBatch(record))
	}()

	<-entered // Consume is now parked mid-Consume, still holding inUse.

	finishErr := s.Finish(context.Background())
	close(release)

	if err := <-consumeErr; err != nil {
		t.Fatalf("Consume call returned an error: %v", err)
	}
	if finishErr == nil {
		t.Fatal("Finish call concurrent with Consume returned nil, want an already-in-use error")
	}
	if s.finished {
		t.Fatal("s.finished is true after the losing Finish call; it must not have run")
	}

	// The guard must not deadlock normal sequential usage after the race resolves.
	if err := s.Finish(context.Background()); err != nil {
		t.Fatalf("Finish call after Consume completed returned an error: %v", err)
	}
	if !s.finished {
		t.Fatal("s.finished is false after a successful Finish call")
	}
}

// arrow.NewSchema only checks field.Type == nil, so a typed-nil DataType like
// (*arrow.TimestampType)(nil) slips through into a schema; open must catch it
// itself before extractorFor's type switch dereferences it. Calling open
// directly (rather than through Consume, as an earlier version of this test
// did) is required: a reported schema disagreeing with a real record's schema
// is rejected by Consume's own record/schema comparison before open ever
// runs, so that path can't reach this check.
func TestSinkOpenNilTypedSchemaFieldReturnsError(t *testing.T) {
	s := &Sink{table: "people", maxPlaceholders: defaultMaxPlaceholders}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: (*arrow.TimestampType)(nil)},
	}, nil)

	err := s.open(schema)
	if err == nil {
		t.Fatal("want an error for a typed-nil schema field type, got nil")
	}
}
