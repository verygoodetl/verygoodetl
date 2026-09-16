package sqlsink_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	msqlite "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsink"
)

// --- test helpers ---

func idNameSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// idNameRecord builds a record for idNameSchema; a nil entry in names
// appends a null for that row.
func idNameRecord(t *testing.T, ids []int64, names []*string) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()

	for i, id := range ids {
		idB.Append(id)
		if names[i] == nil {
			nameB.AppendNull()
		} else {
			nameB.Append(*names[i])
		}
	}

	idArr := idB.NewArray()
	defer idArr.Release()
	nameArr := nameB.NewArray()
	defer nameArr.Release()

	return array.NewRecord(idNameSchema(), []arrow.Array{idArr, nameArr}, int64(len(ids)))
}

func strPtr(s string) *string { return &s }

// allTypesSchema covers all six Arrow types sqlsink supports, one column
// each, all nullable except id.
func allTypesSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "data", Type: arrow.BinaryTypes.Binary, Nullable: true},
		{Name: "created_at", Type: &arrow.TimestampType{Unit: arrow.Microsecond}, Nullable: true},
	}, nil)
}

// allTypesRow holds one row's worth of values for allTypesSchema; a nil
// pointer field appends a null for that column.
type allTypesRow struct {
	id        int64
	score     *float64
	active    *bool
	name      *string
	data      []byte
	createdAt *time.Time
}

func allTypesRecord(t *testing.T, rows []allTypesRow) arrow.Record {
	t.Helper()
	mem := memory.DefaultAllocator

	idB := array.NewInt64Builder(mem)
	defer idB.Release()
	scoreB := array.NewFloat64Builder(mem)
	defer scoreB.Release()
	activeB := array.NewBooleanBuilder(mem)
	defer activeB.Release()
	nameB := array.NewStringBuilder(mem)
	defer nameB.Release()
	dataB := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
	defer dataB.Release()
	createdAtB := array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: arrow.Microsecond})
	defer createdAtB.Release()

	for _, r := range rows {
		idB.Append(r.id)
		if r.score == nil {
			scoreB.AppendNull()
		} else {
			scoreB.Append(*r.score)
		}
		if r.active == nil {
			activeB.AppendNull()
		} else {
			activeB.Append(*r.active)
		}
		if r.name == nil {
			nameB.AppendNull()
		} else {
			nameB.Append(*r.name)
		}
		if r.data == nil {
			dataB.AppendNull()
		} else {
			dataB.Append(r.data)
		}
		if r.createdAt == nil {
			createdAtB.AppendNull()
		} else {
			createdAtB.AppendTime(*r.createdAt)
		}
	}

	idArr := idB.NewArray()
	defer idArr.Release()
	scoreArr := scoreB.NewArray()
	defer scoreArr.Release()
	activeArr := activeB.NewArray()
	defer activeArr.Release()
	nameArr := nameB.NewArray()
	defer nameArr.Release()
	dataArr := dataB.NewArray()
	defer dataArr.Release()
	createdAtArr := createdAtB.NewArray()
	defer createdAtArr.Release()

	return array.NewRecord(allTypesSchema(), []arrow.Array{
		idArr, scoreArr, activeArr, nameArr, dataArr, createdAtArr,
	}, int64(len(rows)))
}

// batchSource replays a fixed list of pre-built batches.
type batchSource struct {
	batches []etl.Batch
}

func (s batchSource) Run(ctx context.Context, out etl.Output) error {
	for _, b := range s.batches {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func run(t *testing.T, sink etl.Sink, batches ...etl.Batch) error {
	t.Helper()
	p := etl.New()
	p.From(batchSource{batches: batches}).To(sink)
	return p.Run(context.Background())
}

func openTestDB(t *testing.T, ddl string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file::memory:?cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.ExecContext(context.Background(), ddl); err != nil {
		t.Fatal(err)
	}
	return db
}

func queryPeople(t *testing.T, db *sql.DB) []struct {
	id   int64
	name sql.NullString
} {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []struct {
		id   int64
		name sql.NullString
	}
	for rows.Next() {
		var row struct {
			id   int64
			name sql.NullString
		}
		if err := rows.Scan(&row.id, &row.name); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

// allTypesRowScan mirrors allTypesRow's fields with sql.Null* wrappers for
// scanning nullable columns back out of the DB.
type allTypesRowScan struct {
	id        int64
	score     sql.NullFloat64
	active    sql.NullBool
	name      sql.NullString
	data      []byte
	createdAt sql.NullTime
}

func queryAllTypes(t *testing.T, db *sql.DB) []allTypesRowScan {
	t.Helper()
	rows, err := db.QueryContext(context.Background(),
		"SELECT id, score, active, name, data, created_at FROM widgets ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []allTypesRowScan
	for rows.Next() {
		var row allTypesRowScan
		if err := rows.Scan(&row.id, &row.score, &row.active, &row.name, &row.data, &row.createdAt); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

// --- tests ---

func TestSinkHappyPathInsertsRows(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2, 3}, []*string{strPtr("a"), strPtr("b"), nil})
	defer rec.Release()

	sink := sqlsink.New(db, "people")
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 3 {
		t.Fatalf("rows=%d, want 3", len(got))
	}
	if got[0].id != 1 || got[0].name.String != "a" {
		t.Fatalf("row 0 = %+v", got[0])
	}
	if got[1].id != 2 || got[1].name.String != "b" {
		t.Fatalf("row 1 = %+v", got[1])
	}
	if got[2].id != 3 || got[2].name.Valid {
		t.Fatalf("row 2 = %+v, want a null name", got[2])
	}
}

func TestSinkMultipleBatchesAcrossMultipleConsumeCalls(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec1 := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec1.Release()
	rec2 := idNameRecord(t, []int64{2, 3}, []*string{strPtr("b"), strPtr("c")})
	defer rec2.Release()

	sink := sqlsink.New(db, "people")
	if err := run(t, sink, etl.NewBatch(rec1), etl.NewBatch(rec2)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 3 {
		t.Fatalf("rows=%d, want 3", len(got))
	}
}

func TestSinkWithUpsertClauseUpdatesOnConflict(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	first := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer first.Release()
	if err := run(t, sqlsink.New(db, "people"), etl.NewBatch(first)); err != nil {
		t.Fatal(err)
	}

	second := idNameRecord(t, []int64{1}, []*string{strPtr("b")})
	defer second.Release()
	upserting := sqlsink.New(db, "people", sqlsink.WithUpsertClause(
		"ON CONFLICT (id) DO UPDATE SET name = excluded.name",
	))
	if err := run(t, upserting, etl.NewBatch(second)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 1 {
		t.Fatalf("rows=%d, want 1 (upsert should not duplicate)", len(got))
	}
	if got[0].name.String != "b" {
		t.Fatalf("name=%q, want %q (upsert should have updated it)", got[0].name.String, "b")
	}
}

// Rejected at construction, rather than surfacing later as a confusing
// placeholder-count/argument mismatch.
func TestSinkWithUpsertClauseContainingQuestionMarkIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithUpsertClause(
		"ON CONFLICT (id) DO UPDATE SET name = ?",
	))
	if err := run(t, sink, etl.NewBatch(rec)); err == nil {
		t.Fatal("want an error for a clause containing a \"?\" placeholder marker, got nil")
	}
}

func TestSinkWithUpsertClauseContainingDollarPlaceholderIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithUpsertClause(
		"ON CONFLICT (id) DO UPDATE SET name = $1",
	))
	if err := run(t, sink, etl.NewBatch(rec)); err == nil {
		t.Fatal("want an error for a clause containing a \"$1\" placeholder marker, got nil")
	}
}

// Confirms the best-effort "$"+digit check doesn't over-reject a literal "$"
// inside a quoted string literal.
func TestSinkWithUpsertClauseContainingDollarNotFollowedByDigitIsAllowed(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithUpsertClause(
		"ON CONFLICT (id) DO UPDATE SET name = '$USD'",
	))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatalf("want no error for a clause containing a literal \"$\" not followed by a digit, got %v", err)
	}
}

func TestSinkWithoutUpsertClauseConflictFailsAndRollsBackWholeBatch(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	// id=1 duplicated forces the single generated INSERT to violate the PK,
	// rolling back the whole batch — including the otherwise-valid id=2 row.
	rec := idNameRecord(t, []int64{1, 2, 1}, []*string{strPtr("a"), strPtr("b"), strPtr("c")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error from the primary key conflict, got nil")
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (batch should have rolled back atomically)", len(got))
	}
}

func TestSinkChunksLargeBatchesAcrossMultipleStatements(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	ids := make([]int64, 10)
	names := make([]*string, 10)
	for i := range ids {
		ids[i] = int64(i + 1)
		names[i] = strPtr("n")
	}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so 10 rows
	// forces 5 separate INSERT statements within the same transaction.
	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 10 {
		t.Fatalf("rows=%d, want 10", len(got))
	}
}

// A non-positive n is not treated as "unlimited" — it's a construction
// error, surfaced from Consume like New's other construction problems.
func TestSinkWithMaxPlaceholdersNonPositiveValueIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	for _, n := range []int{0, -1} {
		sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(n))
		err := run(t, sink, etl.NewBatch(rec))
		if err == nil {
			t.Fatalf("want an error for WithMaxPlaceholders(%d), got nil", n)
		}
	}
}

// Combines chunking (WithMaxPlaceholders) with a conflict landing in a later
// chunk: the whole transaction, including the first chunk's already-written
// rows, must still roll back.
func TestSinkChunkedBatchConflictInLaterStatementRollsBackEarlierChunkToo(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	// 2 columns * maxPlaceholders(4) => 2 rows per statement. The first
	// chunk (id=1, id=2) has no conflict and would succeed on its own; the
	// second chunk (id=3, id=3) violates the primary key.
	ids := []int64{1, 2, 3, 3}
	names := []*string{strPtr("a"), strPtr("b"), strPtr("c"), strPtr("d")}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error from the primary key conflict, got nil")
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (earlier chunk's successful statement should have rolled back too)", len(got))
	}
}

func TestSinkWithDollarPlaceholders(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2}, []*string{strPtr("a"), strPtr("b")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithPlaceholder(sqlsink.Dollar))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 2 {
		t.Fatalf("rows=%d, want 2", len(got))
	}
}

// Must produce a statement the driver actually accepts and binds correctly,
// not just plausible-looking text.
func TestSinkWithPlaceholderFuncOverridesQuestionMarkers(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2}, []*string{strPtr("a"), strPtr("b")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithPlaceholderFunc(func(n int) string {
		return fmt.Sprintf("?%d", n)
	}))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 2 {
		t.Fatalf("rows=%d, want 2", len(got))
	}
	if got[0].id != 1 || got[0].name.String != "a" || got[1].id != 2 || got[1].name.String != "b" {
		t.Fatalf("got %+v, want rows bound in argument order", got)
	}
}

func TestSinkWithPlaceholderFuncTakesPrecedenceOverWithPlaceholder(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people",
		sqlsink.WithPlaceholder(sqlsink.Dollar),
		sqlsink.WithPlaceholderFunc(func(n int) string { return fmt.Sprintf("?%d", n) }),
	)
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 1 {
		t.Fatalf("rows=%d, want 1", len(got))
	}
}

func TestSinkNewNilDBReturnsErrorInsteadOfPanicking(t *testing.T) {
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(nil, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a nil db, got nil")
	}
}

func TestSinkNewEmptyTableReturnsErrorInsteadOfPanicking(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, ""), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for an empty table name, got nil")
	}
}

func TestSinkZeroBatchesIsANoOp(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	if err := run(t, sqlsink.New(db, "people")); err != nil {
		t.Fatal(err)
	}
	if got := queryPeople(t, db); len(got) != 0 {
		t.Fatalf("rows=%d, want 0", len(got))
	}
}

// With zero batches, Consume never runs, so a construction error can only
// surface from Finish — pins down that Finish checks s.constructErr too.
func TestSinkZeroBatchesInvalidTableStillReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	err := run(t, sqlsink.New(db, "order"))
	if err == nil {
		t.Fatal("want an error for an invalid table name even with zero batches, got nil")
	}
}

// A direct Consume call (bypassing the pipeline, which already screens out
// nil batches before Sink ever sees them) with a nil etl.Batch used to panic
// on b.Schema() before any of Consume's own nil checks ran.
func TestSinkConsumeNilBatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	err := sink.Consume(context.Background(), nil)
	if err == nil {
		t.Fatal("want an error for a nil batch, got nil")
	}
}

// A typed-nil *etl.ArrowBatch wrapped in a non-nil etl.Batch interface is a
// distinct case from a bare nil interface: it must be caught by reflection,
// not by a plain == nil comparison.
func TestSinkConsumeTypedNilBatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	var typedNilBatch *etl.ArrowBatch
	err := sink.Consume(context.Background(), typedNilBatch)
	if err == nil {
		t.Fatal("want an error for a typed-nil batch, got nil")
	}
}

func TestSinkConsumeNilSchemaBatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	err := sink.Consume(context.Background(), nilSchemaBatch{})
	if err == nil {
		t.Fatal("want an error for a nil schema, got nil")
	}
}

// Unlike nilSchemaBatch (caught by an earlier nil-Schema check), this batch
// reports a valid schema but a nil Record(), exercising that check on its own.
func TestSinkConsumeNilRecordBatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	err := sink.Consume(context.Background(), nilRecordBatch{})
	if err == nil {
		t.Fatal("want an error for a nil record, got nil")
	}
}

// Unlike TestSinkZeroBatchesIsANoOp (which never calls Consume), this calls
// Consume with a zero-row record to confirm it's a no-op rather than an
// attempt to build and execute an empty INSERT.
func TestSinkConsumeZeroRowRecordIsANoOp(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	first := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer first.Release()
	if err := sink.Consume(context.Background(), etl.NewBatch(first)); err != nil {
		t.Fatal(err)
	}

	empty := idNameRecord(t, nil, nil)
	defer empty.Release()
	if err := sink.Consume(context.Background(), etl.NewBatch(empty)); err != nil {
		t.Fatalf("want no error for a zero-row record, got %v", err)
	}

	got := queryPeople(t, db)
	if len(got) != 1 {
		t.Fatalf("rows=%d, want 1 (a zero-row Consume call must not insert anything)", len(got))
	}
}

func TestSinkSchemaChangeAfterFirstBatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	first := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer first.Release()

	otherSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(2)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	second := array.NewRecord(otherSchema, []arrow.Array{idArr}, 1)
	defer second.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(first), etl.NewBatch(second))
	if err == nil {
		t.Fatal("want an error for a batch whose schema differs from the first, got nil")
	}
}

// Unlike TestSinkSchemaChangeAfterFirstBatchReturnsError (different column
// count, short-circuits before any per-field comparison), this batch has the
// same field count but a different type at the same index — only caught by
// sameColumns' per-field check.
func TestSinkSchemaChangeSameColumnCountReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	first := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer first.Release()

	// Same field count and names as idNameSchema, but "name" is Int64
	// instead of String.
	sameCountSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(2)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewInt64Builder(mem)
	nameB.Append(99)
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	second := array.NewRecord(sameCountSchema, []arrow.Array{idArr, nameArr}, 1)
	defer second.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(first), etl.NewBatch(second))
	if err == nil {
		t.Fatal("want an error for a second batch with the same column count but a different type at the same index, got nil")
	}
}

// open only compares Name/Type when deriving columns (Nullable/Metadata
// never affect the generated INSERT), so nullability-only drift must be
// accepted, not rejected.
func TestSinkConsumeNullabilityOnlySchemaDriftIsAccepted(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	first := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer first.Release()

	// Identical to idNameSchema (id, name) except "name" is non-nullable
	// here instead of nullable — the only difference from the first batch.
	nonNullableNameSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(2)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("b")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	second := array.NewRecord(nonNullableNameSchema, []arrow.Array{idArr, nameArr}, 1)
	defer second.Release()

	if err := run(t, sqlsink.New(db, "people"), etl.NewBatch(first), etl.NewBatch(second)); err != nil {
		t.Fatalf("want no error for a second batch whose schema differs from the first only in a field's Nullable flag, got %v", err)
	}
}

// nilSchemaBatch is an etl.Batch whose Schema() returns nil, to exercise
// Sink's defensive check without needing a custom Source.
type nilSchemaBatch struct{}

func (nilSchemaBatch) Schema() *arrow.Schema { return nil }
func (nilSchemaBatch) NumRows() int64        { return 0 }
func (nilSchemaBatch) Record() arrow.Record  { return nil }
func (nilSchemaBatch) Retain()               {}
func (nilSchemaBatch) Release()              {}

// nilRecordBatch mirrors nilSchemaBatch, but for the record instead of the schema.
type nilRecordBatch struct{}

func (nilRecordBatch) Schema() *arrow.Schema { return idNameSchema() }
func (nilRecordBatch) NumRows() int64        { return 0 }
func (nilRecordBatch) Record() arrow.Record  { return nil }
func (nilRecordBatch) Retain()               {}
func (nilRecordBatch) Release()              {}

// wideSchemaBatch is an etl.Batch whose Schema() and Record() disagree, to
// exercise Sink's defenses against a custom implementation whose two methods
// don't match.
type wideSchemaBatch struct {
	schema *arrow.Schema
	record arrow.Record
}

func (b wideSchemaBatch) Schema() *arrow.Schema { return b.schema }
func (b wideSchemaBatch) NumRows() int64        { return b.record.NumRows() }
func (b wideSchemaBatch) Record() arrow.Record  { return b.record }
func (b wideSchemaBatch) Retain()               {}
func (b wideSchemaBatch) Release()              {}

// A Schema() reporting more fields than Record() actually has columns used
// to make buildInsert index past record.Columns(), panicking instead of erroring.
func TestSinkConsumeSchemaRecordColumnMismatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	sink := sqlsink.New(db, "people")

	// schema claims two columns (id, name); record only has one (id).
	wideSchema := idNameSchema()
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	narrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	rec := array.NewRecord(narrowSchema, []arrow.Array{idArr}, 1)
	defer rec.Release()

	err := sink.Consume(context.Background(), wideSchemaBatch{schema: wideSchema, record: rec})
	if err == nil {
		t.Fatal("want an error when the batch's schema and record column counts disagree, got nil")
	}
}

// A Schema() reporting a different type than Record() actually has (same
// field count, so a NumCols-only check missed it) used to reach an
// extractor's unchecked type assertion and panic instead of erroring.
func TestSinkConsumeSchemaRecordTypeMismatchReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY)`)
	sink := sqlsink.New(db, "people")

	// schema reports "id" as Int64; the record's actual column is Float64.
	reportedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	actualSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewFloat64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	rec := array.NewRecord(actualSchema, []arrow.Array{idArr}, 1)
	defer rec.Release()

	err := sink.Consume(context.Background(), wideSchemaBatch{schema: reportedSchema, record: rec})
	if err == nil {
		t.Fatal("want an error when the batch's reported schema and record disagree on a field's type, got nil")
	}
}

// table is interpolated directly into the generated SQL, so an invalid
// identifier must be rejected rather than silently interpolated.
func TestSinkNewInvalidTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people; DROP TABLE people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a table name containing a semicolon, got nil")
	}
}

// Same interpolation risk as TestSinkNewInvalidTableNameReturnsError, but for
// a column name coming from the batch's Arrow schema.
func TestSinkOpenInvalidColumnNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "bad name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a column name containing a space, got nil")
	}
}

// id is excluded from the all-nulls row since it's non-nullable.
func TestSinkAllSupportedTypesRoundTrip(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (
		id INTEGER PRIMARY KEY,
		score REAL,
		active INTEGER,
		name TEXT,
		data BLOB,
		created_at TIMESTAMP
	)`)

	score := 3.5
	active := true
	name := "widget"
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	createdAt := time.Date(2026, 7, 15, 12, 30, 0, 0, time.UTC)

	rec := allTypesRecord(t, []allTypesRow{
		{id: 1, score: &score, active: &active, name: &name, data: data, createdAt: &createdAt},
		{id: 2}, // all nullable fields left nil
	})
	defer rec.Release()

	sink := sqlsink.New(db, "widgets")
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryAllTypes(t, db)
	if len(got) != 2 {
		t.Fatalf("rows=%d, want 2", len(got))
	}

	row := got[0]
	if row.id != 1 {
		t.Fatalf("row 0 id=%d, want 1", row.id)
	}
	if !row.score.Valid || row.score.Float64 != score {
		t.Fatalf("row 0 score=%+v, want %v", row.score, score)
	}
	if !row.active.Valid || row.active.Bool != active {
		t.Fatalf("row 0 active=%+v, want %v", row.active, active)
	}
	if !row.name.Valid || row.name.String != name {
		t.Fatalf("row 0 name=%+v, want %q", row.name, name)
	}
	if string(row.data) != string(data) {
		t.Fatalf("row 0 data=%v, want %v", row.data, data)
	}
	if !row.createdAt.Valid || !row.createdAt.Time.Equal(createdAt) {
		t.Fatalf("row 0 created_at=%+v, want %v", row.createdAt, createdAt)
	}

	null := got[1]
	if null.id != 2 {
		t.Fatalf("row 1 id=%d, want 2", null.id)
	}
	if null.score.Valid || null.active.Valid || null.name.Valid || null.data != nil || null.createdAt.Valid {
		t.Fatalf("row 1 = %+v, want all nullable fields null", null)
	}
}

// Int32 isn't a type sqlsink handles; must surface as an ordinary Consume
// error, not panic later in extractorFor.
func TestSinkExtractorForUnsupportedTypeReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, count INTEGER)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	countB := array.NewInt32Builder(mem)
	countB.Append(1)
	countArr := countB.NewArray()
	countB.Release()
	defer countArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{idArr, countArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "widgets"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for an unsupported schema field type, got nil")
	}
}

// insert used to divide maxPlaceholders by the column count with no guard;
// zero columns panicked with a division by zero instead of erroring.
func TestSinkZeroFieldSchemaReturnsErrorInsteadOfPanicking(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY)`)

	schema := arrow.NewSchema([]arrow.Field{}, nil)
	rec := array.NewRecord(schema, []arrow.Array{}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "widgets"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a zero-field schema, got nil")
	}
}

// TestSinkMaxPlaceholdersBelowColumnCountReturnsError is a regression test:
// insert used to force rowsPerStmt up to at least 1 even when maxPlaceholders
// was below the column count, silently emitting statements with more
// placeholders than configured; now it's a construction error instead.
func TestSinkMaxPlaceholdersBelowColumnCountReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (
		id INTEGER PRIMARY KEY,
		score REAL,
		active INTEGER,
		name TEXT,
		data BLOB,
		created_at TIMESTAMP
	)`)

	score := 3.5
	rec := allTypesRecord(t, []allTypesRow{{id: 1, score: &score}})
	defer rec.Release()

	// allTypesSchema has 6 columns; cap it below that.
	sink := sqlsink.New(db, "widgets", sqlsink.WithMaxPlaceholders(4))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error when the configured max placeholders is below the schema's column count, got nil")
	}
}

// hasInvalidIdentifierChars used to deny only a specific blocklist, letting
// characters like parens and commas through; it's now an allowlist of ASCII
// letters, digits, and underscore.
func TestSinkNewInvalidTableNameCharactersReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people(x)"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a table name containing parentheses, got nil")
	}
}

// TestSinkOpenInvalidColumnNameCharactersReturnsError extends
// TestSinkOpenInvalidColumnNameReturnsError to cover a character the old
// blocklist let through (a comma) but the allowlist now rejects.
func TestSinkOpenInvalidColumnNameCharactersReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name,evil", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a column name containing a comma, got nil")
	}
}

// isValidIdentifier used to accept a name starting with a digit, which most
// SQL dialects reject or misinterpret unquoted.
func TestSinkNewLeadingDigitTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "1people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a table name starting with a digit, got nil")
	}
}

// TestSinkOpenLeadingDigitColumnNameReturnsError extends the leading-digit
// check to a column name derived from the batch's Arrow schema.
func TestSinkOpenLeadingDigitColumnNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "1name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a column name starting with a digit, got nil")
	}
}

// isValidIdentifier used to accept any bare SQL reserved word as a table
// name, which most SQL dialects reject or misinterpret unquoted.
func TestSinkNewReservedWordTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "order"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a table name that is a bare SQL reserved word, got nil")
	}
}

// TestSinkOpenReservedWordColumnNameReturnsError extends the reserved-word
// check to a column name derived from the batch's Arrow schema, and checks
// that the match is case-insensitive.
func TestSinkOpenReservedWordColumnNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "Select", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a column name that is a bare SQL reserved word (case-insensitively), got nil")
	}
}

// isValidIdentifier's rune loop never fires for "", skipping every check;
// this used to generate a malformed "(, name)" column list instead of erroring.
func TestSinkOpenEmptyColumnNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for an empty column name, got nil")
	}
}

// sqlReservedWords used to omit several words reserved in at least one of
// PostgreSQL/MySQL/SQLite, wrongly accepting them as a bare table name.
func TestSinkNewAdditionalReservedWordTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	for _, word := range []string{"primary", "key", "limit", "user", "case", "join", "like", "exists"} {
		err := run(t, sqlsink.New(db, word), etl.NewBatch(rec))
		if err == nil {
			t.Fatalf("want an error for a table name that is the bare SQL reserved word %q, got nil", word)
		}
	}
}

// More sqlReservedWords gaps, same as
// TestSinkNewAdditionalReservedWordTableNameReturnsError.
func TestSinkNewMoreAdditionalReservedWordTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	for _, word := range []string{"begin", "commit", "rollback", "transaction", "foreign", "having", "grant", "revoke", "with"} {
		err := run(t, sqlsink.New(db, word), etl.NewBatch(rec))
		if err == nil {
			t.Fatalf("want an error for a table name that is the bare SQL reserved word %q, got nil", word)
		}
	}
}

// open used to build columns straight from schema.Fields() with no
// duplicate check, generating an invalid "(id, id)" column list instead of
// erroring at construction.
func TestSinkOpenDuplicateColumnNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, other INTEGER)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	otherB := array.NewInt64Builder(mem)
	otherB.Append(2)
	otherArr := otherB.NewArray()
	otherB.Release()
	defer otherArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, otherArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a schema with two fields sharing a column name, got nil")
	}
}

// Unquoted identifiers match case-insensitively in SQLite/Postgres/MySQL, so
// "id" and "ID" are just as much a duplicate as two fields both named "id".
func TestSinkOpenDuplicateColumnNameDifferentCaseReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, other INTEGER)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "ID", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	otherB := array.NewInt64Builder(mem)
	otherB.Append(2)
	otherArr := otherB.NewArray()
	otherB.Release()
	defer otherArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, otherArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a schema with two fields sharing a column name differing only in case, got nil")
	}
}

// PostgreSQL's quoted identifiers are case-sensitive, so once
// WithIdentifierQuote is set, names differing only in case (unlike
// TestSinkOpenDuplicateColumnNameDifferentCaseReturnsError) are legitimately
// distinct and must not be rejected as duplicates.
//
// SQLite itself folds column-name case at CREATE TABLE time regardless of
// quoting, so it can't actually hold "Name" and "name" as separate physical
// columns the way PostgreSQL can — the table below has only a lowercase
// "name" column. This test only confirms open's duplicate check doesn't
// block Consume from reaching ExecContext; it doesn't rely on SQLite
// treating the columns as truly distinct.
func TestSinkOpenWithIdentifierQuoteAllowsDuplicateColumnNameDifferentCase(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	upperB := array.NewStringBuilder(mem)
	upperB.Append("A")
	upperArr := upperB.NewArray()
	upperB.Release()
	defer upperArr.Release()
	lowerB := array.NewStringBuilder(mem)
	lowerB.Append("b")
	lowerArr := lowerB.NewArray()
	lowerB.Release()
	defer lowerArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, upperArr, lowerArr}, 1)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithIdentifierQuote(func(name string) string {
		return `"` + name + `"`
	}))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatalf("want two column names differing only in case to construct and insert successfully when quoted, got error: %v", err)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, name FROM people`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows=%d, want 1", count)
	}
}

// SQLite doesn't share Postgres/MySQL's multi-schema semantics, so this runs
// zero batches — only New's validation (surfaced from Finish) is under test.
func TestSinkNewSchemaQualifiedTableNameConstructsSuccessfully(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	if err := run(t, sqlsink.New(db, "analytics.orders")); err != nil {
		t.Fatalf("want a schema-qualified table name to construct successfully, got error: %v", err)
	}
}

// isValidTableIdentifier used to apply the reserved-word check only to the
// final (table) segment of a schema-qualified name, wrongly accepting a
// bare-reserved-word schema segment like "select" and generating SQL that
// fails to parse. Every segment is checked now, unquoted or not.
func TestSinkNewReservedWordSchemaSegmentReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	if err := run(t, sqlsink.New(db, "select.orders")); err == nil {
		t.Fatal("want an error for a schema-qualified table name with a reserved-word schema segment, got nil")
	}
}

// TestSinkNewReservedWordTableSegmentReturnsError checks that a reserved
// word in the final (table) segment of a schema-qualified name is rejected,
// same as the schema segment (TestSinkNewReservedWordSchemaSegmentReturnsError).
func TestSinkNewReservedWordTableSegmentReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	err := run(t, sqlsink.New(db, "analytics.order"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a schema-qualified table name whose table segment is a bare SQL reserved word, got nil")
	}
}

// TestSinkNewInvalidSchemaQualifiedTableNameReturnsError covers the invalid
// forms a schema-qualified table name can take: more than one ".", an empty
// segment on either side of a lone ".", and an invalid character in either
// segment.
func TestSinkNewInvalidSchemaQualifiedTableNameReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	names := []string{
		"a.b.c",                               // more than one "."
		".orders",                             // empty schema segment
		"analytics.",                          // empty table segment
		"1analytics.orders",                   // leading digit in schema segment
		"analytics.orders; DROP TABLE people", // invalid character in table segment
	}
	for _, name := range names {
		if err := run(t, sqlsink.New(db, name)); err == nil {
			t.Errorf("want an error for invalid schema-qualified table name %q, got nil", name)
		}
	}
}

// Quoting is exactly what makes an otherwise-invalid reserved-word name valid SQL.
func TestSinkNewWithIdentifierQuoteAllowsReservedWordTableName(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE "order" (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "order", sqlsink.WithIdentifierQuote(func(name string) string {
		return `"` + name + `"`
	}))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatalf("want a reserved-word table name to construct and insert successfully when quoted, got error: %v", err)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, name FROM "order"`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows=%d, want 1", count)
	}
}

// WithIdentifierQuote bypasses isValidTableIdentifier's other checks (see its
// doc comment) but must not also bypass the segment-count cap, or
// quotedTable would silently produce a wrong quoted identifier for a name
// with more than one ".".
func TestSinkNewWithIdentifierQuoteRejectsMultiSegmentTableName(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	sink := sqlsink.New(db, "a.b.c", sqlsink.WithIdentifierQuote(func(name string) string {
		return `"` + name + `"`
	}))
	if err := run(t, sink); err == nil {
		t.Fatal("want an error for a quoted table name with more than one \".\", got nil")
	}
}

// Same rationale as TestSinkNewWithIdentifierQuoteRejectsMultiSegmentTableName:
// quoting bypasses the per-segment character check, so hasValidTableSegments
// must itself reject an empty segment instead of letting quotedTable produce
// a malformed identifier later.
func TestSinkNewWithIdentifierQuoteRejectsEmptyTableSegment(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	names := []string{".orders", "orders."}
	for _, name := range names {
		sink := sqlsink.New(db, name, sqlsink.WithIdentifierQuote(func(id string) string {
			return `"` + id + `"`
		}))
		if err := run(t, sink); err == nil {
			t.Errorf("want an error for a quoted table name with an empty segment %q, got nil", name)
		}
	}
}

// A nil quote is not treated as "use the default unquoted checks" — it's
// itself a construction error.
func TestSinkWithIdentifierQuoteNilIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	sink := sqlsink.New(db, "people", sqlsink.WithIdentifierQuote(nil))
	if err := run(t, sink); err == nil {
		t.Fatal("want an error for WithIdentifierQuote(nil), got nil")
	}
}

// Column counterpart of TestSinkNewWithIdentifierQuoteAllowsReservedWordTableName.
func TestSinkOpenWithIdentifierQuoteAllowsReservedWordColumnName(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, "order" TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "order", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	orderB := array.NewStringBuilder(mem)
	orderB.Append("first")
	orderArr := orderB.NewArray()
	orderB.Release()
	defer orderArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, orderArr}, 1)
	defer rec.Release()

	sink := sqlsink.New(db, "widgets", sqlsink.WithIdentifierQuote(func(name string) string {
		return `"` + name + `"`
	}))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatalf("want a reserved-word column name to construct and insert successfully when quoted, got error: %v", err)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, "order" FROM widgets`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("rows=%d, want 1", count)
	}
}

// WithIdentifierQuote skips the character-set/reserved-word checks, but an
// empty name isn't a legitimate identifier under any quoting scheme.
func TestSinkOpenWithIdentifierQuoteStillRejectsEmptyColumnName(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithIdentifierQuote(func(name string) string {
		return `"` + name + `"`
	}))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for an empty column name even with WithIdentifierQuote set, got nil")
	}
}

// Unlike a table name, a column name can't be schema-qualified in an INSERT
// column list, so a "." must still be rejected.
func TestSinkOpenColumnNameWithDotReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "schema.name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	nameB := array.NewStringBuilder(mem)
	nameB.Append("a")
	nameArr := nameB.NewArray()
	nameB.Release()
	defer nameArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, nameArr}, 1)
	defer rec.Release()

	err := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a column name containing a dot, got nil")
	}
}

// run's pipeline helper can't inject a pre-canceled context, so this calls
// Consume directly.
func TestSinkConsumeWithCanceledContextReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2}, []*string{strPtr("a"), strPtr("b")})
	defer rec.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	sink := sqlsink.New(db, "people")
	err := sink.Consume(ctx, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a canceled context, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (no rows should land when the context is already canceled)", len(got))
	}
}

// A Sink must not be reusable across multiple pipeline runs, even sequentially
// after Finish has already been called on it.
func TestSinkConsumeAfterFinishReturnsError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people")
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	rec2 := idNameRecord(t, []int64{2}, []*string{strPtr("b")})
	defer rec2.Release()

	err := sink.Consume(context.Background(), etl.NewBatch(rec2))
	if err == nil {
		t.Fatal("want an error for Consume called after Finish, got nil")
	}

	got := queryPeople(t, db)
	if len(got) != 1 {
		t.Fatalf("rows=%d, want 1 (the second Consume call must not insert)", len(got))
	}
}

// --- fake database/sql/driver, scoped to the rollback-failure test below ---
//
// modernc.org/sqlite can't be made to fail a real ROLLBACK on demand, so
// exercising insert's errors.Join branch needs a driver whose insert and
// rollback failures are both independently controllable.

var (
	errRollbackFailInsert   = errors.New("sqlsinkrollbackfailfake: insert failed")
	errRollbackFailRollback = errors.New("sqlsinkrollbackfailfake: rollback failed")
)

type rollbackFailDriver struct{}

func (rollbackFailDriver) Open(string) (driver.Conn, error) {
	return rollbackFailConn{}, nil
}

func init() {
	sql.Register("sqlsinkrollbackfailfake", rollbackFailDriver{})
}

// rollbackFailConn always fails statement execution and rollback, reaching
// insert's errors.Join branch on the very first statement.
type rollbackFailConn struct{}

func (rollbackFailConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("sqlsinkrollbackfailfake: Prepare not supported, use ExecContext")
}
func (rollbackFailConn) Close() error { return nil }
func (rollbackFailConn) Begin() (driver.Tx, error) {
	return rollbackFailTx{}, nil
}

func (rollbackFailConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errRollbackFailInsert
}

var _ driver.ExecerContext = rollbackFailConn{}

type rollbackFailTx struct{}

func (rollbackFailTx) Commit() error {
	return errors.New("sqlsinkrollbackfailfake: commit should not be called")
}
func (rollbackFailTx) Rollback() error { return errRollbackFailRollback }

// The returned error must preserve both the insert and rollback failures,
// not just one of them.
func TestSinkInsertJoinsInsertAndRollbackErrorsWhenBothFail(t *testing.T) {
	db, err := sql.Open("sqlsinkrollbackfailfake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	gotErr := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if gotErr == nil {
		t.Fatal("want an error when both the insert and the subsequent rollback fail, got nil")
	}
	if !errors.Is(gotErr, errRollbackFailInsert) {
		t.Errorf("err = %v, want it to wrap the insert error", gotErr)
	}
	if !errors.Is(gotErr, errRollbackFailRollback) {
		t.Errorf("err = %v, want it to wrap the rollback error", gotErr)
	}
}

// --- fake database/sql/driver, scoped to the mid-batch cancellation test
// below ---
//
// Wraps modernc.org/sqlite's real driver instead of faking a connection
// outright: the first chunk must really land (and be rolled back) for the
// assertion below to mean anything. Only the second ExecContext call is
// intercepted, canceling the context instead of executing.

// cancelSecondExecDriver wraps a real modernc.org/sqlite driver.Driver,
// returning connections wrapped by cancelSecondExecConn.
type cancelSecondExecDriver struct {
	inner  driver.Driver
	execN  *int32
	cancel context.CancelFunc
}

func (d cancelSecondExecDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &cancelSecondExecConn{Conn: conn, execN: d.execN, cancel: d.cancel}, nil
}

// cancelSecondExecConn wraps a real sqlite driver.Conn, forwarding
// BeginTx/ExecContext/Prepare (what Sink relies on). The second ExecContext
// call — via either this conn directly or a prepared statement it returns —
// cancels and fails instead of forwarding: insert may reuse a prepared
// statement across chunks, so a fake that only intercepted the conn's own
// ExecContext would miss those calls.
type cancelSecondExecConn struct {
	driver.Conn
	execN  *int32
	cancel context.CancelFunc
}

func (c *cancelSecondExecConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

func (c *cancelSecondExecConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if atomic.AddInt32(c.execN, 1) == 2 {
		c.cancel()
		return nil, context.Canceled
	}
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("sqlsinkcancelfake: underlying conn does not support ExecerContext")
	}
	return ec.ExecContext(ctx, query, args)
}

// Wraps the returned Stmt too, sharing execN/cancel — otherwise a prepared
// statement's ExecContext would bypass this fake and always succeed.
func (c *cancelSecondExecConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &cancelSecondExecStmt{Stmt: stmt, execN: c.execN, cancel: c.cancel}, nil
}

// cancelSecondExecStmt mirrors cancelSecondExecConn: the second ExecContext
// call across either type cancels and fails, regardless of path.
type cancelSecondExecStmt struct {
	driver.Stmt
	execN  *int32
	cancel context.CancelFunc
}

func (s *cancelSecondExecStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if atomic.AddInt32(s.execN, 1) == 2 {
		s.cancel()
		return nil, context.Canceled
	}
	ec, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New("sqlsinkcancelfake: underlying stmt does not support StmtExecContext")
	}
	return ec.ExecContext(ctx, args)
}

var (
	_ driver.ConnBeginTx     = (*cancelSecondExecConn)(nil)
	_ driver.ExecerContext   = (*cancelSecondExecConn)(nil)
	_ driver.StmtExecContext = (*cancelSecondExecStmt)(nil)
)

// cancelFakeDriverSeq gives every call to registerCancelSecondExecDriver a
// fresh sql.Register name: sql.Register panics on a duplicate, and this
// driver needs fresh execN/cancel state per test run (e.g. under -count=2).
var cancelFakeDriverSeq int32

func registerCancelSecondExecDriver(cancel context.CancelFunc) string {
	name := fmt.Sprintf("sqlsinkcancelfake-%d", atomic.AddInt32(&cancelFakeDriverSeq, 1))
	sql.Register(name, cancelSecondExecDriver{inner: &msqlite.Driver{}, execN: new(int32), cancel: cancel})
	return name
}

// Unlike TestSinkConsumeWithCanceledContextReturnsError (canceled before
// Consume runs), here the fake driver cancels right after the first chunk's
// ExecContext succeeds but before the second's — the whole transaction,
// including the first chunk's already-landed rows, must still roll back.
func TestSinkConsumeContextCanceledBetweenChunksRollsBackAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	driverName := registerCancelSecondExecDriver(cancel)
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// WithMaxPlaceholders(2) caps each statement at one (id, name) row, so
	// this 2-row batch requires exactly two INSERT statements/chunks.
	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(2))
	rec := idNameRecord(t, []int64{1, 2}, []*string{strPtr("a"), strPtr("b")})
	defer rec.Release()

	gotErr := sink.Consume(ctx, etl.NewBatch(rec))
	if gotErr == nil {
		t.Fatal("want an error when the context is canceled between chunks, got nil")
	}
	if !errors.Is(gotErr, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", gotErr)
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (the whole transaction must roll back, including the first chunk's rows that already landed)", len(got))
	}
}

// --- fake database/sql/driver, scoped to the statement-reuse tests below ---
//
// insert prepares a chunk shape's statement once via tx.PrepareContext and
// reuses it across every chunk of that shape. This wraps a real sqlite driver
// (so rows really land) purely to count Prepare calls versus conn-level
// versus statement-level ExecContext calls.

// countingPrepareDriver wraps a real modernc.org/sqlite driver.Driver,
// returning connections wrapped by countingPrepareConn.
type countingPrepareDriver struct {
	inner        driver.Driver
	prepareCalls *int32
	connExecs    *int32
	stmtExecs    *int32
}

func (d countingPrepareDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &countingPrepareConn{
		Conn:         conn,
		prepareCalls: d.prepareCalls,
		connExecs:    d.connExecs,
		stmtExecs:    d.stmtExecs,
	}, nil
}

// countingPrepareConn wraps a real sqlite driver.Conn. ExecContext counts a
// direct, un-prepared execution; Prepare counts a prepare call and wraps the
// resulting statement so its reused executions are counted separately.
type countingPrepareConn struct {
	driver.Conn
	prepareCalls *int32
	connExecs    *int32
	stmtExecs    *int32
}

func (c *countingPrepareConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

func (c *countingPrepareConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	atomic.AddInt32(c.connExecs, 1)
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("sqlsinkcountingfake: underlying conn does not support ExecerContext")
	}
	return ec.ExecContext(ctx, query, args)
}

func (c *countingPrepareConn) Prepare(query string) (driver.Stmt, error) {
	atomic.AddInt32(c.prepareCalls, 1)
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &countingPrepareStmt{Stmt: stmt, stmtExecs: c.stmtExecs}, nil
}

var (
	_ driver.ConnBeginTx   = (*countingPrepareConn)(nil)
	_ driver.ExecerContext = (*countingPrepareConn)(nil)
)

// countingPrepareStmt counts each ExecContext call — one per chunk reusing
// this prepared statement.
type countingPrepareStmt struct {
	driver.Stmt
	stmtExecs *int32
}

func (s *countingPrepareStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	atomic.AddInt32(s.stmtExecs, 1)
	ec, ok := s.Stmt.(driver.StmtExecContext)
	if !ok {
		return nil, errors.New("sqlsinkcountingfake: underlying stmt does not support StmtExecContext")
	}
	return ec.ExecContext(ctx, args)
}

var _ driver.StmtExecContext = (*countingPrepareStmt)(nil)

// countingFakeDriverSeq gives every call to registerCountingPrepareDriver a
// fresh sql.Register name, same reason as cancelFakeDriverSeq above.
var countingFakeDriverSeq int32

func registerCountingPrepareDriver() (name string, prepareCalls, connExecs, stmtExecs *int32) {
	prepareCalls, connExecs, stmtExecs = new(int32), new(int32), new(int32)
	name = fmt.Sprintf("sqlsinkcountingfake-%d", atomic.AddInt32(&countingFakeDriverSeq, 1))
	sql.Register(name, countingPrepareDriver{
		inner:        &msqlite.Driver{},
		prepareCalls: prepareCalls,
		connExecs:    connExecs,
		stmtExecs:    stmtExecs,
	})
	return name, prepareCalls, connExecs, stmtExecs
}

// Must prepare the shared shape's statement exactly once and reuse it via
// the resulting *sql.Stmt, not re-issue an identical-shape string per chunk.
func TestSinkChunksOfUniformShapeReuseOnePreparedStatement(t *testing.T) {
	driverName, prepareCalls, connExecs, stmtExecs := registerCountingPrepareDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so this 10-row
	// batch splits into exactly 5 same-shape chunks and no remainder.
	ids := make([]int64, 10)
	names := make([]*string, 10)
	for i := range ids {
		ids[i] = int64(i + 1)
		names[i] = strPtr("n")
	}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	// Snapshot before queryPeople's own Prepare call pollutes the shared counters.
	gotPrepareCalls := atomic.LoadInt32(prepareCalls)
	gotStmtExecs := atomic.LoadInt32(stmtExecs)
	gotConnExecs := atomic.LoadInt32(connExecs)

	got := queryPeople(t, db)
	if len(got) != 10 {
		t.Fatalf("rows=%d, want 10", len(got))
	}
	if gotPrepareCalls != 1 {
		t.Fatalf("Prepare calls = %d, want exactly 1 (the shared shape prepared once and reused)", gotPrepareCalls)
	}
	if gotStmtExecs != 5 {
		t.Fatalf("prepared statement ExecContext calls = %d, want 5 (one per chunk, reusing the one prepared statement)", gotStmtExecs)
	}
	// 1, not 0: the CREATE TABLE statement above also goes through this
	// same connection's ExecContext, un-prepared, before insert ever runs.
	if gotConnExecs != 1 {
		t.Fatalf("un-prepared connection ExecContext calls = %d, want 1 (only the CREATE TABLE setup call; every insert chunk should have reused the prepared statement)", gotConnExecs)
	}
}

// The shorter final remainder chunk has a different shape and, used only
// once, executes directly via tx.ExecContext instead of being prepared.
func TestSinkChunksWithRemainderPrepareOnlyForTheUniformShape(t *testing.T) {
	driverName, prepareCalls, connExecs, stmtExecs := registerCountingPrepareDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so this 9-row
	// batch splits into 4 same-shape (2-row) chunks plus a 1-row remainder.
	ids := make([]int64, 9)
	names := make([]*string, 9)
	for i := range ids {
		ids[i] = int64(i + 1)
		names[i] = strPtr("n")
	}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	// Same snapshot-before-queryPeople reasoning as
	// TestSinkChunksOfUniformShapeReuseOnePreparedStatement.
	gotPrepareCalls := atomic.LoadInt32(prepareCalls)
	gotStmtExecs := atomic.LoadInt32(stmtExecs)
	gotConnExecs := atomic.LoadInt32(connExecs)

	got := queryPeople(t, db)
	if len(got) != 9 {
		t.Fatalf("rows=%d, want 9", len(got))
	}
	if gotPrepareCalls != 1 {
		t.Fatalf("Prepare calls = %d, want exactly 1 (only the 4 uniform-shape full chunks share a prepared statement)", gotPrepareCalls)
	}
	if gotStmtExecs != 4 {
		t.Fatalf("prepared statement ExecContext calls = %d, want 4 (one per full chunk, reusing the one prepared statement)", gotStmtExecs)
	}
	// 2, not 1: the CREATE TABLE statement above also goes through this
	// same connection's ExecContext, un-prepared, before insert ever runs,
	// in addition to the single differently-shaped remainder chunk.
	if gotConnExecs != 2 {
		t.Fatalf("un-prepared connection ExecContext calls = %d, want 2 (the CREATE TABLE setup call plus the single differently-shaped remainder chunk)", gotConnExecs)
	}
}

// Unlike the two tests above (at least two full chunks, so a statement is
// prepared and reused), exactly one full chunk has no reuse benefit: insert's
// numFullChunks > 1 guard must leave stmt nil and execute both the chunk and
// the remainder directly via tx.ExecContext.
func TestSinkChunksExactlyOneFullChunkPlusRemainderSkipsPreparedStatement(t *testing.T) {
	driverName, prepareCalls, connExecs, stmtExecs := registerCountingPrepareDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so this 3-row
	// batch produces exactly one full chunk (rows 0-1) plus a 1-row remainder
	// (row 2): numFullChunks == 1, remainder == 1.
	ids := []int64{1, 2, 3}
	names := []*string{strPtr("a"), strPtr("b"), strPtr("c")}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	// Same snapshot-before-queryPeople reasoning as
	// TestSinkChunksOfUniformShapeReuseOnePreparedStatement.
	gotPrepareCalls := atomic.LoadInt32(prepareCalls)
	gotStmtExecs := atomic.LoadInt32(stmtExecs)
	gotConnExecs := atomic.LoadInt32(connExecs)

	got := queryPeople(t, db)
	if len(got) != 3 {
		t.Fatalf("rows=%d, want 3", len(got))
	}
	if got[0].id != 1 || got[0].name.String != "a" || got[1].id != 2 || got[1].name.String != "b" || got[2].id != 3 || got[2].name.String != "c" {
		t.Fatalf("got %+v, want rows landed in order with matching names", got)
	}
	if gotPrepareCalls != 0 {
		t.Fatalf("Prepare calls = %d, want 0 (a single full chunk plus a remainder has no reuse benefit and must not be prepared)", gotPrepareCalls)
	}
	if gotStmtExecs != 0 {
		t.Fatalf("prepared statement ExecContext calls = %d, want 0 (nothing should have been prepared)", gotStmtExecs)
	}
	// 3, not 2: the CREATE TABLE setup call, plus the one full chunk and the
	// one remainder chunk, both executed directly via tx.ExecContext.
	if gotConnExecs != 3 {
		t.Fatalf("un-prepared connection ExecContext calls = %d, want 3 (CREATE TABLE, the full chunk, and the remainder chunk)", gotConnExecs)
	}
}

func TestSinkWithMaxRowsPerConsumeUnderCapSucceeds(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2, 3}, []*string{strPtr("a"), strPtr("b"), strPtr("c")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxRowsPerConsume(3))
	if err := run(t, sink, etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	got := queryPeople(t, db)
	if len(got) != 3 {
		t.Fatalf("rows=%d, want 3", len(got))
	}
}

// Must fail before any SQL executes, not after a partial write.
func TestSinkWithMaxRowsPerConsumeOverCapReturnsErrorWithoutExecutingSQL(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	rec := idNameRecord(t, []int64{1, 2, 3}, []*string{strPtr("a"), strPtr("b"), strPtr("c")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxRowsPerConsume(2))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for a batch whose row count exceeds the configured max rows per Consume, got nil")
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (no SQL should have been executed for a batch over the cap)", len(got))
	}
}

// A rejected first batch must not lock the Sink into its schema — a retry
// with a differently-shaped batch should still succeed.
func TestSinkWithMaxRowsPerConsumeOverCapFirstBatchLeavesSchemaUnset(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	overCap := idNameRecord(t, []int64{1, 2, 3}, []*string{strPtr("a"), strPtr("b"), strPtr("c")})
	defer overCap.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxRowsPerConsume(2))
	if err := sink.Consume(context.Background(), etl.NewBatch(overCap)); err == nil {
		t.Fatal("want an error for the first batch's row count exceeding the cap, got nil")
	}

	idOnlySchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(9)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	second := array.NewRecord(idOnlySchema, []arrow.Array{idArr}, 1)
	defer second.Release()

	if err := sink.Consume(context.Background(), etl.NewBatch(second)); err != nil {
		t.Fatalf("want no error for a differently-shaped, under-cap second batch after a first-batch rejection, got %v", err)
	}

	got := queryPeople(t, db)
	if len(got) != 1 || got[0].id != 9 {
		t.Fatalf("got %+v, want a single row with id=9 (from the second batch's own schema, not the rejected first batch's)", got)
	}
}

// Same schema-not-locked-in guarantee as
// TestSinkWithMaxRowsPerConsumeOverCapFirstBatchLeavesSchemaUnset, for a
// malformed (schema/record mismatch) first batch instead of an over-cap one.
func TestSinkConsumeRecordSchemaMismatchFirstBatchLeavesSchemaUnset(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)

	// schema claims two columns (id, name); record only has one (id).
	wideSchema := idNameSchema()
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	narrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	rec := array.NewRecord(narrowSchema, []arrow.Array{idArr}, 1)
	defer rec.Release()

	sink := sqlsink.New(db, "people")
	if err := sink.Consume(context.Background(), wideSchemaBatch{schema: wideSchema, record: rec}); err == nil {
		t.Fatal("want an error when the first batch's schema and record column counts disagree, got nil")
	}

	idOnlySchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	secondIDB := array.NewInt64Builder(mem)
	secondIDB.Append(9)
	secondIDArr := secondIDB.NewArray()
	secondIDB.Release()
	defer secondIDArr.Release()
	second := array.NewRecord(idOnlySchema, []arrow.Array{secondIDArr}, 1)
	defer second.Release()

	if err := sink.Consume(context.Background(), etl.NewBatch(second)); err != nil {
		t.Fatalf("want no error for a differently-shaped, well-formed second batch after a first-batch rejection, got %v", err)
	}

	got := queryPeople(t, db)
	if len(got) != 1 || got[0].id != 9 {
		t.Fatalf("got %+v, want a single row with id=9 (from the second batch's own schema, not the rejected first batch's)", got)
	}
}

// TestSinkWithMaxRowsPerConsumeNonPositiveValueIsConstructionError mirrors
// TestSinkWithMaxPlaceholdersNonPositiveValueIsConstructionError:
// WithMaxRowsPerConsume's documented non-positive-n behavior is a
// construction error, not "unlimited" and not silently ignored.
func TestSinkWithMaxRowsPerConsumeNonPositiveValueIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	for _, n := range []int{0, -1} {
		sink := sqlsink.New(db, "people", sqlsink.WithMaxRowsPerConsume(n))
		err := run(t, sink, etl.NewBatch(rec))
		if err == nil {
			t.Fatalf("want an error for WithMaxRowsPerConsume(%d), got nil", n)
		}
	}
}

func TestSinkWithAdditionalReservedWordsRejectsCallerSuppliedWord(t *testing.T) {
	const word = "widget" // not in the built-in sqlReservedWords list

	db := openTestDB(t, `CREATE TABLE widget (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	// Without the option, the word is an ordinary, valid table name.
	if err := run(t, sqlsink.New(db, word), etl.NewBatch(rec)); err != nil {
		t.Fatalf("word %q should be a valid table name without WithAdditionalReservedWords, got error: %v", word, err)
	}

	// With the option (mixed case, to also check the case-insensitive fold),
	// the same word must now be rejected.
	sink := sqlsink.New(db, word, sqlsink.WithAdditionalReservedWords("Widget"))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatalf("want an error for a table name matching a word added via WithAdditionalReservedWords, got nil")
	}
}

func TestSinkWithAdditionalReservedWordsAcceptsMultipleWordsInOneCall(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	words := []string{"widget", "gadget", "gizmo"}
	for _, word := range words {
		sink := sqlsink.New(db, word, sqlsink.WithAdditionalReservedWords(words...))
		if err := run(t, sink, etl.NewBatch(rec)); err == nil {
			t.Errorf("want an error for table name %q, one of several words passed to WithAdditionalReservedWords in one call, got nil", word)
		}
	}
}

// An empty string is itself a construction error, not silently ignored or
// treated as a reserved word.
func TestSinkWithAdditionalReservedWordsEmptyWordIsConstructionError(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`)
	rec := idNameRecord(t, []int64{1}, []*string{strPtr("a")})
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithAdditionalReservedWords("widget", ""))
	err := run(t, sink, etl.NewBatch(rec))
	if err == nil {
		t.Fatal("want an error for WithAdditionalReservedWords called with an empty word, got nil")
	}
}

// TestSinkAllSupportedTypesRoundTrip excludes id (Int64) from its nullable
// set, so this hits int64Extractor's own IsNull branch that test never does.
func TestSinkInt64ExtractorHandlesNullDirectly(t *testing.T) {
	db := openTestDB(t, `CREATE TABLE widgets (id INTEGER PRIMARY KEY, count INTEGER)`)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	mem := memory.DefaultAllocator
	idB := array.NewInt64Builder(mem)
	idB.Append(1)
	idB.Append(2)
	idArr := idB.NewArray()
	idB.Release()
	defer idArr.Release()
	countB := array.NewInt64Builder(mem)
	countB.Append(42)
	countB.AppendNull()
	countArr := countB.NewArray()
	countB.Release()
	defer countArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, countArr}, 2)
	defer rec.Release()

	if err := run(t, sqlsink.New(db, "widgets"), etl.NewBatch(rec)); err != nil {
		t.Fatal(err)
	}

	rows, err := db.QueryContext(context.Background(), `SELECT id, count FROM widgets ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var got []struct {
		id    int64
		count sql.NullInt64
	}
	for rows.Next() {
		var row struct {
			id    int64
			count sql.NullInt64
		}
		if err := rows.Scan(&row.id, &row.count); err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	if len(got) != 2 {
		t.Fatalf("rows=%d, want 2", len(got))
	}
	if !got[0].count.Valid || got[0].count.Int64 != 42 {
		t.Fatalf("row 0 count=%+v, want 42", got[0].count)
	}
	if got[1].count.Valid {
		t.Fatalf("row 1 count=%+v, want null", got[1].count)
	}
}

// --- fake database/sql/driver, scoped to the prepare-failure test below ---
//
// Wraps a real sqlite driver.Driver: insert's tx.PrepareContext call only
// happens once numFullChunks > 1, so a batch shaped to require it needs a
// real connection to reach that call at all. Only Prepare fails; BeginTx and
// ExecContext forward to the real connection so the failure is attributable
// to Prepare specifically.

var errPrepareFailFake = errors.New("sqlsinkpreparefailfake: prepare failed")

// prepareFailDriver wraps a real modernc.org/sqlite driver.Driver, returning
// connections wrapped by prepareFailConn.
type prepareFailDriver struct {
	inner    driver.Driver
	prepareN *int32
}

func (d prepareFailDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &prepareFailConn{Conn: conn, prepareN: d.prepareN}, nil
}

// prepareFailConn fails only the first Prepare call (insert's own
// tx.PrepareContext) and forwards afterward, so a later, unrelated query
// (e.g. this test's read-back) isn't also wrongly failed.
type prepareFailConn struct {
	driver.Conn
	prepareN *int32
}

func (c *prepareFailConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		return bt.BeginTx(ctx, opts)
	}
	return c.Conn.Begin()
}

func (c *prepareFailConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("sqlsinkpreparefailfake: underlying conn does not support ExecerContext")
	}
	return ec.ExecContext(ctx, query, args)
}

func (c *prepareFailConn) Prepare(query string) (driver.Stmt, error) {
	if atomic.AddInt32(c.prepareN, 1) == 1 {
		return nil, errPrepareFailFake
	}
	return c.Conn.Prepare(query)
}

var (
	_ driver.ConnBeginTx   = (*prepareFailConn)(nil)
	_ driver.ExecerContext = (*prepareFailConn)(nil)
)

// prepareFailDriverSeq gives every call to registerPrepareFailDriver a fresh
// sql.Register name, same reason as cancelFakeDriverSeq/countingFakeDriverSeq
// above.
var prepareFailDriverSeq int32

func registerPrepareFailDriver() string {
	name := fmt.Sprintf("sqlsinkpreparefailfake-%d", atomic.AddInt32(&prepareFailDriverSeq, 1))
	sql.Register(name, prepareFailDriver{inner: &msqlite.Driver{}, prepareN: new(int32)})
	return name
}

// A failed Prepare must roll back the transaction and surface the failure,
// even though the connection is otherwise functional.
func TestSinkInsertPrepareFailureRollsBackAndReturnsError(t *testing.T) {
	driverName := registerPrepareFailDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so this 4-row
	// batch requires two full chunks (numFullChunks == 2 > 1), forcing insert
	// to call tx.PrepareContext for the shared shape.
	ids := []int64{1, 2, 3, 4}
	names := []*string{strPtr("a"), strPtr("b"), strPtr("c"), strPtr("d")}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	gotErr := run(t, sink, etl.NewBatch(rec))
	if gotErr == nil {
		t.Fatal("want an error when preparing the shared full-chunk statement fails, got nil")
	}
	if !errors.Is(gotErr, errPrepareFailFake) {
		t.Errorf("err = %v, want it to wrap the prepare failure", gotErr)
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (a failed prepare must roll back the transaction and leave nothing written)", len(got))
	}
}

// --- fake database/sql/driver, scoped to the commit-failure tests below ---
//
// modernc.org/sqlite can't be made to fail a real COMMIT on demand. This
// wraps a real driver so statements really execute; only the returned Tx's
// Commit is faked, and it rolls the real transaction back before returning
// its error — modeling a driver that leaves nothing committed on a failed
// COMMIT, matching Consume's all-or-nothing contract.

var errCommitFailFake = errors.New("sqlsinkcommitfailfake: commit failed")

// commitFailDriver wraps a real modernc.org/sqlite driver.Driver, returning
// connections wrapped by commitFailConn.
type commitFailDriver struct {
	inner driver.Driver
}

func (d commitFailDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return &commitFailConn{Conn: conn}, nil
}

// commitFailConn wraps a real sqlite driver.Conn; BeginTx wraps the returned
// Tx in commitFailTx so every transaction's Commit fails.
type commitFailConn struct {
	driver.Conn
}

func (c *commitFailConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var (
		tx  driver.Tx
		err error
	)
	if bt, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = bt.BeginTx(ctx, opts)
	} else {
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, err
	}
	return &commitFailTx{Tx: tx}, nil
}

func (c *commitFailConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	ec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("sqlsinkcommitfailfake: underlying conn does not support ExecerContext")
	}
	return ec.ExecContext(ctx, query, args)
}

var (
	_ driver.ConnBeginTx   = (*commitFailConn)(nil)
	_ driver.ExecerContext = (*commitFailConn)(nil)
)

// commitFailTx's Commit rolls the real transaction back (undoing whatever
// was written) and returns errCommitFailFake instead of forwarding.
type commitFailTx struct {
	driver.Tx
}

func (t *commitFailTx) Commit() error {
	_ = t.Tx.Rollback()
	return errCommitFailFake
}

// commitFailDriverSeq gives every call to registerCommitFailDriver a fresh
// sql.Register name, same reason as the other fake drivers' own sequence
// counters above.
var commitFailDriverSeq int32

func registerCommitFailDriver() string {
	name := fmt.Sprintf("sqlsinkcommitfailfake-%d", atomic.AddInt32(&commitFailDriverSeq, 1))
	sql.Register(name, commitFailDriver{inner: &msqlite.Driver{}})
	return name
}

// Exercises insert's single-chunk (totalChunks <= 1) commit-failure path: a
// failed commit must error and leave nothing written even though the
// statement executed successfully.
func TestSinkInsertSingleChunkCommitFailureReturnsError(t *testing.T) {
	driverName := registerCommitFailDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// Small enough to stay a single chunk (well under the default max
	// placeholders), so this exercises insert's totalChunks <= 1 branch.
	rec := idNameRecord(t, []int64{1, 2}, []*string{strPtr("a"), strPtr("b")})
	defer rec.Release()

	gotErr := run(t, sqlsink.New(db, "people"), etl.NewBatch(rec))
	if gotErr == nil {
		t.Fatal("want an error when the single-chunk transaction's commit fails, got nil")
	}
	if !errors.Is(gotErr, errCommitFailFake) {
		t.Errorf("err = %v, want it to wrap the commit failure", gotErr)
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (a failed commit must leave nothing written)", len(got))
	}
}

// Multi-statement counterpart of TestSinkInsertSingleChunkCommitFailureReturnsError:
// every chunk executes successfully before the final Commit fails, and the
// whole transaction — including chunks that already ran — must still roll back.
func TestSinkInsertMultiChunkCommitFailureReturnsErrorAfterAllChunksExecuted(t *testing.T) {
	driverName := registerCommitFailDriver()
	db, err := sql.Open(driverName, "file:"+driverName+"?mode=memory&cache=shared&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), `CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	// 2 columns * maxPlaceholders(4) => 2 rows per statement, so this 10-row
	// batch splits into 5 same-shape chunks sharing one prepared statement,
	// all of which must execute successfully before Commit is ever called.
	ids := make([]int64, 10)
	names := make([]*string, 10)
	for i := range ids {
		ids[i] = int64(i + 1)
		names[i] = strPtr("n")
	}
	rec := idNameRecord(t, ids, names)
	defer rec.Release()

	sink := sqlsink.New(db, "people", sqlsink.WithMaxPlaceholders(4))
	gotErr := run(t, sink, etl.NewBatch(rec))
	if gotErr == nil {
		t.Fatal("want an error when the multi-chunk transaction's commit fails, got nil")
	}
	if !errors.Is(gotErr, errCommitFailFake) {
		t.Errorf("err = %v, want it to wrap the commit failure", gotErr)
	}

	got := queryPeople(t, db)
	if len(got) != 0 {
		t.Fatalf("rows=%d, want 0 (a failed commit must roll back every chunk that already executed)", len(got))
	}
}
