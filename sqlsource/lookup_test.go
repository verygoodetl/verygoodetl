package sqlsource_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

// fixedBatchSource emits a fixed sequence of batches, standing in for
// whatever upstream Source (e.g. a sqlsource.Source against a first
// database) actually produced them.
type fixedBatchSource struct {
	batches []etl.Batch
}

func (s fixedBatchSource) Run(ctx context.Context, out etl.Output) error {
	for _, b := range s.batches {
		if err := out.Send(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

func int64Batch(t *testing.T, schema *arrow.Schema, values ...int64) etl.Batch {
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

// idsGenerator builds "SELECT id, status FROM shipments WHERE id IN (?,...)"
// from the Int64 id column (column 0) of the incoming batch, using
// sqlsource.LookupKeys rather than hand-rolling the extraction.
func idsGenerator(b etl.Batch) (string, []any, error) {
	args, err := sqlsource.LookupKeys(b, 0)
	if err != nil {
		return "", nil, err
	}
	placeholders := make([]string, len(args))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("SELECT id, status FROM shipments WHERE id IN (%s)", strings.Join(placeholders, ","))
	return query, args, nil
}

func TestLookupDynamicQueryCorrectness(t *testing.T) {
	upstreamSchema := int64Schema("id")
	resultSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.BinaryTypes.String},
	}, nil)

	f := &fixture{
		columns: []string{"id", "status"},
		rows: [][]driver.Value{
			{int64(1), "delivered"},
			{int64(2), "in_transit"},
		},
	}
	dsn := registerFixture(t, f)
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	lookup, err := sqlsource.NewLookup(db, idsGenerator, resultSchema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	sink := &countingSink{}
	p.From(fixedBatchSource{batches: []etl.Batch{int64Batch(t, upstreamSchema, 1, 2)}}).
		Process(lookup).To(sink)
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	gotQuery, gotArgs := f.lastCall()
	wantQuery := "SELECT id, status FROM shipments WHERE id IN (?,?)"
	if gotQuery != wantQuery {
		t.Fatalf("query=%q, want %q", gotQuery, wantQuery)
	}
	if len(gotArgs) != 2 || gotArgs[0].Value != int64(1) || gotArgs[1].Value != int64(2) {
		t.Fatalf("args=%v, want [1 2]", gotArgs)
	}

	if len(sink.rows) != 2 {
		t.Fatalf("rows=%d, want 2", len(sink.rows))
	}
	if sink.rows[0][0] != int64(1) || sink.rows[0][1] != "delivered" {
		t.Fatalf("row 0 = %v", sink.rows[0])
	}
	if sink.rows[1][0] != int64(2) || sink.rows[1][1] != "in_transit" {
		t.Fatalf("row 1 = %v", sink.rows[1])
	}
}

func TestLookupSkipsZeroRowBatch(t *testing.T) {
	upstreamSchema := int64Schema("id")
	resultSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	dsn := registerFixture(t, &fixture{columns: []string{"id"}})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	called := false
	generate := func(b etl.Batch) (string, []any, error) {
		called = true
		return "SELECT id FROM shipments", nil, nil
	}

	lookup, err := sqlsource.NewLookup(db, generate, resultSchema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	sink := &countingSink{}
	// intBatch with zero values still produces a batch with 0 rows.
	p.From(fixedBatchSource{batches: []etl.Batch{int64Batch(t, upstreamSchema)}}).
		Process(lookup).To(sink)
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if called {
		t.Fatal("want generate not called for a zero-row batch")
	}
	if len(sink.batches) != 0 {
		t.Fatalf("batches=%v, want none", sink.batches)
	}
}

func TestLookupSkipsWhenGeneratorReturnsZeroArgs(t *testing.T) {
	upstreamSchema := int64Schema("id")
	resultSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	f := &fixture{columns: []string{"id"}}
	dsn := registerFixture(t, f)
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Simulates a generator that filters every row out of a non-empty
	// batch (e.g. goetl's "skip Multiparcel shipments" pattern) and would
	// otherwise return a query with no args to bind.
	generate := func(b etl.Batch) (string, []any, error) {
		return "SELECT id FROM shipments WHERE id IN ()", nil, nil
	}

	lookup, err := sqlsource.NewLookup(db, generate, resultSchema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	sink := &countingSink{}
	p.From(fixedBatchSource{batches: []etl.Batch{int64Batch(t, upstreamSchema, 1, 2)}}).
		Process(lookup).To(sink)
	if err := p.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	if gotQuery, _ := f.lastCall(); gotQuery != "" {
		t.Fatalf("want no query run when generate returns zero args, got %q", gotQuery)
	}
	if len(sink.batches) != 0 {
		t.Fatalf("batches=%v, want none", sink.batches)
	}
}

func TestLookupGeneratorError(t *testing.T) {
	upstreamSchema := int64Schema("id")
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	dsn := registerFixture(t, &fixture{columns: []string{"id"}})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	wantErr := errors.New("bad generator")
	generate := func(b etl.Batch) (string, []any, error) { return "", nil, wantErr }

	lookup, err := sqlsource.NewLookup(db, generate, resultSchema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	p.From(fixedBatchSource{batches: []etl.Batch{int64Batch(t, upstreamSchema, 1)}}).
		Process(lookup).To(&countingSink{})
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error from a failing generator")
	}
}

func TestLookupQueryError(t *testing.T) {
	upstreamSchema := int64Schema("id")
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	dsn := registerFixture(t, &fixture{
		columns:  []string{"id"},
		queryErr: errors.New("connection refused"),
	})
	db, err := sql.Open("sqlsourcefake", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	generate := func(b etl.Batch) (string, []any, error) {
		return "SELECT id FROM shipments WHERE id = ?", []any{int64(1)}, nil
	}

	lookup, err := sqlsource.NewLookup(db, generate, resultSchema)
	if err != nil {
		t.Fatal(err)
	}

	p := etl.New()
	p.From(fixedBatchSource{batches: []etl.Batch{int64Batch(t, upstreamSchema, 1)}}).
		Process(lookup).To(&countingSink{})
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("want error propagated from a failing query")
	}
}

func TestLookupNilArguments(t *testing.T) {
	resultSchema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	db, err := sql.Open("sqlsourcefake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	generate := func(b etl.Batch) (string, []any, error) { return "", nil, nil }

	if _, err := sqlsource.NewLookup(nil, generate, resultSchema); err == nil {
		t.Fatal("want error constructing Lookup with a nil db")
	}
	if _, err := sqlsource.NewLookup(db, nil, resultSchema); err == nil {
		t.Fatal("want error constructing Lookup with a nil generate")
	}
	if _, err := sqlsource.NewLookup(db, generate, nil); err == nil {
		t.Fatal("want error constructing Lookup with a nil schema")
	}
}

func TestLookupUnsupportedSchemaType(t *testing.T) {
	resultSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	}, nil)

	db, err := sql.Open("sqlsourcefake", "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	generate := func(b etl.Batch) (string, []any, error) { return "", nil, nil }
	if _, err := sqlsource.NewLookup(db, generate, resultSchema); err == nil {
		t.Fatal("want error constructing Lookup with an unsupported schema field type")
	}
}
