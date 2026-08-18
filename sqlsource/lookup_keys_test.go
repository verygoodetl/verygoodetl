package sqlsource_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

func TestLookupKeysSkipsNullsAndDedupes(t *testing.T) {
	schema := int64Schema("id")
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]int64{1, 2, 1, 0, 3}, []bool{true, true, true, false, true}) // 0 is null
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 5)
	defer rec.Release()

	keys, err := sqlsource.LookupKeys(etl.NewBatch(rec), 0)
	if err != nil {
		t.Fatal(err)
	}

	want := []any{int64(1), int64(2), int64(3)}
	if len(keys) != len(want) {
		t.Fatalf("keys=%v, want %v", keys, want)
	}
	for i, k := range keys {
		if k != want[i] {
			t.Fatalf("keys=%v, want %v", keys, want)
		}
	}
}

func TestLookupKeysColumnOutOfRange(t *testing.T) {
	schema := int64Schema("id")
	rec := array.NewRecord(schema, []arrow.Array{
		func() arrow.Array {
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			b.Append(1)
			return b.NewArray()
		}(),
	}, 1)
	defer rec.Release()

	if _, err := sqlsource.LookupKeys(etl.NewBatch(rec), 5); err == nil {
		t.Fatal("want error for out-of-range column")
	}
	if _, err := sqlsource.LookupKeys(etl.NewBatch(rec), -1); err == nil {
		t.Fatal("want error for negative column")
	}
}

func TestLookupKeysUnsupportedType(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "ids", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)}}, nil)
	b := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64)
	defer b.Release()
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 0)
	defer rec.Release()

	if _, err := sqlsource.LookupKeys(etl.NewBatch(rec), 0); err == nil {
		t.Fatal("want error for unsupported column type")
	}
}
