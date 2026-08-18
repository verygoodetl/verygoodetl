package sqlsource_test

import (
	"math"
	"testing"
	"time"

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

// TestLookupKeysFloatDedupeIsBitExact is a regression test: dedupe used to
// key on fmt.Sprint(v), which collapses every NaN payload to the string
// "NaN" regardless of its exact bits, silently dropping distinct float64
// values. Deduping on math.Float64bits keeps them distinct.
func TestLookupKeysFloatDedupeIsBitExact(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Float64}}, nil)
	nan1 := math.NaN()
	nan2 := math.Float64frombits(0x7FF8000000000002)

	b := array.NewFloat64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues([]float64{1.5, nan1, nan2, 1.5}, nil)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 4)
	defer rec.Release()

	keys, err := sqlsource.LookupKeys(etl.NewBatch(rec), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 {
		t.Fatalf("keys=%v, want 3 distinct values (two different NaN bit patterns must not collide, and the repeated 1.5 must dedupe)", keys)
	}
}

// TestLookupKeysBinaryDedupe checks that Binary columns dedupe on their
// exact byte content.
func TestLookupKeysBinaryDedupe(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.BinaryTypes.Binary}}, nil)
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer b.Release()
	b.Append([]byte("abc"))
	b.Append([]byte("abc"))
	b.Append([]byte("abd"))
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 3)
	defer rec.Release()

	keys, err := sqlsource.LookupKeys(etl.NewBatch(rec), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 {
		t.Fatalf("keys=%v, want 2 distinct values", keys)
	}
}

// TestLookupKeysTimestampReturnsTimeTime is a regression test: LookupKeys
// used to return the raw arrow.Timestamp epoch integer for a Timestamp
// column, which Lookup.Process then passed straight through to
// (*sql.DB).QueryContext as an integer instead of a time.Time.
func TestLookupKeysTimestampReturnsTimeTime(t *testing.T) {
	dt := &arrow.TimestampType{Unit: arrow.Microsecond}
	schema := arrow.NewSchema([]arrow.Field{{Name: "ts", Type: dt}}, nil)
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tsVal, err := arrow.TimestampFromTime(want, arrow.Microsecond)
	if err != nil {
		t.Fatal(err)
	}

	b := array.NewTimestampBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	b.Append(tsVal)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, 1)
	defer rec.Release()

	keys, err := sqlsource.LookupKeys(etl.NewBatch(rec), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Fatalf("keys=%v, want 1", keys)
	}
	got, ok := keys[0].(time.Time)
	if !ok {
		t.Fatalf("keys[0] = %#v (%T), want time.Time", keys[0], keys[0])
	}
	if !got.Equal(want) {
		t.Fatalf("keys[0] = %v, want %v", got, want)
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
