package sqlsource

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	etl "github.com/verygoodetl/verygoodetl"
)

// LookupKeys extracts the non-null values from column i of batch's Record,
// with duplicates removed (first occurrence kept) — ready to use as the
// args for an "IN (...)" clause built inside a QueryGenerator. Supports the
// same column types as Source/Lookup schemas (Int64, Float64, Boolean,
// String, Binary, Timestamp).
//
// Building the placeholder string for those args (e.g. "?,?,?" or
// "$1,$2,$3") in your database's dialect is still your job, consistent
// with a query's SQL text always being caller-authored.
func LookupKeys(batch etl.Batch, column int) ([]any, error) {
	rec := batch.Record()
	if column < 0 || column >= int(rec.NumCols()) {
		return nil, fmt.Errorf("sqlsource: column %d out of range (record has %d columns)", column, rec.NumCols())
	}

	col := rec.Column(column)
	extract, err := valueExtractorFor(col.DataType())
	if err != nil {
		return nil, fmt.Errorf("sqlsource: column %d: %w", column, err)
	}

	seen := make(map[string]struct{})
	var keys []any
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := extract(col, i)
		k := fmt.Sprint(v)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, v)
	}
	return keys, nil
}

func valueExtractorFor(dt arrow.DataType) (func(arrow.Array, int) any, error) {
	switch dt.ID() {
	case arrow.INT64:
		return func(a arrow.Array, i int) any { return a.(*array.Int64).Value(i) }, nil
	case arrow.FLOAT64:
		return func(a arrow.Array, i int) any { return a.(*array.Float64).Value(i) }, nil
	case arrow.BOOL:
		return func(a arrow.Array, i int) any { return a.(*array.Boolean).Value(i) }, nil
	case arrow.STRING:
		return func(a arrow.Array, i int) any { return a.(*array.String).Value(i) }, nil
	case arrow.BINARY:
		return func(a arrow.Array, i int) any { return a.(*array.Binary).Value(i) }, nil
	case arrow.TIMESTAMP:
		return func(a arrow.Array, i int) any { return a.(*array.Timestamp).Value(i) }, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}
