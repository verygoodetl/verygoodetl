package sqlsource

import (
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	etl "github.com/verygoodetl/verygoodetl"
)

// LookupKeys extracts the non-null values from column i of batch's Record,
// with duplicates removed (first occurrence kept) — ready to use as the
// args for an "IN (...)" clause built inside a QueryGenerator. Supports the
// same column types as Source/Lookup schemas (Int64, Float64, Boolean,
// String, Binary, Timestamp). A Timestamp column's values are returned as
// time.Time (converted using the column's declared unit), not as the
// underlying arrow.Timestamp epoch integer, so they can be passed straight
// to (*sql.DB).QueryContext like any other time.Time argument.
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

	seen := make(map[any]struct{})
	var keys []any
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		key, arg := extract(col, i)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, arg)
	}
	return keys, nil
}

// valueExtractorFor returns a function that, given column a and row i,
// returns two values: a dedupe key with an exact, type-specific identity
// (safe as a map key — e.g. float64 bits rather than a formatted string, so
// distinct values, including distinct NaN payloads, are never treated as
// duplicates), and the arg value to hand back to the caller as a query
// parameter (e.g. a time.Time for a Timestamp column, so it round-trips
// through database/sql the same way a time.Time query arg from any other
// source would, rather than as a raw epoch integer).
func valueExtractorFor(dt arrow.DataType) (func(arrow.Array, int) (key, arg any), error) {
	switch dt.ID() {
	case arrow.INT64:
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.Int64).Value(i)
			return v, v
		}, nil
	case arrow.FLOAT64:
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.Float64).Value(i)
			return math.Float64bits(v), v
		}, nil
	case arrow.BOOL:
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.Boolean).Value(i)
			return v, v
		}, nil
	case arrow.STRING:
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.String).Value(i)
			return v, v
		}, nil
	case arrow.BINARY:
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.Binary).Value(i)
			return string(v), v
		}, nil
	case arrow.TIMESTAMP:
		ts, ok := dt.(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("expected *arrow.TimestampType for %s, got %T", dt, dt)
		}
		unit := ts.Unit
		return func(a arrow.Array, i int) (any, any) {
			v := a.(*array.Timestamp).Value(i)
			return int64(v), v.ToTime(unit)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}
