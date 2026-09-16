package sqlsink

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// extractor reads the value at row from col as a database/sql driver value:
// one of the types accepted by driver.DefaultParameterConverter (int64,
// float64, bool, string, []byte, time.Time), or nil for a null.
type extractor func(col arrow.Array, row int) any

// extractorFor returns the extractor for schema field type dt, or an error
// if dt isn't one of the types this package supports.
func extractorFor(dt arrow.DataType) (extractor, error) {
	switch dt.ID() {
	case arrow.INT64:
		return int64Extractor, nil
	case arrow.FLOAT64:
		return float64Extractor, nil
	case arrow.BOOL:
		return boolExtractor, nil
	case arrow.STRING:
		return stringExtractor, nil
	case arrow.BINARY:
		return binaryExtractor, nil
	case arrow.TIMESTAMP:
		ts, ok := dt.(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("expected *arrow.TimestampType for %s, got %T", dt, dt)
		}
		return timestampExtractor(ts.Unit), nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}

func int64Extractor(col arrow.Array, row int) any {
	a := col.(*array.Int64)
	if a.IsNull(row) {
		return nil
	}
	return a.Value(row)
}

func float64Extractor(col arrow.Array, row int) any {
	a := col.(*array.Float64)
	if a.IsNull(row) {
		return nil
	}
	return a.Value(row)
}

func boolExtractor(col arrow.Array, row int) any {
	a := col.(*array.Boolean)
	if a.IsNull(row) {
		return nil
	}
	return a.Value(row)
}

func stringExtractor(col arrow.Array, row int) any {
	a := col.(*array.String)
	if a.IsNull(row) {
		return nil
	}
	return a.Value(row)
}

func binaryExtractor(col arrow.Array, row int) any {
	a := col.(*array.Binary)
	if a.IsNull(row) {
		return nil
	}
	return a.Value(row)
}

// timestampExtractor converts via arrow.Timestamp.ToTime, which works from
// the stored epoch offset alone — the declared TimeZone only affected how
// naive input text was interpreted on the way in, not this value.
func timestampExtractor(unit arrow.TimeUnit) extractor {
	return func(col arrow.Array, row int) any {
		a := col.(*array.Timestamp)
		if a.IsNull(row) {
			return nil
		}
		return a.Value(row).ToTime(unit)
	}
}
