package sqlsource

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// converter builds one Arrow column from database/sql-scanned values. append
// accepts the value's runtime type as reported by Scan, which varies by
// driver even for the same declared SQL column type, so conversions are
// permissive rather than requiring one exact Go type.
type converter interface {
	newBuilder(mem memory.Allocator) array.Builder
	append(b array.Builder, v any) error
}

// converterFor returns the converter for schema field type dt, or an error
// if dt isn't one of the types this package supports.
func converterFor(dt arrow.DataType) (converter, error) {
	switch dt.ID() {
	case arrow.INT64:
		return int64Converter{}, nil
	case arrow.FLOAT64:
		return float64Converter{}, nil
	case arrow.BOOL:
		return boolConverter{}, nil
	case arrow.STRING:
		return stringConverter{}, nil
	case arrow.BINARY:
		return binaryConverter{}, nil
	case arrow.TIMESTAMP:
		return timestampConverter{unit: dt.(*arrow.TimestampType).Unit}, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}

type int64Converter struct{}

func (int64Converter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewInt64Builder(mem)
}

func (int64Converter) append(b array.Builder, v any) error {
	bb := b.(*array.Int64Builder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case int64:
		bb.Append(x)
	case int32:
		bb.Append(int64(x))
	case int:
		bb.Append(int64(x))
	case []byte:
		n, err := strconv.ParseInt(string(x), 10, 64)
		if err != nil {
			return fmt.Errorf("parse int64 from %q: %w", x, err)
		}
		bb.Append(n)
	case string:
		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return fmt.Errorf("parse int64 from %q: %w", x, err)
		}
		bb.Append(n)
	default:
		return fmt.Errorf("cannot convert %T to int64", v)
	}
	return nil
}

type float64Converter struct{}

func (float64Converter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewFloat64Builder(mem)
}

func (float64Converter) append(b array.Builder, v any) error {
	bb := b.(*array.Float64Builder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case float64:
		bb.Append(x)
	case float32:
		bb.Append(float64(x))
	case []byte:
		f, err := strconv.ParseFloat(string(x), 64)
		if err != nil {
			return fmt.Errorf("parse float64 from %q: %w", x, err)
		}
		bb.Append(f)
	case string:
		f, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return fmt.Errorf("parse float64 from %q: %w", x, err)
		}
		bb.Append(f)
	default:
		return fmt.Errorf("cannot convert %T to float64", v)
	}
	return nil
}

type boolConverter struct{}

func (boolConverter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewBooleanBuilder(mem)
}

func (boolConverter) append(b array.Builder, v any) error {
	bb := b.(*array.BooleanBuilder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case bool:
		bb.Append(x)
	case int64:
		bb.Append(x != 0)
	case []byte:
		bv, err := strconv.ParseBool(string(x))
		if err != nil {
			return fmt.Errorf("parse bool from %q: %w", x, err)
		}
		bb.Append(bv)
	case string:
		bv, err := strconv.ParseBool(x)
		if err != nil {
			return fmt.Errorf("parse bool from %q: %w", x, err)
		}
		bb.Append(bv)
	default:
		return fmt.Errorf("cannot convert %T to bool", v)
	}
	return nil
}

type stringConverter struct{}

func (stringConverter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewStringBuilder(mem)
}

func (stringConverter) append(b array.Builder, v any) error {
	bb := b.(*array.StringBuilder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case string:
		bb.Append(x)
	case []byte:
		bb.Append(string(x))
	default:
		return fmt.Errorf("cannot convert %T to string", v)
	}
	return nil
}

type binaryConverter struct{}

func (binaryConverter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
}

func (binaryConverter) append(b array.Builder, v any) error {
	bb := b.(*array.BinaryBuilder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case []byte:
		bb.Append(x)
	case string:
		bb.Append([]byte(x))
	default:
		return fmt.Errorf("cannot convert %T to []byte", v)
	}
	return nil
}

type timestampConverter struct {
	unit arrow.TimeUnit
}

func (c timestampConverter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewTimestampBuilder(mem, &arrow.TimestampType{Unit: c.unit})
}

func (c timestampConverter) append(b array.Builder, v any) error {
	bb := b.(*array.TimestampBuilder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case time.Time:
		ts, err := arrow.TimestampFromTime(x, c.unit)
		if err != nil {
			return fmt.Errorf("convert time.Time to timestamp: %w", err)
		}
		bb.Append(ts)
	default:
		return fmt.Errorf("cannot convert %T to time.Time", v)
	}
	return nil
}
