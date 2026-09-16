package sqlsource

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
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
		ts, ok := dt.(*arrow.TimestampType)
		if !ok {
			return nil, fmt.Errorf("expected *arrow.TimestampType for %s, got %T", dt, dt)
		}
		return timestampConverter{dt: ts, zone: &timestampZone{tz: ts.TimeZone}}, nil
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
	case int64:
		bb.Append(float64(x))
	case int32:
		bb.Append(float64(x))
	case int:
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
	case int32:
		bb.Append(x != 0)
	case int:
		bb.Append(x != 0)
	case []byte:
		// Some drivers (e.g. MySQL/MSSQL BIT(1)) return a raw single byte
		// (0x00/0x01) rather than ASCII text, which ParseBool rejects.
		if len(x) == 1 && (x[0] == 0 || x[0] == 1) {
			bb.Append(x[0] != 0)
			return nil
		}
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

// timestampConverter carries the full schema-declared TimestampType (not
// just Unit) so the builder's type exactly matches the schema field:
// array.NewRecord checks column type against field type for equality
// (including TimeZone), and any dropped attribute here panics at
// record-construction time.
//
// zone lazily resolves dt.TimeZone the first time text driver input actually
// needs it (see parseTimestampText), not eagerly in converterFor: resolving
// a non-UTC/Local zone needs tzdata, which a driver that always returns
// time.Time (never needing the zone) shouldn't be required to have.
type timestampConverter struct {
	dt   *arrow.TimestampType
	zone *timestampZone
}

// timestampZone resolves a schema-declared TimeZone to a *time.Location once
// via sync.Once, caching the result (or error): time.LoadLocation can hit
// disk for tzdata, and a converter is reused across many rows in the hot
// path.
type timestampZone struct {
	tz string

	once sync.Once
	loc  *time.Location
	err  error
}

func (z *timestampZone) resolve() (*time.Location, error) {
	z.once.Do(func() {
		z.loc, z.err = timestampTextLocation(z.tz)
	})
	return z.loc, z.err
}

func (c timestampConverter) newBuilder(mem memory.Allocator) array.Builder {
	return array.NewTimestampBuilder(mem, c.dt)
}

func (c timestampConverter) append(b array.Builder, v any) error {
	bb := b.(*array.TimestampBuilder)
	switch x := v.(type) {
	case nil:
		bb.AppendNull()
	case time.Time:
		// x already carries an absolute instant via its own Location, so
		// converting it doesn't need c.dt's declared TimeZone.
		ts, err := arrow.TimestampFromTime(x, c.dt.Unit)
		if err != nil {
			return fmt.Errorf("convert time.Time to timestamp: %w", err)
		}
		bb.Append(ts)
	case []byte:
		ts, err := c.parseTimestampText(string(x))
		if err != nil {
			return fmt.Errorf("parse timestamp from %q: %w", x, err)
		}
		bb.Append(ts)
	case string:
		ts, err := c.parseTimestampText(x)
		if err != nil {
			return fmt.Errorf("parse timestamp from %q: %w", x, err)
		}
		bb.Append(ts)
	default:
		return fmt.Errorf("cannot convert %T to time.Time", v)
	}
	return nil
}

// parseTimestampText parses a driver's text timestamp value (some drivers
// scan TIMESTAMP as []byte/string, not time.Time) into an arrow.Timestamp
// for c.dt.Unit, honoring c.dt.TimeZone for naive text with no UTC offset.
//
// arrow.TimestampFromString always treats naive text as UTC, ignoring any
// declared TimeZone. That's correct when TimeZone is empty/UTC, but wrong
// for e.g. TimeZone: "America/New_York": "2024-01-15 10:30:00" is a
// wall-clock reading in that zone, not UTC, and parsing it as UTC shifts the
// instant by the zone's offset. Text with its own offset (e.g. RFC3339) is
// unambiguous and left to arrow.TimestampFromString unchanged.
func (c timestampConverter) parseTimestampText(s string) (arrow.Timestamp, error) {
	ts, hadZone, err := arrow.TimestampFromStringInLocation(s, c.dt.Unit, time.UTC)
	if err != nil {
		return 0, err
	}
	if hadZone {
		return ts, nil
	}

	// Naive text: resolve the declared zone now (only the first time), so an
	// unresolvable TimeZone surfaces here rather than failing setup for
	// callers whose driver never sends text.
	loc, err := c.zone.resolve()
	if err != nil {
		return 0, fmt.Errorf("resolve declared time zone %q: %w", c.dt.TimeZone, err)
	}
	if loc == time.UTC {
		return ts, nil
	}

	// Re-parse the wall-clock digits as a reading in the declared zone
	// instead of UTC.
	layout, err := timestampTextLayout(s)
	if err != nil {
		return 0, err
	}
	t, err := time.ParseInLocation(layout, s, loc)
	if err != nil {
		return 0, fmt.Errorf("parse %q in %s: %w", s, loc, err)
	}
	return arrow.TimestampFromTime(t, c.dt.Unit)
}

// timestampTextLocation resolves the *time.Location that naive timestamp
// text should be interpreted in for a schema's declared TimeZone tz. An
// empty tz means UTC, matching arrow.TimestampFromString's behavior.
//
// tz is matched against "UTC" case-insensitively before falling through to
// time.LoadLocation: Arrow treats "UTC"/"utc" as the zero-offset zone, but
// time.LoadLocation only special-cases the exact string "UTC" and would
// otherwise look up "utc" as an IANA zone name, which fails even where
// tzdata is available.
func timestampTextLocation(tz string) (*time.Location, error) {
	if tz == "" || strings.EqualFold(tz, "UTC") {
		return time.UTC, nil
	}
	return time.LoadLocation(tz)
}

// timestampTextLayout returns the time.Parse layout matching one of the
// naive (no zone suffix) forms arrow.TimestampFromString documents:
//
//	YYYY-MM-DD
//	YYYY-MM-DD[T]HH
//	YYYY-MM-DD[T]HH:MM
//	YYYY-MM-DD[T]HH:MM:SS[.zzzzzzzzz]
//
// where [T] is either "T" or a space, mirroring
// arrow.TimestampFromStringInLocation's length-based format selection.
// Callers must only pass s once that function has confirmed it parses and
// carries no zone suffix; this helper doesn't validate or strip one itself.
func timestampTextLayout(s string) (string, error) {
	layout := "2006-01-02"
	switch {
	case len(s) == 10:
		return layout, nil
	case len(s) == 13:
		return layout + string(s[10]) + "15", nil
	case len(s) == 16:
		return layout + string(s[10]) + "15:04", nil
	case len(s) >= 19:
		return layout + string(s[10]) + "15:04:05.999999999", nil
	default:
		return "", fmt.Errorf("invalid timestamp string %q", s)
	}
}
