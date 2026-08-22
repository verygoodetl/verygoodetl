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
		// Some drivers (notably certain MySQL/MSSQL BIT(1) handling) return a
		// BIT column as a raw single-byte binary value (0x00/0x01) rather
		// than ASCII text ("0"/"1"), which strconv.ParseBool doesn't accept.
		// Handle that binary form directly before falling back to text
		// parsing for drivers that do send BIT as ASCII text.
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

// timestampConverter carries the full schema-declared TimestampType, not
// just its Unit, so the builder it constructs has exactly the type the
// caller's schema declared (e.g. a non-empty TimeZone). array.NewRecord
// checks each column's type against the schema field's type for equality,
// so any dropped attribute here would panic at record-construction time
// for any schema that declares one.
//
// zone lazily resolves dt.TimeZone to a *time.Location the first (and only
// the first) time a text ([]byte/string) driver value actually needs it; see
// parseTimestampText. Resolution is deferred rather than done eagerly in
// converterFor because time.LoadLocation requires either the host's zoneinfo
// files or a blank-imported time/tzdata to resolve any zone other than "UTC"
// or "Local" — on a tzdata-less build/container, a driver that always
// returns time.Time (the append case below, which never needs a resolved
// zone: arrow.TimestampFromTime works from the time.Time's own offset) would
// otherwise be unable to use a non-UTC-declared schema at all, even though
// the zone is never actually needed.
type timestampConverter struct {
	dt   *arrow.TimestampType
	zone *timestampZone
}

// timestampZone lazily and cheaply resolves a schema-declared TimeZone
// string to a *time.Location, caching the result (or error) after the first
// resolution via sync.Once so that a timestampConverter reused across many
// rows in the hot path pays the time.LoadLocation cost (which can hit disk
// for tzdata lookups) at most once, rather than fresh on every row that
// needs it, while still not paying it at all when it's never needed.
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
		// x is already an absolute instant (with correct offset baked in
		// via its own Location), so converting it doesn't need c.dt's
		// declared TimeZone at all: Unix()/UnixNano() are zone-independent.
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

// parseTimestampText parses a database/sql driver's text timestamp value
// (some drivers scan TIMESTAMP columns as []byte/string rather than
// time.Time) into an arrow.Timestamp for c.dt.Unit, honoring c.dt.TimeZone
// for values that carry no explicit UTC offset of their own.
//
// arrow.TimestampFromString always parses naive text as if it were UTC,
// silently ignoring any TimeZone the caller's schema declares. That's fine
// when the schema's TimeZone is empty or UTC (naive text has always been
// treated as UTC here, and this preserves that), but it is wrong for a
// schema field like TimestampType{TimeZone: "America/New_York"}: a driver
// value like "2024-01-15 10:30:00" is meant to be a wall-clock reading in
// that zone, not in UTC, and parsing it as UTC would shift the resulting
// instant by the zone's offset relative to what the time.Time branch above
// produces for an equivalent value. Text that does carry its own offset
// (e.g. RFC3339 "2024-01-15T10:30:00-05:00") is unambiguous regardless of
// the schema's declared zone, so that case is left to
// arrow.TimestampFromString unchanged.
func (c timestampConverter) parseTimestampText(s string) (arrow.Timestamp, error) {
	ts, hadZone, err := arrow.TimestampFromStringInLocation(s, c.dt.Unit, time.UTC)
	if err != nil {
		return 0, err
	}
	if hadZone {
		return ts, nil
	}

	// s is naive: only now (and only the first time) do we need the schema's
	// declared zone resolved, so an unresolvable TimeZone surfaces here
	// rather than aborting source setup for callers whose driver only ever
	// hands back time.Time (which never reaches this method at all).
	loc, err := c.zone.resolve()
	if err != nil {
		return 0, fmt.Errorf("resolve declared time zone %q: %w", c.dt.TimeZone, err)
	}
	if loc == time.UTC {
		return ts, nil
	}

	// s is naive and the schema declares a non-UTC zone: re-parse its
	// wall-clock digits as a reading in that zone instead of in UTC.
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

// timestampTextLocation resolves the *time.Location that naive
// ([]byte/string) timestamp text should be interpreted in for a schema
// field whose TimestampType declares TimeZone tz. An empty TimeZone means
// the schema declares no zone at all, so naive text is treated as UTC,
// matching arrow.TimestampFromString's own behavior and this package's
// historical behavior before per-zone text parsing existed.
//
// tz is matched against "UTC" case-insensitively (rather than requiring the
// exact string "UTC") before falling through to time.LoadLocation, because
// Arrow's own TimestampType treats "UTC" and "utc" (and arrow-go's GetZone
// treats both) as the zero-offset zone, whereas time.LoadLocation only
// special-cases the exact, case-sensitive string "UTC" (or "Local") without
// needing tzdata; any other casing would otherwise be looked up as a real
// IANA zone name, which doesn't exist under that casing and fails even where
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
// where [T] is either "T" or a space. It mirrors the length-based format
// selection arrow.TimestampFromStringInLocation uses internally; callers
// must only pass s once TimestampFromStringInLocation has confirmed s
// parses and carries no explicit zone suffix, since this helper does not
// itself validate or strip one.
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
