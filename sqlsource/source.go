package sqlsource

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
)

const defaultBatchSize = 1024

// Source is an etl.Source that runs a SQL query and emits the results as
// Arrow batches, batchSize rows at a time. It holds no per-run state, so a
// Source may be reused across multiple pipeline runs, including
// concurrently — database/sql's *sql.DB is itself a connection pool
// designed for concurrent use.
type Source struct {
	db        *sql.DB
	query     string
	args      []any
	schema    *arrow.Schema
	batchSize int
	mem       memory.Allocator

	converters []converter
}

var _ etl.Source = (*Source)(nil)

// Option configures a Source.
type Option func(*Source)

// WithArgs sets the query's parameter arguments, passed to
// (*sql.DB).QueryContext.
func WithArgs(args ...any) Option {
	return func(s *Source) {
		s.args = append([]any(nil), args...)
	}
}

// WithBatchSize sets how many rows are grouped into each emitted batch.
// Defaults to 1024.
func WithBatchSize(n int) Option {
	return func(s *Source) {
		if n > 0 {
			s.batchSize = n
		}
	}
}

// WithAllocator sets the memory.Allocator used to build batches. Defaults to
// memory.DefaultAllocator. A nil mem — including a typed-nil concrete
// allocator, e.g. WithAllocator((*memory.CheckedAllocator)(nil)) — is
// ignored, keeping the default, rather than being stored and panicking on
// first use inside the batch builder.
func WithAllocator(mem memory.Allocator) Option {
	return func(s *Source) {
		if !nilPointerValue(mem) {
			s.mem = mem
		}
	}
}

// New creates a Source that runs query against db and emits rows matching
// schema, positionally: the query must return exactly len(schema.Fields())
// columns, in the same order as the schema's fields.
//
// Every field in schema must be one of the types this package supports
// (Int64, Float64, Boolean, String, Binary, Timestamp); New returns an error
// immediately for any other field type, rather than failing later when the
// query runs.
func New(db *sql.DB, query string, schema *arrow.Schema, opts ...Option) (*Source, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlsource: New called with a nil db")
	}
	if schema == nil {
		return nil, fmt.Errorf("sqlsource: New called with a nil schema")
	}

	s := &Source{
		db:        db,
		query:     query,
		schema:    schema,
		batchSize: defaultBatchSize,
		mem:       memory.DefaultAllocator,
	}
	for _, opt := range opts {
		opt(s)
	}

	converters := make([]converter, schema.NumFields())
	for i, f := range schema.Fields() {
		if nilPointerValue(f.Type) {
			return nil, fmt.Errorf("sqlsource: field %d (%s): nil type", i, f.Name)
		}
		c, err := converterFor(f.Type)
		if err != nil {
			return nil, fmt.Errorf("sqlsource: field %d (%s): %w", i, f.Name, err)
		}
		converters[i] = c
	}
	s.converters = converters

	return s, nil
}

// nilPointerValue reports whether v is either an untyped nil interface (v ==
// nil) or a typed nil pointer wrapped in a non-nil interface — e.g. a caller
// building arrow.Field{Type: (*arrow.TimestampType)(nil)} (which
// arrow.NewSchema itself does not reject, since it only checks field.Type ==
// nil too) or calling WithAllocator((*memory.CheckedAllocator)(nil)). A plain
// v == nil check misses both cases: the interface carries a concrete type
// descriptor and a nil value pointer, so it compares != nil even though
// using it — type-asserting back to *arrow.TimestampType (or similar) and
// calling a method, or converterFor accessing dt.ID() — panics on the nil
// receiver. arrow-go's DataType and memory.Allocator implementations are
// essentially all pointer types, so checking Kind() == reflect.Ptr covers
// them; Kind is checked before IsNil because IsNil panics on kinds that
// don't support it.
func nilPointerValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// Run implements etl.Source.
func (s *Source) Run(ctx context.Context, out etl.Output) error {
	rows, err := s.db.QueryContext(ctx, s.query, s.args...)
	if err != nil {
		return fmt.Errorf("sqlsource: query: %w", err)
	}
	defer rows.Close()

	return scanRowsToBatches(rows, s.schema, s.converters, s.mem, s.batchSize, func(b etl.Batch) error {
		return out.Send(ctx, b)
	})
}
