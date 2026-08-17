package sqlsource

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
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
	return func(s *Source) { s.args = args }
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
// memory.DefaultAllocator.
func WithAllocator(mem memory.Allocator) Option {
	return func(s *Source) { s.mem = mem }
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
		c, err := converterFor(f.Type)
		if err != nil {
			return nil, fmt.Errorf("sqlsource: field %d (%s): %w", i, f.Name, err)
		}
		converters[i] = c
	}
	s.converters = converters

	return s, nil
}

// Run implements etl.Source.
func (s *Source) Run(ctx context.Context, out etl.Output) error {
	rows, err := s.db.QueryContext(ctx, s.query, s.args...)
	if err != nil {
		return fmt.Errorf("sqlsource: query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlsource: columns: %w", err)
	}
	if len(cols) != len(s.converters) {
		return fmt.Errorf("sqlsource: query returned %d columns, schema has %d fields", len(cols), len(s.converters))
	}

	builders := make([]array.Builder, len(s.converters))
	for i, c := range s.converters {
		builders[i] = c.newBuilder(s.mem)
	}
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	scanVals := make([]any, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range scanVals {
		scanArgs[i] = &scanVals[i]
	}

	rowCount := 0
	flush := func() error {
		if rowCount == 0 {
			return nil
		}
		arrs := make([]arrow.Array, len(builders))
		for i, b := range builders {
			arrs[i] = b.NewArray()
		}
		record := array.NewRecord(s.schema, arrs, int64(rowCount))
		for _, a := range arrs {
			a.Release()
		}
		defer record.Release()
		rowCount = 0
		return out.Send(ctx, etl.NewBatch(record))
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("sqlsource: scan: %w", err)
		}
		for i, v := range scanVals {
			if err := s.converters[i].append(builders[i], v); err != nil {
				return fmt.Errorf("sqlsource: column %d (%s): %w", i, cols[i], err)
			}
		}
		rowCount++
		if rowCount == s.batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlsource: rows: %w", err)
	}
	return flush()
}
