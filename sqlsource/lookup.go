package sqlsource

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
)

// QueryGenerator builds a query and its parameter args from an incoming
// batch — e.g. extracting a key column's values and building an IN clause.
// The number of placeholders in query must match len(args); placeholder
// syntax (?, $1, ...) is the caller's responsibility.
//
// Return a nil or empty args if the batch has nothing worth looking up:
// Lookup skips the query rather than running one with zero parameters, so
// callers never need a dummy value to keep an "IN (...)" clause non-empty.
type QueryGenerator func(batch etl.Batch) (query string, args []any, err error)

// Lookup is an etl.Processor that runs a dynamically generated query per
// incoming batch — for example, using a batch of IDs from one database to
// look up matching rows in a different, unconnected database — and emits
// the results as new Arrow batches. Lookup replaces the stream rather than
// merging with the original batch; use Pipeline.Merge to combine both.
//
// db must not be a pool shared with an upstream Source (or other stage)
// that may still hold an open *sql.Rows on the same database: etl.Pipeline
// runs every stage concurrently, so Lookup's QueryContext can race a Source
// still streaming an earlier query on the same db. If the pool allows only
// one open connection — common for SQLite — that connection is the one the
// upstream Rows holds, so QueryContext blocks forever and deadlocks the
// pipeline. Give Lookup its own *sql.DB (a second pool against the same
// database file works fine) rather than reusing an upstream stage's db.
//
// A batch with zero rows, or whose generate returns zero args, is skipped:
// an empty lookup (e.g. "IN ()") is invalid SQL for most databases.
type Lookup struct {
	db        *sql.DB
	generate  QueryGenerator
	schema    *arrow.Schema
	batchSize int
	mem       memory.Allocator

	converters []converter
}

var _ etl.Processor = (*Lookup)(nil)

// LookupOption configures a Lookup.
type LookupOption func(*Lookup)

// WithLookupBatchSize sets how many result rows are grouped into each
// emitted batch. Defaults to 1024.
func WithLookupBatchSize(n int) LookupOption {
	return func(l *Lookup) {
		if n > 0 {
			l.batchSize = n
		}
	}
}

// WithLookupAllocator sets the memory.Allocator used to build result
// batches. Defaults to memory.DefaultAllocator. A nil mem — including a
// typed-nil concrete allocator, e.g.
// WithLookupAllocator((*memory.CheckedAllocator)(nil)) — is ignored rather
// than stored, avoiding a panic on first use in the builder.
func WithLookupAllocator(mem memory.Allocator) LookupOption {
	return func(l *Lookup) {
		if !nilPointerValue(mem) {
			l.mem = mem
		}
	}
}

// NewLookup creates a Lookup that runs generate(batch) against db for every
// incoming batch and emits rows matching schema, positionally: each query
// generate produces must return exactly len(schema.Fields()) columns, in
// the same order as the schema's fields.
//
// Schema fields are restricted the same as in New (see its doc); NewLookup
// returns an error immediately for an unsupported field type.
func NewLookup(db *sql.DB, generate QueryGenerator, schema *arrow.Schema, opts ...LookupOption) (*Lookup, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlsource: NewLookup called with a nil db")
	}
	if generate == nil {
		return nil, fmt.Errorf("sqlsource: NewLookup called with a nil generate")
	}
	if schema == nil {
		return nil, fmt.Errorf("sqlsource: NewLookup called with a nil schema")
	}

	l := &Lookup{
		db:        db,
		generate:  generate,
		schema:    schema,
		batchSize: defaultBatchSize,
		mem:       memory.DefaultAllocator,
	}
	for _, opt := range opts {
		opt(l)
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
	l.converters = converters

	return l, nil
}

// Process implements etl.Processor.
func (l *Lookup) Process(ctx context.Context, b etl.Batch, out etl.Output) error {
	if b.NumRows() == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	query, args, err := l.generate(b)
	if err != nil {
		return fmt.Errorf("sqlsource: generate query: %w", err)
	}
	if len(args) == 0 {
		return nil // nothing to look up; see QueryGenerator.
	}

	rows, err := l.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("sqlsource: lookup query: %w", err)
	}
	defer rows.Close()

	return scanRowsToBatches(rows, l.schema, l.converters, l.mem, l.batchSize, func(batch etl.Batch) error {
		return out.Send(ctx, batch)
	})
}

// Finish implements etl.Processor. Lookup has no accumulated state to flush
// across batches.
func (l *Lookup) Finish(context.Context, etl.Output) error { return nil }
