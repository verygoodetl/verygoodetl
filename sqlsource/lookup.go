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
// The number of placeholders in query must match len(args); dialect-specific
// placeholder syntax (?, $1, ...) is the caller's responsibility, matching
// how a Source's query is entirely caller-authored.
//
// If generate filters the batch down to nothing worth looking up (e.g.
// every row is excluded by some condition), return a nil or empty args —
// Lookup skips the query in that case rather than running whatever query
// was returned with zero parameters, so a caller never needs a dummy
// placeholder value just to keep an "IN (...)" clause non-empty.
type QueryGenerator func(batch etl.Batch) (query string, args []any, err error)

// Lookup is an etl.Processor that runs a dynamically generated query per
// incoming batch — for example, using a batch of IDs from one database to
// look up matching rows in a different, unconnected database — and emits
// the results as new Arrow batches. Lookup replaces the stream rather than
// merging with the original batch: to combine the original batch's data
// with a Lookup's results, attach both to a Pipeline.Merge and do the
// combination there.
//
// A batch with zero rows, or whose generate returns zero args, is skipped
// without running a query, since an empty lookup (e.g. an "IN ()" clause)
// is invalid SQL for most databases.
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
// batches. Defaults to memory.DefaultAllocator. A nil mem is ignored,
// keeping the default.
func WithLookupAllocator(mem memory.Allocator) LookupOption {
	return func(l *Lookup) {
		if mem != nil {
			l.mem = mem
		}
	}
}

// NewLookup creates a Lookup that runs generate(batch) against db for every
// incoming batch and emits rows matching schema, positionally: each query
// generate produces must return exactly len(schema.Fields()) columns, in
// the same order as the schema's fields.
//
// Every field in schema must be one of the types this package supports
// (Int64, Float64, Boolean, String, Binary, Timestamp); NewLookup returns
// an error immediately for any other field type, rather than failing later
// when a query runs.
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

	query, args, err := l.generate(b)
	if err != nil {
		return fmt.Errorf("sqlsource: generate query: %w", err)
	}
	if len(args) == 0 {
		// generate filtered the batch down to nothing to look up (e.g. every
		// row was excluded by some caller-side condition). Skip the query
		// rather than running whatever generate returned for zero args,
		// which for a query like "WHERE id IN ()" would be invalid SQL.
		return nil
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
