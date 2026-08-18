package sqlsource

import (
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	etl "github.com/verygoodetl/verygoodetl"
)

// scanRowsToBatches scans rows into Arrow batches of at most batchSize rows
// using converters (one per schema field, in schema order), calling send for
// each completed batch, including a final partial batch. Shared by Source
// and Lookup, which differ only in how their *sql.Rows is obtained.
func scanRowsToBatches(rows *sql.Rows, schema *arrow.Schema, converters []converter, mem memory.Allocator, batchSize int, send func(etl.Batch) error) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sqlsource: columns: %w", err)
	}
	if len(cols) != len(converters) {
		return fmt.Errorf("sqlsource: query returned %d columns, schema has %d fields", len(cols), len(converters))
	}

	builders := make([]array.Builder, len(converters))
	for i, c := range converters {
		builders[i] = c.newBuilder(mem)
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
		record := array.NewRecord(schema, arrs, int64(rowCount))
		for _, a := range arrs {
			a.Release()
		}
		defer record.Release()
		rowCount = 0
		return send(etl.NewBatch(record))
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return fmt.Errorf("sqlsource: scan: %w", err)
		}
		for i, v := range scanVals {
			if err := converters[i].append(builders[i], v); err != nil {
				return fmt.Errorf("sqlsource: column %d (%s): %w", i, cols[i], err)
			}
		}
		rowCount++
		if rowCount == batchSize {
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
