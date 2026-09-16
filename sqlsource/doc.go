// Package sqlsource provides an etl.Source that runs a SQL query via the
// standard library's database/sql and emits the results as Arrow batches.
//
// Column types come entirely from a caller-supplied *arrow.Schema, never
// inferred from driver metadata: database/sql's optional
// driver.RowsColumnTypeScanType interface is inconsistently implemented
// across drivers (e.g. common SQLite drivers fall back to a generic
// interface{}), so automatic inference would be unreliable depending on
// driver and version.
//
// This package takes on no SQL driver dependency itself; the caller opens
// a *sql.DB with whatever driver it needs:
//
//	import (
//		"database/sql"
//
//		_ "modernc.org/sqlite" // or any database/sql driver
//	)
//
//	db, err := sql.Open("sqlite", "file:orders.db")
package sqlsource
