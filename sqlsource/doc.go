// Package sqlsource provides an etl.Source that runs a SQL query via the
// standard library's database/sql and emits the results as Arrow batches.
//
// Column types are driven entirely by a caller-supplied *arrow.Schema, never
// inferred from driver metadata. database/sql's optional
// driver.RowsColumnTypeScanType interface — the mechanism that would let a
// library infer Arrow types automatically — is inconsistently implemented
// across drivers (notably, common SQLite drivers don't implement it at all
// and fall back to a generic interface{}), so automatic inference would be
// silently unreliable depending on which driver and version is in use.
// Requiring an explicit schema trades a little convenience for predictable,
// auditable behavior.
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
