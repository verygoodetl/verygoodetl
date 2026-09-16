// Package sqlsink provides an etl.Sink that writes Arrow batches to a SQL
// table via the standard library's database/sql, as a multi-row INSERT per
// batch.
//
// Column names and order come from the schema of the first batch Consume
// receives, not from a caller-supplied schema: unlike sqlsource, there is no
// driver-inference reliability problem to design around here, since the
// Arrow schema arriving with each batch is already explicit.
//
// Conflict handling (upsert) is opt-in and dialect-specific — PostgreSQL and
// SQLite's "ON CONFLICT ... DO UPDATE", MySQL's "ON DUPLICATE KEY UPDATE",
// and so on share no common syntax — so Sink does not generate it. Callers
// who want it supply the whole trailing clause via WithUpsertClause; without
// it, Sink issues a plain INSERT.
//
// Table and column identifiers — the latter taken from the batch's Arrow
// schema field names — are written into the generated SQL text unescaped;
// see Sink's doc comment in sink.go for the full trust boundary.
//
// By default, identifiers are never quoted, so a table or column name must be
// a valid unquoted SQL identifier and must not be (case-insensitively) one of
// a small set of common reserved words (e.g. "order", "user", "key"); see
// isValidIdentifier's doc comment in sink.go for the exact rules and word
// list. WithIdentifierQuote is the escape hatch for a name that needs
// quoting, at the cost of the caller producing valid, safe quoted SQL for
// its dialect.
//
// Supported Arrow types are deliberately limited to those extractorFor in
// types.go handles; an unsupported type is a construction-time error. To add
// support for a new one, extend extractorFor with a new case and extractor
// function following the existing per-type pattern, then add a round-trip
// test alongside the existing all-types test.
//
// This package's test suite runs only against SQLite (via modernc.org/sqlite).
// Dialect-specific generated SQL text — Dollar placeholders, ON CONFLICT/ON
// DUPLICATE KEY upsert clauses, and identifier quoting aimed at Postgres,
// MySQL, or another target — is checked as generated text, not executed
// against a real driver for that dialect, so passing tests confirm the SQL
// Sink builds looks right but not that a given non-SQLite database actually
// accepts it.
//
// This package takes on no SQL driver dependency itself; the caller opens a
// *sql.DB with whatever driver it needs:
//
//	import (
//		"database/sql"
//
//		_ "modernc.org/sqlite" // or any database/sql driver
//	)
//
//	db, err := sql.Open("sqlite", "file:orders.db")
package sqlsink
