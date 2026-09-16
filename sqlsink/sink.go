package sqlsink

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"

	etl "github.com/verygoodetl/verygoodetl"
)

// defaultMaxPlaceholders keeps a single INSERT under SQLite's default
// SQLITE_MAX_VARIABLE_NUMBER (999), the most restrictive common driver limit.
const defaultMaxPlaceholders = 900

// Placeholder selects the parameter-marker syntax used in generated INSERT
// statements. database/sql does not abstract over this — drivers disagree on
// marker syntax and there's no way to ask a *sql.DB which one its driver
// expects.
type Placeholder int

const (
	// Question uses "?" markers (MySQL, SQLite, ...). The default.
	Question Placeholder = iota
	// Dollar uses "$1", "$2", ... markers (PostgreSQL).
	Dollar
)

func (p Placeholder) marker(n int) string {
	if p == Dollar {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}

// Sink writes batches to table in db, one multi-row INSERT statement per
// batch (or several, see WithMaxPlaceholders), each wrapped in its own
// transaction so a batch either lands in full or not at all. Column names
// and order come from the schema of the first batch Consume receives.
//
// Table and column names are written directly into the generated SQL text,
// unescaped — including the batch's Arrow schema field names — so callers
// must not derive them from untrusted input (e.g. a CSV header or API
// response); only row values are bound parameters.
//
// A Sink writes to table exactly once and must be attached to exactly one
// pipeline node; it is not safe to reuse across streams, and Consume returns
// an error if called again after Finish.
//
// Sink does not implement etl.Aborter, unlike filesink.Sink: each Consume
// call fully resolves its own transaction before returning, so there's no
// cross-call resource left to clean up on cancellation.
type Sink struct {
	db              *sql.DB
	table           string
	upsertClause    string
	placeholder     Placeholder
	placeholderFunc func(int) string
	maxPlaceholders int
	identifierQuote func(string) string
	// 0 means unbounded; see WithMaxRowsPerConsume.
	maxRowsPerConsume int
	// Lowercase words from WithAdditionalReservedWords, scoped to this Sink
	// instance; nil if unset. See isReservedWord.
	additionalReservedWords map[string]bool
	constructErr            error

	started    bool
	schema     *arrow.Schema
	columns    []string
	extractors []extractor

	// Blocks a later Consume once Finish has run (sequential reuse).
	finished bool

	// Guards concurrent/reentrant Consume and Finish calls, which would
	// otherwise race unsynchronized on started, schema, columns, extractors,
	// and finished. Shared by both methods so a Consume/Finish race is caught
	// too, not just Consume/Consume or Finish/Finish.
	inUse int32
}

var _ etl.Sink = (*Sink)(nil)

// SinkOption configures a Sink.
type SinkOption func(*Sink)

// WithUpsertClause appends clause verbatim to the end of every generated
// INSERT statement, e.g. "ON CONFLICT (id) DO UPDATE SET name =
// EXCLUDED.name" (PostgreSQL/SQLite) or "ON DUPLICATE KEY UPDATE name =
// VALUES(name)" (MySQL). Without this option, Sink issues a plain INSERT and
// a conflicting row fails that batch's transaction like any other constraint
// violation.
//
// clause must be fully literal SQL — buildInsert only generates and counts
// placeholders for the row VALUES list, so clause cannot contain its own
// placeholder marker; inline any dynamic value as a constant instead.
//
// As a best-effort check, a clause containing a literal "?" or a "$"
// immediately followed by a digit is rejected at construction time. This
// can't catch every dialect's marker syntax (SQL Server's "@p1", Oracle's
// ":1"), and will also reject a legitimate "?" unrelated to a placeholder —
// e.g. PostgreSQL's jsonb "?"/"?|"/"?&" operators, or one inside a quoted
// string literal — with no way around it today.
func WithUpsertClause(clause string) SinkOption {
	return func(s *Sink) {
		if strings.Contains(clause, "?") {
			if s.constructErr == nil {
				s.constructErr = fmt.Errorf("sqlsink: WithUpsertClause: clause %q must not contain a placeholder marker (\"?\"); clause must be fully literal SQL", clause)
			}
			return
		}
		for i := 0; i < len(clause); i++ {
			if clause[i] == '$' && i+1 < len(clause) && clause[i+1] >= '0' && clause[i+1] <= '9' {
				if s.constructErr == nil {
					s.constructErr = fmt.Errorf("sqlsink: WithUpsertClause: clause %q must not contain a placeholder marker (\"$%c\"); clause must be fully literal SQL", clause, clause[i+1])
				}
				return
			}
		}
		s.upsertClause = clause
	}
}

// WithPlaceholder sets the parameter-marker syntax. Defaults to Question.
func WithPlaceholder(p Placeholder) SinkOption {
	return func(s *Sink) { s.placeholder = p }
}

// WithPlaceholderFunc sets a caller-supplied function for rendering the
// marker for the n'th (1-based) bound parameter, overriding whatever
// WithPlaceholder would otherwise produce — for a dialect Placeholder can't
// express, e.g. SQL Server's "@p1, @p2, ..." or Oracle's ":1, :2, ...":
//
//	sqlsink.WithPlaceholderFunc(func(n int) string { return fmt.Sprintf("@p%d", n) })
//
// If both are set, WithPlaceholderFunc always wins regardless of option order.
func WithPlaceholderFunc(f func(n int) string) SinkOption {
	return func(s *Sink) { s.placeholderFunc = f }
}

// WithMaxPlaceholders caps how many "?"/"$N" parameters a single generated
// INSERT statement uses, splitting a large batch across multiple statements
// — all within the same transaction — instead of exceeding a driver's own
// limit. Defaults to 900.
//
// The cap must be at least the batch's column count, since one row's
// placeholders can't be split across statements; if it isn't, the first
// Consume call fails with a construction error. A non-positive n is itself a
// construction error (see New's doc comment).
func WithMaxPlaceholders(n int) SinkOption {
	return func(s *Sink) {
		if n <= 0 {
			if s.constructErr == nil {
				s.constructErr = fmt.Errorf("sqlsink: WithMaxPlaceholders: n must be positive, got %d", n)
			}
			return
		}
		s.maxPlaceholders = n
	}
}

// WithMaxRowsPerConsume caps how many rows a single Consume call may write,
// across however many statements WithMaxPlaceholders splits them into, all
// within that call's one transaction. Unset by default: Consume writes as
// many rows as it's given, which can otherwise hold a long transaction (and
// its locks) open against a production database.
//
// A batch whose row count exceeds n is rejected with an error before any SQL
// runs, rather than being truncated or let through. This check runs before
// schema derivation (open), so a rejected first batch never locks the Sink
// into that batch's schema. A non-positive n is itself a construction error
// (see New's doc comment).
func WithMaxRowsPerConsume(n int) SinkOption {
	return func(s *Sink) {
		if n <= 0 {
			if s.constructErr == nil {
				s.constructErr = fmt.Errorf("sqlsink: WithMaxRowsPerConsume: n must be positive, got %d", n)
			}
			return
		}
		s.maxRowsPerConsume = n
	}
}

// WithIdentifierQuote sets a function that quotes a single table or column
// identifier for inclusion in generated SQL text, e.g. for
// PostgreSQL/SQLite-style double-quoting:
//
//	func(id string) string { return `"` + strings.ReplaceAll(id, `"`, `""`) + `"` }
//
// Whatever quote returns is written directly into the generated SQL text
// with no escaping or inspection of its own, so quote alone is responsible
// for escaping any character special to its quoting scheme — naively
// wrapping id without escaping an embedded quote (e.g. `"`+id+`"`) lets the
// identifier's contents break out of the quoted identifier. Applied to the
// table name (each "."-separated segment of a schema-qualified name
// independently, so "analytics.orders" quotes as `"analytics"."orders"`) and
// to every column name.
//
// When set, this replaces isValidTableIdentifier/isValidIdentifier's
// character-set, leading-digit, and reserved-word checks entirely: those
// exist only to keep an *unquoted* identifier safe, and quoting is the
// mechanism SQL provides for a name — a reserved word, or one with a
// disallowed character — that would otherwise be invalid unquoted.
// isValidTableIdentifier's segment-count and empty-segment checks still
// apply, since those are about whether a schema-qualified name is
// well-formed at all, not about unquoted safety.
//
// A nil quote is a construction error; it is not a way to opt out of
// quoting, which is already the default, unset behavior.
func WithIdentifierQuote(quote func(name string) string) SinkOption {
	return func(s *Sink) {
		if quote == nil {
			if s.constructErr == nil {
				s.constructErr = fmt.Errorf("sqlsink: WithIdentifierQuote: quote must not be nil")
			}
			return
		}
		s.identifierQuote = quote
	}
}

// WithAdditionalReservedWords extends the built-in, non-exhaustive
// sqlReservedWords list (see its doc comment) with words a caller's
// particular dialect also reserves, as an alternative to switching to
// WithIdentifierQuote and losing its character-set/reserved-word safety net
// entirely. Each word is folded to lowercase and scoped to this one Sink
// instance — not shared or global, so concurrent Sinks targeting different
// dialects can each extend the list without racing. An empty string is a
// construction error.
func WithAdditionalReservedWords(words ...string) SinkOption {
	return func(s *Sink) {
		for _, w := range words {
			if w == "" {
				if s.constructErr == nil {
					s.constructErr = fmt.Errorf("sqlsink: WithAdditionalReservedWords: word must not be empty")
				}
				return
			}
			if s.additionalReservedWords == nil {
				s.additionalReservedWords = make(map[string]bool)
			}
			s.additionalReservedWords[strings.ToLower(w)] = true
		}
	}
}

// New creates a Sink that writes batches to table in db. table may
// optionally be schema-qualified as "schema.table" (e.g. "analytics.orders");
// both segments follow the same character-set/leading-digit/reserved-word
// rules (see isValidTableIdentifier).
//
// A nil db, empty table, or table that isn't a valid identifier (or
// schema-qualified pair, see isValidTableIdentifier) is a construction error,
// surfaced from the first Consume or Finish call rather than returned here,
// so New can compose directly into a pipeline (e.g.
// p.To(sqlsink.New(db, "orders"))).
//
// Table and column names, and any schema segment, must not be bare SQL
// reserved words — e.g. "order" or "select" — even though most dialects
// accept them quoted; see isValidIdentifier for the full rules. See
// WithIdentifierQuote for which of these checks it replaces.
func New(db *sql.DB, table string, opts ...SinkOption) *Sink {
	s := &Sink{db: db, table: table, maxPlaceholders: defaultMaxPlaceholders}
	switch {
	case db == nil:
		s.constructErr = fmt.Errorf("sqlsink: New: nil db")
	case table == "":
		s.constructErr = fmt.Errorf("sqlsink: New: empty table name")
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.constructErr == nil && s.identifierQuote == nil && !isValidTableIdentifier(table, s.additionalReservedWords) {
		s.constructErr = fmt.Errorf("sqlsink: New: table name %q is not a valid unquoted SQL identifier, optionally schema-qualified as \"schema.table\"", table)
	}
	if s.constructErr == nil && s.identifierQuote != nil && !hasValidTableSegments(table) {
		s.constructErr = fmt.Errorf("sqlsink: New: table name %q has more than one \".\" or an empty segment, so it cannot be quoted as a table name optionally schema-qualified as \"schema.table\"", table)
	}
	return s
}

// Consume implements etl.Sink.
func (s *Sink) Consume(ctx context.Context, b etl.Batch) error {
	if !atomic.CompareAndSwapInt32(&s.inUse, 0, 1) {
		return fmt.Errorf("sqlsink: Consume called concurrently on the same Sink; a Sink must be attached to exactly one pipeline node")
	}
	defer atomic.StoreInt32(&s.inUse, 0)

	if s.finished {
		return fmt.Errorf("sqlsink: Consume called after Finish; a Sink must not be reused across multiple pipeline runs")
	}

	if s.constructErr != nil {
		return s.constructErr
	}

	if nilPointerValue(b) {
		return fmt.Errorf("sqlsink: batch is nil")
	}

	schema := b.Schema()
	if schema == nil {
		return fmt.Errorf("sqlsink: batch has a nil schema")
	}
	record := b.Record()
	if isNilRecord(record) {
		return fmt.Errorf("sqlsink: batch has a nil record")
	}

	// Checked before open (needs only record.NumRows(), not the derived
	// schema) so a batch rejected here never locks the Sink into its schema.
	if s.maxRowsPerConsume > 0 && int(record.NumRows()) > s.maxRowsPerConsume {
		return fmt.Errorf("sqlsink: batch has %d rows, which exceeds the configured max rows per Consume (%d)", record.NumRows(), s.maxRowsPerConsume)
	}

	// record.Schema() is arrow-go's actual view of record's columns, vs.
	// schema (the batch's reported schema, which a hand-written etl.Batch
	// could make disagree) — catching a mismatch here avoids a panic in an
	// extractor's type assertion later. Checked before open for the same
	// reason as the row-count check above.
	if !record.Schema().Equal(schema) {
		return fmt.Errorf("sqlsink: record schema %s differs from the batch's reported schema %s", record.Schema(), schema)
	}

	if !s.started {
		if err := s.open(schema); err != nil {
			return err
		}
	} else if !sameColumns(schema, s.schema) {
		return fmt.Errorf("sqlsink: batch schema %s differs from the first batch's schema %s", schema, s.schema)
	}

	if record.NumRows() == 0 {
		return nil
	}
	return s.insert(ctx, record)
}

// Finish implements etl.Sink.
func (s *Sink) Finish(context.Context) error {
	if !atomic.CompareAndSwapInt32(&s.inUse, 0, 1) {
		return fmt.Errorf("sqlsink: Finish called concurrently with Consume or Finish on the same Sink; a Sink must be attached to exactly one pipeline node")
	}
	defer atomic.StoreInt32(&s.inUse, 0)

	s.finished = true
	return s.constructErr
}

// sameColumns reports whether a and b have the same fields, by Name and Type
// only — deliberately looser than arrow.Schema.Equal, which also compares
// Nullable and Metadata, neither of which open reads when deriving columns
// and extractors.
func sameColumns(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	af, bf := a.Fields(), b.Fields()
	for i := range af {
		if af[i].Name != bf[i].Name || !arrow.TypeEqual(af[i].Type, bf[i].Type) {
			return false
		}
	}
	return true
}

// open derives column names and value extractors from schema, the first
// time a batch arrives. Column names are checked against isValidIdentifier
// unless WithIdentifierQuote is set, in which case only emptiness is
// rejected (see its doc comment).
//
// The duplicate-column check below folds case only when s.identifierQuote is
// nil: PostgreSQL's quoted identifiers are case-sensitive, so once quoting
// is opted into, "Name" and "name" are legitimately distinct.
func (s *Sink) open(schema *arrow.Schema) error {
	if schema.NumFields() == 0 {
		return fmt.Errorf("sqlsink: schema has no fields")
	}
	if schema.NumFields() > s.maxPlaceholders {
		return fmt.Errorf("sqlsink: schema has %d columns, which exceeds the configured max placeholders (%d); a single row's placeholders cannot be split across statements", schema.NumFields(), s.maxPlaceholders)
	}
	columns := make([]string, schema.NumFields())
	extractors := make([]extractor, schema.NumFields())
	seen := make(map[string]bool, schema.NumFields())
	for i, f := range schema.Fields() {
		if nilPointerValue(f.Type) {
			return fmt.Errorf("sqlsink: field %d (%s): nil type", i, f.Name)
		}
		if s.identifierQuote == nil {
			if !isValidIdentifier(f.Name, s.additionalReservedWords) {
				return fmt.Errorf("sqlsink: field %d (%q): column name contains a character not valid in an unquoted SQL identifier", i, f.Name)
			}
		} else if f.Name == "" {
			return fmt.Errorf("sqlsink: field %d: column name is empty", i)
		}
		key := f.Name
		if s.identifierQuote == nil {
			key = strings.ToLower(f.Name)
		}
		if seen[key] {
			return fmt.Errorf("sqlsink: field %d (%q): duplicate column name", i, f.Name)
		}
		seen[key] = true
		ext, err := extractorFor(f.Type)
		if err != nil {
			return fmt.Errorf("sqlsink: field %d (%s): %w", i, f.Name, err)
		}
		columns[i] = f.Name
		extractors[i] = ext
	}
	s.schema = schema
	s.columns = columns
	s.extractors = extractors
	s.started = true
	return nil
}

// insert writes record's rows to s.table inside one transaction, split
// across as many INSERT statements as s.maxPlaceholders requires.
//
// Full chunks (all but possibly the last) share one shape, so when a batch
// needs more than one, insert prepares that statement once and reuses it
// across every full chunk instead of re-parsing identical SQL each time. A
// single-chunk batch skips preparation (no benefit for one use), and a
// shorter remainder chunk is always executed unprepared for the same reason.
func (s *Sink) insert(ctx context.Context, record arrow.Record) error {
	// Narrows int64 to int, assuming the row count fits a platform int (same
	// assumption the rest of this runtime makes).
	numRows := int(record.NumRows())
	numCols := len(s.columns)

	// Integer division floors: up to numCols-1 placeholders of headroom per
	// statement go unused, which is fine.
	rowsPerStmt := s.maxPlaceholders / numCols

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlsink: begin transaction: %w", err)
	}

	cols := record.Columns()

	numFullChunks := numRows / rowsPerStmt
	remainder := numRows % rowsPerStmt
	totalChunks := numFullChunks
	if remainder > 0 {
		totalChunks++
	}

	// Single-chunk batches: no reuse is possible, so keep the original,
	// simpler direct-ExecContext path.
	if totalChunks <= 1 {
		query, args := s.buildInsert(cols, 0, numRows)
		if err := s.execAndHandleErr(tx, 0, numRows, func() error {
			_, err := tx.ExecContext(ctx, query, args...)
			return err
		}); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("sqlsink: commit transaction: %w", err)
		}
		return nil
	}

	// stmt is closed on every path below (success, error, rollback).
	var stmt *sql.Stmt
	if numFullChunks > 1 {
		query := s.buildInsertQuery(rowsPerStmt)
		prepared, err := tx.PrepareContext(ctx, query)
		if err != nil {
			prepErr := fmt.Errorf("sqlsink: prepare insert statement: %w", err)
			if rbErr := tx.Rollback(); rbErr != nil {
				return errors.Join(prepErr, fmt.Errorf("sqlsink: rollback: %w", rbErr))
			}
			return prepErr
		}
		stmt = prepared
		defer stmt.Close()
	}

	for start := 0; start < numFullChunks*rowsPerStmt; start += rowsPerStmt {
		end := start + rowsPerStmt
		err := s.execAndHandleErr(tx, start, end, func() error {
			if stmt != nil {
				args := s.buildArgs(cols, start, end)
				_, execErr := stmt.ExecContext(ctx, args...)
				return execErr
			}
			query, args := s.buildInsert(cols, start, end)
			_, execErr := tx.ExecContext(ctx, query, args...)
			return execErr
		})
		if err != nil {
			return err
		}
	}

	if remainder > 0 {
		start := numFullChunks * rowsPerStmt
		end := start + remainder
		query, args := s.buildInsert(cols, start, end)
		if err := s.execAndHandleErr(tx, start, end, func() error {
			_, err := tx.ExecContext(ctx, query, args...)
			return err
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlsink: commit transaction: %w", err)
	}
	return nil
}

// execAndHandleErr runs exec (an already-bound ExecContext call, direct or
// via a prepared *sql.Stmt, covering rows [start, end)) and on failure wraps
// the error with that range and rolls tx back, joining a rollback failure
// rather than losing it. Shared by insert's three exec paths.
func (s *Sink) execAndHandleErr(tx *sql.Tx, start, end int, exec func() error) error {
	if err := exec(); err != nil {
		insertErr := fmt.Errorf("sqlsink: insert rows [%d,%d): %w", start, end, err)
		if rbErr := tx.Rollback(); rbErr != nil {
			return errors.Join(insertErr, fmt.Errorf("sqlsink: rollback: %w", rbErr))
		}
		return insertErr
	}
	return nil
}

// buildInsertQuery renders the INSERT text for numRows rows, byte-for-byte
// identical to what buildInsert would produce, but without evaluating any
// argument values — used to prepare a chunk shape without extracting values
// that would be immediately discarded.
func (s *Sink) buildInsertQuery(numRows int) string {
	numCols := len(s.columns)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.quotedTable())
	sb.WriteString(" (")
	for i, c := range s.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(s.quotedIdentifier(c))
	}
	sb.WriteString(") VALUES ")

	n := 1
	for row := 0; row < numRows; row++ {
		if row > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := 0; c < numCols; c++ {
			if c > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s.marker(n))
			n++
		}
		sb.WriteByte(')')
	}

	if s.upsertClause != "" {
		sb.WriteByte(' ')
		sb.WriteString(s.upsertClause)
	}

	return sb.String()
}

// buildInsert renders one INSERT statement covering rows [start, end) of
// cols, plus its positional arguments in the same order as its markers.
func (s *Sink) buildInsert(cols []arrow.Array, start, end int) (string, []any) {
	numCols := len(s.columns)
	args := make([]any, 0, (end-start)*numCols)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.quotedTable())
	sb.WriteString(" (")
	for i, c := range s.columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(s.quotedIdentifier(c))
	}
	sb.WriteString(") VALUES ")

	n := 1
	for row := start; row < end; row++ {
		if row > start {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for c := 0; c < numCols; c++ {
			if c > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s.marker(n))
			n++
			args = append(args, s.extractors[c](cols[c], row))
		}
		sb.WriteByte(')')
	}

	if s.upsertClause != "" {
		sb.WriteByte(' ')
		sb.WriteString(s.upsertClause)
	}

	return sb.String(), args
}

// buildArgs renders just the positional arguments for rows [start, end) of
// cols, in the same order buildInsert would, for reuse against an
// already-prepared statement of that shape.
func (s *Sink) buildArgs(cols []arrow.Array, start, end int) []any {
	numCols := len(s.columns)
	args := make([]any, 0, (end-start)*numCols)
	for row := start; row < end; row++ {
		for c := 0; c < numCols; c++ {
			args = append(args, s.extractors[c](cols[c], row))
		}
	}
	return args
}

// marker renders the marker for the n'th (1-based) bound parameter:
// s.placeholderFunc if set, otherwise s.placeholder's own marker.
func (s *Sink) marker(n int) string {
	if s.placeholderFunc != nil {
		return s.placeholderFunc(n)
	}
	return s.placeholder.marker(n)
}

// quotedIdentifier returns name unchanged, or quoted via s.identifierQuote if
// set. See WithIdentifierQuote.
func (s *Sink) quotedIdentifier(name string) string {
	if s.identifierQuote == nil {
		return name
	}
	return s.identifierQuote(name)
}

// quotedTable renders s.table for use in generated SQL text, applying
// s.identifierQuote to each "."-separated segment of a schema-qualified name
// independently (e.g. "analytics.orders" becomes `"analytics"."orders"`).
func (s *Sink) quotedTable() string {
	if s.identifierQuote == nil {
		return s.table
	}
	parts := strings.Split(s.table, ".")
	for i, p := range parts {
		parts[i] = s.identifierQuote(p)
	}
	return strings.Join(parts, ".")
}

// sqlReservedWords is a small, non-exhaustive set of keywords reserved,
// unquoted, across most of SQLite, PostgreSQL, and MySQL — enough to catch a
// bare-keyword identifier at construction/open time with a clear error
// instead of a confusing driver-level syntax error later. See
// WithAdditionalReservedWords to extend it.
var sqlReservedWords = map[string]bool{
	"select": true, "insert": true, "update": true, "delete": true,
	"from": true, "where": true, "table": true, "order": true,
	"group": true, "by": true, "into": true, "values": true,
	"create": true, "drop": true, "alter": true, "and": true,
	"or": true, "not": true, "null": true, "index": true,
	"primary": true, "key": true, "default": true, "check": true,
	"unique": true, "returning": true, "user": true, "limit": true,
	"offset": true, "column": true, "case": true, "join": true,
	"using": true, "constraint": true, "as": true, "distinct": true,
	"references": true, "asc": true, "desc": true, "in": true,
	"is": true, "like": true, "exists": true, "cross": true,
	"inner": true, "outer": true, "on": true, "all": true,
	"any": true, "when": true, "then": true, "else": true,
	"end": true, "begin": true, "commit": true, "rollback": true,
	"transaction": true, "foreign": true, "having": true, "grant": true,
	"revoke": true, "with": true,
}

// isValidTableIdentifier reports whether name is valid as a table name: a
// plain identifier, or two "."-joined identifiers (schema-qualified, e.g.
// "analytics.orders"). Every segment is checked against the reserved-word
// list — an unquoted schema segment that's a bare reserved word (e.g.
// "select.orders") is just as unparseable as a reserved table name. extra is
// the calling Sink's additionalReservedWords, or nil if unset.
func isValidTableIdentifier(name string, extra map[string]bool) bool {
	if !hasValidTableSegments(name) {
		return false
	}
	for _, p := range strings.Split(name, ".") {
		if !hasValidIdentifierChars(p) {
			return false
		}
		if isReservedWord(p, extra) {
			return false
		}
	}
	return true
}

// hasValidTableSegments reports whether name has at most two "."-separated
// segments, none empty — a plain identifier or one "schema.table" pair. See
// WithIdentifierQuote for why this still applies even when it's set.
func hasValidTableSegments(name string) bool {
	if strings.Count(name, ".") > 1 {
		return false
	}
	for _, p := range strings.Split(name, ".") {
		if p == "" {
			return false
		}
	}
	return true
}

// isValidIdentifier reports whether name is ASCII letters/digits/underscores
// only, with no leading digit, and isn't a bare reserved word. It allowlists
// rather than denylists characters — a fail-closed tripwire against
// interpolating anything but a plain identifier into generated SQL — so it
// will reject some names an unquoted dialect would otherwise accept;
// WithIdentifierQuote is the escape hatch for those. extra is the calling
// Sink's additionalReservedWords, or nil if unset.
func isValidIdentifier(name string, extra map[string]bool) bool {
	return hasValidIdentifierChars(name) && !isReservedWord(name, extra)
}

// hasValidIdentifierChars is the character-set/leading-digit half of
// isValidIdentifier's rules, split out so isValidTableIdentifier can apply it
// to each "."-separated segment of a schema-qualified name individually.
func hasValidIdentifierChars(name string) bool {
	if len(name) == 0 {
		return false
	}
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_':
			continue
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
			continue
		default:
			return false
		}
	}
	return true
}

// isReservedWord reports whether name is a bare reserved word per
// sqlReservedWords or extra (the calling Sink's additionalReservedWords, or
// nil), matched case-insensitively.
func isReservedWord(name string, extra map[string]bool) bool {
	lower := strings.ToLower(name)
	return sqlReservedWords[lower] || extra[lower]
}

// nilPointerValue reports whether v is nil, including a typed nil pointer
// wrapped in a non-nil interface (which a plain v == nil check misses).
// Mirrors sqlsource's identical helper.
func nilPointerValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

// isNilRecord reports whether rec is nil, including a typed nil pointer
// wrapped in a non-nil interface (e.g. a custom etl.Batch whose Record
// method returns a typed-nil concrete arrow.Record). Mirrors filesink's
// identical helper.
func isNilRecord(rec arrow.Record) bool {
	if rec == nil {
		return true
	}
	rv := reflect.ValueOf(rec)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}
