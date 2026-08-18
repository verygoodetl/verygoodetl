package filesink

import (
	"encoding/base64"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type csvFormat struct {
	delimiter         rune
	useCRLF           bool
	writeHeader       bool
	nullString        string
	escapeCharacter   string
	alwaysEncapsulate bool
	escapeFormulas    bool
}

// CSVOption configures the CSV format.
type CSVOption func(*csvFormat)

// WithDelimiter sets the field delimiter. Defaults to ','.
func WithDelimiter(r rune) CSVOption {
	return func(f *csvFormat) { f.delimiter = r }
}

// WithCRLF selects "\r\n" as the line terminator instead of the default
// "\n". Note a standard-library limitation this inherits: encoding/csv
// silently drops a bare '\r' inside field content (one not immediately
// followed by '\n') when CRLF mode is enabled.
func WithCRLF(useCRLF bool) CSVOption {
	return func(f *csvFormat) { f.useCRLF = useCRLF }
}

// WithHeader controls whether a header row of field names is written.
// Defaults to true.
func WithHeader(write bool) CSVOption {
	return func(f *csvFormat) { f.writeHeader = write }
}

// WithNullString sets the text written for a null value. Defaults to an
// empty string. Some consumers expect a specific sentinel instead — for
// example Postgres's COPY command distinguishes a quoted empty string from
// an unquoted one, and conventionally uses `\N` for NULL in text format.
func WithNullString(s string) CSVOption {
	return func(f *csvFormat) { f.nullString = s }
}

// WithEscapeCharacter sets the sequence written before an embedded quote
// character inside a quoted field. Defaults to an empty string, which
// selects RFC 4180's standard doubled-quote escaping. Only override this to
// interoperate with a consumer that expects a different convention (e.g. a
// backslash) — output written with a non-default escape character is not
// RFC 4180-compliant and will not round-trip through a standard CSV reader.
func WithEscapeCharacter(s string) CSVOption {
	return func(f *csvFormat) { f.escapeCharacter = s }
}

// WithAlwaysEncapsulate quotes every field unconditionally, instead of only
// fields that actually require it (those containing the delimiter, a quote
// character, \r, \n, or leading whitespace). Defaults to false.
func WithAlwaysEncapsulate(always bool) CSVOption {
	return func(f *csvFormat) { f.alwaysEncapsulate = always }
}

// WithEscapeFormulas prefixes a string or binary (base64) field's rendered
// text with a leading apostrophe whenever it starts with a character many
// spreadsheet applications (Excel, LibreOffice, Google Sheets) treat as the
// start of a formula on import — '=', '+', '-', '@', a tab, or a carriage
// return. This is a real risk whenever a CSV file containing untrusted or
// user-supplied text might be opened directly in spreadsheet software: this
// package's own RFC 4180 quoting only controls how a CSV *parser* reads a
// field, and does nothing to stop a spreadsheet application from then
// interpreting well-formed, correctly-quoted cell text as a formula once
// it's loaded.
//
// Defaults to false, since prepending an apostrophe changes the field's
// exact byte content — inappropriate for a machine-to-machine CSV consumer
// that expects the source value verbatim. Opt in specifically when a human
// may open the output in spreadsheet software.
func WithEscapeFormulas(escape bool) CSVOption {
	return func(f *csvFormat) { f.escapeFormulas = escape }
}

// formulaTriggerChars are the leading characters that make a spreadsheet
// application treat an imported CSV cell as a formula rather than literal
// text.
const formulaTriggerChars = "=+-@\t\r"

// escapeFormula prefixes s with an apostrophe if its first byte is one of
// formulaTriggerChars. Spreadsheet applications hide a leading apostrophe
// and treat the rest of the cell as text, so this defuses the formula
// without otherwise changing what a reader sees.
func escapeFormula(s string) string {
	if s == "" || strings.IndexByte(formulaTriggerChars, s[0]) < 0 {
		return s
	}
	return "'" + s
}

// withFormulaEscape wraps fm so its rendered text is passed through
// escapeFormula.
func withFormulaEscape(fm formatter) formatter {
	return func(arr arrow.Array, i int) string {
		return escapeFormula(fm(arr, i))
	}
}

// isFormulaEscapable reports whether dt's rendered CSV text can contain
// arbitrary, human-authored or human-observable characters — as opposed to
// a fixed-format numeric/boolean/timestamp rendering, which can never begin
// with a formula-trigger character.
func isFormulaEscapable(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.BINARY:
		return true
	default:
		return false
	}
}

// CSV selects the CSV file format. Column order and names come from the
// schema (not inferred from data). Encoding defaults to RFC 4180 — minimal
// quoting, doubled-quote escaping, no formula escaping — via a small fork of
// the standard library's encoding/csv (see csv_writer.go) that adds
// WithEscapeCharacter and WithAlwaysEncapsulate as opt-in, non-standard
// extensions. WithEscapeFormulas is a separate opt-in extension, layered on
// top rather than forked from encoding/csv, guarding against spreadsheet
// formula injection.
func CSV(opts ...CSVOption) Format {
	f := csvFormat{delimiter: ',', writeHeader: true}
	for _, opt := range opts {
		opt(&f)
	}
	return f
}

func (csvFormat) ContentType() string { return "text/csv" }

func (f csvFormat) NewWriter(schema *arrow.Schema, w io.Writer) (RecordWriter, error) {
	formatters := make([]formatter, schema.NumFields())
	for i, field := range schema.Fields() {
		fm, err := csvFormatterFor(field.Type)
		if err != nil {
			return nil, fmt.Errorf("field %d (%s): %w", i, field.Name, err)
		}
		if f.escapeFormulas && isFormulaEscapable(field.Type) {
			fm = withFormulaEscape(fm)
		}
		formatters[i] = fm
	}

	cw := newCSVWriter(w)
	cw.Comma = f.delimiter
	cw.UseCRLF = f.useCRLF
	cw.EscapeCharacter = f.escapeCharacter
	cw.AlwaysEncapsulate = f.alwaysEncapsulate

	return &csvRecordWriter{
		w:              cw,
		schema:         schema,
		formatters:     formatters,
		nullString:     f.nullString,
		writeHeader:    f.writeHeader,
		escapeFormulas: f.escapeFormulas,
		row:            make([]string, schema.NumFields()),
	}, nil
}

type csvRecordWriter struct {
	w              *csvWriter
	schema         *arrow.Schema
	formatters     []formatter
	nullString     string
	writeHeader    bool
	escapeFormulas bool
	headerDone     bool
	row            []string
}

func (w *csvRecordWriter) Write(rec arrow.Record) error {
	if !csvSchemaCompatible(rec.Schema(), w.schema) {
		return fmt.Errorf("filesink: csv: record schema %s does not match writer schema %s", rec.Schema(), w.schema)
	}

	if w.writeHeader && !w.headerDone {
		header := make([]string, w.schema.NumFields())
		for i, field := range w.schema.Fields() {
			name := field.Name
			if w.escapeFormulas {
				name = escapeFormula(name)
			}
			header[i] = name
		}
		if err := w.w.Write(header); err != nil {
			return fmt.Errorf("write header: %w", err)
		}
		w.headerDone = true
	}

	cols := make([]arrow.Array, rec.NumCols())
	for c := range cols {
		cols[c] = rec.Column(c)
	}

	for r := 0; r < int(rec.NumRows()); r++ {
		for c, col := range cols {
			if col.IsNull(r) {
				w.row[c] = w.nullString
				continue
			}
			w.row[c] = w.formatters[c](col, r)
		}
		if err := w.w.Write(w.row); err != nil {
			return fmt.Errorf("write row %d: %w", r, err)
		}
	}

	return nil
}

// Close flushes any output buffered across every prior Write call. Write
// itself deliberately does not flush per batch — csvWriter wraps a
// bufio.Writer, so flushing here rather than after every batch lets writes
// coalesce into fewer, larger calls to the underlying blob writer.
func (w *csvRecordWriter) Close() error {
	w.w.Flush()
	return w.w.Error()
}

// csvSchemaCompatible reports whether a and b have the same number of
// fields, with each pair sharing a name and type. CSV serialization renders
// a field using only its name (for the header) and type (to pick a
// formatter), so this deliberately ignores nullability and field metadata —
// a batch whose schema differs from the writer's only in those respects
// (e.g. a projected or joined batch) renders identical CSV output and
// should not be rejected.
func csvSchemaCompatible(a, b *arrow.Schema) bool {
	if a.NumFields() != b.NumFields() {
		return false
	}
	for i, fa := range a.Fields() {
		fb := b.Field(i)
		if fa.Name != fb.Name || !arrow.TypeEqual(fa.Type, fb.Type) {
			return false
		}
	}
	return true
}

// formatter renders one non-null value from column arr at row i as CSV
// field text. Unlike sqlsource's converters, no error is possible here:
// these read from Arrow's own already-typed arrays, not messy driver
// values.
type formatter func(arr arrow.Array, i int) string

func csvFormatterFor(dt arrow.DataType) (formatter, error) {
	switch dt.ID() {
	case arrow.INT64:
		return func(arr arrow.Array, i int) string {
			return strconv.FormatInt(arr.(*array.Int64).Value(i), 10)
		}, nil
	case arrow.FLOAT64:
		return func(arr arrow.Array, i int) string {
			return strconv.FormatFloat(arr.(*array.Float64).Value(i), 'g', -1, 64)
		}, nil
	case arrow.BOOL:
		return func(arr arrow.Array, i int) string {
			return strconv.FormatBool(arr.(*array.Boolean).Value(i))
		}, nil
	case arrow.STRING:
		return func(arr arrow.Array, i int) string {
			return arr.(*array.String).Value(i)
		}, nil
	case arrow.BINARY:
		return func(arr arrow.Array, i int) string {
			return base64.StdEncoding.EncodeToString(arr.(*array.Binary).Value(i))
		}, nil
	case arrow.TIMESTAMP:
		unit := dt.(*arrow.TimestampType).Unit
		return func(arr arrow.Array, i int) string {
			ts := arr.(*array.Timestamp).Value(i)
			return ts.ToTime(unit).Format(time.RFC3339Nano)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}
