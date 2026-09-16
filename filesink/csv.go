package filesink

import (
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
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

// WithCRLF selects "\r\n" as the line terminator instead of "\n". Inherits
// a stdlib limitation: a bare '\r' in field content (not followed by '\n')
// is silently dropped in CRLF mode.
func WithCRLF(useCRLF bool) CSVOption {
	return func(f *csvFormat) { f.useCRLF = useCRLF }
}

// WithHeader controls whether a header row of field names is written.
// Defaults to true.
func WithHeader(write bool) CSVOption {
	return func(f *csvFormat) { f.writeHeader = write }
}

// WithNullString sets the text written for a null value. Defaults to an
// empty string; some consumers expect a sentinel instead (e.g. Postgres
// COPY's `\N` for text-format NULL).
func WithNullString(s string) CSVOption {
	return func(f *csvFormat) { f.nullString = s }
}

// WithEscapeCharacter sets the sequence written before an embedded quote in
// a quoted field. Defaults to empty, which selects RFC 4180's doubled-quote
// escaping. Override only to interoperate with a non-standard consumer
// (e.g. a backslash) — the output then won't round-trip through a standard
// CSV reader.
func WithEscapeCharacter(s string) CSVOption {
	return func(f *csvFormat) { f.escapeCharacter = s }
}

// WithAlwaysEncapsulate quotes every field unconditionally, instead of only
// fields that actually require it (those containing the delimiter, a quote
// character, \r, \n, or leading whitespace). Defaults to false.
func WithAlwaysEncapsulate(always bool) CSVOption {
	return func(f *csvFormat) { f.alwaysEncapsulate = always }
}

// WithEscapeFormulas prefixes a leading apostrophe onto any string or
// binary field whose rendered text starts with a formula-trigger character
// ('=', '+', '-', '@', tab, or CR), guarding against spreadsheet formula
// injection when untrusted CSV output may be opened directly in a
// spreadsheet. RFC 4180 quoting alone doesn't prevent this — it only
// governs how a CSV parser reads the field, not how a spreadsheet
// interprets the cell after loading.
//
// Defaults to false: prepending an apostrophe changes the field's exact
// byte content, which a machine-to-machine consumer wouldn't expect. Opt in
// only when a human may open the output in spreadsheet software.
func WithEscapeFormulas(escape bool) CSVOption {
	return func(f *csvFormat) { f.escapeFormulas = escape }
}

// formulaTriggerChars are the leading characters that make a spreadsheet
// application treat an imported CSV cell as a formula rather than literal
// text.
const formulaTriggerChars = "=+-@\t\r"

// escapeFormula prefixes s with an apostrophe if its first byte is a
// formula-trigger character; spreadsheets hide the apostrophe and treat
// the rest as text.
func escapeFormula(s string) string {
	if s == "" || strings.IndexByte(formulaTriggerChars, s[0]) < 0 {
		return s
	}
	return "'" + s
}

func withFormulaEscape(fm formatter) formatter {
	return func(arr arrow.Array, i int) string {
		return escapeFormula(fm(arr, i))
	}
}

// isFormulaEscapable reports whether dt's rendered text can start with a
// formula-trigger character. Only string/binary are escaped: negative
// INT64/FLOAT64 and +Inf actually render with a leading '-'/'+', but
// numeric fields are deliberately left unescaped so their values stay
// parseable as numbers.
func isFormulaEscapable(dt arrow.DataType) bool {
	switch dt.ID() {
	case arrow.STRING, arrow.BINARY:
		return true
	default:
		return false
	}
}

// CSV selects the CSV file format. Column order and names come from the
// schema. Encoding defaults to RFC 4180 (minimal quoting, doubled-quote
// escaping, no formula escaping) via a fork of encoding/csv (see
// csv_writer.go) that adds WithEscapeCharacter and WithAlwaysEncapsulate;
// WithEscapeFormulas is a separate, non-forked extension.
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

	nullString := f.nullString
	if f.escapeFormulas {
		nullString = escapeFormula(nullString)
	}

	return &csvRecordWriter{
		w:              cw,
		schema:         schema,
		formatters:     formatters,
		nullString:     nullString,
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

	if err := w.maybeWriteHeader(); err != nil {
		return err
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

// maybeWriteHeader writes the header row exactly once, on first call.
// Called from both Write and Close so a zero-batch write against an
// explicit schema (see Sink.Finish) still emits a header.
func (w *csvRecordWriter) maybeWriteHeader() error {
	if !w.writeHeader || w.headerDone {
		return nil
	}
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
	return nil
}

// Close emits the header if maybeWriteHeader hasn't already (e.g. a
// zero-batch write against an explicit schema), then flushes. Write itself
// doesn't flush per batch, letting buffered writes coalesce into fewer,
// larger calls to the underlying writer.
func (w *csvRecordWriter) Close() error {
	if err := w.maybeWriteHeader(); err != nil {
		return err
	}
	w.w.Flush()
	return w.w.Error()
}

// csvSchemaCompatible reports whether a and b share field names and types
// in order. CSV rendering only depends on name and type, so nullability
// and metadata differences (e.g. a projected or joined batch) don't count
// as a mismatch.
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
// field text. Unlike sqlsource's converters, no error is possible: it
// reads from Arrow's own typed arrays, not driver values.
type formatter func(arr arrow.Array, i int) string

// csvTimestampLocation resolves tz to the *time.Location a TIMESTAMP
// field's declared zone should render in, rather than always UTC. Empty or
// "UTC" (case-insensitive) short-circuits to time.UTC; other zones go
// through time.LoadLocation, which needs the host's zoneinfo or a
// blank-imported time/tzdata. Mirrors sqlsource's timestampTextLocation.
func csvTimestampLocation(tz string) (*time.Location, error) {
	if tz == "" || strings.EqualFold(tz, "UTC") {
		return time.UTC, nil
	}
	return time.LoadLocation(tz)
}

// isNilDataType reports whether dt is nil, including a typed nil pointer
// wrapped in a non-nil interface (which arrow.NewSchema's own dt == nil
// check misses). Mirrors sqlsource's nilPointerValue.
func isNilDataType(dt arrow.DataType) bool {
	if dt == nil {
		return true
	}
	rv := reflect.ValueOf(dt)
	return rv.Kind() == reflect.Ptr && rv.IsNil()
}

func csvFormatterFor(dt arrow.DataType) (formatter, error) {
	if isNilDataType(dt) {
		return nil, fmt.Errorf("nil field type")
	}
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
		ts := dt.(*arrow.TimestampType)
		loc, err := csvTimestampLocation(ts.TimeZone)
		if err != nil {
			return nil, fmt.Errorf("resolve declared time zone %q: %w", ts.TimeZone, err)
		}
		unit := ts.Unit
		return func(arr arrow.Array, i int) string {
			val := arr.(*array.Timestamp).Value(i)
			return val.ToTime(unit).In(loc).Format(time.RFC3339Nano)
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", dt)
	}
}
