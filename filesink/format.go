package filesink

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// Format encodes batches into a specific file format and writes them to an
// io.Writer. Format is open so other columnar formats can be added outside
// this package, consistent with etl.Source/Processor/Sink also being open
// interfaces.
type Format interface {
	// ContentType is used as the default blob.WriterOptions.ContentType for
	// objects written in this format.
	ContentType() string

	// NewWriter returns a RecordWriter that encodes records matching schema
	// to w. w is not closed by the returned RecordWriter.
	NewWriter(schema *arrow.Schema, w io.Writer) (RecordWriter, error)
}

// RecordWriter writes records to a single stream and finalizes the stream's
// format-level footer/metadata on Close. Close does not close the
// underlying io.Writer.
type RecordWriter interface {
	Write(rec arrow.Record) error
	Close() error
}

type arrowIPCFormat struct{}

// ArrowIPC selects the Arrow IPC file format (Feather V2).
func ArrowIPC() Format { return arrowIPCFormat{} }

func (arrowIPCFormat) ContentType() string { return "application/vnd.apache.arrow.file" }

func (arrowIPCFormat) NewWriter(schema *arrow.Schema, w io.Writer) (RecordWriter, error) {
	return ipc.NewFileWriter(w, ipc.WithSchema(schema))
}

type parquetFormat struct {
	compression compress.Compression
}

// ParquetOption configures the Parquet format.
type ParquetOption func(*parquetFormat)

// WithCompression sets the Parquet compression codec. Parquet defaults to
// Snappy.
func WithCompression(codec compress.Compression) ParquetOption {
	return func(f *parquetFormat) { f.compression = codec }
}

// Parquet selects the Parquet file format. By default it compresses with
// Snappy and stores the exact Arrow schema in file metadata (rather than
// relying on Parquet's lossier physical-type inference) so written data can
// be read back with its original Arrow types.
func Parquet(opts ...ParquetOption) Format {
	f := parquetFormat{compression: compress.Codecs.Snappy}
	for _, opt := range opts {
		opt(&f)
	}
	return f
}

func (parquetFormat) ContentType() string { return "application/vnd.apache.parquet" }

func (f parquetFormat) NewWriter(schema *arrow.Schema, w io.Writer) (RecordWriter, error) {
	props := parquet.NewWriterProperties(parquet.WithCompression(f.compression))
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	return pqarrow.NewFileWriter(schema, w, props, arrProps)
}
