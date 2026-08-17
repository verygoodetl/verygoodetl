// Command archive-fanout demonstrates a single stream of batches fanning
// out to both a durable archive and a processed sink, via CopyTo.
//
//	go run ./examples/archive-fanout
package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob/memblob"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/archive"
)

// ordersSource emits a single fixed batch of order IDs. A real Source would
// pull from wherever orders actually live (a queue, an API, a database via
// sqlsource — see examples/sql-to-archive).
type ordersSource struct{}

func (ordersSource) Run(ctx context.Context, out etl.Output) error {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)

	values := builder.NewArray()
	defer values.Release()

	record := array.NewRecord(schema, []arrow.Array{values}, 5)
	defer record.Release()

	return out.Send(ctx, etl.NewBatch(record))
}

func main() {
	ctx := context.Background()

	// A real deployment would open an S3 or GCS bucket here instead:
	//   bucket, err := blob.OpenBucket(ctx, "s3://my-bucket?region=us-west-2")
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	pipeline := etl.New()
	orders := pipeline.From(ordersSource{})

	// CopyTo preserves the stream: batches flow to the archive AND continue
	// on to the sink below, unmodified.
	orders.CopyTo(archive.NewSink(bucket, "orders.parquet", archive.Parquet()))

	rowCount := 0
	orders.To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			rowCount += int(b.NumRows())
			return nil
		},
	})

	if err := pipeline.Run(ctx); err != nil {
		panic(err)
	}

	archived, err := bucket.Exists(ctx, "orders.parquet")
	if err != nil {
		panic(err)
	}

	fmt.Printf("processed %d rows\n", rowCount)
	fmt.Printf("archived to orders.parquet: %v\n", archived)
}
