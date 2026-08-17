package etl_test

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"gocloud.dev/blob/memblob"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/archive"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

type exampleSource struct{}

func (exampleSource) Run(ctx context.Context, out etl.Output) error {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues([]int64{10, 20, 30}, nil)

	values := builder.NewArray()
	defer values.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := array.NewRecord(schema, []arrow.Array{values}, 3)
	defer record.Release()

	return out.Send(ctx, etl.NewBatch(record))
}

func Example() {
	pipeline := etl.New()
	stream := pipeline.From(exampleSource{})

	stream.To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, batch etl.Batch) error {
			fmt.Println(batch.NumRows())
			return nil
		},
	})

	if err := pipeline.Run(context.Background()); err != nil {
		panic(err)
	}

	// Output:
	// 3
}

type ordersSource struct {
	record arrow.Record
}

func (s ordersSource) Run(ctx context.Context, out etl.Output) error {
	return out.Send(ctx, etl.NewBatch(s.record))
}

// Example_archival shows a single stream of batches fanning out to both a
// durable archive (via CopyTo) and a processed sink, using archive.Sink
// backed by an in-memory bucket.
func Example_archival() {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues([]int64{1, 2, 3}, nil)
	values := builder.NewArray()
	record := array.NewRecord(schema, []arrow.Array{values}, 3)
	values.Release()
	builder.Release()
	defer record.Release()

	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	pipeline := etl.New()
	orders := pipeline.From(ordersSource{record: record})
	orders.CopyTo(archive.NewSink(bucket, "orders.parquet", archive.Parquet()))

	rowCount := 0
	orders.To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			rowCount += int(b.NumRows())
			return nil
		},
	})

	if err := pipeline.Run(context.Background()); err != nil {
		panic(err)
	}

	archived, err := bucket.Exists(context.Background(), "orders.parquet")
	if err != nil {
		panic(err)
	}

	fmt.Println("rows processed:", rowCount)
	fmt.Println("archived:", archived)

	// Output:
	// rows processed: 3
	// archived: true
}

// Example_sqlSource shows a sqlsource.Source reading a real (in-memory)
// SQLite database into a pipeline.
func Example_sqlSource() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE orders (id INTEGER, name TEXT)`); err != nil {
		panic(err)
	}
	if _, err := db.Exec(`INSERT INTO orders (id, name) VALUES (1, 'widget'), (2, 'gadget')`); err != nil {
		panic(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	source, err := sqlsource.New(db, "SELECT id, name FROM orders ORDER BY id", schema)
	if err != nil {
		panic(err)
	}

	var names []string
	pipeline := etl.New()
	pipeline.From(source).To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			col := b.Record().Column(1).(*array.String)
			for i := 0; i < col.Len(); i++ {
				names = append(names, col.Value(i))
			}
			return nil
		},
	})

	if err := pipeline.Run(context.Background()); err != nil {
		panic(err)
	}

	fmt.Println(names)

	// Output:
	// [widget gadget]
}
