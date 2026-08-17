package etl_test

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	etl "github.com/verygoodetl/verygoodetl"
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
