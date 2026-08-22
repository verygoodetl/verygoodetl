// Command sql-to-archive demonstrates the full story this repo is built
// around: extract from a real source (a SQL database), archive the raw
// batches durably, and process the same stream into a reporting sink —
// with the reporting sink rebuildable from the archive alone.
//
//	go run ./examples/sql-to-archive
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"gocloud.dev/blob"
	"gocloud.dev/blob/memblob"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/filesink"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, `CREATE TABLE orders (id INTEGER, name TEXT)`); err != nil {
		panic(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO orders (id, name) VALUES (1, 'widget'), (2, 'gadget'), (3, 'gizmo')`); err != nil {
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

	// A real deployment would open an S3 or GCS bucket here instead:
	//   bucket, err := blob.OpenBucket(ctx, "gs://my-bucket")
	bucket := memblob.OpenBucket(nil)
	defer bucket.Close()

	pipeline := etl.New()
	orders := pipeline.From(source)

	// The raw, unmodified rows go to a durable archive.
	// WithWriterOptions{IfNotExist: true} makes this an archive rather than
	// an ordinary overwrite-on-every-run file sink: writing orders.parquet
	// a second time fails.
	orders.CopyTo(filesink.New(bucket, "orders.parquet", filesink.Parquet(),
		filesink.WithWriterOptions(&blob.WriterOptions{IfNotExist: true})))

	// ...while the same rows also flow to a reporting sink. If the
	// reporting sink's logic ever needs to change, it can be rebuilt from
	// orders.parquet without re-querying the source database.
	var reported []string
	orders.To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			names := b.Record().Column(1).(*array.String)
			for i := 0; i < names.Len(); i++ {
				reported = append(reported, names.Value(i))
			}
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

	fmt.Println("reported:", reported)
	fmt.Println("archived to orders.parquet:", archived)
}
