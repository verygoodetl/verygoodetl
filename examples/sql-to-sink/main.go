// Command sql-to-sink demonstrates sqlsource.Source reading a real database
// (an in-memory SQLite instance, for a self-contained example) into a
// pipeline, with no archival step.
//
//	go run ./examples/sql-to-sink
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

func main() {
	ctx := context.Background()

	// A real deployment would open a real database instead, e.g.:
	//   db, err := sql.Open("postgres", "postgres://user:pass@host/db")
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

	// sqlsource requires an explicit schema rather than inferring column
	// types from driver metadata — see ARCHITECTURE.md for why.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	source, err := sqlsource.New(db, "SELECT id, name FROM orders ORDER BY id", schema)
	if err != nil {
		panic(err)
	}

	pipeline := etl.New()
	pipeline.From(source).To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			names := b.Record().Column(1).(*array.String)
			for i := 0; i < names.Len(); i++ {
				fmt.Println(names.Value(i))
			}
			return nil
		},
	})

	if err := pipeline.Run(ctx); err != nil {
		panic(err)
	}
}
