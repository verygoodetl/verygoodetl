// Command sql-to-sql demonstrates sqlsource.Source feeding sqlsink.Sink
// directly: extract from one table, load into another, with
// sqlsink.WithUpsertClause opted in so re-running the pipeline updates
// existing rows instead of failing on the primary key.
//
//	go run ./examples/sql-to-sql
package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsink"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

func main() {
	ctx := context.Background()

	// Same *sql.DB for source and destination purely to keep this example
	// self-contained; a real deployment would likely use two.
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
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
	if _, err := db.ExecContext(ctx, `CREATE TABLE reporting_orders (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		panic(err)
	}

	// sqlsource requires an explicit schema (see ARCHITECTURE.md for why);
	// sqlsink needs none — it derives columns from each batch's own schema.
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)

	loadOrders := func() {
		// Deliberately small so this example's three rows span multiple
		// batches (and transactions); size much larger in production to
		// amortize per-transaction commit overhead.
		source, err := sqlsource.New(db, "SELECT id, name FROM orders ORDER BY id", schema, sqlsource.WithBatchSize(2))
		if err != nil {
			panic(err)
		}

		// A Sink is single-use, so a fresh one is built for each run.
		sink := sqlsink.New(db, "reporting_orders", sqlsink.WithUpsertClause(
			"ON CONFLICT (id) DO UPDATE SET name = excluded.name",
		))

		pipeline := etl.New()
		pipeline.From(source).To(sink)
		if err := pipeline.Run(ctx); err != nil {
			panic(err)
		}
	}

	loadOrders()
	loadOrders()

	rows, err := db.QueryContext(ctx, `SELECT id, name FROM reporting_orders ORDER BY id`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			panic(err)
		}
		fmt.Println(id, name)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}
