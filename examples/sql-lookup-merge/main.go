// Command sql-lookup-merge demonstrates querying two unconnected databases
// and combining the results in Go: shipments from a "label-api" database
// are used to look up matching rows in a separate "app" database, and both
// the original label-api rows and the app-database lookup results are fed
// into a Pipeline.Merge that does the actual combining. No SQL join is
// possible here since the databases aren't connected to each other.
//
//	go run ./examples/sql-lookup-merge
package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	_ "modernc.org/sqlite"

	etl "github.com/verygoodetl/verygoodetl"
	"github.com/verygoodetl/verygoodetl/sqlsource"
)

// combiner joins label-api rows (schema: id, carrier) with app-database
// rows (schema: id, status, weight) by id. Pipeline.Merge delivers batches
// from both inputs through the same Process call with no input identity
// (see ARCHITECTURE.md's "Future multi-input semantics"), so which side a
// batch came from is determined here by its schema shape.
type combiner struct {
	carriers map[int64]string
	appData  map[int64]appRow
}

type appRow struct {
	status string
	weight float64
}

func newCombiner() *combiner {
	return &combiner{carriers: map[int64]string{}, appData: map[int64]appRow{}}
}

func hasField(schema *arrow.Schema, name string) bool {
	for _, f := range schema.Fields() {
		if f.Name == name {
			return true
		}
	}
	return false
}

func (c *combiner) Process(ctx context.Context, b etl.Batch, out etl.Output) error {
	rec := b.Record()
	schema := rec.Schema()

	type match struct {
		id      int64
		carrier string
		app     appRow
	}
	var matches []match

	switch {
	case hasField(schema, "carrier"):
		ids := rec.Column(0).(*array.Int64)
		carriers := rec.Column(1).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			id, carrier := ids.Value(i), carriers.Value(i)
			if app, ok := c.appData[id]; ok {
				matches = append(matches, match{id, carrier, app})
				delete(c.appData, id)
			} else {
				c.carriers[id] = carrier
			}
		}
	case hasField(schema, "status"):
		ids := rec.Column(0).(*array.Int64)
		statuses := rec.Column(1).(*array.String)
		weights := rec.Column(2).(*array.Float64)
		for i := 0; i < int(rec.NumRows()); i++ {
			id := ids.Value(i)
			app := appRow{status: statuses.Value(i), weight: weights.Value(i)}
			if carrier, ok := c.carriers[id]; ok {
				matches = append(matches, match{id, carrier, app})
				delete(c.carriers, id)
			} else {
				c.appData[id] = app
			}
		}
	default:
		return fmt.Errorf("combiner: unrecognized batch schema %s", schema)
	}

	if len(matches) == 0 {
		return nil
	}

	idB := array.NewInt64Builder(memory.DefaultAllocator)
	defer idB.Release()
	carrierB := array.NewStringBuilder(memory.DefaultAllocator)
	defer carrierB.Release()
	statusB := array.NewStringBuilder(memory.DefaultAllocator)
	defer statusB.Release()
	weightB := array.NewFloat64Builder(memory.DefaultAllocator)
	defer weightB.Release()

	for _, m := range matches {
		idB.Append(m.id)
		carrierB.Append(m.carrier)
		statusB.Append(m.app.status)
		weightB.Append(m.app.weight)
	}

	combinedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "carrier", Type: arrow.BinaryTypes.String},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	cols := []arrow.Array{idB.NewArray(), carrierB.NewArray(), statusB.NewArray(), weightB.NewArray()}
	combinedRec := array.NewRecord(combinedSchema, cols, int64(len(matches)))
	for _, a := range cols {
		a.Release()
	}
	defer combinedRec.Release()

	return out.Send(ctx, etl.NewBatch(combinedRec))
}

func (c *combiner) Finish(ctx context.Context, out etl.Output) error {
	if len(c.carriers) > 0 || len(c.appData) > 0 {
		fmt.Printf("unmatched: %d label-api rows, %d app rows\n", len(c.carriers), len(c.appData))
	}
	return nil
}

func main() {
	ctx := context.Background()

	labelAPIDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer labelAPIDB.Close()
	if _, err := labelAPIDB.ExecContext(ctx, `CREATE TABLE shipments (id INTEGER, carrier TEXT)`); err != nil {
		panic(err)
	}
	if _, err := labelAPIDB.ExecContext(ctx, `INSERT INTO shipments (id, carrier) VALUES (1, 'ups'), (2, 'fedex'), (3, 'usps')`); err != nil {
		panic(err)
	}

	appDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		panic(err)
	}
	defer appDB.Close()
	if _, err := appDB.ExecContext(ctx, `CREATE TABLE shipments (id INTEGER, status TEXT, weight REAL)`); err != nil {
		panic(err)
	}
	if _, err := appDB.ExecContext(ctx, `INSERT INTO shipments (id, status, weight) VALUES (1, 'delivered', 3.2), (2, 'in_transit', 1.1)`); err != nil {
		panic(err)
	}

	labelAPISchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "carrier", Type: arrow.BinaryTypes.String},
	}, nil)
	source, err := sqlsource.New(labelAPIDB, "SELECT id, carrier FROM shipments ORDER BY id", labelAPISchema)
	if err != nil {
		panic(err)
	}

	appSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "status", Type: arrow.BinaryTypes.String},
		{Name: "weight", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	// generate builds "WHERE id IN (?, ?, ...)" from whatever batch of
	// label-api shipment ids arrived — the two databases aren't connected,
	// so this is the only way to find the matching app-database rows.
	// LookupKeys handles skipping nulls and de-duplicating ids for us.
	generate := func(b etl.Batch) (string, []any, error) {
		args, err := sqlsource.LookupKeys(b, 0)
		if err != nil {
			return "", nil, err
		}
		placeholders := make([]string, len(args))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query := fmt.Sprintf("SELECT id, status, weight FROM shipments WHERE id IN (%s)", strings.Join(placeholders, ","))
		return query, args, nil
	}
	lookup, err := sqlsource.NewLookup(appDB, generate, appSchema)
	if err != nil {
		panic(err)
	}

	pipeline := etl.New()
	labelAPI := pipeline.From(source)
	appMatches := labelAPI.Process(lookup)

	// labelAPI is reused directly as a Merge input alongside appMatches:
	// no "passthrough" processor is needed, since a Stream already fans out
	// to multiple downstream attachments on its own.
	combined := pipeline.Merge(newCombiner(), labelAPI, appMatches)

	var results []string
	combined.To(etl.SinkFuncs{
		ConsumeFunc: func(_ context.Context, b etl.Batch) error {
			rec := b.Record()
			ids := rec.Column(0).(*array.Int64)
			carriers := rec.Column(1).(*array.String)
			statuses := rec.Column(2).(*array.String)
			weights := rec.Column(3).(*array.Float64)
			for i := 0; i < int(rec.NumRows()); i++ {
				results = append(results, fmt.Sprintf("id=%d carrier=%s status=%s weight=%.1f",
					ids.Value(i), carriers.Value(i), statuses.Value(i), weights.Value(i)))
			}
			return nil
		},
	})

	if err := pipeline.Run(ctx); err != nil {
		panic(err)
	}

	for _, r := range results {
		fmt.Println(r)
	}
}
