// Package archive provides etl.Sink implementations that write batches to a
// durable archive object — Parquet or Arrow IPC (Feather V2) — using
// gocloud.dev/blob as the storage abstraction. The same Sink works against
// S3, Google Cloud Storage, or local disk purely by which *blob.Bucket the
// caller supplies.
//
// This package depends only on gocloud.dev/blob's core types, never on a
// cloud SDK. To target S3 or GCS, the caller blank-imports the matching
// driver package and opens a bucket by URL:
//
//	import (
//		"gocloud.dev/blob"
//		_ "gocloud.dev/blob/s3blob"
//	)
//
//	bucket, err := blob.OpenBucket(ctx, "s3://my-bucket?region=us-west-2")
package archive
