# Arrow-based system table scans

This module documents the new, optional Arrow output path introduced in Phase A of the migration plan.

## New RPC surface

`ScanSystemTableRequest` now exposes an `OutputFormat` enum (`ROWS` vs. `ARROW_IPC`). Clients can request `ARROW_IPC` when they want columnar payloads. The response keeps both `rows` *and* `arrow_batches` repeated fields, but only one of them is populated depending on the selected format. The existing text/row contract remains unchanged unless the caller opts-in to `ARROW_IPC`.

## Columnar execution plumbing

- `ColumnarBatch` (package `ai.floedb.floecat.systemcatalog.columnar`) defines the arrow-native execution contract: every batch exposes a `VectorSchemaRoot` and must be closed by the caller to release vectors.
- `RowStreamToArrowBatchAdapter` wraps existing `SystemObjectRow` streams. It batches `SystemObjectRow`s, normalizes array-valued columns (stringifies them since the Arrow schema is flat), builds an Arrow `Schema` from `SchemaColumn` metadata, and delegates to `ArrowConversion.fill()` to populate the vectors. This adapter is intentionally conservative—its constructor takes a `BufferAllocator`, the logical schema, and a batch size (default 512 in the service).
- `RowStreamToArrowBatchAdapterTest` proves the adapter emits valid Arrow vectors for integers, strings, and array columns.

## Service integration

`QuerySystemScanServiceImpl` still resolves a scanner via `SystemScannerResolver`, runs `SystemRowFilter`/`SystemRowProjector` (the row helpers stay as the reference semantics), and then branches on `request.getOutputFormat()`. When `ARROW_IPC` is requested, the new `arrowResponse` helper:

1. Creates a `RootAllocator` (engine-owned per request) and the `RowStreamToArrowBatchAdapter`.
2. Streams `ColumnarBatch` instances, serializing each `VectorSchemaRoot` with `ArrowStreamWriter` into a `ByteString`.
3. Accumulates those batches in `ScanSystemTableResponse.arrow_batches`.
4. Closes every `ColumnarBatch`.

`serializeArrowBatch`/`serializeArrowRoot` wrap all Arrow and IO resources in try-with-resources so the allocator and vectors are released immediately.

## Ownership notes

- Every `ColumnarBatch` owns its `VectorSchemaRoot` vectors and implements `AutoCloseable`. The service consumes the stream and closes each batch right after serialization, so the adapter never leaves dangling buffers.
- The `BufferAllocator` is created inside `arrowResponse` and closed before returning to the caller, ensuring the Arrow path does not leak native memory.

## Arrow filtering (Phase D)

- Arrow filtering runs during the Arrow path when the system property `ai.floedb.floecat.arrow-filter-enabled` is set to `true`. The new `ArrowFilterOperator` evaluates the `Expr` tree produced by `SystemRowFilter.EXPRESSION_PROVIDER` directly against each `VectorSchemaRoot`, produces a boolean mask of matching rows, and then copies the selected vectors into a fresh `VectorSchemaRoot` before the Arrow IPC serializer sees the batch.
- The row helpers remain the reference path, so you can compare row-filtered results with Arrow-filtered batches while the feature gate is active; once confidence is high, you can enable the property to make the Arrow filter path authoritative without touching the legacy code.

## Arrow projection (Phase E)

- The Arrow projection operator runs on the Arrow path independent of filtering, zero-copying only the requested columns into a new `VectorSchemaRoot` by transferring the underlying buffers (`TransferPair`) from the filtered batch. This ensures the RPC response contains exactly the columns the caller asked for without materializing extra rows or objects.
- When `ScanSystemTableRequest.required_columns` is empty, the operator becomes a no-op and the original batch flows through (so the arrow path degrades gracefully for clients that still want the full schema).

## Testing

The new adapter is covered by `RowStreamToArrowBatchAdapterTest`. The existing row-filter/projector tests continue to serve as the semantic baseline; the Arrow path reuses them before conversion, so predicate/projection coverage remains untouched.

Future phases can replace `RowStreamToArrowBatchAdapter` with native scanners, but Phase A keeps the row helpers as the ground truth while letting clients stream Arrow IPC without touching the row-based execution stack.
