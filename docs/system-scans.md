# System table scanning

This document explains the current system-table scan paths. Arrow IPC is now the default response format, but `ROWS` is still available as a compatibility option for clients that rely on the legacy text-based serialization. For background on system object definitions, scanners, and providers, see [System Objects](system-objects.md).

## Output formats

`ScanSystemTableRequest` supports an `OutputFormat` enum (`ROWS` vs. `ARROW_IPC`). `ARROW_IPC` is the default, so omitting the field behaves the same as explicitly requesting Arrow batches. If the caller explicitly sets `output_format = ROWS`, the request is routed to the legacy row production path (`SystemRowFilter`, `SystemRowProjector`, etc.) and the response returns `rows`. Each response still contains both `rows` and `arrow_batches`, but only one field is populated depending on the requested format.

## Arrow-capable scanners

Scanners can now declare which formats they support via `ScanOutputFormat`. The default implementation only supports `ROWS`, but Arrow-native scanners override `supportedFormats()` and implement `scanArrow(ctx, predicate, requiredColumns, allocator)` so the service can stream `ColumnarBatch` objects directly. `QuerySystemScanServiceImpl` prefers this Arrow stream when the request asks for Arrow, falling back to rows and the adapter otherwise.

## Rows execution path

The row path is retained so existing clients receive the familiar list of `SystemTableRow` objects. When `ROWS` is selected, the service materializes `SystemObjectRow`s, applies `SystemRowFilter`/`SystemRowProjector`, maps them to RPC rows via `SystemRowMappers`, and returns the `rows` list in `ScanSystemTableResponse`. This path is marked as compatibility-only; the Arrow path shares the same reference filter/projector logic to keep semantics identical, then converts the remaining rows into columnar batches before applying any Arrow-native filtering or projection.

## Arrow execution path

If the scanner supports `ScanOutputFormat.ARROW_IPC`, the service calls `scanArrow(...)` and streams the batches directly (via `arrowResponseFromScanner`). Otherwise, it still materializes `SystemObjectRow`s and reuses the adapter-based path below.

1. `QuerySystemScanServiceImpl` resolves the scanner, runs `SystemRowFilter`/`SystemRowProjector` for semantic parity, and hands the resulting `SystemObjectRow` stream to `RowStreamToArrowBatchAdapter` when the scanner only produces rows.
2. The adapter batches rows (default size 512), normalizes array-valued columns, builds an Arrow `Schema` from the scanner’s `SchemaColumn` metadata, and populates a `VectorSchemaRoot` via `ArrowConversion.fill()`.
3. Each `ColumnarBatch` (implements `AutoCloseable`) flows through the optional `ArrowFilterOperator`, optional `ArrowProjectOperator`, and `serializeArrowBatch`, which writes the root with `ArrowStreamWriter`.
4. The service closes the batch immediately after serialization and streams the serialized IPC batches back to the caller.

`ColumnarBatch` owns its vectors, and the allocator used by `arrowResponse` is closed before the RPC returns, so the Arrow path never leaks native buffers.

## Arrow filtering & projection

When the system property `ai.floedb.floecat.arrow-filter-enabled` is `true`, `ArrowFilterOperator` evaluates the predicate `Expr` tree (exported by `SystemRowFilter.EXPRESSION_PROVIDER`) directly against the vectors, builds a boolean mask, copies the selected vectors into a fresh `VectorSchemaRoot`, and produces the filtered batch for serialization.

`ArrowProjectOperator` only runs when the caller requested a subset of columns. It transfers the requested vectors via `TransferPair`, so projection is zero-copy even when the requested order differs from the schema. If `required_columns` is empty, the projection step is skipped and the original vectors flow through.

## Compatibility & testing

`ROWS` remains the compatibility mode: tests that need row-based assertions request `OutputFormat.ROWS` explicitly, while the new `informationSchemaReturnsArrowBatchesForQueryCatalog` integration test covers the Arrow output path end-to-end and inspects the decoded batches. Arrow-specific unit tests include `RowStreamToArrowBatchAdapterTest`, `ArrowFilterOperatorTest`, and `ArrowProjectOperatorTest`.
