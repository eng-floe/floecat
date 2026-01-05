# System table scanning

This document explains the current streaming system-table scan contract. Arrow IPC is the default response format, but the legacy `ROWS` path remains available for clients that still expect text-like row payloads. For background on system object definitions, scanners, and providers, see [System Objects](system-objects.md).

## RPC contract

`ScanSystemTableRequest` takes:
1. `query_id` – the caller’s query context.
2. `table_id` – the system table being queried.
3. `required_columns` – optional projection.
4. `predicates` – canonical filters exported from `SystemRowFilter`.
5. `output_format` – prefers `ROWS` vs `ARROW_IPC`. Unspecified defaults to Arrow.

The service no longer returns a single response message. Instead it streams `ScanSystemTableChunk` messages:

```
message ScanSystemTableChunk {
  oneof payload {
    bytes arrow_schema_ipc = 1;
    bytes arrow_batch_ipc = 2;
    SystemTableRow row = 3;
  }
}
```

When Arrow is requested, the stream always begins with `arrow_schema_ipc`, then the remaining batches appear as `arrow_batch_ipc`. When `output_format = ROWS`, the stream only emits row chunks, one per `SystemTableRow`.

## Execution paths

### Arrow-first path (default)

1. `QuerySystemScanServiceImpl` resolves the scanner, gathers the predicates/projections, and produces an `ArrowScanPlan`. The RootAllocator size caps itself via `ai.floedb.floecat.arrow.max-bytes` (default **1 GiB**) so a single scan cannot exhaust native memory.
2. If the scanner advertises `ScanOutputFormat.ARROW_IPC`, the scanner itself emits `ColumnarBatch` instances that already respect the schema. Otherwise the adapter takes the filtered/projected `SystemObjectRow` stream, batches it (`RowStreamToArrowBatchAdapter`), and builds Arrow `VectorSchemaRoot` objects.
3. Each `ColumnarBatch` flows through optional `ArrowFilterOperator` (vectorized mask evaluation) and optional `ArrowProjectOperator` (zero-copy `TransferPair` projection), then the Arrow IPC message serializer emits a schema message once, followed by record-batch messages for each batch.
4. The first streamed chunk carries that schema message, subsequent `arrow_batch_ipc` chunks carry only the record-batch bodies, and the allocator is closed at the end so no native buffers leak.

### Row compatibility path

When `output_format = ROWS`, the service now streams `SystemObjectRow`s directly: the scanner `Stream` feeds `SystemRowFilter.filter(...)` and `SystemRowProjector.project(...)`, and each projected row is emitted as a `row` chunk without Arrow conversion. This path remains compatibility-only; Arrow consumers route through the columnar pipeline to benefit from zero-copy projection and vectorized filtering.

## Predicate & projection semantics

`ArrowFilterOperator` sees the `Expr` tree produced by `SystemRowFilter.EXPRESSION_PROVIDER`, evaluates each leaf over typed vectors (integer, float, varchar, boolean, `IS NULL`), builds a boolean mask, and copies the matching vectors into a new root. Invalid boolean literals (anything other than `true`/`false`, case-insensitive) produce zero matches, and numeric comparisons are exact (floating-point predicates require exact equality). `ArrowProjectOperator` transfers only the requested columns via `TransferPair`, so projection is always zero-copy even when the order changes; unknown columns are ignored and simply dropped from the output batch.

## Streaming guarantees & testing

`QuerySystemScanServiceIT` validates the new server-streaming contract (rows in `ROWS` mode, schema followed by batches in Arrow mode, and Arrow as the default when unspecified). Arrow-specific unit tests include `RowStreamToArrowBatchAdapterTest`, `ArrowFilterOperatorTest`, and `ArrowProjectOperatorTest`. This layered approach keeps the legacy predicate/projector logic as the truth while shipping a fully Arrow-native execution lane on the default path.
