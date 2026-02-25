# System table scans (gRPC)

This document defines the gRPC streaming contract for system-table scans.

For cross-transport overview, see [system-scans.md](system-scans.md).

## RPC contract

`ScanSystemTableRequest` takes:

1. `query_id` – caller query context.
2. `table_id` – system table `ResourceId`.
3. `required_columns` – optional projection.
4. `predicates` – canonical filter tree.
5. `output_format` – `ARROW_IPC` (default) or `ROWS`.

The response is a stream of `ScanSystemTableChunk`:

```protobuf
message ScanSystemTableChunk {
  oneof payload {
    bytes arrow_schema_ipc = 1;
    bytes arrow_batch_ipc = 2;
    SystemTableRow row = 3;
  }
}
```

## Output format behavior

### `ARROW_IPC` (default)

- First chunk: `arrow_schema_ipc`
- Remaining chunks: `arrow_batch_ipc`
- No `row` payloads in this mode

### `ROWS`

- Stream emits `row` chunks only
- No Arrow schema/batch payloads

## Execution path

### Arrow-first path (default)

1. `QuerySystemScanServiceImpl` resolves scanner + context.
2. Build `ArrowScanPlan` with predicates/projection.
3. If scanner output is already columnar (`ScanOutputFormat.ARROW_IPC`), consume batches
   directly; otherwise adapt row stream to Arrow batches (`RowStreamToArrowBatchAdapter`).
4. Apply vectorized filter/projection operators as needed, then stream through
   `ArrowBatchSerializer`.
5. Emit one schema chunk first, then record-batch chunks.
6. Close allocator/resources on completion/cancel/error.

Arrow allocator cap is controlled by `ai.floedb.floecat.arrow.max-bytes` (default 1 GiB).

### Row compatibility path

When `output_format = ROWS`, the service streams `SystemObjectRow` payloads directly after
filtering/projecting rows. This mode is compatibility-only; Arrow remains the primary path.

## Predicate/projection notes

- Matching is case-insensitive for requested columns.
- Unknown requested columns are dropped.
- Duplicate requested columns are de-duplicated preserving first occurrence.
- Filter evaluation uses canonical expression form emitted by `SystemRowFilter`.
- `ArrowProjectOperator` uses `TransferPair`, so projection is zero-copy even when reordered.
- Invalid boolean literals in predicates evaluate to no-match.
- Numeric comparisons are exact (floating-point equality is exact, not epsilon-based).

## Optional statistics

Scanners can read best-effort stats through `StatsProvider` in
`MetadataResolutionContext`/`SystemObjectScanContext`. The provider exposes optional
`TableStatsView` / `ColumnStatsView` and is wired via `StatsProviderFactory` with per-query cache
behavior.

- Stats are optional (`StatsProvider.NONE` fallback).
- Missing stats for system/unpinned objects are expected.
- `ColumnStatsView` may include logical type, null/nan counts, canonical min/max, and NDV summary.
- Consumers must treat relation stats as advisory, not required.
- Bundle-level `RelationInfo.stats` remains best-effort; callers should guard `relation.hasStats()`.

## Auth and errors

Auth and call context are enforced through normal inbound gRPC interceptors. Failures return typed
gRPC statuses (`UNAUTHENTICATED`, `PERMISSION_DENIED`, etc.) rather than generic internal errors.
