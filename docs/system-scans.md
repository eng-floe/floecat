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

## Optional statistics

Scanners now receive best-effort statistics through the `MetadataResolutionContext` that backs each scan. The context exposes a `StatsProvider` (defined in `core/catalog/src/main/java/ai/floedb/floecat/systemcatalog/spi/scanner/StatsProvider.java`) which returns `Optional<TableStatsView>` and `Optional<ColumnStatsView>`; the service wires an implementation via `StatsProviderFactory` so lookups are cached per `(tableId, snapshotId)` and never mutate the query context. `ColumnStatsView` now surfaces richer metadata (logical type, nan count, canonical min/max strings, and available NDV summaries) so scanners can make better decisions when stats materialize.

Stats remain optional: the provider defaults to `StatsProvider.NONE`, statistics are only present when a pinned snapshot has cached metadata, and callers must treat the `Optional` results as best-effort decorations. System tables (and any unpinned relation) keep working when the stats provider yields `Optional.empty()`. The same best-effort numbers flow into `RelationInfo.stats` on catalog bundles, so bundle consumers should guard `relation.hasStats()` before reading the values (keep in mind that a `0` may simply mean “unknown” depending on the upstream source) and treat missing stats as expected.

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

## Arrow Flight transport

System tables are also accessible over a native Arrow Flight server that runs alongside the gRPC server. The Flight path uses the same scanners, filter/projection operators, and `ArrowBatchSerializer` loop as the gRPC path — only the transport layer differs.

### Two-phase protocol

**Phase 1 — `GetFlightInfo`**

The client sends a `SystemTableFlightCommand` (proto in `system_scan_flight.proto`) as the `FlightDescriptor` command bytes. Fields mirror the gRPC request:

| Field | Purpose |
|---|---|
| `table_id` | System table `ResourceId` |
| `query_id` | Optional correlation ID for logging |
| `required_columns` | Projection (empty = all columns) |
| `predicates` | Same canonical predicates as the gRPC path |

The server resolves the scanner, computes the projected schema, and returns a `FlightInfo` with the schema and a single opaque `SystemTableFlightTicket` (wrapping the command bytes plus a version tag). No scanning or I/O happens in this phase.

**Phase 2 — `GetStream`**

The client redeems the ticket. The server hands off to a worker executor thread (never blocks the event-loop), decodes the ticket, runs the full `ArrowScanPlan` pipeline, and streams Arrow IPC record batches via `FlightArrowBatchSink`.

### Auth & context headers

The Flight server registers `InboundContextFlightMiddleware`, which provides full parity with the gRPC `InboundContextInterceptor`. All inbound calls must carry the same headers:

| Header | Purpose |
|---|---|
| `authorization` or session header | OIDC bearer token or session token |
| `x-engine-kind` | Target engine kind (optional) |
| `x-engine-version` | Target engine version (optional) |
| `x-query-id` | Caller's query ID (optional, for logging) |
| `x-correlation-id` | Correlation ID; generated if absent, echoed in response |

`catalog.read` permission is enforced on both `GetFlightInfo` and `GetStream`. Auth failures map to `UNAUTHENTICATED` / `UNAUTHORIZED` Flight statuses (not `UNKNOWN`).

### Configuration

| Property | Default | Purpose |
|---|---|---|
| `floecat.flight.port` | `47470` | Port the Flight server listens on |
| `floecat.flight.host` | `localhost` | Routable hostname returned in `FlightEndpoint`; set to an externally reachable address in production |
| `floecat.flight.tls` | `false` | TLS (not yet supported; set to `false`) |
| `floecat.flight.memory.max-bytes` | `0` | Global cap for Flight allocator (0 = unbounded) |
| `ai.floedb.floecat.arrow.max-bytes` | `1073741824` | Per-stream allocator memory cap (1 GiB) |

The `floecat.flight.host` value is stamped into every `FlightEndpointRef` returned by `GetUserObjects`, so clients can discover the Flight address from the catalog bundle. A warning is logged at startup if `localhost` / `127.0.0.1` is configured outside the `dev` or `test` Quarkus profile.

### Scanner & engine context resolution

`SystemScannerResolver.resolve(correlationId, tableId, engineContext)` is used for both transports. It includes the XOR-based cross-engine table-ID translation (`translateToDefault`), so engine-specific table IDs are automatically remapped to the FLOECAT default catalog when no engine-specific scanner exists.

## Streaming guarantees & testing

`QuerySystemScanServiceIT` validates the gRPC server-streaming contract (rows in `ROWS` mode, schema followed by batches in Arrow mode, and Arrow as the default when unspecified). Arrow-specific unit tests include `RowStreamToArrowBatchAdapterTest`, `ArrowFilterOperatorTest`, and `ArrowProjectOperatorTest`. The Flight transport is validated end-to-end by `SystemTableFlightIT` (getFlightInfo schema, schema/stream consistency, data streaming, column projection). This layered approach keeps the legacy predicate/projector logic as the truth while shipping a fully Arrow-native execution lane on both transports.
