# System table scans

This document is the high-level overview for system-table scanning in Floecat.

System-table scans are exposed over two transports:

- gRPC streaming (`QuerySystemScanService`)
- Arrow Flight (`GetFlightInfo` + `GetStream`)

Both transports use the same scanner resolution, filter/projection semantics, and Arrow execution
pipeline. Transport differences are mostly protocol and framing.

## Which document to read

- gRPC protocol details: [system-scans-grpc.md](system-scans-grpc.md)
- Arrow Flight protocol + producer/consumer implementation: [arrow-flight.md](arrow-flight.md)
- System object/scanner model: [system-objects.md](system-objects.md)

## Shared execution model

Regardless of transport:

1. Resolve the system scanner for the target table.
2. Apply canonical predicates (`SystemRowFilter`) and column projection (`required_columns`).
3. Build an `ArrowScanPlan` and stream batches through `ArrowBatchSerializer`.
4. Enforce allocator limits and close native resources on completion/cancel/error.

The row-compatibility path remains available for gRPC callers that request `ROWS`, but Arrow is the
default/primary path.

Scanner resolution is shared across transports and uses the same engine-context-aware path. When an
engine-specific table ID is presented, resolver translation (`translateToDefault`) can remap to the
default FLOECAT catalog ID where applicable.

## Transport comparison

| Area | gRPC scan stream | Arrow Flight |
|---|---|---|
| Entry point | `ScanSystemTable` | `GetFlightInfo` then `GetStream` |
| Request identity | `query_id` in request | `query_id` in command and/or `x-query-id` header |
| Target table | `table_id` (`ResourceId`) | `target` (`name` or `id`) |
| Result framing | `ScanSystemTableChunk` union | Arrow Flight schema + record batches |
| Discovery | Direct service endpoint | `FlightEndpointRef` in catalog bundle + `FlightInfo` endpoint |

## Shared filtering/projection semantics

- `required_columns` is optional. Empty means “all columns”.
- Unknown projected columns are ignored (dropped from output).
- Predicate trees are canonicalized through `SystemRowFilter.EXPRESSION_PROVIDER`.
- Projection order is preserved in output schema/batches.

## Optional statistics (shared behavior)

Scanners receive a best-effort `StatsProvider` in scan context. Stats are optional and may be
absent for system/unpinned relations; callers must treat them as advisory only.

## Memory and cancellation

- Per-stream allocators are child allocators with an explicit cap.
- Cancellation signals terminate streaming work as early as practical.
- Stream cleanup always closes sink/batches/allocators to avoid native memory leaks.

## Configuration knobs (summary)

- `ai.floedb.floecat.arrow.max-bytes`: per-stream Arrow allocator cap.
- `floecat.flight.memory.max-bytes`: Flight server parent allocator cap.
- `floecat.flight.host` / `floecat.flight.port`: endpoint advertised to consumers.

See:

- [system-scans-grpc.md](system-scans-grpc.md)
- [arrow-flight.md](arrow-flight.md)
- [operations.md](operations.md)
