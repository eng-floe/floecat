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

## `sys.stats_*` system tables

Floecat exposes persisted stats through these system tables:

- `sys.stats_snapshot`
- `sys.stats_table`
- `sys.stats_column`
- `sys.stats_expression`

Schema summary:

- `sys.stats_snapshot`: table identity (`account_id`, `catalog`, `schema`, `table`, `table_id`),
  `snapshot_id`, and metadata/coverage (`completeness`, `provenance`, `confidence`,
  `capture_time`, `refresh_time`, `rows_seen_count`, `files_seen_count`, `row_groups_seen_count`).
- `sys.stats_table`: table identity + `snapshot_id`, table aggregates (`row_count`, `file_count`,
  `total_bytes`), and metadata (`completeness`, `provenance`, `confidence`, times).
- `sys.stats_column`: table identity + `snapshot_id`, `column_id`, scalar stats fields
  (`value_count`, `null_count`, `nan_count`, `distinct_count`, `min_value`, `max_value`,
  `histogram_json`) and metadata/coverage.
- `sys.stats_expression`: table identity + `snapshot_id`, expression identity (`engine_kind`,
  `expression_key`), scalar stats fields, and metadata/coverage.

Contract and behavior:

- Read path is persisted-only. Querying these tables never triggers capture/recompute.
- Default snapshot behavior is "latest/current snapshot per table".
  If no `snapshot_id` predicate is supplied, rows come from the table's current snapshot.
- `snapshot_id = 0` is valid and supported.
- Some fields are intentionally nullable when upstream payloads do not provide them yet (for
  example `ordinal`, `avg_width_bytes`, and MCV-related columns in `sys.stats_column`).
- Generic post-scan predicates/projection still apply (`SystemRowFilter` and `required_columns`).

## Memory and cancellation

- Per-stream allocators are child allocators with an explicit cap.
- Cancellation signals terminate streaming work as early as practical.
- Stream cleanup always closes sink/batches/allocators to avoid native memory leaks.

## Configuration knobs (summary)

- `ai.floedb.floecat.arrow.max-bytes`: per-stream Arrow allocator cap.
- `floecat.flight.memory.max-bytes`: Flight server parent allocator cap.
- `floecat.flight.advertised-host`: host advertised to consumers.
- `floecat.flight.advertised-port`: port advertised to consumers.
- `quarkus.grpc.server.port` and `quarkus.grpc.server.plain-text`: actual shared gRPC/Flight
  listener and transport mode.

See:

- [system-scans-grpc.md](system-scans-grpc.md)
- [arrow-flight.md](arrow-flight.md)
- [operations.md](operations.md)
