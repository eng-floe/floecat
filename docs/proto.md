# Protobuf & RPC Contracts

## Overview
Floecat's public surface is entirely gRPC. The `core/proto/` module defines canonical protobuf
structures for resource identifiers, catalog services, query lifecycle metadata, connectors, statistics, and
helper schemas. Every other module depends on these contracts for serialization, validation, and
compatibility.

The contract files are organised by domain (`common/`, `catalog/`, `query/`, `execution/`,
`connector/`, `account/`, `types/`, `statistics/`). Generated Java stubs live under the `ai.floedb.floecat.*.rpc`
packages and are consumed by the Quarkus service, connectors, CLI, and reconciler.

## Architecture & Responsibilities
- **`common/common.proto`** – Defines `QueryInput`, `ResourceId`, `NameRef`, `SnapshotRef`, pagination, rich error
  payloads, `PrincipalContext`, and idempotency/optimistic-concurrency helpers. Every other schema
  imports this file.
- **`catalog/*.proto`** – CRUD APIs for catalogs, namespaces, tables, views, snapshots, directory
  lookups, and table statistics. Each service exposes the same Create/List/Get/Update/Delete (CLGUD)
  lifecycle with `PageRequest`/`PageResponse` support; stats schemas also cover per-file statistics.
- **`connector/connector.proto`** – Connector management RPCs plus reconciliation job tracking and
  validation routines.
- **`query/lifecycle.proto`** – Query lifecycle (`BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`) and the
  snapshot pin metadata sent down to the SQL planner.
- **`query/system_objects_registry.proto`** – `GetSystemObjects` plus message definitions for builtin functions,
  operators, casts, collations, aggregates, and types loaded from static files.
- **`query/user_objects_bundle.proto`** – `GetUserObjects` streams resolved relation metadata for planner binding. 
- **`execution/scan.proto`** – Scan metadata (data/delete files + per-file stats) produced by
  connectors and consumed at execution time.
- **`types/types.proto`** – Logical type registry (Boolean/Decimal/etc.) and scalar encodings used by
  statistics and bundles.
- **`account/account.proto`** – Account CRUD service for multi-tenancy bootstrap.
- **`statistics/statistics.proto`** – Staging schema for alternative stats pipelines (mirrors
  `catalog/stats.proto` but with different versioning).

## Public API / Surface Area
### Core Services
| Service | Key RPCs | Inputs/Outputs |
|---------|----------|----------------|
| `CatalogService` | `ListCatalogs`, `GetCatalog`, `CreateCatalog`, `UpdateCatalog`, `DeleteCatalog` | Accepts `CatalogSpec`, optional `IdempotencyKey`, `Precondition`, and `FieldMask` for partial updates. Returns `Catalog` + `MutationMeta`. |
| `NamespaceService` | `ListNamespaces`, `GetNamespace`, `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace` | Supports hierarchical selectors (`path`, `recursive`, `children_only`). |
| `TableService` | `ListTables`, `GetTable`, `CreateTable`, `UpdateTable`, `DeleteTable` | `TableSpec` carries `UpstreamRef` with connector link, schema JSON, and partition info. |
| `ViewService` | Similar CRUD semantics, storing SQL definitions and metadata. |
| `SnapshotService` | `ListSnapshots`, `GetSnapshot`, `CreateSnapshot`, `DeleteSnapshot` | Pins upstream checkpoints and timestamps. |
| `TableStatisticsService` | `GetTableStats`, `ListColumnStats`, `ListFileColumnStats`, `PutTableStats`, client-streaming `PutColumnStats` + `PutFileColumnStats` | Accepts per-snapshot NDV/histogram payloads and per-file column stats; streaming RPCs collapse multiple batches into a single call. |
| `DirectoryService` | `Resolve*` & `Lookup*` RPCs | Translates between names and `ResourceId`s with pagination for batched lookups. |
| `AccountService` | Account CRUD. |
| `Connectors` | Connector CRUD, `ValidateConnector`, `TriggerReconcile`, `GetReconcileJob`. |
| `QueryService` | `BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`, `FetchScanBundle`. |
| `UserObjectsService` | `GetUserObjects` | Streams catalog metadata chunks (header → relations → end) as the service resolves each relation so planners can start binding earlier. |
| &nbsp;&nbsp;&nbsp;— Consumption pattern | | Clients read `UserObjectsBundleChunk` in three phases: 1) header chunk (cheap metadata), 2) zero or more `resolutions` chunk batches where each `RelationResolution` carries `input_index` + FOUND/NOT_FOUND/ERROR, and 3) a single end chunk with summary counts. Use `input_index` to map back to planner `TableReferenceCandidate`s and bind as soon as a `FOUND` arrives. |
| `SystemObjectsService` | `GetSystemObjects` | Returns the builtin catalog filtered by the `x-engine-kind` / `x-engine-version` headers supplied with the request. |

Each RPC requires a populated `account_id` within the `ResourceId`s; the Quarkus service checks this
before hitting repository storage.

### Planner Lifecycle & Execution Scan Schemas
`query/lifecycle.proto` captures everything the planner needs to hold a lease:
- `QueryDescriptor` mirrors the live query context (IDs, expiry timestamps, snapshot pins, expansion
  maps, and table obligations). Per-table scan manifests are retrieved lazily via
  `QueryService.FetchScanBundle`, which returns the `execution/scan.proto` records for a specific
  table.

`execution/scan.proto` describes the scan inputs that executors consume:
- `ScanFile` entries include the file path, size, record count, format, per-column stats, and whether
  the file is data vs equality/position deletes.
- `ScanFileContent` enumerates the delete/data categories.

`query/system_objects_registry.proto` exposes immutable builtin metadata via `SystemObjectsService.GetSystemObjects`
so planners can hydrate functions/operators/types once per engine version. Clients send the
`x-engine-kind` and `x-engine-version` headers and always receive the filtered catalog for that
engine release.

## Important Internal Details
- **Field numbering** – All proto files reserve low numbers for required identity fields and push
  experimental metadata to `map<string,string> properties = 99`. Adapt new fields by appending to
  the end to preserve wire compatibility.
- **`ResourceKind` enforcement** – Services verify that IDs have the expected kind (for example
  tables must be `RK_TABLE`). Clients should populate the `kind` enum to improve error messages.
- **`SnapshotRef` semantics** – `oneof which { snapshot_id | as_of | special }`. `special` currently
  allows `SS_CURRENT`. Planner RPCs interpret `as_of` timestamps when enumerating snapshots.
- **File-level stats** – `FileColumnStats` mirrors `ColumnStats` but anchors counts and sketches to
  a file path. `PutFileColumnStats` uses the same idempotency key across streamed batches for the
  same table/snapshot pair; the service enforces consistent `table_id`/`snapshot_id` in a stream.
- **Idempotency/Preconditions** – Mutating RPCs accept `IdempotencyKey` or `Precondition` (expected
  CAS version/ETag). Repository logic mirrors these fields, so clients should obey the same values
  when retrying.
- **Query Lifecycle** – `QueryDescriptor.query_status` moves through `SUBMITTED → COMPLETED/FAILED`
  depending on connector planning success. Lease expirations are surfaced via `expires_at`.

## Data Flow & Lifecycle
1. Clients authenticate using the configured OIDC session/authorization headers (see
   [`docs/service.md`](service.md#security-and-context)) and call gRPC endpoints.
2. Mutations include `IdempotencyKey` for once-and-only-once semantics; the service persists a hash
  of the request along with the resultant `MutationMeta` so replays yield the previous payload.
3. Connectors written against the SPI return `ScanFile` and stats payloads that exactly match the
  protos defined here; the reconciler pipes them back via the catalog/statistics services.
4. Planners call `QueryService.BeginQuery` to create query leases, optionally extend them via
  `RenewQuery`, call `FetchScanBundle` per table when they need manifests, and close leases out via
  `EndQuery` once execution is complete.

_State diagram for the query lease protocol:_

```
[BeginQuery] --> (QueryContext: SUBMITTED)
    | planning succeeds
    v
(QueryContext: COMPLETED) --renew--> (extend expires_at)
    | EndQuery(commit=true/false)
    v
(ENDED_COMMIT or ENDED_ABORT) --grace--> [expiry]
```

## Configuration & Extensibility
- **Evolving protos** – Prefer `optional` fields for new metadata. Keep enum values stable; add new
  entries to the end. Reserve field numbers explicitly if deprecating to avoid reuse.
- **Custom properties** – Many records expose `map<string,string> properties` for lightweight
  extensions. Document keys in the consuming module (for example connector-specific hints in
  [`docs/connectors-spi.md`](connectors-spi.md)).
- **Query leases** – Clients decide how aggressively to renew leases; planners should renew before
  `expires_at` and call `EndQuery` even on failure so `QueryContextStore` can release pins eagerly.

## Examples & Scenarios
### Creating a table via gRPC
```bash
grpcurl -plaintext -d '{
  "spec": {
    "catalog_id": {"account_id":"T","id":"C","kind":"RK_CATALOG"},
    "namespace_id": {"account_id":"T","id":"N","kind":"RK_NAMESPACE"},
    "display_name": "events",
    "schema_json": "{...Iceberg schema...}",
    "upstream": {
      "connector_id": {"account_id":"T","id":"conn","kind":"RK_CONNECTOR"},
      "uri": "s3://warehouse",
      "namespace_path": ["prod"],
      "table_display_name": "events"
    }
  },
  "idempotency": {"key": "create-events"}
}' localhost:9100 ai.floedb.floecat.catalog.TableService/CreateTable
```

### Beginning a query lifecycle lease
```bash
grpcurl -plaintext -d '{
  "inputs": [
    {"name": {"catalog":"demo","path":["sales"],"name":"events"}}
  ]
}' localhost:9100 ai.floedb.floecat.query.QueryService/BeginQuery
```

## Cross-References
- Service runtime, interceptors, and repository adapters: [`docs/service.md`](service.md)
- Connector SPI implementations consuming these protos:
  [`docs/connectors-spi.md`](connectors-spi.md),
  [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
- Query lifecycle internals: [`docs/service.md#query-lifecycle-service`](service.md#query-lifecycle-service)
