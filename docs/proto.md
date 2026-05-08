# Protobuf & RPC Contracts

## Overview
Floecat's public surface is entirely gRPC. The `core/proto/` module defines canonical protobuf
structures for resource identifiers, catalog services, query lifecycle metadata, connectors, statistics, and
helper schemas. Every other module depends on these contracts for serialization, validation, and
compatibility.

The contract files are organised by domain (`common/`, `catalog/`, `query/`, `execution/`,
`connector/`, `account/`, `types/`, `statistics/`, `reconciler/`). Generated Java stubs live
under the `ai.floedb.floecat.*.rpc` packages and are consumed by the Quarkus service, connectors,
CLI, and reconciler.

## Architecture & Responsibilities
- **`common/common.proto`** – Defines `QueryInput`, `ResourceId`, `NameRef`, `SnapshotRef`, pagination, rich error
  payloads, `PrincipalContext`, and idempotency/optimistic-concurrency helpers. Every other schema
  imports this file.
- **`catalog/*.proto`** – CRUD APIs for catalogs, namespaces, tables, views, snapshots, directory
  lookups, table statistics, and index artifacts. Each service exposes the same
  Create/List/Get/Update/Delete (CLGUD) lifecycle with `PageRequest`/`PageResponse` support; stats
  schemas also cover per-file statistics and index artifact metadata.
- **`connector/connector.proto`** – Connector management RPCs plus reconciliation job tracking and
  validation routines.
- **`query/lifecycle.proto`** – Query lifecycle (`BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`) and the
  snapshot pin metadata sent down to the SQL planner.
- **`query/system_objects_registry.proto`** – `GetSystemObjects` plus message definitions for builtin functions,
  operators, casts, collations, aggregates, and types loaded from static files.
- **`query/user_objects_bundle.proto`** – `GetUserObjects` streams resolved relation metadata for planner binding, including per-column `ColumnResult` outcomes (`READY` with `ColumnInfo` or `FAILED` with `ColumnFailure`).
- **`execution/scan.proto`** – Scan metadata (data/delete files + per-file stats) produced by
  connectors and consumed at execution time.
- **`execution/capture.proto`** – File-group execution RPCs used by the combined stats/index
  capture worker path.
- **`reconciler/reconciler.proto`** – Reconcile queue/job tracking contracts, including split
  planning/execution job kinds and per-file execution results.
- **`types/types.proto`** – Logical type registry (Boolean/Decimal/etc.) and scalar encodings used by
  statistics and bundles.
- **`account/account.proto`** – Account CRUD service for multi-tenancy.

## Public API / Surface Area
### Core Services
| Service | Key RPCs | Inputs/Outputs |
|---------|----------|----------------|
| `CatalogService` | `ListCatalogs`, `GetCatalog`, `CreateCatalog`, `UpdateCatalog`, `DeleteCatalog` | Accepts `CatalogSpec`, optional `IdempotencyKey`, `Precondition`, and `FieldMask` for partial updates. Returns `Catalog` + `MutationMeta`. |
| `NamespaceService` | `ListNamespaces`, `GetNamespace`, `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace` | Supports hierarchical selectors (`path`, `recursive`, `children_only`). |
| `TableService` | `ListTables`, `GetTable`, `CreateTable`, `UpdateTable`, `DeleteTable` | `TableSpec` carries `UpstreamRef` with connector link, schema JSON, and partition info. |
| `ViewService` | Similar CRUD semantics, storing SQL definitions and metadata. |
| `SnapshotService` | `ListSnapshots`, `GetSnapshot`, `CreateSnapshot`, `DeleteSnapshot` | Pins upstream checkpoints and timestamps. |
| `TableStatisticsService` | `GetTargetStats`, `ListTargetStats`, client-streaming `PutTargetStats` | Accepts per-snapshot target stats envelopes (table/column/expression/file). `ListTargetStats` supports target-kind filtering (currently at most one kind per request); streaming writes collapse multiple batches into a single call. |
| `TableIndexService` | `GetIndexArtifact`, `ListIndexArtifacts`, client-streaming `PutIndexArtifacts` | Stores and resolves snapshot-scoped parquet sidecar artifact metadata keyed by table, snapshot, and target file. |
| `TableConstraintsService` | `GetTableConstraints`, `ListTableConstraints`, `PutTableConstraints`, `MergeTableConstraints`, `AppendTableConstraints`, `DeleteTableConstraints`, `AddTableConstraint`, `DeleteTableConstraint` | Snapshot-scoped constraints CRUD for user tables. `PutTableConstraints` is full-bundle upsert, `MergeTableConstraints` is server-side merge by `constraint.name` plus shallow merge of bundle `properties` (incoming keys win), `AppendTableConstraints` is server-side append-only (duplicate names rejected), and `AddTableConstraint`/`DeleteTableConstraint` are single-constraint partial mutations. All write operations require snapshot existence (`NOT_FOUND` when missing). |
| `DirectoryService` | `Resolve*` & `Lookup*` RPCs | Translates between names and `ResourceId`s with pagination for batched lookups. |
| `AccountService` | Account CRUD. |
| `Connectors` | Connector CRUD, `ValidateConnector`, `StartCapture`, `GetReconcileJob`. |
| `QueryService` | `BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`, `FetchScanBundle`. |
| `ReconcileExecutorControl` | `LeaseReconcileJob`, `GetLeasedPlan*Input`, `SubmitLeasedPlan*Result`, `GetLeasedFileGroupExecution`, `SubmitLeasedFileGroupExecutionResult` | Executor-facing lease protocol for split reconcile workers. Carries typed planner and file-group payloads plus progress, cancellation, and completion RPCs. |
| `PlannerStatsService` | `GetTargetStats`, `GetTableConstraints` | Split planner-facing streams for target stats and table constraints; `GetTargetStats(include_constraints=true)` remains as a combined single-roundtrip convenience mode. |
| `UserObjectsService` | `GetUserObjects` | Streams catalog metadata chunks (header → relations → end) as the service resolves each relation so planners can start binding earlier. |
| &nbsp;&nbsp;&nbsp;— Consumption pattern | | Clients read `UserObjectsBundleChunk` in three phases: 1) header chunk (cheap metadata), 2) zero or more `resolutions` chunk batches where each `RelationResolution` carries `input_index` + FOUND/NOT_FOUND/ERROR, and 3) a single end chunk with summary counts. Use `input_index` to map back to planner `TableReferenceCandidate`s and bind as soon as a `FOUND` arrives. For each `RelationInfo`, inspect `columns[*].status`: `COLUMN_STATUS_OK` exposes `columns[*].column`, while `COLUMN_STATUS_FAILED` exposes `columns[*].failure` with typed `ColumnFailureCode` plus details. Extension-defined failures must use `COLUMN_FAILURE_CODE_ENGINE_EXTENSION` and set `extension_code_value`; clients branch on `extension_code_value` inside the engine domain (for FloeDB, see `FloeDecorationFailureCode` in `extensions/floedb/src/main/proto/engine_floe.proto`). |
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
- **File-level stats** – `FileTargetStats` anchors counts and sketches to
  a file path. File stats are written as `TargetStatsRecord` values with `target.file` identity via
  `PutTargetStats`; the service enforces consistent `table_id`/`snapshot_id` in a stream.
- **Index artifact streams** – `PutIndexArtifacts` requires each client stream to target exactly one
  `table_id` and one `snapshot_id`. Multiple snapshots must be written through separate client
  streams.
- **Leased file-group result submission** – `SubmitLeasedFileGroupExecutionResult.Success` and
  `.Failure` both require `result_id`. The control plane uses that identifier for top-level replay
  protection on the entire request payload, while service-side writes also keep per-item
  idempotency for stats and index artifacts.
- **Executor leasing filters** – `LeaseReconcileJobRequest` accepts execution class, lane, job kind,
  `executor_id`, and repeated `executor_ids` selectors so a worker fleet can advertise both its
  concrete worker identity and the executor implementations it is willing to run.
- **Stats vs constraints snapshot policy** – `PutTargetStats` currently accepts unknown snapshots
  (lenient ordering), while `PutTableConstraints` is strict and requires a materialized snapshot
  row before write. Rationale: stats keeps existing capture ordering compatibility, while
  constraints are modeled as snapshot-attached relational facts.
- **Planner split vs combined retrieval** – `PlannerStatsService.GetTableConstraints` provides a
  dedicated constraints-only stream, while `GetTargetStats(include_constraints=true)` remains
  available as a combined convenience mode. Split mode is relation-scoped (table visibility pruning
  only) because `FetchTableConstraintsRequest` does not carry column projection context; combined
  mode can apply relation+column request-shape-aware pruning. For constraints lookups,
  `provider_missing` means no bundle exists, while `provider_empty` means a bundle exists and is
  explicitly empty. For planner client simplicity, both are currently surfaced as
  `BUNDLE_RESULT_STATUS_NOT_FOUND` (same as `pruned_empty`), with
  `failure.details.reason` preserving the distinction.
  For CHECK masking to work correctly, connector constraint payloads should populate
  `ConstraintDefinition.columns` with referenced local column IDs.
- **FOREIGN KEY metadata model** – `ConstraintDefinition` carries ANSI-style FK behavior
  fields (`referenced_constraint_name`, `match_option`, `update_rule`, `delete_rule`) so
  `information_schema.referential_constraints` can be populated without connector-specific
  interpretation. Writers may omit these fields; scanners default unspecified rules to
  `NONE` / `NO ACTION` / `NO ACTION`.
- **Idempotency/Preconditions** – Mutating RPCs accept `IdempotencyKey` or `Precondition` (expected
  CAS version/ETag). Repository logic mirrors these fields, so clients should obey the same values
  when retrying.
- **Query Lifecycle** – `QueryDescriptor.query_status` moves through `SUBMITTED → COMPLETED/FAILED`
  depending on connector planning success. Lease expirations are surfaced via `expires_at`.
- **AuthConfig** – Connector auth carries structured `credentials` (for example `bearer`, `cli`,
  `client`, `token-exchange-*`) plus free-form properties; the service resolves secrets and exchanges
  before connectors consume them.

## Data Flow & Lifecycle
1. Clients authenticate using the configured OIDC session/authorization headers (see
   [`docs/service.md`](service.md#security-and-context)) and call gRPC endpoints.
2. Mutations include `IdempotencyKey` for once-and-only-once semantics; the service persists a hash
  of the request along with the resultant `MutationMeta` so replays yield the previous payload.
3. Connectors written against the SPI return `ScanFile`, stats, and file-group capture metadata
  that exactly match the protos defined here; the reconciler pipes them back via the
  catalog/statistics/index services.
4. Planners call `QueryService.BeginQuery` to create query leases, optionally extend them via
   `RenewQuery`, call `FetchScanBundle` per table when they need manifests, and close leases out via
   `EndQuery` once execution is complete.

   * BeginQuery allows clients to provide an optional `query_id` (duplicates are rejected) and
     a list of `common.QueryInput` records so the lifecycle service can pin snapshots and expansions
     at creation time for deterministic replay. Schema resolution and planning still occur in the
     downstream services.

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
- **Temporal precision** – `types.LogicalType.temporal_precision` is optional. Absence means
  default microsecond precision, while an explicit `0` represents second precision.
- **Interval range** – `types.LogicalType.interval_range` distinguishes `INTERVAL YEAR TO MONTH`
  vs `INTERVAL DAY TO SECOND`. In the JVM model, absence is normalised to `IR_UNSPECIFIED`.
  Leading and fractional precisions live in `interval_leading_precision` and
  `interval_fractional_precision`.
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
