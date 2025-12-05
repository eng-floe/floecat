# Iceberg REST Gateway for Metacat

This document describes the Iceberg REST protocol gateway that fronts Metacat and delegates to existing gRPC services.

## Scope

- Exposes the Iceberg REST Catalog API backed by Metacat gRPC (`protocol-gateway/iceberg-rest`).
- Keeps protocol handling isolated so other protocols can plug in later.
- Reuses Metacat auth/tenancy, logging, metrics; persisted state lives in existing services (tables, snapshots, staged tables via the catalog DB).
- Implements the full single-catalog write path expected by Iceberg REST clients: stage-create, transaction commit, per-table commit (updates, snapshots, refs), metrics ingestion, and reconcile triggers.
- Current non-goals: multi-catalog transactions, `/plan` task pagination (`/tasks`), and inline manifest serving. These still require new RPCs or connector-level support.

## Metacat gRPC coverage vs Iceberg REST

| Iceberg REST surface | Metacat gRPC | Notes |
| --- | --- | --- |
| `/v1/config` | `CatalogService` + gateway config | Build response from catalog metadata and static mappings (warehouse/prefix → catalog_id). |
| Namespace list/load/create/update/delete | `NamespaceService` | Supports properties + field masks. Exists/HEAD via Get. |
| Table list/load/create/delete | `TableService` | CRUD covered. |
| Table rename/move | `TableService.UpdateTable` | Supported via `display_name` and `namespace_id` masks; see `TableMutationIT.tableMove`. |
| View list/load/create/update/delete | `ViewService` | CRUD covered. |
| View rename/move | `ViewService.UpdateView` | Cross-namespace/name via `display_name` and `namespace_id` masks (integration tests exist). |
| Snapshots/history | `SnapshotService` | List/Get/Create/Delete snapshots; includes schema_json and the new partition spec metadata that backs schema/partition history endpoints. |
| Schema fetch | `SchemaService.GetSchema` | Can back Iceberg schema responses. |
| Table/view directory resolution | `DirectoryService` | For name → id resolution; used internally by gateway. |
| Stats | `TableStatisticsService` | Can expose optional stats endpoints if desired. |
| Commit/transactions | `TableService` + `SnapshotService` + staging store | Stage-create, per-table commit, and `/transactions/commit` realized via a staged metadata store plus existing RPCs. Multi-table atomicity beyond staged payload replay is not yet supported. |
| Scan planning | `QueryService` | Gateway calls `BeginQuery` to return “completed” plan responses (all files in one payload). Plan-task pagination and `/tasks` are still unimplemented. |
| Scan/plan manifests, streaming tasks | Missing | Need planner RPCs that page plan-tasks or serve manifests incrementally. |
| Table metrics | `TableStatisticsService` | `/metrics` maps to `PutTableStats` (ingests scan/commit reports). |
| View rename endpoints (Iceberg) | Map to `UpdateView` | REST rename path maps to update with mask. |

## Gap list
- **Plan-task pagination** – `/plan` currently returns every file in one response. `/tasks` is unimplemented, so large scans aren’t streamed/paged yet.
- **Load credentials** – `/tables/{table}/credentials` requires a `LoadCredentials` RPC to surface vended storage credentials.
- **Multi-table transactions** – `/v1/{prefix}/transactions/commit` replays staged payloads one table at a time. There’s no ACID guarantee across tables or catalogs.
- **Manifest/file serving** – not modeled; may require signed URL service or storage gateway.

## Module layout

- `protocol-gateway/`
  - `protocol-gateway-iceberg-rest/` – Quarkus RESTEasy Reactive app hosting the Iceberg REST endpoints.
  - `protocol-gateway-common/` – shared auth/error/metrics utilities and gRPC client wiring.

## Implementation architecture

- Quarkus REST controllers cover each Iceberg resource (Config, Namespace, Table, View, Snapshot, Stats). Planning and rename endpoints live with the table/view controllers, while `/transactions/*` is handled by the table resource.
- Each controller injects `GrpcWithHeaders` clients for the underlying Metacat services (Catalog, Namespace, Table, View, Snapshot, Schema, Directory, TableStatistics, Query) so requests stay in-process and follow tenant context.
- Stage storage: `StagedTableRepository` records create/commit payloads keyed by tenant/catalog/namespace/table + stage-id; `StagedTableService` keeps the payload lifecycle and TTL enforcement.
- Snapshot handling: `SnapshotMetadataService` directly rewrites Iceberg metadata/snapshot APIs, while `TableCommitService` now materializes snapshot metadata files (via `MaterializeMetadataService`) before updating `TableService` to avoid dangling `metadata-location` references.
- Response mappers/DTOs (`LoadTableResultDto`, `CommitTableResponseDto`, `TableMetadataView`) synthesize the Iceberg spec from catalog metadata and snapshot refs; field masks drive partial updates (rename/move/props) and connectors use the resolved metadata location.
- Connector wiring: `TableCommitSideEffectService` creates or updates connectors, updates table upstreams, and runs metadata capture/reconcile after commits. REST helpers mirror this path when staging or admin operations materialize metadata and return credentials/config.
- Auth/config: `TenantHeaderFilter` propagates tenant headers and `IcebergGatewayConfig` + catalog mappings resolve prefixes/catalog IDs and runtime overrides for metadata copying or connector creation.

## Write path and transaction flow

This section summarizes how the gateway mirrors Iceberg’s two-phase workflow so Trino and other REST clients can stage metadata before committing.

### Stage-create (`POST /tables` with `stage-create=true`)

1. Gateway validates the `CreateTableRequest`, normalizes schema/spec/order, and derives connector metadata (location, properties, requirements).
2. A `StagedTableEntry` is stored via `StagedTableService.saveStage`, keyed by tenant/catalog/namespace/table + a generated stage-id (unless the client supplies one via `Iceberg-Transaction-Id`).
3. The response returns `StageCreateResponse` fields (stage-id, requirements, config overrides, storage credentials). No table is materialized yet.
4. Idempotency: issuing the same stage-create (same composite key + stage-id) returns the cached entry instead of overwriting metadata.

### Commit (`POST /tables/{table}`) without direct stage reference

1. Gateway resolves the table. If it does not exist and no stage is supplied, the request fails with 404.
2. If the table is missing but a staged entry exists (either via `stage-id` header/body or via “latest stage” lookup for the tenant/catalog/table), `StageCommitProcessor` materializes the table through `TableService.createTable`, wires connectors, and deletes the staged record.
3. Snapshot/metadata updates (add/remove snapshot, refs, schemas/specs, statistics, location) are replayed using `SnapshotService`, `TableService.updateTable`, and helper methods to mutate Iceberg metadata blobs.
4. The response uses `TableResponseMapper.toCommitResponse`, which always includes the latest snapshots referenced by `current-snapshot-id` and `refs`, ensuring Iceberg clients can deserialize the metadata.

### `/transactions/commit`

1. The endpoint receives Iceberg’s commit payload (list of staged references + requirements + update list).
2. For each staged reference the gateway:
   - Loads the staged entry from `StagedTableService`.
   - Validates requirements (e.g., assert-create) against actual table existence.
   - Materializes or updates the table using the same logic as `/tables/{table}` commit.
3. Requirements in the payload (assert-current-schema, assert-ref-snapshot-id, etc.) are reevaluated using the freshly loaded metadata/snapshots.
4. After all stages succeed, the gateway triggers connector reconcile/sync tasks and deletes the staged entries. On failures, stages are marked aborted for observability.
5. The response mirrors Iceberg’s `CommitTableResponse` with metadata location, metadata view, config overrides, and storage credentials.

### Snapshot handling

- Snapshot placeholders (add/remove) leverage `SnapshotService` RPCs so Metacat remains the source of truth for manifests, refs, and history.
- `TableResponseMapper` synthesizes missing schema/spec/order data, aligns refs with actual snapshots, and bumps `last-sequence-number` to the highest known snapshot sequence.
- Metadata files (`metadata-location`) remain inside Metacat’s storage (e.g., `metacat:///tables/<id>`). Client table locations only contain Iceberg manifests/data; JSON metadata is retrieved via the gateway.

### Housekeeping & resilience

- `StagedTableService.expireStages` is invoked periodically to remove stale entries based on configurable TTLs.
- Operations are idempotent: stage-create uses deterministic keys, snapshot creates leverage per-snapshot idempotency keys, and commit replays tolerate retries.
- Logging/metrics: TableResource logs stage usage, stage commit outcomes, and snapshot counts to help trace stage→commit flows end-to-end.

## Endpoint mapping highlights

- Config (`/v1/config`): build endpoints and default properties from gateway config + Metacat catalog properties; expose the Iceberg configuration map clients expect.
- Namespace operations: direct `NamespaceService`; map properties and parents/path to Iceberg namespace parts.
- Table operations: `TableService`; ensure upstream format = ICEBERG; translate schema_json; rename/move via `update_mask` on `namespace_id` and `display_name`; stage-create/commit leverage the staging store described above.
- View operations: `ViewService`; rename/move via `update_mask`; SQL passthrough.
- Snapshot/history: `SnapshotService`; map snapshot_id, parent_id, timestamps, schema_json, and partition-spec metadata to Iceberg history responses. Snapshots now embed `schemaJson` plus `PartitionSpecInfo` (specId, specName, partition field `fieldId/name/transform`) sourced from connectors.
- Schema history: `/v1/{prefix}/namespaces/{namespace}/tables/{table}/schemas` replays each snapshot's `schemaJson` along with its snapshotId, `upstreamCreatedAt`, and `ingestedAt`.
- Partition spec history: `/v1/{prefix}/namespaces/{namespace}/tables/{table}/partition-specs` replays each snapshot's `PartitionSpecInfo` so clients can inspect how partition layouts evolved.
- Schema fetch: `SchemaService` to build Iceberg schema response.
- Optional stats: `TableStatisticsService` mapped to Iceberg metrics if exposed.

## Testing strategy

- Unit tests cover DTO translators, plan response mappers, error mapping, and config builders.
- Contract tests use RestAssured and mocked gRPC stubs (`RestResourceTest`) to validate endpoints, error shapes, and rename/move semantics.
- End-to-end tests (`IcebergRestTest`) boot the full Metacat service stack (in-memory backend, real gRPC services) in a separate JVM, then exercise the gateway over HTTP to verify namespace CRUD and config endpoints behave correctly through the actual gRPC implementations.
- Integration: run the gateway against Metacat services (docker or local dev) and compare responses with Iceberg spec expectations.
