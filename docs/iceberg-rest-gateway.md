# Iceberg REST Gateway for Metacat

This document describes the Iceberg REST protocol gateway that fronts Metacat and delegates to existing gRPC services.

## Scope

- Exposes the Iceberg REST Catalog API backed by Metacat gRPC (`protocol-gateway/iceberg-rest`).
- Keeps protocol handling isolated so other protocols can plug in later.
- Reuses Metacat auth/tenancy, logging, metrics; adds no new persistent state beyond plan IDs cached in QueryService.
- Current non-goals: full Iceberg commit/transaction semantics, async plan-task pagination (`/tasks`), and inline manifest serving. These still require new RPCs or connector-level support.

## Reference

- Apache Gravitino Iceberg REST server for routing and DTO patterns: `gravitino/iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/server/GravitinoIcebergRESTServer.java` and `.../service/rest/Iceberg*Operations.java`.

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
| Commit/transactions | Missing | Iceberg commit semantics not modeled; requires new RPCs. |
| Scan planning | `QueryService` | Gateway calls `BeginQuery` to return “completed” plan responses (all files in one payload). Plan-task pagination and `/tasks` are still unimplemented. |
| Scan/plan manifests, streaming tasks | Missing | Need planner RPCs that page plan-tasks or serve manifests incrementally. |
| Table metrics | `TableStatisticsService` | `/metrics` maps to `PutTableStats` (ingests scan/commit reports). |
| View rename endpoints (Iceberg) | Map to `UpdateView` | REST rename path maps to update with mask. |

## Gap list

- **Commit/transaction endpoints** – need new gRPC APIs to persist metadata/manifest updates atomically.
- **Register table** – `/v1/{prefix}/namespaces/{namespace}/register` still returns 501 until a register RPC exists.
- **Plan-task pagination** – `/plan` currently returns every file in one response. `/tasks` is unimplemented, so large scans aren’t streamed/paged yet.
- **Load credentials** – `/tables/{table}/credentials` requires a `LoadCredentials` RPC to surface vended storage credentials.
- **Transactions** – `/v1/{prefix}/transactions/commit` is a stub without multi-table transaction support.
- **Manifest/file serving** – not modeled; may require signed URL service or storage gateway.

## Module layout

- `protocol-gateway/`
  - `protocol-gateway-iceberg-rest/` – Quarkus RESTEasy Reactive app hosting the Iceberg REST endpoints.
  - `protocol-gateway-common/` – shared auth/error/metrics utilities and gRPC client wiring.

## Implementation architecture

- Quarkus REST controllers per resource: Config, Namespace, Table, View, Snapshot, Stats (metrics). Planning and rename endpoints live under the table/view controllers.
- gRPC clients injected for Catalog, Namespace, Table, View, Snapshot, Schema, Directory, TableStatistics, and Query services via `GrpcClients`.
- Auth: `TenantHeaderFilter` enforces tenant/auth headers and pushes metadata into gRPC calls using `GrpcWithHeaders`.
- Config resolution: `/v1/config` builds defaults/overrides/endpoints from gateway config (`IcebergGatewayConfig`) and the catalog mapping.
- DTO translation: dedicated records (`LoadTableResultDto`, `PlanResponseDto`, etc.) map proto responses to the Iceberg spec. Field masks handle partial updates (rename/move).
- Error mapping: `ErrorMapper` wraps gRPC `StatusRuntimeException` into Iceberg’s `{"error":{message,type,code}}` contract.
- Planning: `/plan` calls `QueryService.beginQuery`, `/plan/{planId}` calls `GetQuery`, `/plan/{planId}` DELETE calls `EndQuery`. Large plans are currently returned in one payload (no `/tasks` pagination yet).
- Metrics: `/tables/{table}/metrics` writes reports via `TableStatisticsService.putTableStats`.

## Endpoint mapping highlights

- Config (`/v1/config`): build endpoints and default properties from gateway config + Metacat catalog properties; follow Gravitino’s `IcebergConfigOperations`.
- Namespace operations: direct `NamespaceService`; map properties and parents/path to Iceberg namespace parts.
- Table operations: `TableService`; ensure upstream format = ICEBERG; translate schema_json; rename/move via `update_mask` on `namespace_id` and `display_name`.
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
- Integration: run the gateway against Metacat services (docker or local dev) and compare responses with Gravitino examples (`docs/iceberg-rest-service.md`).
