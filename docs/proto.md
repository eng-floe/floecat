# Protobuf & RPC Contracts

## Overview
Metacat's public surface is entirely gRPC. The `proto/` module defines canonical protobuf
structures for resource identifiers, catalog services, planning bundles, connectors, statistics, and
helper schemas. Every other module depends on these contracts for serialization, validation, and
compatibility.

The contract files are organised by domain (`common/`, `catalog/`, `planning/`, `connector/`,
`tenant/`, `types/`, `statistics/`). Generated Java stubs live under the `ai.floedb.metacat.*.rpc`
packages and are consumed by the Quarkus service, connectors, CLI, and reconciler.

## Architecture & Responsibilities
- **`common/common.proto`** – Defines `ResourceId`, `NameRef`, `SnapshotRef`, pagination, rich error
  payloads, `PrincipalContext`, and idempotency/optimistic-concurrency helpers. Every other schema
  imports this file.
- **`catalog/*.proto`** – CRUD APIs for catalogs, namespaces, tables, views, snapshots, directory
  lookups, and table statistics. Each service exposes the same Create/List/Get/Update/Delete (CLGUD)
  lifecycle with `PageRequest`/`PageResponse` support.
- **`connector/connector.proto`** – Connector management RPCs plus reconciliation job tracking and
  validation routines.
- **`planning/planning.proto`** – Plan lifecycle (`BeginPlan`, `Renew`, `End`, `GetPlan`) and the
  `CatalogBundle` assembly schema consumed by query planners.
- **`types/types.proto`** – Logical type registry (Boolean/Decimal/etc.) and scalar encodings used by
  statistics and bundles.
- **`tenant/tenant.proto`** – Tenant CRUD service for multi-tenancy bootstrap.
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
| `TableStatisticsService` | `GetTableStats`, `ListColumnStats`, `PutTableStats`, `PutColumnStatsBatch` | Accepts per-snapshot NDV, histogram, TDigest payloads. |
| `DirectoryService` | `Resolve*` & `Lookup*` RPCs | Translates between names and `ResourceId`s with pagination for batched lookups. |
| `TenantService` | Tenant CRUD. |
| `Connectors` | Connector CRUD, `ValidateConnector`, `TriggerReconcile`, `GetReconcileJob`. |
| `Planning` | `BeginPlan`, `RenewPlan`, `EndPlan`, `GetPlan`, `GetCatalogBundle`. |

Each RPC requires a populated `tenant_id` within the `ResourceId`s; the Quarkus service checks this
before hitting repository storage.

### Catalog Bundle Schema
`planning.proto` introduces higher-level records tailored to planners:
- `CatalogRelation` wraps tables, views, or session temps with namespace paths and catalog metadata.
- `TableRelation` embeds stored `Table` definitions, derived `Column` descriptors, constraints, and
  optional statistics.
- `TypeRegistry` surfaces as a list of `TypeInfo` entries, letting multiple relations reference the
  same complex type by ID.
- `Builtins` enumerates functions, operators, casts, collations, and aggregates a planner may need.
- `SessionMetadata` echoes overrides (GUCs, search path, temp relations) returned to the caller.

## Important Internal Details
- **Field numbering** – All proto files reserve low numbers for required identity fields and push
  experimental metadata to `map<string,string> properties = 99`. Adapt new fields by appending to
  the end to preserve wire compatibility.
- **`ResourceKind` enforcement** – Services verify that IDs have the expected kind (for example
  tables must be `RK_TABLE`). Clients should populate the `kind` enum to improve error messages.
- **`SnapshotRef` semantics** – `oneof which { snapshot_id | as_of | special }`. `special` currently
  allows `SS_CURRENT`. Planner RPCs interpret `as_of` timestamps when enumerating snapshots.
- **Idempotency/Preconditions** – Mutating RPCs accept `IdempotencyKey` or `Precondition` (expected
  CAS version/ETag). Repository logic mirrors these fields, so clients should obey the same values
  when retrying.
- **Plan Lifecycle** – `PlanDescriptor.plan_status` moves through `SUBMITTED → COMPLETED/FAILED`
  depending on connector planning success. Lease expirations are surfaced via `expires_at`.

## Data Flow & Lifecycle
1. Clients populate `PrincipalContext` metadata in the `x-principal-bin` header (see
   [`docs/service.md`](service.md#security-and-context)) and call gRPC endpoints.
2. Mutations include `IdempotencyKey` for once-and-only-once semantics; the service persists a hash
  of the request along with the resultant `MutationMeta` so replays yield the previous payload.
3. Connectors written against the SPI return `PlanFile` and stats payloads that exactly match the
  protos defined here; the reconciler pipes them back via `catalog` and `planning` services.
4. Planners issue `GetCatalogBundle` with a list of `RelationRef`s, optionally specifying
  `RelationKind` and `SnapshotRef`. The response remains stable until referenced snapshots change.

_State diagram for the plan lease protocol:_

```
[BeginPlan] --> (PlanContext: SUBMITTED)
    | planning succeeds
    v
(PlanContext: COMPLETED) --renew--> (extend expires_at)
    | EndPlan(commit=true/false)
    v
(ENDED_COMMIT or ENDED_ABORT) --grace--> [expiry]
```

## Configuration & Extensibility
- **Evolving protos** – Prefer `optional` fields for new metadata. Keep enum values stable; add new
  entries to the end. Reserve field numbers explicitly if deprecating to avoid reuse.
- **Custom properties** – Many records expose `map<string,string> properties` for lightweight
  extensions. Document keys in the consuming module (for example connector-specific hints in
  [`docs/connectors-spi.md`](connectors-spi.md)).
- **Bundles** – Clients can skip built-ins via `CatalogBundleRequest.include_builtins=false` if they
  cache previous responses. Session temp relations can be supplied inline by populating
  `SessionOverrides.temp_relations`.

## Examples & Scenarios
### Creating a table via gRPC
```bash
grpcurl -plaintext -d '{
  "spec": {
    "catalog_id": {"tenant_id":"T","id":"C","kind":"RK_CATALOG"},
    "namespace_id": {"tenant_id":"T","id":"N","kind":"RK_NAMESPACE"},
    "display_name": "events",
    "schema_json": "{...Iceberg schema...}",
    "upstream": {
      "connector_id": {"tenant_id":"T","id":"conn","kind":"RK_CONNECTOR"},
      "uri": "s3://warehouse",
      "namespace_path": ["prod"],
      "table_display_name": "events"
    }
  },
  "idempotency": {"key": "create-events"}
}' localhost:9100 ai.floedb.metacat.catalog.TableService/CreateTable
```

### Fetching a catalog bundle for a query plan
```bash
grpcurl -plaintext -d '{
  "relations": [
    {"relation_kind": "RELATION_KIND_TABLE",
     "name": {"catalog":"demo", "path":["sales"], "name":"events"},
     "snapshot": {"special": "SS_CURRENT"}}
  ],
  "session": {"search_path": ["demo.sales"], "gucs": {"role": "analyst"}}
}' localhost:9100 ai.floedb.metacat.planning.Planning/GetCatalogBundle
```

## Cross-References
- Service runtime, interceptors, and repository adapters: [`docs/service.md`](service.md)
- Connector SPI implementations consuming these protos:
  [`docs/connectors-spi.md`](connectors-spi.md),
  [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
- Planner bundle assembly internals: [`docs/service.md#planning-and-bundle-assembly`](service.md#planning-and-bundle-assembly)
