# Floecat RPC surface

Four audiences. A client binds one of them and can ignore the rest. Every service carries its
audience as the first word of its leading comment.

## ENGINE

What a query engine adapter binds. Browse and bind never pin a snapshot; planning and execution do.

| Service | Proto | Role |
|---|---|---|
| `CatalogService.ListCatalogs` | `catalog/catalog.proto` | list catalogs |
| `NamespaceService.ListNamespaces` | `catalog/namespace.proto` | list schemas/namespaces |
| `RelationService` | `catalog/relation.proto` | list, resolve and describe tables and views, kind-neutral |
| `SqlCatalogService` | `query/sql_catalog.proto` | types, functions, operators, casts, collations, aggregates |
| `QueryService` | `query/lifecycle.proto` | query lifecycle |
| `QuerySchemaService` | `query/schema.proto` | pinned planning schema |
| `UserObjectsService` | `query/user_objects_bundle.proto` | pinned planner bundle |
| `PlannerStatsService` | `query/planner_stats_bundle.proto` | planner stats and constraints |
| `QueryScanService`, `QuerySystemScanService` | `query/scan.proto` | execution |
| `StorageAuthorities` | `storage/authority.proto` | storage credentials |

The engine read path is `ListCatalogs` → `ListNamespaces` → `ListRelations` / `ResolveRelations` /
`GetRelation`, then the query lifecycle for anything that needs a pinned snapshot. See
[generic engine RPC contract](../../docs/generic-engine-rpc-contract.md).

## MANAGEMENT

CRUD and per-resource metadata for the CLI and operators. These carry fields an engine does not
bind against — creation timestamps, descriptions, catalog and namespace ids, write specs — which is
why `Relation` does not fold them in.

`CatalogService.GetCatalog` and catalog CRUD, `NamespaceService.GetNamespace` and namespace CRUD,
`TableService`, `ViewService`, `DirectoryService`, `SnapshotService`, `TableStatisticsService`,
`TableIndexService`, `TableConstraintsService`.

Schema reads: `RelationService.GetRelation(include_schema=true)` for the current logical schema,
`SnapshotService.GetSnapshotSchema` for one snapshot, and the planner paths when a pin is needed.

## CONTROL PLANE

Provisioning and operations: `AccountService`, `Connectors`, `CatalogIntegrations`,
`CatalogOverlays`, `ReconcileControl`.

## INTERNAL

Service-to-service, not for external clients: `ReconcileExecutorControl`, `CaptureExecutionService`,
`Transactions`, `ObjectAccess`.

## Conventions

- Identity is `common.ResourceId`; its `kind` is the canonical kind of anything it names.
- Names are `common.NameRef`. Pagination is `common.PageRequest` / `common.PageResponse`, and page
  tokens are opaque.
- Per-item failures in a batch use `common.Error`.
- Engine selection comes from the `x-engine-kind` / `x-engine-version` headers, never request fields.
