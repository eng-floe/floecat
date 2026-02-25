# Service Runtime

## Overview
The `service/` module is the authoritative runtime for Floecat. It hosts the Quarkus gRPC server,
implements every public API from [`proto/`](proto.md), manages multi-account security contexts,
translates requests into pointer/blob mutations, assembles execution scan bundles, and operates background
tasks such as idempotency + CAS blob GC and repository seeding.

It is structured for testability: each gRPC service delegates to repository abstractions, which in
turn encapsulate storage backends. Tests such as
`service/src/test/java/ai/floedb/floecat/service/repo/impl/TableRepositoryTest.java` and
`service/src/test/java/ai/floedb/floecat/service/it/QueryServiceIT.java` probe repository semantics and
query lifecycle / scan bundle logic.

## Architecture & Responsibilities
```
┌────────────────────────────────────────────────────────────────────────────┐
│ Quarkus runtime                                                            │
│  ├─ Interceptors (context, localization, metering)                         │
│  ├─ Security (PrincipalProvider, Authorizer)                               │
│  ├─ Services (Catalog, Namespace, Table, View, Snapshot, Account,           │
│  │            Directory, Statistics, Connectors, QueryService)             │
│  ├─ QueryService (QueryContextStore, QueryServiceImpl)                     │
│  ├─ Repositories (CatalogRepository, NamespaceRepository, TableRepository, │
│  │                ViewRepository, SnapshotRepository, StatsRepository,     │
│  │                ConnectorRepository, AccountRepository,                   │
│  │                IdempotencyRepositoryImpl)                               │
│  └─ GC & Bootstrap (IdempotencyGc, CasBlobGc, SeedRunner)                  │
└────────────────────────────────────────────────────────────────────────────┘
```

### Key packages
- `service/common` – Shared helpers (`BaseServiceImpl`, `IdempotencyGuard`, `Canonicalizer`,
  pagination utilities, structured logging).
- `service/context` – gRPC interceptors injecting `PrincipalContext`, correlation IDs, query IDs,
  engine versions, and bridging outbound headers.
- `service/security` – Minimal `PrincipalProvider` and `Authorizer` scaffolding; pluggable for
  production identity providers.
- `service/repo` – Resource repositories layering pointer/blob stores and parsing protobuf payloads;
  includes key generation utilities (`Keys`, `ResourceKey`) and value normalizers.
- `service/catalog` / `directory` / `account` / `statistics` / `connector` – gRPC service
  implementations.
- `catalog/builtin` – Shared builtin catalog data model, validator, and loader helpers.
- `service/query` – Query lifecycle management (`QueryContext`, `QueryContextStore`,
  `QueryServiceImpl`).
- `service/query/graph` – MetadataGraph cache + immutable node models shared by planners/executors
  (see [`docs/metadata-graph.md`](metadata-graph.md)).
- `service/gc` – Scheduled cleanup of stale idempotency entries.
- `service/bootstrap` – Optional seeding of demo accounts and catalog data.
- `service/metrics` – `ServiceTelemetryInterceptor` + `StorageUsageMetrics` for Micrometer integration.

## Public API / Surface Area
Each gRPC implementation derives from `BaseServiceImpl`, gaining retry semantics, error mapping, and
helpers like `randomResourceId` (UUIDv4). Highlights:

- **CatalogServiceImpl** – Enforces `catalog.read`/`catalog.write` permissions, canonicalises names,
  uses `IdempotencyGuard` for Create, and ensures namespace cascading checks during Delete.
- **NamespaceServiceImpl** – Handles hierarchical selectors, supports recursive listing, and ensures
  `require_empty` semantics on deletion by inspecting repository counts.
- **TableServiceImpl** – Validates `UpstreamRef`, enforces unique names before writing, supports
  partial updates via `FieldMask`, and coordinates snapshot/statistics purging.
- **ViewServiceImpl** – Stores SQL definitions and references to base tables.
- **SnapshotServiceImpl** – Binds snapshots to tables, ensuring parent-child relationships remain
  intact.
- **TableStatisticsServiceImpl** – Persists per-snapshot table/column/file stats; validates
  NDV/histogram payloads; paginates table, column, and file-level listings; uses client-streaming
  `PutColumnStats`/`PutFileColumnStats` to batch writes per stream.
- **DirectoryServiceImpl** – Provides fast name↔ID lookup via `MetadataGraph` (Resolve*/Lookup*) and
  reuses the graph’s ResolveFQ helpers for list/prefix pagination.
- **AccountServiceImpl** – Administers accounts and enforces conventional permissions.
- **ConnectorsImpl** – Manages connector lifecycle, validates `ConnectorSpec` via SPI factories,
  wires reconciliation job submission, and exposes `ValidateConnector` + `TriggerReconcile`.
  `SyncCapture` maps to reconciler capture modes:
  - `include_statistics=false` -> `METADATA_ONLY_CORE`
  - `include_statistics=true` -> `STATS_ONLY_ASYNC`
- **QueryServiceImpl** – Administers query leases (`BeginQuery`, `RenewQuery`, `EndQuery`,
  `GetQuery`) and exposes `FetchScanBundle` so planners can request connector scan metadata on
  demand.
- **SystemObjectsServiceImpl** – Loads immutable builtin catalogs from disk/classpath, caches them
  per engine version, and serves them via `GetSystemObjects`.

## Important Internal Details
### BaseServiceImpl & Idempotency
`BaseServiceImpl` centralises retry policies (`BACKOFF_MIN/MAX`, jitter, `RETRIES`), correlation-ID
propagation, and error translation (storage/repository exceptions → gRPC status codes per
`errors_en.properties`). `IdempotencyGuard` stores request fingerprints inside
`IdempotencyRepository` (backed by the pointer/blob store) so replays reuse prior results.

### Repository Layer
Each repository extends `BaseResourceRepository<T>`:
- Reserves pointer keys via CAS before writing blobs.
- Writes blobs with checksum verification (`sha256B64`).
- Maintains `MutationMeta` (pointer key, blob URI, pointer version, ETag, timestamp).
- Provides convenience accessors such as `getByName`, `getById`, `list`, and `metaForSafe`.
- Deletes tolerate missing blobs when cleaning up pointers, so skewed pointer/blob states can still be removed safely.

`BaseResourceRepository` also exposes `reserveAllOrRollback` for multi-key updates, and
`compareAndDelete` semantics for CAS-based deletions. Tests ensure parity between in-memory and AWS
implementations.

### Security and Context
`InboundContextInterceptor` reads `x-query-id`, `x-engine-version`, and `x-correlation-id` headers,
plus optional OIDC session/authorization headers, validates account membership, hydrates
MDC/OpenTelemetry attributes, and enforces the configured `floecat.auth.mode`.
`OutboundContextClientInterceptor` mirrors the same headers for internal gRPC calls
(service-to-service).

`Authorizer` currently performs simple list membership checks on `PrincipalContext.permissions`; it
can be replaced by injecting a custom implementation.

External session header authentication is documented in
[`docs/external-authentication.md`](external-authentication.md).

### Query Lifecycle Service
`QueryContextStore` is a Caffeine cache keyed by query ID. Each `QueryContext` tracks state,
expiration, `PrincipalContext`, encoded `SnapshotSet`, and `ExpansionMap`.
`QueryServiceImpl.beginQuery` resolves name or ID references via Directory/Snapshot/Table services,
pins snapshots, and stores the lease. Planners request connector `ScanBundle`s later via
`FetchScanBundle`, which streams data/delete files for a specific table. The lease data (snapshots,
expansion map, obligations) is returned to the caller inside the `QueryDescriptor`.

### Builtin Catalog Service
`SystemObjectsLoader` reads immutable builtin catalogs (`<engine_kind>.pb[pbtxt]`) from the
configured location, caches them by engine kind, and exposes them through
`SystemObjectsService.GetSystemObjects`. Clients must send both `x-engine-kind` and
`x-engine-version`; the RPC always returns the filtered builtin bundle for the requested engine.

### GC and Bootstrap
`IdempotencyGc` runs on a configurable cadence (see `floecat.gc.*` config) and sweeps expired
idempotency records in slices to avoid starvation. `CasBlobGc` enumerates blob prefixes and removes
CAS blobs with no remaining pointers once they exceed the configured min-age. `SeedRunner`
populates demo data when `floecat.seed.enabled=true`.

For connector-backed fixture tables, seeding now runs two reconcile passes per fixture scope:
- core metadata pass (`METADATA_ONLY_CORE`)
- stats capture pass (`STATS_ONLY_ASYNC`)

This ensures query scan bundles and Delta-compat manifest materialization have required file stats
available immediately after startup.

### Statistics streaming semantics
`TableStatisticsServiceImpl` enforces a single `table_id` + `snapshot_id` per streamed call to
`PutColumnStats`/`PutFileColumnStats`, rejects mixed idempotency keys within a stream, and applies
idempotent writes when a key is present. Each stream returns one response summarising the total
rows upserted after all batches have been consumed.

## Data Flow & Lifecycle
### Typical request path
```
client → Quarkus Server
  → InboundContextInterceptor (principal/query/correlation)
  → LocalizeErrorsInterceptor (message catalog)
  → ServiceTelemetryInterceptor (metrics/latency)
  → ServiceImpl (authz + validation)
      → Repository (CAS pointer/blob operations)
  ← response + MutationMeta
```

## Configuration & Extensibility
Notable `application.properties` keys:

| Property | Purpose |
|----------|---------|
| `quarkus.grpc.server.*` | Port, HTTP2, plaintext/reflection toggles. |
| `quarkus.grpc.clients.floecat.*` | Loopback client config for internal RPC calls. |
| `floecat.seed.enabled` | Enable demo data seeding. |
| `floecat.kv` / `floecat.blob` | Select pointer/blob store implementation (`memory`, `dynamodb`, `s3`). |
| `floecat.query.*` | Default TTL, grace period, max cache size, safety expiry for query contexts. |
| `floecat.gc.idempotency.*` | Cadence, page size, batch limit, slice duration for idempotency GC. |
| `floecat.gc.cas.*` | Cadence, page size, min-age, tick slice settings for CAS blob GC. |
| `floecat.gc.pointer.*` | Cadence, page size, min-age, tick slice settings for pointer GC. |
| `floecat.gc.reconcile-jobs.*` | Cadence, retention, and slice settings for durable reconcile-job GC. |
| `floecat.reconciler.job-store.*` | Durable reconcile queue selection and retry/lease tuning. |
| `quarkus.log.*` | JSON logging, file rotation, audit handlers per RPC package. |
| `quarkus.otel.*` / `quarkus.micrometer.*` | Observability exporters (see [`docs/operations.md`](operations.md)). |
| `floecat.auth.mode` | Auth enforcement mode (`oidc`, `dev`). |
| `floecat.auth.platform-admin.role` | IdP role name granted permission to manage accounts (default `platform-admin`). |
| `floecat.secrets.aws.role-arn` | Optional role to assume per account when using AWS Secrets Manager. |

Extension points:
- **Storage** – Provide custom `PointerStore`/`BlobStore` (see [`docs/storage-spi.md`](storage-spi.md)).
- **Security** – Replace `Authorizer` or interceptors with CDI alternatives.
- **Connectors** – Register new SPI implementations and expose them via `ConnectorRepository`.
- **QueryService** – Extend query metadata by enriching `QueryContext` creation or injecting
  additional connector metadata via the `FetchScanBundle` RPC / `ScanBundleService`. `BeginQuery`
  optionally accepts a client-specified `query_id` plus `common.QueryInput` records so lifecycle can
  pre-pin snapshots/expansions for deterministic replay.

Secrets Manager integration (tags + optional per-account assume-role) is documented in
[`docs/secrets-manager.md`](secrets-manager.md).

## Examples & Scenarios
- **Create Catalog** – `CatalogServiceImpl.createCatalog` canonicalises `display_name`, allocates a
  UUIDv4 identifier, reserves `/accounts/{account}/catalogs/by-name/{name}` and `/by-id/{uuid}`
  pointer keys, writes the `catalog.pb` blob, and returns `MutationMeta`. If the caller supplies an
  `IdempotencyKey`, the repository short-circuits duplicates.
- **Delete Namespace** – Namespace deletions with `require_empty=true` check child counts via
  `NamespaceRepository.countChildren`. If tables exist, the service raises `MC_CONFLICT.namespace.not_empty`.
- **Query lease renewal** – Clients call `QueryService.RenewQuery` before `expires_at`; the store extends
  the TTL if the query remains `ACTIVE`. A stale or ended query returns `MC_NOT_FOUND.query.not_found`.

## Cross-References
- RPC contracts: [`docs/proto.md`](proto.md)
- Connector SPI & implementations: [`docs/connectors-spi.md`](connectors-spi.md),
  [`docs/connectors-iceberg.md`](connectors-iceberg.md), [`docs/connectors-delta.md`](connectors-delta.md)
- Storage implementations consumed by repositories:
  [`docs/storage-spi.md`](storage-spi.md), [`docs/storage-memory.md`](storage-memory.md),
  [`docs/storage-aws.md`](storage-aws.md)
- Reconciler orchestrating connectors: [`docs/reconciler.md`](reconciler.md)
