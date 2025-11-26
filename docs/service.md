# Service Runtime

## Overview
The `service/` module is the authoritative runtime for Metacat. It hosts the Quarkus gRPC server,
implements every public API from [`proto/`](proto.md), manages multi-tenant security contexts,
translates requests into pointer/blob mutations, assembles execution scan bundles, and operates background
tasks such as idempotency GC and repository seeding.

It is structured for testability: each gRPC service delegates to repository abstractions, which in
turn encapsulate storage backends. Tests such as
`service/src/test/java/ai/floedb/metacat/service/repo/impl/TableRepositoryTest.java` and
`service/src/test/java/ai/floedb/metacat/service/it/QueryServiceIT.java` probe repository semantics and
query lifecycle / scan bundle logic.

## Architecture & Responsibilities
```
┌────────────────────────────────────────────────────────────────────────────┐
│ Quarkus runtime                                                            │
│  ├─ Interceptors (context, localization, metering)                         │
│  ├─ Security (PrincipalProvider, Authorizer)                               │
│  ├─ Services (Catalog, Namespace, Table, View, Snapshot, Tenant,           │
│  │            Directory, Statistics, Connectors, QueryService)             │
│  ├─ QueryService (QueryContextStore, QueryServiceImpl)                     │
│  ├─ Repositories (CatalogRepository, NamespaceRepository, TableRepository, │
│  │                ViewRepository, SnapshotRepository, StatsRepository,     │
│  │                ConnectorRepository, TenantRepository,                   │
│  │                IdempotencyRepositoryImpl)                               │
│  └─ GC & Bootstrap (IdempotencyGc, SeedRunner)                             │
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
- `service/catalog` / `directory` / `tenant` / `statistics` / `connector` – gRPC service
  implementations.
- `service/catalog/builtin` – Builtin catalog loader/cache plus gRPC service adapter.
- `service/query` – Query lifecycle management (`QueryContext`, `QueryContextStore`,
  `QueryServiceImpl`).
- `service/gc` – Scheduled cleanup of stale idempotency entries.
- `service/bootstrap` – Optional seeding of demo tenants and catalog data.
- `service/metrics` – `MeteringInterceptor` + `StorageUsageMetrics` for Micrometer integration.

## Public API / Surface Area
Each gRPC implementation derives from `BaseServiceImpl`, gaining retry semantics, error mapping, and
helpers like `deterministicUuid`. Highlights:

- **CatalogServiceImpl** – Enforces `catalog.read`/`catalog.write` permissions, canonicalises names,
  uses `IdempotencyGuard` for Create, and ensures namespace cascading checks during Delete.
- **NamespaceServiceImpl** – Handles hierarchical selectors, supports recursive listing, and ensures
  `require_empty` semantics on deletion by inspecting repository counts.
- **TableServiceImpl** – Validates `UpstreamRef`, ensures deterministic IDs derived from canonical
  specs, supports partial updates via `FieldMask`, and coordinates snapshot/statistics purging.
- **ViewServiceImpl** – Stores SQL definitions and references to base tables.
- **SnapshotServiceImpl** – Binds snapshots to tables, ensuring parent-child relationships remain
  intact.
- **TableStatisticsServiceImpl** – Persists per-snapshot table/column/file stats; validates
  NDV/histogram payloads; paginates table, column, and file-level listings; uses client-streaming
  `PutColumnStats`/`PutFileColumnStats` to batch writes per stream.
- **DirectoryServiceImpl** – Provides fast name↔ID lookup using pointer prefixes.
- **TenantServiceImpl** – Administers tenants and enforces conventional permissions.
- **ConnectorsImpl** – Manages connector lifecycle, validates `ConnectorSpec` via SPI factories,
  wires reconciliation job submission, and exposes `ValidateConnector` + `TriggerReconcile`.
- **QueryServiceImpl** – Administers query leases (`BeginQuery`, `RenewQuery`, `EndQuery`,
  `GetQuery`) and fetches connector scan metadata (`ScanBundle`s) to include in the lease payload.
- **BuiltinCatalogServiceImpl** – Loads immutable builtin catalogs from disk/classpath, caches them
  per engine version, and serves them via `GetBuiltinCatalog`.

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

`BaseResourceRepository` also exposes `reserveAllOrRollback` for multi-key updates, and
`compareAndDelete` semantics for CAS-based deletions. Tests ensure parity between in-memory and AWS
implementations.

### Security and Context
`InboundContextInterceptor` reads `x-principal-bin`, `x-query-id`, `x-engine-version`, and `x-correlation-id` headers,
validates tenant membership, hydrates MDC/OpenTelemetry attributes, and falls back to a development
principal (full permissions) if no credentials exist. `OutboundContextClientInterceptor` mirrors the
same headers for internal gRPC calls (service-to-service).

`Authorizer` currently performs simple list membership checks on `PrincipalContext.permissions`; it
can be replaced by injecting a custom implementation.

### Query Lifecycle Service
`QueryContextStore` is a Caffeine cache keyed by query ID. Each `QueryContext` tracks state,
expiration, `PrincipalContext`, encoded `SnapshotSet`, and `ExpansionMap`.
`QueryServiceImpl.beginQuery` resolves name or ID references via Directory/Snapshot/Table services,
pins snapshots, stores the lease, and optionally contacts connectors to fetch `ScanBundle`s
(data/delete files) so that planners have scan metadata up front. The lease data (snapshots,
expansion map, obligations, scan files) is returned to the caller inside the `QueryDescriptor`.

### Builtin Catalog Service
`BuiltinCatalogLoader` reads immutable builtin catalogs (`builtin_catalog_<engine_version>.pb[pbtxt]`)
from the configured location, caches them by engine version, and exposes them through
`BuiltinCatalogService.GetBuiltinCatalog`. Clients must send `x-engine-version`; when the caller’s
`current_version` matches the cached version the RPC returns an empty response to avoid retransfers.

### GC and Bootstrap
`IdempotencyGc` runs on a configurable cadence (see `metacat.gc.*` config) and sweeps expired
idempotency records in slices to avoid starvation. `SeedRunner` populates demo data when
`metacat.seed.enabled=true`.

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
  → MeteringInterceptor (metrics/latency)
  → ServiceImpl (authz + validation)
      → Repository (CAS pointer/blob operations)
  ← response + MutationMeta
```

## Configuration & Extensibility
Notable `application.properties` keys:

| Property | Purpose |
|----------|---------|
| `quarkus.grpc.server.*` | Port, HTTP2, plaintext/reflection toggles. |
| `quarkus.grpc.clients.metacat.*` | Loopback client config for internal RPC calls. |
| `metacat.seed.enabled` | Enable demo data seeding. |
| `metacat.kv` / `metacat.blob` | Select pointer/blob store implementation (`memory`, `dynamodb`, `s3`). |
| `metacat.query.*` | Default TTL, grace period, max cache size, safety expiry for query contexts. |
| `metacat.builtins.location` | Filesystem or classpath location for builtin catalog files (default `classpath:builtins`). |
| `metacat.gc.idempotency.*` | Cadence, page size, batch limit, slice duration for GC. |
| `quarkus.log.*` | JSON logging, file rotation, audit handlers per RPC package. |
| `quarkus.otel.*` / `quarkus.micrometer.*` | Observability exporters. |

Extension points:
- **Storage** – Provide custom `PointerStore`/`BlobStore` (see [`docs/storage-spi.md`](storage-spi.md)).
- **Security** – Replace `Authorizer` or interceptors with CDI alternatives.
- **Connectors** – Register new SPI implementations and expose them via `ConnectorRepository`.
- **QueryService** – Extend query metadata by enriching `QueryContext` creation or injecting
  additional connector metadata via `QueryContext.fetchScanBundle`.

## Examples & Scenarios
- **Create Catalog** – `CatalogServiceImpl.createCatalog` canonicalises `display_name`, generates a
  deterministic UUID from the fingerprint, reserves `/tenants/{tenant}/catalogs/by-name/{name}` and
  `/by-id/{uuid}` pointer keys, writes the `catalog.pb` blob, and returns `MutationMeta`. If the
  caller supplies an `IdempotencyKey`, the repository short-circuits duplicates.
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
