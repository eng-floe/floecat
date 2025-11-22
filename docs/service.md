# Service Runtime

## Overview
The `service/` module is the authoritative runtime for Metacat. It hosts the Quarkus gRPC server,
implements every public API from [`proto/`](proto.md), manages multi-tenant security contexts,
translates requests into pointer/blob mutations, assembles planner bundles, and operates background
tasks such as idempotency GC and repository seeding.

It is structured for testability: each gRPC service delegates to repository abstractions, which in
turn encapsulate storage backends. Tests such as
`service/src/test/java/ai/floedb/metacat/service/repo/impl/TableRepositoryTest.java` and
`CatalogBundleAssemblerTest.java` probe repository semantics and planner assembly logic.

## Architecture & Responsibilities
```
┌────────────────────────────────────────────────────────────────────────────┐
│ Quarkus runtime                                                            │
│  ├─ Interceptors (context, localization, metering)                         │
│  ├─ Security (PrincipalProvider, Authorizer)                               │
│  ├─ Services (Catalog, Namespace, Table, View, Snapshot, Tenant,           │
│  │            Directory, Statistics, Connectors, Planning)                 │
│  ├─ Planning (PlanContextStore, CatalogBundleAssembler, TypeRegistry)      │
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
- `service/context` – gRPC interceptors injecting `PrincipalContext`, correlation IDs, plan IDs, and
  bridging outbound headers.
- `service/security` – Minimal `PrincipalProvider` and `Authorizer` scaffolding; pluggable for
  production identity providers.
- `service/repo` – Resource repositories layering pointer/blob stores and parsing protobuf payloads;
  includes key generation utilities (`Keys`, `ResourceKey`) and value normalizers.
- `service/catalog` / `directory` / `tenant` / `statistics` / `connector` – gRPC service
  implementations.
- `service/planning` – Plan lifecycle management (`PlanContext`, `PlanContextStore`) and bundle
  assembly (`CatalogBundleAssembler`, `TypeRegistry`, `BuiltinMetadata`).
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
- **PlanningImpl** – Administers plan leases (`BeginPlan`, `RenewPlan`, `EndPlan`, `GetPlan`) and
  proxies `GetCatalogBundle` to `CatalogBundleAssembler`.

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
`InboundContextInterceptor` reads `x-principal-bin`, `x-plan-id`, and `x-correlation-id` headers,
validates tenant membership, hydrates MDC/OpenTelemetry attributes, and falls back to a development
principal (full permissions) if no credentials exist. `OutboundContextClientInterceptor` mirrors the
same headers for internal gRPC calls (service-to-service).

`Authorizer` currently performs simple list membership checks on `PrincipalContext.permissions`; it
can be replaced by injecting a custom implementation.

### Planning and Bundle Assembly
`PlanContextStore` is a Caffeine cache keyed by plan ID. Each `PlanContext` tracks state, expiration,
`PrincipalContext`, encoded `SnapshotSet`, and `ExpansionMap`. `PlanningImpl.beginPlan` resolves
name or ID references via Directory/Snapshot/Table services, pins snapshots, stores the plan, and
optionally contacts connectors to fetch `PlanBundle`s (data/delete files).

`CatalogBundleAssembler` resolves tables/views by ID or name, enforces tenancy/kind expectations,
reads Iceberg schemas to emit `Column`/`TypeSpec` entries, queries statistics for pinned snapshots,
normalizes session temp relations, and attaches builtin function/type metadata. `TypeRegistry` ensures
all relation columns reference deduplicated `TypeInfo`s.

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
  → InboundContextInterceptor (principal/plan/correlation)
  → LocalizeErrorsInterceptor (message catalog)
  → MeteringInterceptor (metrics/latency)
  → ServiceImpl (authz + validation)
      → Repository (CAS pointer/blob operations)
  ← response + MutationMeta
```

### Planner bundle assembly sequence
1. `Planning.GetCatalogBundle` receives `CatalogBundleRequest` with relation refs.
2. `CatalogBundleAssembler` uses repositories to fetch catalog/namespace/table/view records.
3. When relations reference tables, it loads the Iceberg schema JSON using
   `org.apache.iceberg.SchemaParser`, converts each field into `Column` entries, resolves type IDs via
   `TypeRegistry`, and captures partition flags.
4. If a `SnapshotRef` is specified, the assembler queries `SnapshotRepository` for the snapshot ID,
   then `StatsRepository` for table/column stats.
5. Session overrides are normalised (`search_path`, default namespace, temp relations) and merged
   with builtin metadata, forming the final `CatalogBundle`.

## Configuration & Extensibility
Notable `application.properties` keys:

| Property | Purpose |
|----------|---------|
| `quarkus.grpc.server.*` | Port, HTTP2, plaintext/reflection toggles. |
| `quarkus.grpc.clients.metacat.*` | Loopback client config for internal RPC calls. |
| `metacat.seed.enabled` | Enable demo data seeding. |
| `metacat.kv` / `metacat.blob` | Select pointer/blob store implementation (`memory`, `dynamodb`, `s3`). |
| `metacat.plan.*` | Default TTL, grace period, max cache size, safety expiry for plan contexts. |
| `metacat.gc.idempotency.*` | Cadence, page size, batch limit, slice duration for GC. |
| `quarkus.log.*` | JSON logging, file rotation, audit handlers per RPC package. |
| `quarkus.otel.*` / `quarkus.micrometer.*` | Observability exporters. |

Extension points:
- **Storage** – Provide custom `PointerStore`/`BlobStore` (see [`docs/storage-spi.md`](storage-spi.md)).
- **Security** – Replace `Authorizer` or interceptors with CDI alternatives.
- **Connectors** – Register new SPI implementations and expose them via `ConnectorRepository`.
- **Planning** – Supply additional builtin metadata or `CatalogRelation` enrichments by extending
  `BuiltinMetadata`.

## Examples & Scenarios
- **Create Catalog** – `CatalogServiceImpl.createCatalog` canonicalises `display_name`, generates a
  deterministic UUID from the fingerprint, reserves `/tenants/{tenant}/catalogs/by-name/{name}` and
  `/by-id/{uuid}` pointer keys, writes the `catalog.pb` blob, and returns `MutationMeta`. If the
  caller supplies an `IdempotencyKey`, the repository short-circuits duplicates.
- **Delete Namespace** – Namespace deletions with `require_empty=true` check child counts via
  `NamespaceRepository.countChildren`. If tables exist, the service raises `MC_CONFLICT.namespace.not_empty`.
- **Bundle fetch** – When a planner issues `GetCatalogBundle`, `PlanningImpl` verifies
  `catalog.read`, passes the request to `CatalogBundleAssembler`, and responds with a `CatalogBundle`
  referencing deduplicated `TypeInfo`s and builtin functions. The assembler caches builtin metadata
  so includes can be toggled per request.
- **Plan lease renewal** – Clients call `Planning.RenewPlan` before `expires_at`; the store extends
  the TTL if the plan remains `ACTIVE`. A stale or ended plan returns `MC_NOT_FOUND.plan.not_found`.

## Cross-References
- RPC contracts: [`docs/proto.md`](proto.md)
- Connector SPI & implementations: [`docs/connectors-spi.md`](connectors-spi.md),
  [`docs/connectors-iceberg.md`](connectors-iceberg.md), [`docs/connectors-delta.md`](connectors-delta.md)
- Storage implementations consumed by repositories:
  [`docs/storage-spi.md`](storage-spi.md), [`docs/storage-memory.md`](storage-memory.md),
  [`docs/storage-aws.md`](storage-aws.md)
- Reconciler orchestrating connectors: [`docs/reconciler.md`](reconciler.md)
