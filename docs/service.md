# Service Runtime

## Overview
The `service/` module is the authoritative runtime for Floecat. It hosts the Quarkus gRPC server,
implements every public API from [`proto/`](proto.md), manages multi-account security contexts,
translates requests into pointer/blob mutations, assembles execution scan bundles, and operates background
tasks such as idempotency/CAS/pointer/transaction/reconcile-job GC and repository seeding.

For durable reconcile jobs, the service now owns a native domain-split queue model rather than one
generic pointer-prefix queue abstraction:
- canonical job-index domain
- ready-queue domain
- lease-coordination domain
- canonical payload-artifact references on job rows
- projection/root-summary observability domain

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
│  │      Directory, Statistics, Integrations, Overlays, Connectors, Query)   │
│  ├─ QueryService (QueryContextStore, QueryServiceImpl)                     │
│  ├─ Repositories (CatalogRepository, NamespaceRepository, TableRepository, │
│  │                ViewRepository, SnapshotRepository, StatsRepository,     │
│  │                ConnectorRepository, AccountRepository,                   │
│  │                IdempotencyRepositoryImpl)                               │
│  └─ GC & Bootstrap (IdempotencyGc, CasBlobGc, PointerGc, TransactionGc,    │
│                  ReconcileJobGc, SeedRunner)                                │
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
- `service/catalog` / `directory` / `account` / `statistics` / `integration` / `connector` – gRPC service
  implementations.
- `catalog/builtin` – Shared builtin catalog data model, validator, and loader helpers.
- `service/query` – Query lifecycle management (`QueryContext`, `QueryContextStore`,
  `QueryServiceImpl`).
- `service/metagraph` – MetadataGraph runtime (façade, loader, resolvers, hint manager, topology
  cache); the immutable node models live in `core/metagraph`, and the shared caches
  (`ImmutableBlobCache`, `PointerTtlCache`) in `service/repo/cache/` (see
  [`docs/metadata-graph.md`](metadata-graph.md) and [`docs/caching.md`](caching.md)).
- `service/gc` – Scheduled cleanup for idempotency records, orphan pointers/blobs, stale transaction
  artifacts, and durable reconcile jobs.
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
- **TableStatisticsServiceImpl** – Persists per-snapshot target/file stats; validates
  NDV/histogram payloads; paginates target listings (optionally filtered by target kind); uses
  client-streaming `PutTargetStats` to batch writes per stream.
- **DirectoryServiceImpl** – Provides fast name↔ID lookup via `MetadataGraph` (Resolve*/Lookup*) and
  reuses the graph’s ResolveFQ helpers for list/prefix pagination.
- **AccountServiceImpl** – Administers accounts and enforces conventional permissions. Account
  deletion installs a durable write fence, then uses strongly consistent enumeration to remove
  catalog overlays before their integrations, followed by legacy connectors and local catalog
  descendants. Retrying a committed account delete resumes cleanup using the deletion record's
  original mutation metadata.
- **CatalogIntegrationsImpl** – Provides CRUD for external catalog identity records. The contract is
  intentionally independent of legacy Connectors. It persists typed non-secret authentication
  configuration for OAuth client credentials, bearer tokens, AWS assume-role/access-key, and AWS
  SigV4. Secret values are accepted only in write-only credential messages, stored through the
  SecretsManager under a deterministic integration-ID/credential-generation key, and never included
  in resource responses. A typed credential-store resolver is available to follow-on catalog clients. A dedicated
  authentication update RPC replaces configuration and rotates credentials while advancing a
  visible credential generation. Unity integrations accept OAuth or bearer authentication; Iceberg
  REST integrations accept every structurally valid authentication form the API supports.
  Vendor/endpoint-specific compatibility is intentionally deferred to adapter selection and
  validation in a follow-on change; this service does not yet perform connectivity validation or
  reconciliation. Catalog endpoint URIs must
  be hierarchical HTTP(S) URLs and cannot contain user-info, secret-bearing query parameters, or
  fragments. Type is immutable through update; endpoint updates preserve the Integration identity
  while advancing its etag. The API exposes `CM_REPLACE` and
  `CM_RETURN_EXISTING` as create-conflict primitives.
  Replacement publishes a new resource identity and atomically swaps the name pointer before the
  old identity is removed. Integration replacement is rejected while overlays depend on the old
  identity. Cascading delete uses a durable fence so retries resume dependent-overlay cleanup before
  removing the integration, dependency marker, and fence together. Idempotent creates publish the
  immutable success receipt in the same pointer transaction as the resource, so a retry never
  rebuilds its response from subsequently mutable state. Creates carrying credentials support
  idempotency. Secret values are excluded from durable request fingerprints and receipts, so the
  first successfully published credential value wins for retries of the same key. Credential writes
  happen before resource publication; definite publication failures remove the unpublished
  generation, and replaced credentials are deleted only after the resource CAS commits. If the
  publication acknowledgement is uncertain, the new secret is retained so a visible integration can
  never reference a deleted secret. Durable cleanup records are drained by pointer GC once a
  generation is provably superseded.
  Get accepts either resource ID or
  exact display name. Reads require
  `catalog-integration.read`; writes require `catalog-integration.write`, and cascading deletion of
  dependent overlays also requires `catalog-overlay.delete`.
- **CatalogOverlaysImpl** – Binds an integration and optional upstream namespace filters to an
  existing Catalog. The Catalog remains independently named, writable, and managed, and multiple
  overlays may target it. Creating an overlay atomically publishes Integration and Catalog
  dependency pointers while requiring both parents to remain at their validated versions, and
  advances both dependency markers in the same transaction. `CM_REPLACE` atomically swaps in a new
  overlay identity and may bind it to another Integration or Catalog while advancing all affected
  dependency markers; `CM_RETURN_EXISTING`
  returns the existing object. Get accepts either
  resource ID or exact display name. An empty include list
  selects all namespaces; paths select subtrees, exclusions
  take precedence, and matching is case-sensitive. The Integration and Catalog bindings are immutable through
  update; updates only replace the selected include/exclude lists or rename the overlay. Reads require
  `catalog-overlay.read`; create and update require `catalog-overlay.write`, and creation also
  requires `catalog-integration.use` and `catalog.write`.
  Reconciliation requires `catalog-overlay.reconcile` plus `catalog-integration.use`; deletion
  requires `catalog-overlay.delete`. These dedicated permissions are not
  inferred from `catalog-overlay.write` or namespace/table/view permissions. Connectivity is
  deferred.
- **ConnectorsImpl** – Manages connector lifecycle, validates `ConnectorSpec` via SPI factories,
  wires reconciliation job submission, and exposes `ValidateConnector` + `StartCapture`.
  `CaptureNow` maps to reconciler capture modes:
  - metadata only -> `METADATA_ONLY`
  - capture only -> `CAPTURE_ONLY`
  - metadata plus capture -> `METADATA_AND_CAPTURE`
- **QueryServiceImpl** – Administers query leases (`BeginQuery`, `RenewQuery`, `EndQuery`,
  `GetQuery`) and exposes the scan streaming helpers (`InitScan`, `StreamDeleteFiles`,
  `StreamDataFiles`, `CloseScan`) so planners can request connector metadata safely.
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
pins snapshots, and stores the lease. Planners request connector file lists with `InitScan` (which
returns table metadata and a scan handle), then consume `StreamDeleteFiles` followed by
`StreamDataFiles`, and finally call `CloseScan` when done. Ordering is strict: `StreamDeleteFiles`
must be fully consumed before `StreamDataFiles` begins, otherwise the server rejects the data stream
with `FAILED_PRECONDITION`. The server auto-releases the scan session as soon as both streams finish,
so `CloseScan` is best-effort but still recommended to tidy server resources sooner. Each `DataFile`
currently reports `DeleteRef.all_deletes=true`; finer-grain delete references will come later once the
applicability logic is defined. The lease data (snapshots, expansion map, obligations) is returned to
the caller inside the `QueryDescriptor`.

### Builtin Catalog Service
`SystemObjectsLoader` reads immutable builtin catalogs (`<engine_kind>.pb[pbtxt]`) from the
configured location, caches them by engine kind, and exposes them through
`SystemObjectsService.GetSystemObjects`. Clients must send both `x-engine-kind` and
`x-engine-version`; the RPC always returns the filtered builtin bundle for the requested engine.

### GC and Bootstrap
`IdempotencyGc` runs on a configurable cadence (see `floecat.gc.*` config) and sweeps expired
idempotency records in slices to avoid starvation. `CasBlobGc` performs a reachability-based sweep
per account: the referenced set is built from live pointers, the pin roots of live query contexts,
and the chains walked out of current table roots (root blob, manifest pages, and every
definition/snapshot/generation-manifest/constraints blob they reference). A pinned root protects
its whole chain, so pinned blobs stay readable for the query's lifetime. Deletes are fenced by a
30 s min-age (`floecat.gc.cas.min-age-ms`, age since the blob was written), and any failed
root-chain walk poisons the account's delete phase — the referenced set is untrustworthy, so
nothing is deleted that pass (fail closed). CAS GC is disabled by default because query pin roots
are process-local. In a multi-replica deployment, enable `FLOECAT_GC_CAS_ENABLED=true` on exactly
one designated control-plane replica only when all live query contexts are visible to that replica;
otherwise leave it disabled. A retained account continuation is abandoned after
`floecat.gc.cas.max-consecutive-continuation-ticks` so one large account cannot starve every other
account; raise that bound if the oldest-sweep-age metric shows a large account repeatedly restarting.
Snapshot compatibility artifacts under `snapshots/<id>/compat/` are gateway-managed mutable
artifacts, not CAS objects, and remain owned by explicit snapshot/table lifecycle cleanup rather
than this CAS sweep. `PointerGc` removes
orphan/stale pointers. `TransactionGc` reaps expired/aborted transaction artifacts and dangling
intent indices. `ReconcileJobGc` enforces durable reconcile retention and queue/dedupe cleanup for
terminal jobs. The default finished reconcile-job retention window is 24 h
(`floecat.gc.reconcile-jobs.retention-ms=86400000`); older terminal jobs are removed from job-list
and job-detail views after that window unless operators configure a longer retention period or update
connector settings with `connector settings update --finished-job-retention-sec`. It is
retention-oriented GC, not a queue repair or index rebuild loop. `SeedRunner` populates demo data
when `floecat.seed.enabled=true`.

For connector-backed fixture tables, seeding runs a combined reconcile pass per fixture scope
using `METADATA_AND_CAPTURE`.

This ingests metadata/snapshots and runs capture through the reconcile job tree for stats.
Query scan bundles remain available immediately; stats availability follows queued capture completion.
Follow-up payloads use reconcile scoped stats requests
(`table_id`, `snapshot_id`, `target_spec`, `column_selectors`) so background capture stays targeted
without depending on separate unresolved snapshot-id and target lists.
When a `CAPTURE_ONLY` batch captures only a subset of requested items, the reconcile result
is degraded; when none of the requested items are captured, the reconcile result fails instead of
silently reporting zero processed stats.

### Statistics streaming semantics
`TableStatisticsServiceImpl` enforces a single `table_id` + `snapshot_id` per streamed call to
`PutTargetStats`, rejects mixed idempotency keys within a stream, and applies
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
| `floecat.query.resolver.max_parallel_inputs` | Per-request query-input pin-resolution fan-out. Defaults to `8`; values are clamped to `1`–`16`. |
| `floecat.query.metadata-io.max-concurrency` | Process-wide admission bound for blocking metadata I/O shared by all requests. Missing values use `64`; present malformed, blank, or out-of-range values fail startup. |
| `floecat.catalog.bundle.max_parallel_relations` | Per-chunk relation-build fan-out for GetUserObjects. Defaults to `8`. |
| `floecat.catalog.bundle.max_parallel_stats_warms` | Per-chunk stats-warm fan-out and shared process-wide stats-warm ceiling. Defaults to `16`; clamped to `>= 1`. |
| `floecat.gc.idempotency.*` | Cadence, page size, batch limit, slice duration for idempotency GC. |
| `floecat.gc.cas.*` | Cadence, page size, min-age, tick slice settings for CAS blob GC. |
| `floecat.gc.pointer.*` | Cadence, page size, min-age, tick slice settings for pointer GC. |
| `floecat.gc.reconcile-jobs.*` | Cadence, retention, and slice settings for durable reconcile-job GC. Finished terminal jobs default to 24 h retention. |
| `floecat.reconciler.worker-affinity` | Exact job-tree contract cohort shared by a control plane and its executor fleet (default `reconciler-v1`). |
| `floecat.reconciler.job-store.*` | Durable reconcile queue selection and retry/lease tuning. |
| `quarkus.log.*` | JSON logging, file rotation, audit handlers per RPC package. |
| `quarkus.otel.*` / `quarkus.micrometer.*` | Observability exporters (see [`docs/operations.md`](operations.md)). |
| `floecat.auth.mode` | Auth enforcement mode (`oidc`, `dev`). |
| `floecat.auth.platform-admin.role` | IdP role name granted permission to manage accounts (default `platform-admin`). |
| `floecat.secrets.aws.role-arn` | Optional role to assume per account when using AWS Secrets Manager. |

Extension points:
- **Storage** – Provide custom `PointerStore`/`BlobStore` (see [`docs/storage-spi.md`](storage-spi.md)).
- **Durable reconcile storage** – The queue-facing reconciler store is split into native
  job-index, ready, lease, and projection domains. Those are the extension seams to target if the
  durable reconcile backend evolves further; they are no longer intended to share one generic
  pointer-prefix queue abstraction.
- **Security** – Replace `Authorizer` or interceptors with CDI alternatives.
- **Connectors** – Register new SPI implementations and expose them via `ConnectorRepository`.
- **QueryService** – Extend query metadata by enriching `QueryContext` creation or injecting
  additional connector metadata via the `QueryScanService` streaming RPCs / `ScanBundleService` on the query
  path. Reconcile planning/execution does not use `ScanBundleService`; it goes through
  `FloecatConnector` directly. `BeginQuery` optionally accepts a client-specified `query_id` plus
  `common.QueryInput` records so lifecycle can pre-pin snapshots/expansions for deterministic
  replay.

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
