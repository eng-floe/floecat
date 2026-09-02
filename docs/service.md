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
- **NamespaceServiceImpl** – Handles hierarchical selectors, supports recursive listing, refuses
  restructuring that would strand descendants, and refuses deleting a namespace that still holds
  child namespaces, tables, or views.
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

### Namespace Shape Fencing
A namespace's shape is its set of child namespaces plus its set of tables and views. Two secondary
index keys are derived from a *different* row's mutable fields:

| Derived key | Derived from |
| --- | --- |
| `namespaces/by-path/{path...}` | catalog id + ancestors' display names + own display name |
| `{tables,views,relations}/by-name/{name}` | catalog id + namespace id + relation name |

A repository recomputes secondary keys only for the row it writes, so nothing re-derives the keys of
rows underneath. Renaming or re-parenting a namespace therefore moves its own `by-path` pointer
while its descendants' pointers stay under the old path, and moving a namespace to another catalog
re-keys its relations without moving their pointers. The service refuses these operations rather
than performing a partial restructure:

| Operation | Refused when | Asserts |
| --- | --- | --- |
| Rename namespace | it has any descendant namespace | `markers/children`, and its parent's when it has one |
| Re-parent namespace | it has any descendant namespace | `markers/children`, and the destination parent's when it has one |
| Move namespace to another catalog | it has any descendant namespace, table, or view | both markers, the destination catalog's child set, and the destination parent's when it has one |
| Delete namespace | it has any descendant namespace, table, or view | both markers, removed with the row |
| Move a namespace beneath itself | always | — |

The check is for a descendant at any depth, not an immediate child. A depth-one check is only
sufficient by induction: a descendant can only be added by a writer that asserts its parent's child
marker, so the parent's marker is enough provided every namespace's parent exists. Every writer
materialises its ancestors, so that holds going forward — but it did not always, so a row at `a/b/c`
with no `a/b` row can exist from before, and an immediate-child check cannot see it. A prefix count
also costs one read where a depth-one check costs a paged scan.

Renaming a leaf that holds only relations is allowed, because a relation's key carries the namespace
id rather than the path. Moving a namespace beneath itself is refused outright: the destination parent
resolves to the namespace itself, so every emptiness check passes and the write then vacates the path
it just claimed to live under.

A rename joins its parent's child set, so it serializes against concurrent creates of its siblings
for the same reason relation creates serialize with one another. A **top-level** namespace has no
parent to join, so it asserts no parent condition; what covers that case is the catalog's own child
set, below.

The refusal is atomic with the write. `MarkerStore.childNamespacesFence` and
`MarkerStore.relationsFence` return the marker versions as `PointerConditions.markerVersions`, which
the repository compiles into its own CAS batch as "require this version, then advance it".

Each call site samples them *before* the emptiness checks. That ordering is the call site's
responsibility, not something these methods can enforce: a version sampled after the check is the
version a concurrent writer already moved, so the assertion would confirm that writer instead of
losing to it.

Exclusion holds only because every writer that changes a shape participates in the same assertion.
Two batches that share no key cannot lose to each other, so a single non-participating writer would
void the guard rather than weaken it:

| Writer | Asserts |
| --- | --- |
| `NamespaceServiceImpl` create, update, delete | the parent's or its own markers, in its own batch |
| `TableServiceImpl`, `ViewServiceImpl` create and update | the destination relation marker and namespace row when a relation enters a container where it was not already counted; leaving the source needs no fence |
| `TableServiceImpl`, `ViewServiceImpl` delete | nothing. A namespace delete racing a relation delete can only find the namespace emptier than it counted, which orphans nothing, so asserting here would cost a write to a hot key for no exclusion |
| `CatalogOverlayReconciler` create, update, retire | the shape assertion joined onto its overlay fence via `PointerConditions.and` |
| `TransactionIntentApplierSupport` table intents | the relation marker and the namespace's row, as CAS ops in the same pointer transaction -- on the same terms as a request, so only when the relation enters a container it was not already counted in |

A writer that loses its assertion retries on the shared `FenceRetry` policy, the same budget and
backoff as any other contended write, re-asserting on each attempt what the lost fence
may have invalidated -- the namespace it is joining may have been deleted by whoever won. One that
keeps losing raises a retryable abort.

Because relation writes assert a marker per namespace, concurrent creates into one namespace
serialize on it -- including the transaction commit path, which retries the whole batch when it
loses. "Serialize" here means retry, not queue: without a local retry the loser surfaces to the
client as `TS_APPLY_FAILED_RETRYABLE`, so the commit path carries the same bounded retry the request
paths do. The marker is one key per namespace, so sustained concurrent writes into a single namespace
still contend; sharding it is the remaining work.

That cost is inherent to proving a namespace empty atomically. It is not enough for
both writers merely to read a common key: the delete's emptiness check happens before its batch, so
a create landing in between is only excluded if the create itself *writes* something the delete
asserts. A read-dependency cannot do that, which is why the marker is advanced rather than checked.

The two are used for different jobs. The marker excludes a namespace delete racing a relation write.
A read-dependency on the namespace's own pointer -- which its delete removes -- excludes a relation
write against a namespace that is *already* deleted, which the marker cannot catch because a write
arriving afterwards samples the post-delete version and matches it. Relation writes carry both.

Bootstrap seeding asserts these fences too, and materialises every ancestor on a path it seeds
rather than writing a nested row directly. It is check-then-create, but it runs on an executor while
traffic is served and, on a restart, seeds into a catalog and namespaces it did not create in that
run -- so it is a concurrent writer against a user's delete or rename like any other. Writing a
nested row without its ancestors would make it the one writer that could strand one: it would assert
the catalog's child set but no ancestor's, so a user deleting that ancestor could sample its marker,
scan before the seed row committed, and commit alongside it.

### Catalog Deletion
A catalog holds two kinds of child, counted separately: namespaces and catalog overlays. A namespace's
`by-path` key embeds the catalog **id**, so a catalog deleted from under one leaves that namespace
addressable under a catalog that does not exist.

Only delete can do this. A catalog's own `by-name` key derives from its own display name, which the
repository recomputes when it writes that row, and its children reference it by id — so renaming or
moving a catalog re-keys nothing beneath it.

| Operation | Refused when | Asserts |
| --- | --- | --- |
| Delete catalog | it holds any namespace, or any overlay | `catalogs/{id}/markers/children` and `catalogs/overlays-marker/{id}`, both in the delete's own batch |

Each marker is asserted at the version read before the emptiness checks, and what is asserted depends
on whether it exists. One that has been written is required at that version and **deleted** with the
catalog rather than advanced — the resource it counts for is going, so advancing would leave a row
behind counting nothing. One that has never been written is required **absent**, because the writer
that adds the first child of that kind is the one that creates it, and that write then loses the batch
carrying this assertion. Advancing a marker that was never written would not merely leave a stale row
behind: an absent marker reads as version zero, so the advance would *create* a row for a resource
being deleted in the same batch. The namespace count is read strongly, for the same reason the namespace
guard's is: a marker is sampled with a consistent point read, so a namespace committed just before
that sample is already in the marker version and an eventually-consistent count would read zero
while the CAS matched.

Every writer that adds a namespace to a catalog asserts that catalog's children marker in its own
batch — `CreateNamespace`, the path-chain create behind it, `CatalogOverlayReconciler`'s
materialization, a catalog move (which gains the destination catalog a child exactly as a create
does), and bootstrap seeding. Seeding is fenced here even though it is check-then-create, because on
a restart it seeds into a catalog it did not create in that run, which makes it a genuine concurrent
writer against a user's `DeleteCatalog`.

The catalog half is not a refinement of the parent-namespace fence. A namespace at the root of a
catalog has no parent namespace, so its parent fence is empty and the catalog fence is the only thing
standing between it and a concurrent delete.

Namespace deletes assert nothing about their catalog. Removal is the direction that orphans nothing.

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
- **Delete Namespace** – The service refuses a namespace that still holds child namespaces, tables,
  or views, raising `MC_CONFLICT.namespace.not_empty`. The emptiness checks and the delete commit
  together under the namespace's shape markers, so a child or relation created concurrently either
  loses its assertion or makes the delete lose one.
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
