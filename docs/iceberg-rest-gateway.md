# Iceberg REST Gateway (Floecat)

This module (`protocol-gateway/iceberg-rest`) implements the Apache Iceberg REST catalog contract on top of Floecat’s gRPC services. The gateway translates HTTP requests into the existing table, namespace, view, snapshot, planning, and connector APIs so Iceberg clients (Trino, DuckDB, Spark, custom services) can manage Floecat tables without native gRPC bindings.

---

## Scope and Goals

- **Protocol adapter:** expose the official Iceberg REST catalog surface while reusing Floecat’s authentication, tenancy, logging, and connector infrastructure.
- **Parity with Iceberg REST spec:** support namespace CRUD, table CRUD/commit/register, view CRUD/commit, scan planning (`/plan` + `/tasks`), table-level credentials, and transactional commit via Iceberg table-change payloads.
- **Single catalog focus:** one Floecat catalog per REST prefix (multi-catalog ACID transactions remain out of scope).
- **Reusability:** keep Iceberg-specific plumbing isolated, allowing additional protocols to reuse the same staging, metadata, and connector services.

Non-goals for the current release:

- Serving manifests/files directly from the gateway.
- Async/streaming scan planning.
- Stats persistence (metrics payloads are validated and logged, not stored).

---

## REST Surface vs Floecat gRPC

| Iceberg REST surface | Floecat service(s) | Notes |
| --- | --- | --- |
| `/v1/config` | Gateway config only | Synthesizes prefixes, default properties, supported endpoints; `warehouse` is optional and defaults to the configured prefix. |
| Namespace CRUD | `NamespaceService` | Includes property mutation and existence checks (HEAD); create returns 200 with `CreateNamespaceResponse`. |
| Table CRUD, commit, register | `TableService`, `SnapshotService`, connector services | Stage-create uses staged metadata; commit delegates to transactional commit orchestration. Register imports Iceberg metadata via `TableMetadataImportService`. |
| Table rename/move | `TableService.UpdateTable` | Uses field masks for namespace + display name changes. |
| `/tables/{table}/plan`, `/tasks` | `QueryService`, `PlanTaskManager` | Runs synchronous planning, persists result, and exposes per-task payloads; failures return error responses (not 200). |
| `/tables/{table}/credentials` | `ConnectorClient` + gateway defaults | Returns vended credentials based on access delegation mode (defaults to gateway config). |
| `/tables/{table}/metrics` | Logging only | Validates payloads; wiring to `TableStatisticsService` is future work (spec has no stats surface). |
| `/tables/rename`, `/transactions/commit` | Table services + transaction service | Validates requirement/update payloads and commits all table changes in one backend transaction (with idempotent replay). |
| View CRUD/commit/rename | `ViewService` + `ViewMetadataService` | Maintains Iceberg view schemas, versions, and summaries. |
| `/oauth/tokens` | Disabled | Floecat uses existing auth headers; endpoint returns OAuth error `unsupported_grant_type` (400). |
| `/register-view` | `ViewService` + `ViewMetadataService` | Registers an Iceberg view from a metadata location. |
| Snapshots/history endpoints | Not in OpenAPI | Snapshot metadata is surfaced via table load/commit responses; gRPC snapshot APIs are used internally for writes. |
| Schema fetch endpoints | Not in OpenAPI | Schema gRPC stubs exist but no REST endpoint calls them today. |

---

## Module & Package Layout

```
protocol-gateway/iceberg-rest/
├── src/main/java/ai/floedb/floecat/gateway/iceberg/rest
│   ├── api/              # Iceberg REST DTOs, request models, metadata views, error payloads
│   ├── catalog/          # Catalog/name resolution and shared table gateway helpers
│   ├── compat/           # Delta-to-Iceberg compatibility translation/materialization
│   ├── namespace/        # Namespace resources and namespace service logic
│   ├── support/          # Filters, error mappers, commit logging, metadata/location helpers, gRPC facade
│   ├── system/           # Config and OAuth endpoints
│   ├── table/            # Table resources, lifecycle, commit planning/apply, metadata mapping, staging
│   └── view/             # View resources and view metadata/service logic
└── src/test/java/...     # RestAssured tests, unit tests mirroring the same package structure
```

Tests mirror this structure. Most unit tests live beside the concrete package they exercise, which keeps package-private collaborators such as planners, staged repositories, metadata builders, and mappers testable without widening visibility.

---

## Runtime Architecture Overview

1. **Request entry:** Quarkus REST controllers receive Iceberg REST requests. `AccountHeaderFilter` enforces tenant/auth headers and optionally rewrites “prefix-less” paths to a configured default.
2. **Context resolution:** `CatalogResolver`, `ResourceResolver`, `NameResolution`, and `TableGatewaySupport` resolve prefixes, namespaces, table IDs, view IDs, storage defaults, and current table state by calling Floecat directory/table gRPC services.
3. **Workflow orchestration:** Resource classes under `namespace/`, `table/`, and `view/` delegate into concrete services such as `TableLifecycleService`, `TransactionCommitService`, `TransactionExecutor`, `CommitPlanBuilder`, `ViewService`, and `NamespaceService`.
4. **gRPC translation:** `GrpcWithHeaders` and `GrpcServiceFacade` ensure backend RPC calls inherit Floecat auth headers and request context while keeping the REST surface decoupled from individual stubs.
5. **Response mapping:** `TableResponseMapper`, `TableMetadataBuilder`, `ViewMetadataService`, and namespace/view DTO mappers synthesize the Iceberg contract from Floecat table state plus resolved Iceberg metadata. They also inject config overrides such as `write.metadata.path` and vended credentials.
6. **Commit orchestration:** table commits are normalized into transaction commits, parsed once into typed internal updates, resolved against authoritative current table facts, planned into catalog + snapshot pointer changes, optionally pre-materialized into canonical Iceberg metadata files, and then applied atomically through backend `begin/prepare/commitTransaction`.
7. **Plan/task caching:** `PlanTaskManager` persists planning results with TTL (default 10 minutes) and chunk size limits, exposing read-once task IDs for `/tasks`.

---

## Authentication Notes

- The gateway uses `floecat.gateway.auth-mode=oidc` to require a JWT and derives the account from the configured claim (`floecat.gateway.account-claim`, default `account_id`).
- The JWT is read from `floecat.gateway.authHeader` (default `authorization`) and is validated by Quarkus OIDC.
- For OIDC validation you must enable/configure Quarkus OIDC in the gateway:
  - `quarkus.oidc.tenant-enabled=true`
  - One of: `quarkus.oidc.auth-server-url=...` or `quarkus.oidc.public-key=...`
  - Optional: `quarkus.oidc.token.audience=...`
- If you customize the auth header, continue sending `Bearer <jwt>` as the value.

---

## Stage-create & Commit Flow

### Stage-create (`POST /v1/{prefix}/namespaces/{namespace}/tables` with `stage-create=true`)
1. Validate schema, spec, write order, and properties. Normalize namespace/table identifiers.
2. Apply a default table location when the caller omitted one, reserve the exact bootstrap metadata path (`00000-...metadata.json`), and inject `table-uuid` plus `metadata-location` into the staged create properties.
3. Build a `TableSpec` and persist a `StagedTableEntry` keyed by account + catalog + namespace + table + stage-id. The stage-id comes from `Iceberg-Transaction-Id`, then `Idempotency-Key`, else a generated UUID. Staged entries are kept in `StagedTableRepository`.
4. Return a load-table-style `LoadTableResultDto` carrying the reserved metadata location, config overrides, and storage credentials. No catalog mutation occurs yet.

### Commit (`POST /tables/{table}`)
1. Resolve catalog/namespace/table context with one request-scoped `TableGatewaySupport` instance and reject unsupported commit modes (for example Delta read-only tables). Pre-commit reads, durable commit, and post-commit hydration all use the same support object so one request observes one catalog/table context.
2. Normalize the request into the correct first-write shape before planning:
   - if this is a staged create commit and the table still has no committed snapshot, prepend staged create initialization and inject the reserved metadata location when needed
   - if this is a non-staged first write after create and the table still has no committed snapshot, rebuild the same bootstrap from the cached pending create state
   - once a committed snapshot already exists, no bootstrap is injected and the commit is treated as a normal existing-table update
3. Wrap the effective table commit into a single-entry `TransactionCommitRequest` and delegate to `TransactionCommitService`.
4. `TransactionCommitService` runs explicit phases:
   - request validation
   - backend `beginTransaction`
   - backend `getTransaction` readback to confirm the transaction handle and idempotent replay hash
   - request-context construction (`CommitRequestContext`)
   - authoritative current-state loading and target resolution
   - normalization, requirement validation, update compilation, and metadata planning
   - durable backend `prepareTransaction` / `commitTransaction`
   - local outbox/finalization
   - optional response hydration
5. `CommitPlanBuilder` resolves one `CurrentTableState` per table change. That object is the source for requirement validation and planning fields such as schema/spec/sort-order IDs, last assigned IDs, table UUID, ref snapshot IDs, and metadata location.
6. The gateway then applies commit updates to canonical metadata and materializes the next `metadata.json`. For first-write create commits it writes the reserved exact metadata location; for later commits it writes the next versioned metadata file.
7. Backend `prepareTransaction` + `commitTransaction` apply all prepared changes atomically; success returns HTTP 204 from the transactional layer.
8. The table endpoint then performs best-effort response hydration and returns `CommitTableResponseDto` (HTTP 200). If the durable commit succeeded but hydration fails or is incomplete, the gateway returns a minimal success response instead of surfacing a misleading commit failure.

### What the client writes vs what the gateway writes

- Engines such as Trino write data files, manifests, manifest lists, delete files, and statistics files referenced by their commit payloads.
- The gateway writes the canonical Iceberg `metadata.json` file and advances the catalog pointer to that metadata location.
- DuckDB commonly uses the staged-create path; Trino commonly uses the non-staged create followed by a first snapshot commit. Both flows are supported by the current implementation.

Idempotency behavior:
- `Idempotency-Key` is the request replay key used by commit orchestration.
- Same key + same payload replays the prior response.
- Same key + different payload returns `409 Conflict`.
- `IN_PROGRESS` records are guarded with timeout; stale records can be retried.
- Prior `5xx` failures are retryable; prior `4xx` failures are terminal and replayed.

### `/transactions/commit`

Receives Iceberg’s transaction payload (`table-changes` with `identifier`, `requirements`, `updates`).
The gateway validates each change, builds one backend transaction containing all table and snapshot
pointer mutations, and commits atomically. The endpoint returns:

- `204` only when backend state is `TS_APPLIED`.
- `409` for deterministic conflicts/failed preconditions.
- `5xx` when commit state is unknown.

The request is parsed once near the entry point into `ValidatedTableChange` / `ParsedCommit`, and
later phases consume that typed internal form instead of repeatedly reinterpreting raw
`List<Map<String,Object>>` update payloads.

Outcome handling is centralized in `TransactionOutcomePolicy`:

- deterministic failures before remote side effects remain normal validation/conflict errors
- begin accepted but readback failed is treated as `AMBIGUOUS_BEGIN_READBACK`
- prepare handoff failures with uncertain remote state are treated as `AMBIGUOUS_PREPARE_CONFIRMATION`
- commit responses or confirmation states that cannot prove applied/not-applied are treated as `AMBIGUOUS_COMMIT_STATE`
- post-commit response-building problems are treated as hydration failures, not commit failures

For ambiguous backend outcomes, the gateway does not blindly abort. In particular, if
`beginTransaction` succeeds but `getTransaction` fails, the gateway does not reuse prepare-failure
mapping and does not translate that condition into `NoSuchTableException` solely because readback
returned `NOT_FOUND`. It is surfaced as unknown transaction state.

For ambiguous backend outcomes after prepare/commit handoff (for example retryable/aborted
ambiguity), the gateway performs a short bounded confirmation poll before returning unknown-state
errors. Poll behavior is configurable:

- `floecat.gateway.commit.confirm.max-attempts` (default `6`)
- `floecat.gateway.commit.confirm.initial-sleep-ms` (default `25`)
- `floecat.gateway.commit.confirm.max-sleep-ms` (default `200`)

### Authoritative table facts

The gateway now resolves current table facts in one place through `CurrentTableState`.

- Iceberg metadata JSON is authoritative when present.
- Mirrored backend table properties are fallback only when the Iceberg metadata genuinely lacks the field.
- If metadata and mirrored properties both exist and disagree, the gateway fails closed rather than silently reconciling them.

This rule is used for table UUID, current schema ID, default spec ID, default sort order ID,
last assigned field/partition IDs, and ref snapshot IDs.

`metadata-location` has request-aware precedence during commit planning and materialization:

- if the client supplied a valid `metadata-location` in the commit payload, that location is authoritative for that request
- otherwise the gateway uses the persisted current metadata location

The gateway never falls back to internal Floecat `resourceId` values to satisfy Iceberg
`table-uuid` semantics.

---

## Scan Planning & Task Consumption

1. `TablePlanService` resolves the table ID, applies snapshot filters (start/end snapshot, stats fields, filter expressions), and issues `BeginQuery` + `FetchScanBundle` against Floecat’s `QueryService`.
2. The resulting plan bundle is immediately completed (no async state today). `PlanTaskManager` registers the descriptor (plan-id, namespace, table, credentials, delete files) and chunks file scan tasks into deterministic task IDs (`{planId}-task-{n}`) based on configured chunk size.
3. `/tables/{table}/plan` returns the plan descriptor, aggregated file scan tasks, delete files, storage credentials, and the list of `planTasks`.
4. `/tables/{table}/tasks` accepts a `planTask` ID and consumes it exactly once, returning only the payload for that task. Invalid/consumed IDs return Iceberg-style 404 responses.
5. `/tables/{table}/plan/{planId}` GET/DELETE expose cached descriptors and allow clients to cancel a plan (which also cancels the underlying query if still open).

Limits/Follow-ups:
- Plans are returned as `"completed"` today; failures return Iceberg error responses instead of `status=failed`.
- TTL (default 10 minutes) and chunk size (default 128 files per task) are configurable via `application.properties`.

---

## View Semantics

- `ViewMetadataService` builds Iceberg-compatible view metadata blobs (schemas, versions, version logs, representations) and stores them along with user properties.
- REST view requests/responses mirror the OpenAPI contract (SQL text, schema JSON, properties, requirements/updates).
- View rename/move paths map to `ViewService.UpdateView` by updating `namespace_id` + `display_name`.

---

## Delta Compatibility Layer

The gateway now supports loading Floecat Delta tables through the Iceberg REST surface so engines
like DuckDB can query `examples.delta.<table>` via the same REST attach used for native Iceberg
tables.

### How it works

1. On Delta table load, the gateway translates Delta table/snapshot/schema state into Iceberg
   metadata JSON (including `snapshot-log`, `refs`, and Iceberg-compatible primitive type names).
2. For each returned Delta snapshot that lacks a manifest list, the gateway materializes Iceberg
   compat artifacts:
   - data manifest: `<table-root>/metadata/<snapshot-id>-compat-m0.avro`
   - delete manifest (when Delta delete vectors exist): `<table-root>/metadata/<snapshot-id>-compat-d0.avro`
   - position-delete files (generated from Delta DV bitmaps): `<table-root>/metadata/<snapshot-id>-compat-pd-*.avro`
   - manifest list: `<table-root>/metadata/snap-<snapshot-id>-compat.avro`
3. On each Delta load/query, compat artifacts are resolved by deterministic snapshot path:
   - existing `snap-<snapshot-id>-compat.avro`: reuse
   - missing `snap-<snapshot-id>-compat.avro`: regenerate manifest + manifest-list from the
     original Delta snapshot state at read time

This gives "refresh-on-read" behavior without requiring clients to know anything about Delta,
and no marker file/state is required.

Load responses follow Iceberg REST `snapshots` semantics:
- `snapshots=all` returns all valid snapshots
- `snapshots=refs` returns only snapshots currently referenced by branches/tags (empty if no refs)

ETags for load responses are representation-aware and vary by `snapshots` mode.

### Configuration

- `floecat.gateway.delta-compat.enabled=true` enables Delta compatibility translation/materialization.
- `floecat.gateway.delta-compat.read-only=true` keeps behavior read-only from the compatibility path.

### Storage behavior

- Compat files are written to object storage under the Delta table’s own `metadata/` prefix
  (same bucket/prefix family as the source Delta table), not served from in-memory-only state.

### Current limitations

- Supported delete behavior:
  - Delta `remove` actions that fully remove parquet files are reflected correctly (removed files
    are absent from generated Iceberg data manifests).
  - Copy-on-write deletes (remove old parquet file, add rewritten parquet file) are reflected
    correctly from the active Delta snapshot file set.
- Only on-disk Delta deletion vectors are projected today; inline deletion vectors are currently skipped.
- Equality-delete projection is not implemented; compatibility materialization emits Iceberg position deletes.

---

## Commit Guarantees (Current)

- **Single-table core state:** synchronous and strongly consistent within the request. Table/snapshot metadata needed for the next client commit/read is advanced in the core path.
- **Post-core side effects (stats sync/reconcile trigger/snapshot prune):** best-effort after backend apply. These are not atomic with the core commit.
- **Post-commit response hydration:** best-effort after durable commit. If hydration fails, the commit remains successful and the gateway returns a minimal success response rather than a misleading commit failure.
- **Multi-table `/transactions/commit`:** atomic backend transaction across all table changes in one request.

---

## Testing

- **REST contract tests:** `*ResourceTest` (RestAssured) validates namespace/table/view endpoints against mocked services.
- **Integration tests:** `IcebergRestFixtureIT` boots real services (via `RealServiceTestResource`) and exercises stage-create, commit, plan, and view flows end-to-end.
- **Unit tests:** live under `src/test/java/.../rest/*` mirroring the main packages so collaborators such as planners, staged repositories, metadata builders, mappers, and property services can be verified with Mockito.
- **Compose smoke:** `make compose-smoke` exercises real engine behavior against the gateway. In LocalStack mode it covers DuckDB staged commits, Trino non-staged commits, and Delta fixture reads, including `examples.delta.dv_demo_delta = 2` after a delete.

---

## Operational Notes & Current Limitations

- **Register IO scope:** `POST /v1/{prefix}/namespaces/{namespace}/register` now treats
  FileIO properties as request-scoped connector config. Runtime/global storage wiring
  (`floecat.storage.aws.*`)
  is no longer required for register flows. Use the register payload `properties` for
  `io-impl`, `s3.endpoint`, `s3.region`, `s3.access-key-id`, `s3.secret-access-key`,
  `s3.path-style-access`, etc. when non-default storage wiring is needed (for example LocalStack).
  Request-supplied FileIO properties are merged over gateway defaults from
  `floecat.gateway.storage-credential.properties.*`.
- **Credentials:** `/tables/{table}/credentials` returns vended credentials based on access
  delegation; per-request signing is not yet implemented. Auth resolution supports `aws.profile`
  and `aws.profile_path` when clients expect AWS SDK profile-based access.
- **Metrics persistence:** `/tables/{table}/metrics` validates and logs payloads but does not persist them to `TableStatisticsService`.
- **Async planning:** plans are synchronous/completed only; streaming manifests and async planning (`/plans/{id}`) are future work.
- **Multi-table ACID scope:** atomic within a single `/transactions/commit` backend transaction; request validation rejects duplicate table identifiers.
- **Side-effect orchestration:** post-commit sync/prune actions are best-effort and can lag committed table state.
- **Commit ambiguity:** when backend state cannot be confirmed after begin/prepare/commit handoff, the gateway prefers explicit unknown-state errors over unsafe cleanup or misleading table/conflict mappings.
- **Manifest/file serving:** the gateway does not serve manifests or data files directly; clients access storage through the credentials/config returned in REST responses.

---

## Client Quick Start

### DuckDB

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET floe_secret (
  TYPE s3,
  KEY_ID '<access-key>',
  SECRET '<secret-key>',
  SCOPE 's3://<bucket>/',
  REGION '<AWS region>'
);

ATTACH 'analytics' AS iceberg_floecat
  (TYPE iceberg,
   ENDPOINT 'http://localhost:9200/',
   AUTHORIZATION_TYPE none,
   ACCESS_DELEGATION_MODE 'none');

CREATE TABLE iceberg_floecat.core.quark_events (event_id INTEGER);
INSERT INTO iceberg_floecat.core.quark_events VALUES (1), (2), (3), (4);
SELECT * FROM iceberg_floecat.core.quark_events;
```

### Trino

`etc/catalog/analytics_rest.properties`:

```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.prefix=analytics
iceberg.rest-catalog.uri=http://host.docker.internal:9200
iceberg.rest-catalog.warehouse=s3://my-warehouse/
iceberg.rest-catalog.view-endpoints-enabled=false

fs.native-s3.enabled=true
s3.aws-access-key=<access-key>
s3.aws-secret-key=<secret-key>
s3.region=<AWS region>

# OIDC (Keycloak)
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=trino-client:trino-secret
iceberg.rest-catalog.oauth2.server-uri=http://host.docker.internal:8080/realms/floecat/protocol/openid-connect/token
iceberg.rest-catalog.oauth2.scope=openid
```

Note: if Trino runs on the same Docker network as Keycloak (`docker_floecat`), you can use
`http://keycloak:8080/realms/floecat/protocol/openid-connect/token` instead. If it does not share
the network, use `host.docker.internal`.

Restart Trino and run:

```sql
CREATE TABLE analytics.sales.stream_orders (
  order_id BIGINT,
  region VARCHAR
) WITH (
  format = 'PARQUET',
  location = 's3://my-warehouse/analytics/sales/stream_orders/'
);

INSERT INTO analytics.sales.stream_orders VALUES (1, 'east'), (2, 'west');
SELECT * FROM analytics.sales.stream_orders;
```

Trino picks up the REST prefix/warehouse from the catalog properties, while the gateway injects consistent metadata paths and credentials.

---

## References

- [Iceberg REST catalog spec](https://github.com/apache/iceberg/blob/master/open-api/rest-catalog-open-api.yaml)
- Floecat module: `protocol-gateway/iceberg-rest`
- Configuration: `protocol-gateway/iceberg-rest/src/main/resources/application.properties`
