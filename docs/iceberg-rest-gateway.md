# Iceberg REST Gateway (Floecat)

This module (`protocol-gateway/iceberg-rest`) implements the Apache Iceberg REST catalog contract on top of Floecat’s gRPC services. The gateway translates HTTP requests into the existing table, namespace, view, snapshot, planning, and connector APIs so Iceberg clients (Trino, DuckDB, Spark, custom services) can manage Floecat tables without native gRPC bindings.

---

## Scope and Goals

- **Protocol adapter:** expose the official Iceberg REST catalog surface while reusing Floecat’s authentication, tenancy, logging, and connector infrastructure.
- **Parity with Iceberg REST spec:** support namespace CRUD, table CRUD/commit/register, view CRUD/commit, scan planning (`/plan` + `/tasks`), table-level credentials, and transactional commit via staged payloads.
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
| Table CRUD, commit, register | `TableService`, `SnapshotService`, connector services | Stage-create/commit leverage staged metadata plus `TableService`. Register imports Iceberg metadata via `TableMetadataImportService`. |
| Table rename/move | `TableService.UpdateTable` | Uses field masks for namespace + display name changes. |
| `/tables/{table}/plan`, `/tasks` | `QueryService`, `PlanTaskManager` | Runs synchronous planning, persists result, and exposes per-task payloads; failures return error responses (not 200). |
| `/tables/{table}/credentials` | `ConnectorClient` + gateway defaults | Returns vended credentials based on access delegation mode (defaults to gateway config). |
| `/tables/{table}/metrics` | Logging only | Validates payloads; wiring to `TableStatisticsService` is future work (spec has no stats surface). |
| `/tables/rename`, `/transactions/commit` | Table services + staging store | Replays staged payloads per Iceberg semantics; no cross-table ACID. |
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
│   ├── api/              # Request/response DTOs & serializers
│   ├── common/           # Cross-cutting helpers (context factory, filters, error mappers, metadata utils)
│   ├── resources/        # JAX-RS controllers (config, namespace, table, view, system)
│   └── services/         # Adapters and workflows (accounts, catalog, table, metadata, planning, staging, clients)
└── src/test/java/...     # RestAssured tests, unit tests mirroring the same package structure
```

Key packages under `services`:

- `services.catalog` – low-level helpers (table lifecycle, connector wiring, requirement enforcement, metadata sync).
- `services.table` / `services.view` / `services.namespace` – high-level use cases (create, commit, register, delete, property updates, rename).
- `services.metadata` – metadata import/materialization (Iceberg metadata files, snapshot transforms).
- `services.planning` – scan planning orchestration plus `PlanTaskManager`.
- `services.staging` – staged payload repository and TTL management.
- `services.client` – typed gRPC clients (TableClient, NamespaceClient, ViewClient, SnapshotClient, QueryClient, ConnectorClient, etc.).

Tests mirror this layout so package-private collaborators (e.g., staged table repositories, planners) are still accessible without widening visibility.

---

## Runtime Architecture Overview

1. **Request entry:** Quarkus REST controllers receive Iceberg REST requests. `AccountHeaderFilter` enforces tenant/auth headers and optionally rewrites “prefix-less” paths to a configured default.
2. **Context resolution:** `RequestContextFactory` resolves catalog prefixes, namespace paths, and table IDs by calling Floecat’s `DirectoryService` and `TableLifecycleService`. Context records travel with each request.
3. **Service orchestration:** Controllers delegate to `services.table/*`, `services.view/*`, `services.namespace/*`, etc. These orchestrators build gRPC requests, enforce requirements, and interact with staging, metadata, connectors, or planning as needed.
4. **gRPC translation:** Typed clients (`TableClient`, `SnapshotClient`, `ViewClient`, etc.) wrap `GrpcWithHeaders` so every call inherits Floecat’s auth context and telemetry.
5. **Response mapping:** `TableResponseMapper`, `ViewResponseMapper`, `NamespaceResponseMapper`, and metadata builders synthesize the Iceberg contract (schemas, specs, refs, history) from Floecat responses. They also inject config overrides (e.g., `write.metadata.path`, storage credentials).
6. **Connectors & credentials:** `TableCommitSideEffectService` updates connector records, triggers reconcile for async stats, and resolves storage credentials returned to clients. Snapshot format metadata is backfilled directly from the committed `metadata.json`.
7. **Plan/task caching:** `PlanTaskManager` persists planning results with TTL (default 10 minutes) and chunk size limits, exposing read-once task IDs for `/tasks`.

---

## Stage-create & Commit Flow

### Stage-create (`POST /v1/{prefix}/namespaces/{namespace}/tables` with `stage-create=true`)
1. Validate schema, spec, write order, and properties. Normalize namespace/table identifiers.
2. Compute metadata location, connector configuration, and default storage credentials.
3. Persist a `StagedTableEntry` keyed by account + catalog + namespace + table + stage-id (provided via `Iceberg-Transaction-Id`/`Idempotency-Key` or generated). `StagedTableService` enforces TTL and idempotency.
4. Return `StageCreateResponse` (stage-id, requirements, config overrides, storage credentials). No catalog mutation occurs yet.

### Commit (`POST /tables/{table}`)
1. Resolve catalog/table IDs. If table is missing but exactly one staged entry exists, implicitly use that stage-id; otherwise require the client to provide `stage-id`.
2. `CommitStageResolver` fetches/validates the staged payload (assert-create, assert-current-schema, etc.) and either returns a resolved table ID or synthesizes a new table via `StageCommitProcessor`.
3. `TableUpdatePlanner` diff’s the incoming requirements/updates against the current table metadata, building a `TableSpec` + `FieldMask` for catalog updates. Snapshot changes fan out through `SnapshotMetadataService`.
4. `TableCommitService` sends the update, materializes metadata files via `MaterializeMetadataService`, tags the commit with the final metadata location, and logs stage outcomes.
5. `TableCommitSideEffectService` syncs connector metadata (create/update external connectors, update table upstream, run reconcile). Snapshot format metadata is synced from the committed `metadata.json` in the REST layer.
6. Response: `CommitTableResponseDto` containing the resolved metadata location, metadata view, config overrides, and storage credentials. ETags are set to the metadata location so clients can cache responses.

### `/transactions/commit`

Receives Iceberg’s transaction payload (list of table changes referencing stage-ids). The gateway replays each staged change sequentially using the same path as per-table commits. Requirements (assert stage, schema, snapshot refs, etc.) are enforced per change. The endpoint is idempotent but **does not** offer multi-table ACID guarantees beyond staged payload replay.

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

## Testing

- **REST contract tests:** `*ResourceTest` (RestAssured) validates namespace/table/view endpoints against mocked services.
- **Integration tests:** `IcebergRestFixtureIT` boots real services (via `RealServiceTestResource`) and exercises stage-create, commit, plan, and view flows end-to-end.
- **Unit tests:** live under `src/test/java/.../services/*` mirroring the main packages so service collaborators (planners, staged repositories, metadata builders) can be verified with Mockito.

---

## Operational Notes & Current Limitations

- **Credentials:** `/tables/{table}/credentials` returns vended credentials based on access delegation; per-request signing is not yet implemented.
- **Metrics persistence:** `/tables/{table}/metrics` validates and logs payloads but does not persist them to `TableStatisticsService`.
- **Async planning:** plans are synchronous/completed only; streaming manifests and async planning (`/plans/{id}`) are future work.
- **Multi-table ACID:** `/transactions/commit` replays staged changes sequentially without cross-table rollback.
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
iceberg.rest-catalog.oauth2.server-uri=http://host.docker.internal:12221/realms/floecat/protocol/openid-connect/token
iceberg.rest-catalog.oauth2.scope=openid
```

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
