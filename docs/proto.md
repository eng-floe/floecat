# Protobuf & RPC Contracts

## Overview
Floecat's public surface is entirely gRPC. The `core/proto/` module defines canonical protobuf
structures for resource identifiers, catalog services, query lifecycle metadata, connectors, statistics, and
helper schemas. Every other module depends on these contracts for serialization, validation, and
compatibility.

### Remote executor versioning

Snapshot plans now require one immutable file execution plan for every declared file path. Remote
planner/finalizer executors and the control plane must therefore be cut over together after existing
leases are drained. Rolling or split deployments containing both plan revisions are unsupported;
there is intentionally no empty-plan compatibility fallback.

The contract files are organised by domain (`common/`, `catalog/`, `query/`, `execution/`,
`connector/`, `account/`, `types/`, `statistics/`, `reconciler/`). Generated Java stubs live
under the `ai.floedb.floecat.*.rpc` packages and are consumed by the Quarkus service, connectors,
CLI, and reconciler.

## Architecture & Responsibilities
- **`common/common.proto`** – Defines `QueryInput`, `ResourceId`, `NameRef`, `SnapshotRef`, pagination, rich error
  payloads, `PrincipalContext`, and idempotency/optimistic-concurrency helpers. Every other schema
  imports this file.
- **`catalog/*.proto`** – CRUD APIs for catalogs, namespaces, tables, views, snapshots, directory
  lookups, table statistics, and index artifacts. Each service exposes the same
  Create/List/Get/Update/Delete (CLGUD) lifecycle with `PageRequest`/`PageResponse` support; stats
  schemas also cover per-file statistics and index artifact metadata.
- **`connector/connector.proto`** – Connector management RPCs plus reconciliation job tracking and
  validation routines.
- **`query/lifecycle.proto`** – Query lifecycle (`BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`) and the
  snapshot pin metadata sent down to the SQL planner.
- **`query/system_objects_registry.proto`** – `GetSystemObjects` plus message definitions for builtin functions,
  operators, casts, collations, aggregates, and types loaded from static files.
- **`query/user_objects_bundle.proto`** – `GetUserObjects` streams resolved relation metadata for planner binding, including per-column `ColumnResult` outcomes (`READY` with `ColumnInfo` or `FAILED` with `ColumnFailure`).
- **`execution/scan.proto`** – Scan metadata (data/delete files + per-file stats) produced by
  connectors and consumed at execution time.
- **`execution/capture.proto`** – File-group execution RPCs used by the combined stats/index
  capture worker path.
- **`reconciler/reconciler.proto`** – Reconcile queue/job tracking contracts, including split
  planning/execution job kinds and per-file execution results.
- **`types/types.proto`** – Logical type registry (Boolean/Decimal/etc.) and scalar encodings used by
  statistics and bundles.
- **`account/account.proto`** – Account CRUD service for multi-tenancy.

## Public API / Surface Area
### Core Services
| Service | Key RPCs | Inputs/Outputs |
|---------|----------|----------------|
| `CatalogService` | `ListCatalogs`, `GetCatalog`, `CreateCatalog`, `UpdateCatalog`, `DeleteCatalog` | Accepts `CatalogSpec`, optional `IdempotencyKey`, `Precondition`, and `FieldMask` for partial updates. Returns `Catalog` + `MutationMeta`. |
| `NamespaceService` | `ListNamespaces`, `GetNamespace`, `CreateNamespace`, `UpdateNamespace`, `DeleteNamespace` | Supports hierarchical selectors (`path`, `recursive`, `children_only`). |
| `TableService` | `ListTables`, `GetTable`, `CreateTable`, `UpdateTable`, `DeleteTable` | `TableSpec` carries `UpstreamRef` with connector link, schema JSON, and partition info. |
| `ViewService` | Similar CRUD semantics, storing SQL definitions and metadata. |
| `SnapshotService` | `ListSnapshots`, `GetSnapshot`, `GetLatestFinalizedSnapshot`, `CreateSnapshot`, `DeleteSnapshot` | Pins upstream checkpoints and timestamps. The bounded latest-finalized lookup selects a root-published reuse basis without scanning snapshot history. Finalized snapshots expose a system-owned `reuse_manifest_ref`; `SnapshotSpec` does not accept that field. |
| `TableStatisticsService` | `GetTargetStats`, `ListTargetStats`, client-streaming `PutTargetStats` | Accepts per-snapshot target stats envelopes (table/column/expression/file). `ListTargetStats` supports target-kind filtering (currently at most one kind per request); streaming writes collapse multiple batches into a single call. |
| `TableIndexService` | `GetIndexArtifact`, `GetIndexCaptureStatus`, `ListIndexArtifacts`, client-streaming `PutIndexArtifacts` | Stores and resolves snapshot-scoped parquet sidecar artifact metadata and bounded-cost finalized capture status keyed by table and snapshot. |
| `TableConstraintsService` | `GetTableConstraints`, `ListTableConstraints`, `PutTableConstraints`, `MergeTableConstraints`, `AppendTableConstraints`, `DeleteTableConstraints`, `AddTableConstraint`, `DeleteTableConstraint` | Snapshot-scoped constraints CRUD for user tables. `PutTableConstraints` is full-bundle upsert, `MergeTableConstraints` is server-side merge by `constraint.name` plus shallow merge of bundle `properties` (incoming keys win), `AppendTableConstraints` is server-side append-only (duplicate names rejected), and `AddTableConstraint`/`DeleteTableConstraint` are single-constraint partial mutations. All write operations require snapshot existence (`NOT_FOUND` when missing). |
| `DirectoryService` | `Resolve*` & `Lookup*` RPCs | Translates between names and `ResourceId`s with pagination for batched lookups. |
| `AccountService` | Account CRUD. |
| `Connectors` | Connector CRUD, `ValidateConnector`, `StartCapture`, `GetReconcileJob`. |
| `QueryService` | `BeginQuery`, `RenewQuery`, `EndQuery`, `GetQuery`. |
| `QuerySchemaService` | `DescribeInputs`. |
| `QueryScanService` | `InitScan`, `StreamDeleteFiles`, `StreamDataFiles`, `CloseScan`. |
| `ReconcileControl` | `CaptureNow`, `StartCapture`, `GetReconcileJob`, `GetReconcileJobTree`, `ListReconcileJobs`, `GetFinalizedSnapshotStatus`, `CancelReconcileJob`, `GetReconcilerSettings`, `UpdateReconcilerSettings` | Client-facing reconcile control plane for synchronous execution, queued jobs, finalized snapshot status lookup, cancellation, and automatic reconcile defaults. `GetFinalizedSnapshotStatus` returns `FSS_PENDING` when no finalized snapshot record exists yet for the requested table/snapshot pair; `FSS_FINALIZED` includes `finalized_at` and `finalizer_job_id`. |
| `ReconcileExecutorControl` | `LeaseReconcileJob`, `GetLeasedPlan*Input`, `SubmitLeasedPlan*Result`, `GetLeasedFileGroupExecution`, `CommitLeasedFileGroupResult` | Executor-facing lease protocol for split reconcile workers. Carries typed planner payloads, immutable file execution identity, reusable bundle selections, artifact-bundle commits, progress, cancellation, and completion RPCs. |
| `PlannerStatsService` | `GetTargetStats`, `GetTableConstraints` | Split planner-facing streams for target stats and table constraints; `GetTargetStats(include_constraints=true)` remains as a combined single-roundtrip convenience mode. |
| `UserObjectsService` | `GetUserObjects` | Streams catalog metadata chunks (header → relations → end) as the service resolves each relation so planners can start binding earlier. |
| &nbsp;&nbsp;&nbsp;— Consumption pattern | | Clients read `UserObjectsBundleChunk` in three phases: 1) header chunk (cheap metadata), 2) zero or more `resolutions` chunk batches where each `RelationResolution` carries `input_index` + FOUND/NOT_FOUND/ERROR, and 3) a single end chunk with summary counts. Use `input_index` to map back to planner `TableReferenceCandidate`s and bind as soon as a `FOUND` arrives. For each `RelationInfo`, inspect `columns[*].status`: `COLUMN_STATUS_OK` exposes `columns[*].column`, while `COLUMN_STATUS_FAILED` exposes `columns[*].failure` with typed `ColumnFailureCode` plus details. Extension-defined failures must use `COLUMN_FAILURE_CODE_ENGINE_EXTENSION` and set `extension_code_value`; clients branch on `extension_code_value` inside the engine domain (for FloeDB, see `FloeDecorationFailureCode` in `extensions/floedb/src/main/proto/engine_floe.proto`). |
| `SystemObjectsService` | `GetSystemObjects` | Returns the builtin catalog filtered by the `x-engine-kind` / `x-engine-version` headers supplied with the request. |

Each RPC requires a populated `account_id` within the `ResourceId`s; the Quarkus service checks this
before hitting repository storage.

### Planner Lifecycle & Execution Scan Schemas
`query/lifecycle.proto` captures everything the planner needs to hold a lease:
- `QueryDescriptor` mirrors the live query context (IDs, expiry timestamps, snapshot pins, expansion
  maps, and table obligations). Per-table scan manifests are retrieved lazily via
  `QueryScanService`: clients call `InitScan`, fully consume `StreamDeleteFiles`, then consume
  `StreamDataFiles` for a specific table before `CloseScan`.

`execution/scan.proto` describes the scan inputs that executors consume:
- `ScanFile` entries include the file path, size, record count, format, per-column stats, and whether
  the file is data vs equality/position deletes.
- `ScanFileContent` enumerates the delete/data categories.

`query/system_objects_registry.proto` exposes immutable builtin metadata via `SystemObjectsService.GetSystemObjects`
so planners can hydrate functions/operators/types once per engine version. Clients send the
`x-engine-kind` and `x-engine-version` headers and always receive the filtered catalog for that
engine release.

## Important Internal Details
- **Field numbering** – All proto files reserve low numbers for required identity fields and push
  experimental metadata to `map<string,string> properties = 99`. Adapt new fields by appending to
  the end to preserve wire compatibility.
- **`ResourceKind` enforcement** – Services verify that IDs have the expected kind (for example
  tables must be `RK_TABLE`). Clients should populate the `kind` enum to improve error messages.
- **`SnapshotRef` semantics** – `oneof which { snapshot_id | as_of | special }`. `special` currently
  allows `SS_CURRENT`. Planner RPCs interpret `as_of` timestamps when enumerating snapshots.
- **Snapshot artifact reuse state** – `Snapshot.reuse_manifest_ref` identifies the finalized
  `SnapshotCaptureManifest` by URI, byte length, SHA-256, and stats-generation manifest URI. It is
  service-owned and is not part of `SnapshotSpec`. Its `format_version` must match the current
  capture-manifest contract; older references are rejected before their blobs are read.
  `SnapshotManifestEntry.reuse_stats_generation_ref` heads the compact stats generation used by
  that manifest.
- **External reusable-index compatibility** – external planner and finalizer deployments must
  regenerate bindings for `ReusableArtifactIndexReference` format 1 and its immutable sorted-run,
  run-manifest, and block messages. The old trie-root representation has no compatibility reader.
  `AppendOnlySnapshotBase.reusable_artifact_index` is an opaque inherited input that must be
  authenticated when present. External finalizers submit the current
  `reusable_artifact_bundles` delta and construct the complete structurally shared
  `SnapshotCaptureManifest.reusable_artifact_index`; unchanged immutable runs remain referenced and
  bounded level compaction replaces merged runs. Roughly 512 KiB logical data blocks are packed
  into content-addressed objects of up to 64 MiB; block references carry byte ranges and hashes so
  point lookup can use object-store range reads. Run objects up to 64 KiB may be carried in
  `inline_payload`; larger objects use their content-addressed URI. The service binds the inactive
  generation directly to that capture manifest and performs fenced activation without copying
  inherited mappings or reopening inherited bundles. File-group-only executors are unaffected
  because their
  leased payload still contains resolved bundle selections rather than index runs.
  `AppendOnlySnapshotBase.chain_depth` counts inherited append-only links so planners can force a
  periodic full-capture checkpoint.
- **File-level stats** – `FileTargetStats` anchors counts and sketches to
  a file path. File stats are written as `TargetStatsRecord` values with `target.file` identity via
  `PutTargetStats`; the service enforces consistent `table_id`/`snapshot_id` in a stream. Its
  canonical target storage ID is `file-<sha256>` over `F`, `0x1f`, and the trimmed UTF-8 file path.
  This differs from the `file:<source-file-path>` identity used by file index targets.
- **Index artifact streams** – `PutIndexArtifacts` requires each client stream to target exactly one
  `table_id` and one `snapshot_id`. Multiple snapshots must be written through separate client
  streams.
- **Leased file-group result commit** – `CommitLeasedFileGroupResult.Success` and `.Failure` both
  require `result_id`. Success carries a `FileGroupArtifactBundleDescriptor`: one immutable bundle
  object plus the file-stats and index target IDs mapped to it. The durable result descriptor's
  `artifact_references_sha256` binds the expanded target mappings to that result. The control plane
  first accepts the result and completes the child job, then protects the bundle, stages
  generation-scoped references without reading it, and writes the prepared marker. Snapshot
  finalization waits for that marker.
  These writes are ordered and idempotent rather than one atomic storage transaction. A timeout,
  retryable error, or uncertain outcome requires an exact replay of the same success request, even
  when the child job is already terminal. Snapshot finalization carries group descriptors,
  normalized reusable-bundle compatibility indexes, aggregate pointers, and counts. It requires
  every prepared marker and publishes the stats and index generations with the snapshot root in one
  visibility transition. Successful finalization clears protections; failed or cancelled full
  rescans delete the unpublished generation.
  `FileGroupResultPayload.reusable_artifact_bundle` describes the shared bundle and its per-target
  fingerprints and capture signatures. `SnapshotCaptureManifest.reusable_artifact_bundles` carries
  only bundles produced by the current file groups. The service applies those entries to the
  authenticated prior `reusable_artifact_index` runs and publishes the resulting immutable run set
  as the complete compatibility index. A later planner can select reusable targets without
  reading bundle payloads, source files, or page-index sidecars. A selected file-group worker
  verifies and reads each compact bundle once. This compatibility index is executor-authored
  metadata: the control plane validates its structure, leased ownership, counts, content-addressed
  bundle descriptor, and staged target mappings, but deliberately does not GET the bundle to derive
  or compare metadata. The consuming worker verifies bundle length and SHA-256 and rejects a
  selected record that is absent or incompatible before reuse.
  `FileGroupResultPayload.realized_stats_selectors` and `.realized_index_selectors` record the
  concrete selector aliases materialized by each group. The finalizer aggregates them into the
  corresponding `SnapshotCaptureManifest` fields so durable content state can satisfy later
  requests expressed through an equivalent name, field ID, or narrower default selection. Every
  explicitly requested selector must be reported verbatim; equivalent aliases are additional
  coverage and are not inferred from selector counts. Reusable index metadata repeats the concrete
  selectors for each wrapper so planning can prove explicit coverage without reading its bundle;
  default index capture signatures also bind the execution schema that resolved the selection.
  Index sidecar placement remains executor-controlled through `IndexArtifactRecord.artifact_uri`.
  The serialized wrapper is stored in the file-group reusable artifact bundle. Finalize manifests
  repeat the leased capture policy exactly, including opaque properties.
- **File reuse identity** – `FileExecutionPlan.content_identity` comes from immutable connector
  metadata; an empty value disables cross-snapshot reuse. `source_fingerprint`,
  `index_source_fingerprint`, auxiliary stats fingerprints, and stats/index capture signatures bind
  each selected wrapper to its physical source context and requested policy.
- **File-group delete artifacts** – `FileExecutionPlan` attaches Iceberg position/equality delete
  files and Delta deletion vectors to a planned data file. When stats are requested, exact group
  coverage includes the planned data files, each distinct attached Iceberg delete path, and each
  on-disk Delta deletion vector (`storage_type` `u` or `p`). Inline Delta vectors (`i`) remain
  unsupported. Auxiliary targets increase stats descriptor counts but do not increase data-file
  progress counts or page-index coverage, and they are excluded from table/column aggregate
  rollups. One Iceberg delete file may recur in multiple groups; those repeated references are
  duplicate work rather than additional logical files. Snapshot finalization verifies equivalent
  compatibility metadata and retains one canonical target mapping without reading the delete-file
  content. File-group descriptors retain group-level counts, while
  `SnapshotCaptureManifest.file_stats_record_count` reports the deduplicated unique-target count.
- **File-group planning ceiling** – snapshot planning uses at most 128 files per group by default.
  `floecat.reconciler.snapshot-plan.max-files-per-group` configures that ceiling and is clamped to
  at least one. The service rejects submitted plans containing a group above the same configured
  ceiling and validates commits against the immutable planned group.
- **Executor leasing filters** – `LeaseReconcileJobRequest` accepts execution class, lane, job kind,
  `executor_id`, and repeated `executor_ids` selectors so a worker fleet can advertise both its
  concrete worker identity and the executor implementations it is willing to run. Versioned
  workers also declare `worker_affinity`; the server rejects a mismatch and echoes the job's
  affinity in `LeasedReconcileJob` for worker-side validation. Empty affinity requests are rejected
  by a versioned control plane, keeping unversioned workers isolated to an unversioned deployment.
- **Stats vs constraints snapshot policy** – `PutTargetStats` currently accepts unknown snapshots
  (lenient ordering), while `PutTableConstraints` is strict and requires a materialized snapshot
  row before write. Rationale: stats keeps existing capture ordering compatibility, while
  constraints are modeled as snapshot-attached relational facts.
- **Planner split vs combined retrieval** – `PlannerStatsService.GetTableConstraints` provides a
  dedicated constraints-only stream, while `GetTargetStats(include_constraints=true)` remains
  available as a combined convenience mode. Split mode is relation-scoped (table visibility pruning
  only) because `FetchTableConstraintsRequest` does not carry column projection context; combined
  mode can apply relation+column request-shape-aware pruning. For constraints lookups,
  `provider_missing` means no bundle exists, while `provider_empty` means a bundle exists and is
  explicitly empty. For planner client simplicity, both are currently surfaced as
  `BUNDLE_RESULT_STATUS_NOT_FOUND` (same as `pruned_empty`), with
  `failure.details.reason` preserving the distinction.
  For CHECK masking to work correctly, connector constraint payloads should populate
  `ConstraintDefinition.columns` with referenced local column IDs.
- **FOREIGN KEY metadata model** – `ConstraintDefinition` carries ANSI-style FK behavior
  fields (`referenced_constraint_name`, `match_option`, `update_rule`, `delete_rule`) so
  `information_schema.referential_constraints` can be populated without connector-specific
  interpretation. Writers may omit these fields; scanners default unspecified rules to
  `NONE` / `NO ACTION` / `NO ACTION`.
- **Idempotency/Preconditions** – Mutating RPCs accept `IdempotencyKey` or `Precondition` (expected
  CAS version/ETag). Repository logic mirrors these fields, so clients should obey the same values
  when retrying.
- **Query Lifecycle** – `QueryDescriptor.query_status` moves through `SUBMITTED → COMPLETED/FAILED`
  depending on connector planning success. Lease expirations are surfaced via `expires_at`.
- **AuthConfig** – Connector auth carries structured `credentials` (for example `bearer`, `cli`,
  `client`, `token-exchange-*`) plus free-form properties; the service resolves secrets and exchanges
  before connectors consume them.

## Data Flow & Lifecycle
1. Clients authenticate using the configured OIDC session/authorization headers (see
   [`docs/service.md`](service.md#security-and-context)) and call gRPC endpoints.
2. Mutations include `IdempotencyKey` for once-and-only-once semantics; the service persists a hash
  of the request along with the resultant `MutationMeta` so replays yield the previous payload.
3. Connectors written against the SPI return `ScanFile`, stats, and file-group capture metadata
  that exactly match the protos defined here; the reconciler pipes them back via the
  catalog/statistics/index services.
4. Planners call `QueryService.BeginQuery` to create query leases, optionally resolve additional
   inputs via `QuerySchemaService.DescribeInputs`, extend leases via `RenewQuery`, call
   `QueryScanService` per table when they need manifests, and close leases out via `EndQuery` once
   execution is complete.

   * BeginQuery allows clients to provide an optional `query_id` (duplicates are rejected) and
     a list of `common.QueryInput` records so the lifecycle service can pin snapshots and expansions
     at creation time for deterministic replay. Schema resolution and planning still occur in the
     downstream services.

_State diagram for the query lease protocol:_

```
[BeginQuery] --> (QueryContext: SUBMITTED)
    | planning succeeds
    v
(QueryContext: COMPLETED) --renew--> (extend expires_at)
    | EndQuery(commit=true/false)
    v
(ENDED_COMMIT or ENDED_ABORT) --grace--> [expiry]
```

## Configuration & Extensibility
- **Evolving protos** – Prefer `optional` fields for new metadata. Keep enum values stable; add new
  entries to the end. Reserve field numbers explicitly if deprecating to avoid reuse.
- **Temporal precision** – `types.LogicalType.temporal_precision` is optional. Absence means
  default microsecond precision, while an explicit `0` represents second precision.
- **Interval range** – `types.LogicalType.interval_range` distinguishes `INTERVAL YEAR TO MONTH`
  vs `INTERVAL DAY TO SECOND`. In the JVM model, absence is normalised to `IR_UNSPECIFIED`.
  Leading and fractional precisions live in `interval_leading_precision` and
  `interval_fractional_precision`.
- **Custom properties** – Many records expose `map<string,string> properties` for lightweight
  extensions. Document keys in the consuming module (for example connector-specific hints in
  [`docs/connectors-spi.md`](connectors-spi.md)).
- **Query leases** – Clients decide how aggressively to renew leases; planners should renew before
  `expires_at` and call `EndQuery` even on failure so `QueryContextStore` can release pins eagerly.

## Examples & Scenarios
### Creating a table via gRPC
```bash
grpcurl -plaintext -d '{
  "spec": {
    "catalog_id": {"account_id":"T","id":"C","kind":"RK_CATALOG"},
    "namespace_id": {"account_id":"T","id":"N","kind":"RK_NAMESPACE"},
    "display_name": "events",
    "schema_json": "{...Iceberg schema...}",
    "upstream": {
      "connector_id": {"account_id":"T","id":"conn","kind":"RK_CONNECTOR"},
      "uri": "s3://warehouse",
      "namespace_path": ["prod"],
      "table_display_name": "events"
    }
  },
  "idempotency": {"key": "create-events"}
}' localhost:9100 ai.floedb.floecat.catalog.TableService/CreateTable
```

### Beginning a query lifecycle lease
```bash
grpcurl -plaintext -d '{
  "inputs": [
    {"name": {"catalog":"demo","path":["sales"],"name":"events"}}
  ]
}' localhost:9100 ai.floedb.floecat.query.QueryService/BeginQuery
```

## Cross-References
- Service runtime, interceptors, and repository adapters: [`docs/service.md`](service.md)
- Connector SPI implementations consuming these protos:
  [`docs/connectors-spi.md`](connectors-spi.md),
  [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
- Query lifecycle internals: [`docs/service.md#query-lifecycle-service`](service.md#query-lifecycle-service)
