# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a durable queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service's gRPC APIs to create or update tables, views, snapshots, statistics, and index artifacts.

This component decouples connector execution from the main service so long-running metadata and
file-scoped execution work do not block public gRPC threads. The service submits capture jobs via
`ReconcileControl.StartCapture`, which creates jobs in the reconciler's store.

The current job model is split by responsibility:

- **`PLAN_CONNECTOR`**: top-level connector discovery job. Plans table and view work and enqueues
  child planning jobs.
- **`PLAN_TABLE`**: child table planning job. Ensures destination table metadata exists and enqueues
  snapshot planning work for snapshots that need processing.
- **`PLAN_VIEW`**: child view planning job. Creates or updates exactly one destination view.
- **`PLAN_SNAPSHOT`**: child snapshot planning job. Freezes the immutable snapshot plan on the
  parent job payload, records explicit file-group coverage metadata, and enqueues
  `EXEC_FILE_GROUP` plus `FINALIZE_SNAPSHOT_CAPTURE` children.
- **`EXEC_FILE_GROUP`**: child execution job. Reads the planned source parquet files through
  `FloecatConnector`, captures file-target stats, generates parquet sidecar index artifacts,
  persists per-file execution results, and does not commit snapshot-wide aggregate outputs.
- **`FINALIZE_SNAPSHOT_CAPTURE`**: child finalization job. Validates the persisted snapshot
  coverage, waits for all planned `EXEC_FILE_GROUP` children to finish with persisted success
  results, verifies that the stats store contains exactly the expected file-target records for the
  snapshot, and then writes snapshot-wide aggregate outputs such as table/column stats.

## Architecture & Responsibilities
- **`ReconcileJobStore`**: interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **`RemoteReconcileExecutorPoller`**: Quarkus scheduled bean that leases reconcile jobs through
  the `ReconcileExecutorControl` gRPC control plane, starts heartbeats, invokes the matching local
  executor implementation, reports progress/completion, and repolls while worker capacity is
  available. In `worker.mode=local`, the poller talks to the colocated service over gRPC; in
  `worker.mode=remote`, executor-only nodes use the same lease protocol against a separate control
  plane.
- **`ReconcilerService`**: core planning/orchestration:
  1. Resolves connector metadata via the service's `Connectors` RPC.
  2. Plans connector-scoped table and view work while preserving destination namespace/table
     overrides.
  3. Ensures destination catalogs/namespaces/tables exist and updates connector metadata with
     resolved destination IDs.
  4. Instantiates the connector via `ConnectorFactory`.
  5. Handles incremental vs full-rescan logic.
  6. Supports explicit capture modes:
     - `METADATA_ONLY`: advances catalog, table, view, and snapshot metadata without capture.
     - `CAPTURE_ONLY`: captures stats / index artifacts for explicitly scoped destination tables
       without reconciling view metadata.
     - `METADATA_AND_CAPTURE`: ingests metadata and runs capture for matching table work in the same
       job tree.
- **Lease executors**:
  - `RemotePlannerReconcileExecutor` handles `PLAN_CONNECTOR`.
  - `RemoteDefaultReconcileExecutor` handles `PLAN_TABLE` and `PLAN_VIEW`.
  - `RemoteSnapshotPlanningReconcileExecutor` handles `PLAN_SNAPSHOT`.
  - `RemoteFileGroupReconcileExecutor` handles `EXEC_FILE_GROUP`.
  - `SnapshotFinalizeReconcileExecutor` handles `FINALIZE_SNAPSHOT_CAPTURE`.
- **`GrpcClients`**: provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors, ReconcileExecutorControl).
- **`FloecatConnector`**: remains the only component allowed to touch upstream catalogs, table
  metadata, and object storage. Reconcile planning and file-group execution both go through the
  connector instance; reconcile does not use `ScanBundleService`.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behavior through the
reconcile control RPCs:

- `ReconcileControl.StartCapture(scope, mode, full_rescan, execution_policy)`: enqueues a
  top-level `PLAN_CONNECTOR` job via `ReconcileJobStore`.
- `ReconcileControl.CaptureNow(...)`: uses the same split path, but waits for the aggregated
  outcome of the top-level plan job plus any child planning/execution jobs.
- `ReconcileControl.GetReconcileJob(job_id)` / `ListReconcileJobs(...)`: expose both top-level and
  child jobs. `PLAN_CONNECTOR` and `PLAN_SNAPSHOT` reads aggregate child counters for user-facing
  status.

Internally, the worker poller exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding**: when reconciling, the service ensures the connector's declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion**: stats persistence is centralized behind the stats control plane
  and the reconcile executor control plane. `CAPTURE_ONLY` routes capture planning through the
  same reconcile job tree without metadata reconciliation. `METADATA_AND_CAPTURE` performs metadata
  reconciliation and capture within the same planner/executor job tree. Remote file-group workers
  submit file-target stats and staged index artifacts back through
  `SubmitLeasedFileGroupExecutionResult`, and the service persists those results before
  `FINALIZE_SNAPSHOT_CAPTURE` writes any snapshot-wide aggregate stats.
- **Snapshot planning persistence**: the immutable snapshot plan is stored on the parent
  `PLAN_SNAPSHOT` job payload rather than in a separate plan repository. That payload includes the
  explicit file-group coverage metadata required by `FINALIZE_SNAPSHOT_CAPTURE`. Child
  `EXEC_FILE_GROUP` and `FINALIZE_SNAPSHOT_CAPTURE` jobs reference the parent plan by
  `parentJobId`.
- **File-group execution**:
  - `EXEC_FILE_GROUP` resolves the parent snapshot plan, captures file-target stats, and records
    per-file execution results on the child job payload.
  - Snapshot-wide aggregate outputs are intentionally deferred to
    `FINALIZE_SNAPSHOT_CAPTURE`, which acts as the barrier for complete snapshot capture.
  - Sidecar generation and artifact registration happen per source parquet file.
  - Service-side result submission persists only file-target stats from file-group workers;
    aggregate table/column outputs are rejected from file-group completion and recomputed once at
    snapshot finalization time.
  - `SubmitLeasedFileGroupExecutionResult` requires `result_id`. The service records top-level
    idempotency for the whole submit payload and per-item idempotency for individual stats/artifact
    writes so worker retries can safely replay the same result.
  - Current snapshot reads surface `file_groups_total`, `file_groups_completed`,
    `file_groups_failed`, `files_total`, `files_completed`, and `files_failed`.
- **Index artifacts**:
  - sidecars are parquet artifacts written by execution workers and registered through
    `IndexArtifactRecord`.
  - service-side lookup/list/read is exposed by `TableIndexService`.
- **Connector security boundary**: all upstream I/O remains inside `FloecatConnector`.
  `ScanBundleService` stays query-plane only; reconcile snapshot planning uses connector-native
  snapshot file planning.
- **Mode-aware behavior**:
  - in `CAPTURE_ONLY`, destination-table misses are treated as skip/no-op rather than job-fatal
    errors.
  - `CAPTURE_ONLY` is table-scoped: view scope is rejected, and scoped capture requests must
    resolve to explicit destination table IDs.
  - in `METADATA_AND_CAPTURE`, planner/executor availability is validated up front based on scope:
    table scope requires `PLAN_TABLE`, view scope requires `PLAN_VIEW`, and broad metadata reconcile
    requires both.
- **Plan failure behavior**: if a parent plan job fails or is cancelled after enqueuing child jobs,
  the control plane cancels those children.
- **View reconciliation semantics**: reconcile is current-state, not history-preserving. When an
  upstream view already exists in Floecat, reconcile updates the stored canonical definition in
  place rather than appending a backend version history.
- **Job leasing**: `DurableReconcileJobStore` leases from persisted ready pointers, marks jobs
  running/succeeded/failed through CAS updates, and reclaims expired leases on a best-effort
  interval. Failed jobs are retried with backoff up to configured attempt limits before terminal
  failure.

### gRPC auth
- Reconcile workers use the gRPC control plane for leasing, progress, and standalone worker
  payload/result exchange.
- `floecat.reconciler.authorization.header` configures the outbound header name used for bearer
  auth on worker gRPC calls.
- In OIDC mode, background workers obtain a machine token via client credentials using
  `floecat.reconciler.oidc.issuer`, `client-id`, `client-secret`,
  `token-refresh-skew-seconds`, and `connect-timeout`.
- Precedence is explicit:
  1. request-scoped propagated auth header when present
  2. reconciler machine OIDC token
  3. no auth header
- `ReconcileExecutorControl` accepts either normal propagated user auth (`connector.manage`) or
  the dedicated internal worker permission carried by the reconciler service principal.

## Data Flow & Lifecycle
```text
Connector StartCapture / CaptureNow
  → ReconcileJobStore.enqueue(PLAN_CONNECTOR)
  → RemoteReconcileExecutorPoller.pollOnce
      → leaseNext
      → markRunning
      → if PLAN_CONNECTOR:
          → ReconcilerService.planTableTasks / planViewTasks
          → enqueue PLAN_TABLE / PLAN_VIEW children
      → if PLAN_TABLE:
          → ensure destination table metadata
          → enumerate snapshots via FloecatConnector
          → enqueue PLAN_SNAPSHOT children
      → if PLAN_VIEW:
          → describeView
          → ensure destination namespace exists
          → create or update the destination view
      → if PLAN_SNAPSHOT:
          → ask FloecatConnector for planned parquet file membership
          → persist grouped file plan on parent job payload
          → enqueue EXEC_FILE_GROUP children
          → enqueue FINALIZE_SNAPSHOT_CAPTURE child
      → if EXEC_FILE_GROUP:
          → resolve parent PLAN_SNAPSHOT payload
          → instantiate FloecatConnector
          → capture file-target stats for planned files
          → generate parquet sidecar index artifacts
          → persist per-file execution results and artifact registrations
      → if FINALIZE_SNAPSHOT_CAPTURE:
          → validate explicit planned coverage metadata
          → wait for all planned EXEC_FILE_GROUP children to succeed with persisted results
          → verify exact file-target coverage in the stats store
          → roll up snapshot-wide aggregate outputs
      → markSucceeded or markFailed
```

Jobs include `fullRescan`, `executionPolicy`, `jobKind`, and optional task payloads. Snapshot plan
jobs, file-group jobs, and snapshot finalization jobs also surface file-group/file counters for
current execution state.
`RemoteReconcileExecutorPoller` uses `AtomicBoolean` and in-flight counters to avoid over-leasing
within the same instance while continuing to repoll until worker slots are full.

## Configuration & Extensibility
- Scheduling cadence via `reconciler.pollEvery` (defaults to `1s`).
- Worker mode via `floecat.reconciler.worker.mode`:
  - `local` runs the lease poller in the same JVM as the control plane.
  - `remote` keeps the same gRPC lease protocol but is intended for executor-only nodes. Set
    `reconciler.max-parallelism=0` on control-plane-only nodes.
- Worker capacity via `reconciler.max-parallelism`.
- Job store selection:
  - `floecat.reconciler.job-store=durable` (service default) uses persisted queue records plus
    retry/lease tuning via:
    `floecat.reconciler.job-store.max-attempts`, `base-backoff-ms`, `max-backoff-ms`, `lease-ms`,
    `reclaim-interval-ms`, and `ready-scan-limit`.
  - `floecat.reconciler.job-store=memory` uses the in-memory queue implementation.
- Executor toggles:
  - `floecat.reconciler.executor.remote-default.enabled`
  - `floecat.reconciler.executor.remote-planner.enabled`
  - `floecat.reconciler.executor.remote-snapshot-planner.enabled`
  - `floecat.reconciler.executor.remote-file-group.enabled`
  - `FINALIZE_SNAPSHOT_CAPTURE` is handled by the service-local `SnapshotFinalizeReconcileExecutor`
    and is not behind a separate feature toggle.
- Swap out `ReconcileJobStore` for additional backends by providing a CDI alternative (job ID
  references must remain stable for `GetReconcileJob`).
- Extend `FloecatConnector` to add richer snapshot planning or file execution behavior. Query scan
  planning remains separate behind `ScanBundleService`.

## Examples & Scenarios
- **Full metadata rescan**: operator triggers
  `connector trigger demo-glue --full --mode metadata-only`. The job store enqueues a
  full `PLAN_CONNECTOR` job, the worker poller leases it, and the reconciler walks connector
  discovery and metadata planning across the full upstream history.
- **Incremental capture run**: operator triggers
  `connector trigger demo-glue --mode metadata-and-capture --capture stats`. The reconcile path
  captures table/file/column stats for matching table work while still allowing metadata mutation.
- **Incremental run**: without `--full`, `ReconcilerService` restricts its work to the connector's
  configured `source.table` (if set) and only processes newly discovered snapshots.

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector/query RPCs: [`docs/service.md`](service.md)
- Rust file-group worker implementation guide:
  [`docs/rust-remote-capture-executor.md`](rust-remote-capture-executor.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
