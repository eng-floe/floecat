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
  parent job payload and enqueues `EXEC_FILE_GROUP` children.
- **`EXEC_FILE_GROUP`**: child execution job. Reads the planned source parquet files through
  `FloecatConnector`, captures file-target stats, generates parquet sidecar index artifacts, and
  records per-file execution results.

## Architecture & Responsibilities
- **`ReconcileJobStore`**: interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **`ReconcilerScheduler`**: Quarkus scheduled bean that polls the job store, leases a job,
  transitions it to running, invokes the appropriate executor, records success/failure counters, and
  cancels child jobs when a parent plan job terminates unsuccessfully after partial enqueue.
- **`ReconcilerService`**: core planning/orchestration:
  1. Resolves connector metadata via the service's `Connectors` RPC.
  2. Plans connector-scoped table and view work while preserving destination namespace/table
     overrides.
  3. Ensures destination catalogs/namespaces/tables exist and updates connector metadata with
     resolved destination IDs.
  4. Instantiates the connector via `ConnectorFactory`.
  5. Handles incremental vs full-rescan logic.
  6. Supports explicit capture modes:
     - `METADATA_ONLY`: advances catalog, table, view, and snapshot metadata without writing stats.
     - `STATS_ONLY`: routes batched snapshot-target capture through the stats control plane.
     - `METADATA_AND_STATS`: ingests metadata and enqueues scoped `STATS_ONLY` follow-up jobs only
       for tables covered by the scoped stats filter.
- **Planning executors**:
  - `ConnectorPlanningReconcileExecutor` handles `PLAN_CONNECTOR`.
  - `DefaultReconcileExecutor` handles `PLAN_TABLE` and `PLAN_VIEW`.
  - `SnapshotPlanningReconcileExecutor` handles `PLAN_SNAPSHOT`.
  - `FileGroupExecutionReconcileExecutor` handles `EXEC_FILE_GROUP`.
- **`GrpcClients`**: provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors, ReconcileExecutorControl).
- **`FloecatConnector`**: remains the only component allowed to touch upstream catalogs, table
  metadata, and object storage. Reconcile planning and file-group execution both go through the
  connector instance; reconcile does not use `ScanBundleService`.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behavior through the
reconcile control RPCs:

- `ReconcileControl.StartCapture(connector_id, full_rescan)`: enqueues a top-level
  `PLAN_CONNECTOR` job via `ReconcileJobStore`.
- `ReconcileControl.CaptureNow(...)`: uses the same split path, but waits for the aggregated
  outcome of the top-level plan job plus any child planning/execution jobs.
- `ReconcileControl.GetReconcileJob(job_id)` / `ListReconcileJobs(...)`: expose both top-level and
  child jobs. `PLAN_CONNECTOR` and `PLAN_SNAPSHOT` reads aggregate child counters for user-facing
  status.

Internally, the scheduler exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding**: when reconciling, the service ensures the connector's declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion**: stats persistence is centralized behind the stats control plane
  (`StatsCaptureControlPlane` / orchestrator). `STATS_ONLY` runs route capture through registry
  engines. `METADATA_AND_STATS` schedules those captures asynchronously via follow-up jobs.
- **Snapshot planning persistence**: the immutable snapshot plan is stored on the parent
  `PLAN_SNAPSHOT` job payload rather than in a separate plan repository. Child `EXEC_FILE_GROUP`
  jobs reference the parent plan by `parentJobId`.
- **File-group execution**:
  - `EXEC_FILE_GROUP` resolves the parent snapshot plan, captures file-target stats, and records
    per-file execution results on the child job payload.
  - Sidecar generation and artifact registration happen per source parquet file.
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
  - in `STATS_ONLY`, destination-table misses are treated as skip/no-op rather than job-fatal
    errors.
  - in `METADATA_AND_STATS`, follow-up `STATS_ONLY` jobs are only enqueued for tables that actually
    match the scoped stats filter.
- **Plan failure behavior**: if a parent plan job fails or is cancelled after enqueuing child jobs,
  the scheduler cancels those children.
- **View reconciliation semantics**: reconcile is current-state, not history-preserving. When an
  upstream view already exists in Floecat, reconcile updates the stored canonical definition in
  place rather than appending a backend version history.
- **Job leasing**: `DurableReconcileJobStore` leases from persisted ready pointers, marks jobs
  running/succeeded/failed through CAS updates, and reclaims expired leases on a best-effort
  interval. Failed jobs are retried with backoff up to configured attempt limits before terminal
  failure.

### Backend selection
- `floecat.reconciler.backend` (default `local`) selects which backend implementation
  `ReconcilerService` uses. Set it to `remote` when the reconciler runs as a separate process and
  must talk to the service over gRPC. In remote mode the reconciler uses
  `floecat.reconciler.authorization.header` / `token` to send an authorization header on every
  call. Per-request tokens supplied via `ReconcileContext` override the static token, so the
  precedence is request-scoped token, then configured token, then no header.

## Data Flow & Lifecycle
```text
Connector StartCapture / CaptureNow
  → ReconcileJobStore.enqueue(PLAN_CONNECTOR)
  → ReconcilerScheduler.pollOnce
      → leaseNext
      → markRunning
      → if PLAN_CONNECTOR:
          → ReconcilerService.planTableTasks / planViewTasks
          → enqueue PLAN_TABLE / PLAN_VIEW children
      → if PLAN_TABLE:
          → ensure destination table metadata
          → enumerate snapshots via FloecatConnector
          → enqueue PLAN_SNAPSHOT children
          → optionally enqueue scoped STATS_ONLY follow-up only for matching tables
      → if PLAN_VIEW:
          → describeView (or listViewDescriptors fallback)
          → ensure destination namespace exists
          → create or update the destination view
      → if PLAN_SNAPSHOT:
          → ask FloecatConnector for planned parquet file membership
          → persist grouped file plan on parent job payload
          → enqueue EXEC_FILE_GROUP children
      → if EXEC_FILE_GROUP:
          → resolve parent PLAN_SNAPSHOT payload
          → instantiate FloecatConnector
          → capture file-target stats for planned files
          → generate parquet sidecar index artifacts
          → persist per-file execution results and artifact registrations
      → markSucceeded or markFailed
```

Jobs include `fullRescan`, `executionPolicy`, `jobKind`, and optional task payloads. Snapshot plan
jobs and file-group jobs also surface file-group/file counters for current execution state.
`ReconcilerScheduler` uses an `AtomicBoolean` guard to prevent concurrent runs within the same
instance.

## Configuration & Extensibility
- Scheduling cadence via `reconciler.pollEvery` (defaults to `1s`).
- Job store selection:
  - `floecat.reconciler.job-store=durable` (service default) uses persisted queue records plus
    retry/lease tuning via:
    `floecat.reconciler.job-store.max-attempts`, `base-backoff-ms`, `max-backoff-ms`, `lease-ms`,
    `reclaim-interval-ms`, and `ready-scan-limit`.
  - `floecat.reconciler.job-store=memory` uses the in-memory queue implementation.
- Swap out `ReconcileJobStore` for additional backends by providing a CDI alternative (job ID
  references must remain stable for `GetReconcileJob`).
- Extend `FloecatConnector` to add richer snapshot planning or file execution behavior. Query scan
  planning remains separate behind `ScanBundleService`.

## Examples & Scenarios
- **Full rescan**: operator triggers `connector trigger demo-glue --full`. The job store enqueues a
  full `PLAN_CONNECTOR` job, the scheduler leases it, and the reconciler walks connector discovery,
  table planning, snapshot planning, and file-group execution across the full upstream history.
- **Incremental run**: without `--full`, `ReconcilerService` restricts its work to the connector's
  configured `source.table` (if set) and only processes newly discovered snapshots.

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector/query RPCs: [`docs/service.md`](service.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
