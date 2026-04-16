# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service’s gRPC APIs to create tables, snapshots, statistics, and scan bundles.

This component decouples connector execution from the main service so that long-running scans do not
block gRPC threads. The service submits capture jobs via `ReconcileControl.StartCapture`, which
creates jobs in the reconciler’s store.

The current job model is split:

- **`PLAN_CONNECTOR`** – top-level connector job. Planning resolves the connector, enumerates the
  table- and view-scoped work, and enqueues child jobs.
- **`EXEC_TABLE`** – child table job. Execution performs metadata and/or stats work for exactly one
  planned table task.
- **`EXEC_VIEW`** – child view job. Execution materializes or updates exactly one planned view task.

## Architecture & Responsibilities
- **`ReconcileJobStore`** – Interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **`ReconcilerScheduler`** – Quarkus scheduled bean that polls the job store, leases a job,
  transitions it to running, invokes `ReconcilerService` or the planner executor, records
  success/failure stats, and cancels child table/view jobs when a parent plan job terminates
  unsuccessfully after partial enqueue.
- **`ReconcilerService`** – Core orchestration:
  1. Resolves connector metadata via the service’s `Connectors` RPC.
  2. Plans table-scoped work for connector jobs, preserving destination namespace/table overrides.
  3. Ensures destination catalogs/namespaces/tables exist (creating them if needed).
  4. Instantiates the connector via `ConnectorFactory`.
  5. Executes table-scoped metadata/stats work and updates connectors with resolved destination IDs.
  6. Handles incremental vs full-rescan logic.
  7. Supports explicit capture modes:
     - `METADATA_ONLY`: advances/creates table + snapshot state without writing stats.
     - `STATS_ONLY`: writes stats only.
     - `METADATA_AND_STATS`: ingests metadata/snapshots/constraints, then enqueues scoped
       `STATS_ONLY` follow-up jobs for discovered snapshots.
- **`GrpcClients`** – Provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors).
- **`NameParts`** – Utility for parsing namespace/table names.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behaviour through the
reconcile control RPCs:
- `ReconcileControl.StartCapture(connector_id, full_rescan)` – Enqueues a top-level
  `PLAN_CONNECTOR` job via `ReconcileJobStore`.
- `ReconcileControl.CaptureNow(...)` – Uses the same split path, but waits for the aggregated
  outcome of the top-level plan job plus any child `EXEC_TABLE` / `EXEC_VIEW` jobs.
- `ReconcileControl.GetReconcileJob(job_id)` / `ListReconcileJobs(...)` – Expose both top-level and
  child jobs. Plan jobs aggregate child counters for user-facing status reads.

Internally, the scheduler exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding** – When reconciling, the service ensures the connector’s declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion** – Stats persistence is centralized behind the stats control-plane
  (`StatsCaptureControlPlane` / orchestrator). `STATS_ONLY` runs route capture through registry
  engines; `METADATA_AND_STATS` schedules those captures asynchronously via follow-up jobs.
- **Snapshot constraints ingestion** – Reconciler ingests snapshot constraints through
  `PutTableConstraints` after snapshot/stats handling.
  - Behavior is intentionally strict (not best-effort): constraint extraction/write failures fail
    the current table reconcile, including when table stats were already captured.
  - Constraints writes are repeatable/upsert-like: reconciler may rewrite the same
    table+snapshot constraints on repeated runs (no separate "constraints already captured"
    tracking is introduced here).
  - Idempotency keys are derived from `SnapshotConstraints.toByteArray()` (byte-level,
    order-sensitive payload hashing), not semantic normalization.
- **Mode-aware behavior** – In `STATS_ONLY`, destination-table misses are treated as skip/no-op
  rather than job-fatal errors.
- **Plan failure behavior** – If a `PLAN_CONNECTOR` job fails or is cancelled after it already
  enqueued some `EXEC_TABLE` / `EXEC_VIEW` children, the scheduler cancels those children.
  User-facing plan job reads surface the parent failure/cancel state immediately rather than waiting
  for queued/running children to drain.
- **Scoped plan misses** – A destination namespace scope miss is treated as “no matching table
  tasks” rather than a planner crash. If a scoped destination table filter matches nothing, the
  planner still returns an explicit `No tables matched scope` failure so operators get a clear
  diagnostic instead of a silent success.
- **Split-phase view behavior** – In split mode, view tasks are planned once per top-level
  `PLAN_CONNECTOR` job and executed as dedicated `EXEC_VIEW` children, not as part of child
  `EXEC_TABLE` jobs. This avoids duplicate or skipped view updates while preserving per-view retry
  and cancellation semantics.
- **View reconciliation semantics** – Reconcile is current-state, not history-preserving. When an
  upstream view already exists in Floecat, reconcile updates the stored canonical definition in
  place rather than appending a first-class backend version history.
- **Error handling** – Exceptions inside the per-table loop are caught, logged, and recorded in the
  job summary (`errors++`). The job proceeds to the next table, incrementing `scanned` regardless of
  success.
- **Job leasing** – `DurableReconcileJobStore` leases from persisted ready pointers, marks jobs
  running/succeeded/failed through CAS updates, and reclaims expired leases on a best-effort interval.
  Failed jobs are retried with backoff up to configured attempt limits before terminal failure.
- **Durable pointer model** – Durable reconcile queue pointers (`/reconcile/jobs/by-id`,
  `/accounts/by-id/reconcile/jobs/by-id`, `/accounts/by-id/reconcile/jobs/ready`, and
  `/reconcile/dedupe`) are blob-backed and store the current reconcile job JSON blob URI. When a
  job state transition writes a new canonical blob version, the store CAS-updates lookup/ready/dedupe
  pointers to the same blob URI.
- **GC ownership** – `ReconcileJobGc` remains responsible for reconcile lifecycle cleanup (terminal-state
  queue/dedupe cleanup and retention-based deletion of old durable job records). `PointerGc` handles
  structural orphan cleanup, but it does not enforce reconcile retention/state policy.
  Terminal jobs are removed from active scheduling immediately, but their canonical records remain
  queryable until reconcile-job retention expires and GC reaps them.

### Backend selection
- `floecat.reconciler.backend` (default `local`) selects which backend implementation `ReconcilerService`
  uses. Set it to `remote` when the reconciler runs as a separate process and must talk to the service
  over gRPC. In remote mode the reconciler uses `floecat.reconciler.authorization.header`/`token` to send
  an authorization header on every call. Per-request tokens supplied via `ReconcileContext` override the
  static token, so the precedence is: request-scoped token → configured token → no header.

## Data Flow & Lifecycle
```
Connector StartCapture / CaptureNow → ReconcileJobStore.enqueuePlan
  → ReconcilerScheduler.pollOnce
      → jobs.leaseNext (returns account + connector IDs)
      → markRunning
      → if PLAN_CONNECTOR:
          → ReconcilerService.planTableTasks / planViewTasks
          → enqueue child EXEC_TABLE / EXEC_VIEW jobs
      → if EXEC_TABLE:
          → ReconcilerService.reconcile (capture mode on leased job)
              → Connectors.GetConnector → ConnectorConfig
              → Ensure destination catalog/namespace/table
              → ConnectorFactory.create + try-with-resources
                  → describe / enumerateSnapshots
                  → ingest metadata/snapshots/constraints
                  → (for METADATA_AND_STATS) enqueue STATS_ONLY follow-up by snapshot
                  → (for STATS_ONLY) capture via stats control-plane/engine registry
              → Update connector destination IDs if missing
      → if EXEC_VIEW:
          → ReconcilerService.reconcileView
              → ConnectorFactory.create + try-with-resources
                  → describeView (or listViewDescriptors fallback)
                  → ensure destination namespace exists
                  → create or update the destination view
      → markSucceeded or markFailed
```
Jobs include `fullRescan`, `executionPolicy`, `jobKind`, optional `tableTask`, and track
`tablesScanned`, `tablesChanged`, `viewsScanned`, `viewsChanged`, and `errors`.
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
- Extend `ReconcilerService` to support partial selection (for example column filters) by inspecting
  `SourceSelector.columns`.
- Add health checks/metrics by tapping into `ReconcileJobStore` stats or `MeterRegistry` in the
  scheduler.

## Examples & Scenarios
- **Full rescan** – Operator triggers `connector trigger demo-glue --full`. The job store enqueues a
  full scan, scheduler leases it, and runs the requested capture mode across the full upstream
  history. The job transitions to `JS_SUCCEEDED` once the pass completes.
- **Incremental run** – Without `--full`, `ReconcilerService` restricts its work to the connector’s
  configured `source.table` (if set) and only ingests new snapshots (parents already known via
  `SnapshotRepository`).

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector RPCs: [`docs/service.md`](service.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
