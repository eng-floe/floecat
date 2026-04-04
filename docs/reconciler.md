# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service's gRPC APIs to create tables, snapshots, statistics, and scan bundles.

This component decouples connector execution from the main service so that long-running scans do not
block gRPC threads. The service submits capture jobs via `ReconcileControl.StartCapture`, which
creates jobs in the reconciler's store.

## Architecture & Responsibilities
- **`ReconcileJobStore`** - Interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **Control plane** - `ReconcileControlImpl` enqueues/inspects/cancels jobs and
  `ReconcileExecutorControlImpl` exposes lease/heartbeat/progress/completion RPCs to executor nodes.
- **`ConnectorPlanningReconcileExecutor`** - Planner executor that expands connector-scoped plan
  jobs into concrete table execution jobs.
- **`ReconcilerScheduler`** - Local in-process executor poller used for all-in-one deployments.
  It leases from the local job store, transitions jobs to running, invokes a `ReconcileExecutor`,
  and records success/failure stats.
- **`RemoteReconcileExecutorPoller`** - Remote executor-node poller that leases jobs over gRPC from
  the control plane and executes them locally via the same `ReconcileExecutor` SPI.
- **`ReconcileExecutor`** - Execution SPI. `DefaultReconcileExecutor` handles table execution jobs,
  while the planner executor handles connector planning jobs.
- **`ReconcilerService`** - Core orchestration:
  1. Resolves connector metadata via the service's `Connectors` RPC.
  2. Ensures destination catalogs/namespaces/tables exist (creating them if needed).
  3. Instantiates the connector via `ConnectorFactory`.
  4. Delegates planning to the connector SPI or ingests one table task during execution.
  5. Handles incremental vs full-rescan logic.
  6. Supports explicit capture modes:
     - `METADATA_ONLY`: advances/creates table + snapshot state without writing stats.
     - `STATS_ONLY`: writes stats only.
     - `METADATA_AND_STATS`: ingests both metadata and stats in one pass.
- **`GrpcClients`** - Provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors).
- **`NameRefNormalizer`** - Shared utility for canonical catalog/name/display-name normalization
  used by both reconciler and service-side backends.

## Public API / Surface Area
User-facing behaviour continues to flow through the reconcile control RPCs:
- `ReconcileControl.StartCapture(connector_id, full_rescan)` - Enqueues a connector planning job and
  returns that top-level reconcile handle.
- `ReconcileControl.GetReconcileJob(job_id)` - For ordinary jobs, reads the stored job record. For a
  `PLAN_CONNECTOR` job, it aggregates child `EXEC_TABLE` jobs so the returned status/progress reflects
  the overall reconcile run rather than only the planning phase.
- `ReconcileControl.CancelReconcileJob(job_id)` - Cancels the addressed job. For a `PLAN_CONNECTOR`
  job, cancellation cascades to child `EXEC_TABLE` jobs that have already been enqueued.

Executor-facing behaviour is now a separate internal RPC surface:
- `ReconcileExecutorControl.LeaseReconcileJob` - Leases one eligible job for an executor id.
- `ReconcileExecutorControl.RenewReconcileLease` - Heartbeats a leased job and returns cancellation state.
- `ReconcileExecutorControl.ReportReconcileProgress` - Reports progress and doubles as a heartbeat.
- `ReconcileExecutorControl.CompleteLeasedReconcileJob` - Writes a terminal job state.
- `ReconcileExecutorControl.GetReconcileCancellation` - Allows executor-side cancellation polling.

## Important Internal Details
- **Destination binding** - When reconciling, the service ensures the connector's declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion** - Table stats plus column/file stats are streamed one request per item via
  `PutColumnStats` / `PutFileColumnStats`, keeping a single idempotency key per table/snapshot.
- **Snapshot constraints ingestion** - Reconciler ingests snapshot constraints through
  `PutTableConstraints` after snapshot/stats handling.
  - Behavior is intentionally strict (not best-effort): constraint extraction/write failures fail
    the current table reconcile, including when table stats were already captured.
  - Constraints writes are repeatable/upsert-like: reconciler may rewrite the same
    table+snapshot constraints on repeated runs (no separate "constraints already captured"
    tracking is introduced here).
  - Idempotency keys are derived from `SnapshotConstraints.toByteArray()` (byte-level,
    order-sensitive payload hashing), not semantic normalization.
- **Mode-aware behavior** - In `STATS_ONLY`, destination-table misses are treated as skip/no-op
  rather than job-fatal errors.
- **Error handling** - Exceptions inside the per-table loop are caught, logged, and recorded in the
  job summary (`errors++`). The job proceeds to the next table, incrementing `scanned` regardless of
  success.
- **Job kinds** - The queue now stores explicit work types:
  - `PLAN_CONNECTOR` discovers concrete work for one connector reconcile.
  - `EXEC_TABLE` executes one table-scoped reconcile task.
  - `EXEC_CONNECTOR` remains available for broad connector execution and compatibility paths.
- **Job leasing** - `DurableReconcileJobStore` leases from persisted ready pointers, marks jobs
  running/succeeded/failed through CAS updates, and reclaims expired leases on a best-effort interval.
  Failed jobs are retried with backoff up to configured attempt limits before terminal failure.
- **Durable pointer model** - Durable reconcile queue pointers (`/reconcile/jobs/by-id`,
  `/accounts/by-id/reconcile/jobs/by-id`, `/accounts/by-id/reconcile/jobs/ready`, and
  `/reconcile/dedupe`) are blob-backed and store the current reconcile job JSON blob URI. When a
  job state transition writes a new canonical blob version, the store CAS-updates lookup/ready/dedupe
  pointers to the same blob URI. Readers are expected to resolve the current blob through those
  pointers rather than caching blob URIs directly; old blobs may be deleted once the pointer set has
  been moved forward.
- **GC ownership** - `ReconcileJobGc` remains responsible for reconcile lifecycle cleanup (terminal-state
  queue/dedupe cleanup and retention-based deletion of old durable job records). `PointerGc` handles
  structural orphan cleanup, but it does not enforce reconcile retention/state policy.

### Backend selection and deployment modes
- `floecat.reconciler.backend` (default `local`) selects which backend implementation `ReconcilerService`
  uses. Set it to `remote` when the reconciler runs as a separate process and must talk to the service
  over gRPC. In remote mode the reconciler uses `floecat.reconciler.authorization.header`/`token` to send
  an authorization header on every call. Per-request tokens supplied via `ReconcileContext` override the
  static token, so the precedence is: request-scoped token -> configured token -> no header.
- `QUARKUS_PROFILE=reconciler-control` turns the runtime into a control-plane node:
  queue ownership, public reconcile APIs, local planner, planner executor enabled, no executor polling.
- `QUARKUS_PROFILE=reconciler-executor` turns the runtime into a remote executor node:
  no local queue polling, no planner auto-enqueue, no planner executor, `ReconcilerService` talks to the
  control plane over gRPC, and the node greedily leases eligible work through `ReconcileExecutorControl`.
- Without one of those profiles, the default runtime remains all-in-one for local/dev use.

## Data Flow & Lifecycle
```text
Connector StartCapture / auto planner tick -> ReconcileJobStore.enqueuePlan
  -> control plane persists PLAN_CONNECTOR job + routing policy
  -> planner-capable executor leases plan work
      -> local mode: ReconcilerScheduler -> jobs.leaseNext(...)
      -> remote mode: RemoteReconcileExecutorPoller -> ReconcileExecutorControl.LeaseReconcileJob(...)
  -> ConnectorPlanningReconcileExecutor.execute(...)
      -> ReconcilerService.planTableTasks(...)
      -> ReconcileJobStore.enqueueTableExecution(...) for each discovered table
  -> executor plane leases EXEC_TABLE work
  -> ReconcileExecutor.execute(...)
      -> DefaultReconcileExecutor -> ReconcilerService.reconcile(..., tableTask, ...)
          -> Connectors.GetConnector -> ConnectorConfig
          -> Ensure destination catalog/namespace/table
          -> ConnectorFactory.create + try-with-resources
              -> describe / enumerateSnapshotsWithStats
              -> ingest snapshots/stats via service RPCs
          -> Update connector destination IDs if missing
  -> executor plane heartbeats + reports progress
  -> control plane writes success/failure/cancelled terminal state
```

Jobs include `fullRescan` and track `tablesScanned`, `tablesChanged`, `errors`. Planning jobs fan
out table tasks, but the public `StartCapture` / `GetReconcileJob` / `CancelReconcileJob` contract
still treats the `PLAN_CONNECTOR` job id as the top-level reconcile handle by aggregating or
cascading over child jobs in the service layer. `ReconcilerScheduler` uses an `AtomicBoolean`
guard to prevent concurrent runs within the same instance.

Planning ownership is connector-first: the planner executor calls `FloecatConnector.planTableTasks`
to discover executable table work. Connectors own upstream table discovery and scope application;
the reconciler no longer contains fallback planning logic for expanding namespaces into table tasks.

Planner routing is intentionally different from table-execution routing: `PLAN_CONNECTOR` jobs run
wherever a planner executor is present, regardless of execution lane, while the child `EXEC_TABLE`
jobs carry the actual execution policy (execution class, lane, optional pinned executor id) that
local or remote workers enforce later.

Local scheduling also resolves work per executor capability tuple, not by unioning all local
executor capabilities together. Each executor advertises its own execution classes, lanes, executor
id, and job kinds, and the scheduler leases against that exact capability set before dispatch.

## Configuration & Extensibility
- Scheduling cadence via `reconciler.pollEvery` (defaults to `1s`).
- Execution routing:
  - `floecat.reconciler.scheduler.enabled`
  - `floecat.reconciler.remote-executor.enabled`
  - `floecat.reconciler.executor.planner.enabled`
  - `floecat.reconciler.executor.default.enabled`
  - `floecat.reconciler.auto.execution-class`
  - `floecat.reconciler.auto.execution-lane`
  - `floecat.reconciler.auto.pinned-executor-id`
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
- **Full rescan** - Operator triggers `connector trigger demo-glue --full`. The job store enqueues a
  connector planning job, the planner emits one table execution job per eligible table, and
  executors run those table jobs across the full upstream history.
- **Incremental run** - Without `--full`, planning still discovers eligible tables for the connector's
  configured source namespace, and each table execution job ingests only new snapshots.

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector RPCs: [`docs/service.md`](service.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
