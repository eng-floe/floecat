# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service’s gRPC APIs to create tables, snapshots, statistics, and scan bundles.

This component decouples connector execution from the main service so that long-running scans do not
block gRPC threads. The service submits capture jobs via `ReconcileControl.StartCapture`, which
creates jobs in the reconciler’s store.

## Architecture & Responsibilities
- **`ReconcileJobStore`** – Interface abstracting job persistence and leasing. In service runtime,
  the default is the durable implementation (`DurableReconcileJobStore`) selected by
  `floecat.reconciler.job-store=durable`; in-memory (`InMemoryReconcileJobStore`) remains available
  for lightweight/local usage when `floecat.reconciler.job-store=memory`.
- **`ReconcilerScheduler`** – Quarkus scheduled bean that polls the job store, leases a job,
  transitions it to running, invokes `ReconcilerService` in phases, and records success/failure stats.
- **`ReconcilerService`** – Core orchestration:
  1. Resolves connector metadata via the service’s `Connectors` RPC.
  2. Ensures destination catalogs/namespaces/tables exist (creating them if needed).
  3. Instantiates the connector via `ConnectorFactory`.
  4. Iterates upstream tables, ingests snapshots + stats, and updates connectors with resolved
     destination IDs.
  5. Handles incremental vs full-rescan logic.
  6. Supports explicit capture modes:
     - `METADATA_ONLY`: advances/creates table + snapshot state without writing stats.
     - `STATS_ONLY`: writes stats only.
     - `METADATA_AND_STATS`: ingests both metadata and stats in one pass.
- **`GrpcClients`** – Provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors).
- **`NameParts`** – Utility for parsing namespace/table names.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behaviour through the
reconcile control RPCs:
- `ReconcileControl.StartCapture(connector_id, full_rescan)` – Enqueues a job via `ReconcileJobStore`.
- `ReconcileControl.GetReconcileJob(job_id)` – Reads job status, mirroring the store’s fields
  (`state`, `message`, `tables_scanned`, `tables_changed`, `errors`).

Internally, the scheduler exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding** – When reconciling, the service ensures the connector’s declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion** – Target stats (including file targets) are streamed one request per
  item via `PutTargetStats`, keeping a single idempotency key per table/snapshot.
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

### Backend selection
- `floecat.reconciler.backend` (default `local`) selects which backend implementation `ReconcilerService`
  uses. Set it to `remote` when the reconciler runs as a separate process and must talk to the service
  over gRPC. In remote mode the reconciler uses `floecat.reconciler.authorization.header`/`token` to send
  an authorization header on every call. Per-request tokens supplied via `ReconcileContext` override the
  static token, so the precedence is: request-scoped token → configured token → no header.

## Data Flow & Lifecycle
```
Connector StartCapture → ReconcileJobStore.enqueue
  → ReconcilerScheduler.pollOnce
      → jobs.leaseNext (returns account + connector IDs)
      → markRunning
      → ReconcilerService.reconcile (capture mode on leased job)
          → Connectors.GetConnector → ConnectorConfig
          → Ensure destination catalog/namespace/table
          → ConnectorFactory.create + try-with-resources
              → listTables / describe / enumerateSnapshotsWithStats
              → ingest snapshots/stats via service RPCs
          → Update connector destination IDs if missing
      → markSucceeded or markFailed
```
Jobs include `fullRescan` and track `tablesScanned`, `tablesChanged`, `errors`. `ReconcilerScheduler`
uses an `AtomicBoolean` guard to prevent concurrent runs within the same instance.

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
