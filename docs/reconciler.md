# Reconciler

## Overview
The `reconciler/` module automates ingestion from upstream connectors. It manages a queue of
reconciliation jobs, leases work to workers, instantiates connectors via the SPI, and calls the
service’s gRPC APIs to create tables, snapshots, statistics, and plan bundles.

This component decouples connector execution from the main service so that long-running scans do not
block gRPC threads. The service triggers reconciliations via `Connectors.TriggerReconcile`, which
creates jobs in the reconciler’s store.

## Architecture & Responsibilities
- **`ReconcileJobStore`** – Interface abstracting job persistence and leasing. The default
  implementation (`InMemoryReconcileJobStore`) keeps jobs in-memory (separate stores can be added for
  persistent or distributed queues).
- **`ReconcilerScheduler`** – Quarkus scheduled bean that polls the job store, leases a job,
  transitions it to running, invokes `ReconcilerService`, and records success/failure stats.
- **`ReconcilerService`** – Core orchestration:
  1. Resolves connector metadata via the service’s `Connectors` RPC.
  2. Ensures destination catalogs/namespaces/tables exist (creating them if needed).
  3. Instantiates the connector via `ConnectorFactory`.
  4. Iterates upstream tables, ingests snapshots + stats, and updates connectors with resolved
     destination IDs.
  5. Handles incremental vs full-rescan logic.
- **`GrpcClients`** – Provides blocking stubs for all service RPCs (Catalog, Namespace, Table,
  Snapshot, Statistics, Directory, Connectors).
- **`NameParts`** – Utility for parsing namespace/table names.

## Public API / Surface Area
While the reconciler itself runs as an internal Quarkus app, it exposes behaviour through the
connector RPCs:
- `Connectors.TriggerReconcile(connector_id, full_rescan)` – Enqueues a job via `ReconcileJobStore`.
- `Connectors.GetReconcileJob(job_id)` – Reads job status, mirroring the store’s fields
  (`state`, `message`, `tables_scanned`, `tables_changed`, `errors`).

Internally, the scheduler exposes `pollEvery` via `@Scheduled` (default every second).

## Important Internal Details
- **Destination binding** – When reconciling, the service ensures the connector’s declared
  destination catalog/namespace/table IDs align with actual resources. Any mismatch triggers a
  `ConnectorState` update or raises conflicts.
- **Statistics ingestion** – Table stats plus column/file stats are streamed one request per item via
  `PutColumnStats` / `PutFileColumnStats`, keeping a single idempotency key per table/snapshot.
- **Error handling** – Exceptions inside the per-table loop are caught, logged, and recorded in the
  job summary (`errors++`). The job proceeds to the next table, incrementing `scanned` regardless of
  success.
- **Job leasing** – `InMemoryReconcileJobStore` tracks leased IDs to avoid double processing and only
  transitions jobs to `JS_SUCCEEDED`/`JS_FAILED` after `ReconcilerScheduler` finishes the run.

## Data Flow & Lifecycle
```
Connector TriggerReconcile → ReconcileJobStore.enqueue
  → ReconcilerScheduler.pollOnce
      → jobs.leaseNext (returns tenant + connector IDs)
      → markRunning
      → ReconcilerService.reconcile
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
- Swap out `ReconcileJobStore` for persistent backends by providing a CDI alternative and wiring it
  in `ConnectorsImpl` (job ID references must remain stable for `GetReconcileJob`).
- Extend `ReconcilerService` to support partial selection (for example column filters) by inspecting
  `SourceSelector.columns`.
- Add health checks/metrics by tapping into `ReconcileJobStore` stats or `MeterRegistry` in the
  scheduler.

## Examples & Scenarios
- **Full rescan** – Operator triggers `connector trigger demo-glue --full`. The job store enqueues a
  full scan, scheduler leases it, `ReconcilerService` calls `connector.listTables` to fetch every
  table, updates dest namespace IDs, and ingests snapshots. The job transitions to `JS_SUCCEEDED`
  once tables/stats are synced.
- **Incremental run** – Without `--full`, `ReconcilerService` restricts its work to the connector’s
  configured `source.table` (if set) and only ingests new snapshots (parents already known via
  `SnapshotRepository`).

## Cross-References
- Connector SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Service connector RPCs: [`docs/service.md`](service.md)
- Concrete connectors: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
