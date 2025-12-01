# Connector SPI

## Overview
`connectors/catalogs/spi/` defines the contract that every upstream metadata connector must implement. The
SPI abstracts discovery of namespaces/tables, enumeration of snapshots with statistics (table,
column, and file-level), enumeration of physical files for a snapshot, and authentication adapters. It
also packages shared tooling for column statistics, file statistics, and NDV estimation.

Connectors implement `MetacatConnector` and typically wrap an upstream catalog API (Iceberg REST,
Unity Catalog, etc.), translating its schemas, snapshots, and metrics into Metacat protobufs.

## Architecture & Responsibilities
- **`MetacatConnector`** – Primary interface (extends `Closeable`). Methods:
  - `id()` → stable connector identifier.
  - `format()` → `ConnectorFormat` (ICEBERG, DELTA, etc.).
  - `listNamespaces()`.
  - `listTables(namespaceFq)`.
  - `describe(namespaceFq, tableName)` → `TableDescriptor` with location, schema JSON, partition
    keys, and properties.
  - `enumerateSnapshotsWithStats(...)` → `SnapshotBundle`s containing per-snapshot table/column/file stats plus optional Iceberg metadata blobs.
  - `plan(namespaceFq, tableName, snapshotId, asOfTime)` → `ScanBundle` describing data/delete files pinned to the snapshot. (The SPI keeps the upstream “plan” term so Iceberg/Delta authors can map it directly to their native APIs.)
- _Terminology note_: elsewhere in the repo “planning” refers to the dedicated planner service. Within the SPI the `plan()` verb simply mirrors upstream engines (`TableScan.planFiles()` in Iceberg, Delta manifests) to keep connector authors oriented; the returned `ScanBundle` is purely execution metadata.
- **`ConnectorFactory`** – Instantiates connectors given a `ConnectorConfig` (URI, options,
  authentication). The service uses it to validate specs and the reconciler uses it during runs.
- **`ConnectorConfigMapper`** – Bidirectional conversion between RPC `Connector` protobufs and the
  SPI’s `ConnectorConfig` records.
- **Auth providers** – `AuthProvider` + concrete implementations such as `NoAuthProvider` and
  `AwsSigV4AuthProvider` supply credentials or headers per connector.
- **Stats helpers** – `StatsEngine`, `GenericStatsEngine`, `ProtoStatsBuilder`, and NDV utilities
  (`NdvProvider`, `SamplingNdvProvider`, `ParquetNdvProvider`, `FilteringNdvProvider`,
  `StaticOnceNdvProvider`, `NdvApprox`, `NdvSketch`). These components interpret Parquet footers,
  combine NDV approximations, and emit `TableStats`/`ColumnStats` protobufs.

## Public API / Surface Area
The SPI is intentionally small:
```java
interface MetacatConnector extends Closeable {
  String id();
  ConnectorFormat format();
  List<String> listNamespaces();
  List<String> listTables(String namespaceFq);
  TableDescriptor describe(String namespaceFq, String tableName);
  List<SnapshotBundle> enumerateSnapshotsWithStats(...);
  ScanBundle plan(...);
}
```
`TableDescriptor`, `SnapshotBundle`, and `ScanBundle` are immutable records; connectors populate them
with canonical metadata that the reconciler ingests. Iceberg connectors attach the full table
metadata snapshot (`IcebergMetadata`) to each `SnapshotBundle`, preserving schema/spec/sort-order/log
history at ingest time. `SnapshotBundle.fileStats` is optional but should be populated when Parquet
footers or upstream metadata can provide per-file row counts, sizes, and per-column stats. Snapshot
bundles also carry manifest-list URIs, sequence numbers, summary maps, and the metadata blob so
downstream APIs can mirror Iceberg’s REST contract.

`ConnectorConfig` encodes:
- Kind + source/destination selectors (`SourceSelector`, `DestinationTarget`).
- URI and arbitrary `properties` map.
- Authentication configuration (scheme, headers, secrets, properties).

`ConnectorProvider` is a lightweight registry allowing CDI discovery of connector factories.

## Important Internal Details
- **NDV estimation** – `NdvProvider` implementations chain together (sampling → filtering → backing
  store) so connectors can merge Parquet-level NDV sketches with streaming approximations. The SPI
  exposes `NdvApprox` structures mirroring `catalog/stats.proto` for compatibility.
- **Parquet helpers** – `ParquetFooterStats` and `ParquetNdvProvider` parse Parquet metadata once and
  reuse the results for multiple columns to minimize IO; `ProtoStatsBuilder.toFileColumnStats`
  packages footer-derived stats into `FileColumnStats` payloads.
- **Planner integration** – `Planner` interface (under `connector/common`) converts connector output
  into executor-facing `ScanFile` lists, ensuring file stats include `ColumnStats` when available.
- **Error propagation** – Connector implementations should wrap transient upstream failures inside
  unchecked exceptions so `ReconcilerService` can count them and continue scanning other tables.

## Data Flow & Lifecycle
```
ConnectorFactory.create(ConnectorConfig)
  → MetacatConnector (opens upstream clients, auth providers)
      → listNamespaces/listTables → service repo ensures namespace/table existence
      → describe → Table specs persisted with upstream references
      → enumerateSnapshotsWithStats → StatsRepository writes Table/Column/File stats per snapshot
      → plan → `QueryService.FetchScanBundle` returns data/delete file manifests to planners
  ← close() cleans up HTTP/S3/DB connections
```
`ConnectorFactory.create` is invoked both in `ConnectorsImpl.validate` (short-lived) and in the
reconciler (long-running); connectors must tolerate repeated instantiation and release resources in
`close()`.

## Configuration & Extensibility
- To add a new connector type, implement `MetacatConnector` plus a factory and annotate it so CDI can
  expose it via `ConnectorProvider`. Map new SPI kinds to RPC `ConnectorKind` values.
- Implement custom `AuthProvider`s when upstream APIs need bespoke headers or token exchanges.
- Extend stats support by creating a new `NdvProvider` or `StatsEngine` – `GenericStatsEngine`
  accepts pluggable NDV providers and Parquet readers.

## Examples & Scenarios
- **Validating a connector spec** – `ConnectorsImpl.validateConnector` builds a `ConnectorConfig`
  from RPC input, invokes `ConnectorFactory.create`, calls `listNamespaces()` to ensure the upstream
  responds, then closes the connector.
- **Reconciliation run** – `ReconcilerService` constructs a connector inside a try-with-resources
  block, iterates `listTables`, calls `enumerateSnapshotsWithStats`, and writes the returned
  `SnapshotBundle`s (table stats + column stats) through the service’s gRPC API.
- **Query lifecycle** – `QueryService.FetchScanBundle` fetch file lists pinned to the requested snapshot
directly from table and file statistics stored in the catalog (via TableStatisticsService).

## Cross-References
- Service connector management and reconciliation triggers:
  [`docs/service.md`](service.md), [`docs/reconciler.md`](reconciler.md)
- Concrete implementations: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
