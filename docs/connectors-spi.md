# Connector SPI

## Overview
`connectors-spi/` defines the contract that every upstream metadata connector must implement. The
SPI abstracts discovery of namespaces/tables, enumeration of snapshots with statistics, planning of
physical files for a snapshot, and authentication adapters. It also packages shared tooling for
column statistics and NDV estimation.

Connectors implement `MetacatConnector` and typically wrap an upstream catalog API (Iceberg REST,
Unity Catalog, etc.), translating its schemas, snapshots, and metrics into Metacat protobufs.

## Architecture & Responsibilities
- **`MetacatConnector`** – Primary interface (extends `Closeable`). Methods:
  - `id()` → stable connector identifier.
  - `format()` → `ConnectorFormat` (ICEBERG, DELTA, etc.).
  - `listNamespaces()`.
  - `listTables(namespaceFq)`.
  - `describe(namespaceFq, tableName)` → `TableDescriptor` with location, schema JSON, partition
    keys, properties.
  - `enumerateSnapshotsWithStats(...)` → `SnapshotBundle`s containing per-snapshot table/column stats.
  - `plan(namespaceFq, tableName, snapshotId, asOfTime)` → `PlanBundle` describing data/delete files.
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
  PlanBundle plan(...);
}
```
`TableDescriptor`, `SnapshotBundle`, and `PlanBundle` are immutable records; connectors populate them
with canonical metadata that the reconciler ingests.

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
  reuse the results for multiple columns to minimize IO.
- **Planner integration** – `Planner` interface (under `connector/common`) converts connector output
  into planner-specific `PlanFile` lists, ensuring file stats include `ColumnStats` when available.
- **Error propagation** – Connector implementations should wrap transient upstream failures inside
  unchecked exceptions so `ReconcilerService` can count them and continue scanning other tables.

## Data Flow & Lifecycle
```
ConnectorFactory.create(ConnectorConfig)
  → MetacatConnector (opens upstream clients, auth providers)
      → listNamespaces/listTables → service repo ensures namespace/table existence
      → describe → Table specs persisted with upstream references
      → enumerateSnapshotsWithStats → StatsRepository writes Table/Column stats per snapshot
      → plan → PlanContext.runPlanning returns data/delete file manifests to planners
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
- **Planning** – During `PlanContext.runPlanning`, if a table has an `UpstreamRef.connector_id`, the
  planning code instantiates that connector via the SPI and calls `plan()` to fetch file lists pinned
  to the requested snapshot.

## Cross-References
- Service connector management and reconciliation triggers:
  [`docs/service.md`](service.md), [`docs/reconciler.md`](reconciler.md)
- Concrete implementations: [`docs/connectors-iceberg.md`](connectors-iceberg.md),
  [`docs/connectors-delta.md`](connectors-delta.md)
