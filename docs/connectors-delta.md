# Delta / Unity Catalog Connector

## Overview
`connectors/delta/` implements a connector targeting Databricks Unity Catalog-powered Delta Lake
warehouses. It uses the Delta Kernel, Unity Catalog REST APIs, Databricks SQL endpoints, and AWS S3
(thru the v2 client) to enumerate tables, collect statistics, and plan files.

The primary implementation is `UnityDeltaConnector`, exposed via `DeltaConnectorProvider`. Supporting
classes manage OAuth2/SP token acquisition, Databricks SQL execution, and custom file readers for S3.

## Architecture & Responsibilities
- **`UnityDeltaConnector`** – Concrete `MetacatConnector` that:
  - Talks to Unity Catalog REST (`UcHttp`) to list catalogs/schemas/tables.
  - Uses Delta Kernel (`io.delta.kernel.Table`) for schema and snapshot access.
  - Executes Databricks SQL statements via `SqlStmtClient` if a warehouse is configured.
  - Reads Parquet data with `S3V2FileSystemClient` and `ParquetS3V2InputFile` for NDV/statistics.
  - Plans files using `DeltaPlanner`, emitting `ScanFile`s for data/delete manifests.
- **`DatabricksAuthFactory`** – Produces `AuthProvider`s (OAuth2 bearer tokens, service principal
  tokens, CLI profiles) consumed by `UcHttp` and SQL client.
- **`UcBaseSupport` / `UcHttp`** – HTTP helpers for constructing API URLs, encoding parameters, and
  handling retries/timeouts.
- **`DeltaTypeMapper`** – Maps Delta/Parquet logical types into Metacat logical types for stats.

## Public API / Surface Area
`UnityDeltaConnector` implements the SPI methods:
- `listNamespaces()` – Fetches catalogs via `/api/2.1/unity-catalog/catalogs`, then enumerates
  schemas per catalog, returning `catalog.schema` pairs.
- `listTables(namespace)` – Calls `/api/2.1/unity-catalog/tables` filtered by catalog/schema, then
  filters to `data_source_format == DELTA`.
- `describe(namespace, table)` – Fetches table metadata from Unity Catalog, reads the Delta schema via
  Delta Kernel, and returns a `TableDescriptor` containing location, partition keys, and properties.
- `enumerateSnapshotsWithStats(...)` – Iterates Delta snapshots, optionally samples Parquet files to
  produce NDV stats (`SamplingNdvProvider`, `ParquetNdvProvider`), and emits `SnapshotBundle`s with
  per-file stats (row count/size plus per-column metrics when available).
- `plan(namespace, table, snapshotId, asOf)` – Uses `DeltaPlanner` to read snapshot manifests and
  produce a `ScanBundle` whose entries are labelled with `ScanFileContent` (data, position deletes,
  equality deletes).

## Important Internal Details
- **Authentication** – Supports OAuth2 bearer tokens, Databricks CLI profiles, and service principal
  tokens. `DatabricksAuthFactory` inspects connector properties such as `auth.scheme`, `auth.secret`
  references, or CLI hints.
- **HTTP & SQL clients** – `UcHttp` centralises base URI, connect/read timeouts, and error mapping.
  `SqlStmtClient` optionally executes SQL statements (for example to inspect statistics tables) via
  Databricks SQL warehouses.
- **S3 integration** – Uses AWS SDK v2 (`S3Client`) with region from connector properties to read
  data files. `S3RangeReader` provides efficient range reads for Parquet file access.
- **NDV sampling** – Controlled by `stats.ndv.enabled`, `stats.ndv.sample_fraction`, and
  `stats.ndv.max_files`. Samples combine streaming NDV with Parquet footers for accuracy.
- **Type mapping** – `DeltaTypeMapper` ensures nested Delta/Parquet types are faithfully represented
  when computing stats, aligning with `types/` definitions.

## Data Flow & Lifecycle
```
ConnectorFactory.create(cfg)
  → UnityDeltaConnector.create(uri, options, authProvider)
      → Instantiate S3 client + Delta Kernel engine
      → Configure Unity Catalog HTTP and optional SQL client
  → listNamespaces/listTables via Unity Catalog REST
  → describe via REST + Delta Kernel schema inspection
  → enumerateSnapshotsWithStats
      → Delta Kernel Snapshot → Parquet stats engine → SnapshotBundle (table/column/file stats)
  → plan
      → DeltaPlanner traverses _delta_log → ScanBundle entries for data/delete files
```
Connector resources (HTTP clients, S3 client, Delta engine) are closed when `close()` is invoked.

## Configuration & Extensibility
Important connector properties:
- `http.connect.ms`, `http.read.ms` – Timeout controls for Unity Catalog HTTP calls.
- `databricks.sql.warehouse_id` – Enables SQL statement execution when set.
- `s3.region` / `aws.region` – Region for the S3 client used to read Parquet files.
- `stats.ndv.*` – Sampling knobs identical to the Iceberg connector.
- Authentication-specific options (`auth.scheme`, `auth.properties`) – See
  `DatabricksAuthFactory` for supported schemes (OAuth2, service principal, CLI profile).

Extensibility points:
- Implement new auth schemes by extending `AuthProvider` and adding cases in
  `DatabricksAuthFactory`.
- Plug in additional NDV providers if Delta tables store custom sketches.
- Extend `DeltaPlanner` to emit additional metadata (for example z-order hints) when the upstream API
  exposes them.

## Examples & Scenarios
- **Connector Spec** – A Unity Catalog connector might specify:
  ```json
  {
    "display_name":"delta-unity",
    "kind":"CK_DELTA",
    "uri":"https://dbc-1234.cloud.databricks.com",
    "properties":{
      "databricks.sql.warehouse_id":"abcd",
      "s3.region":"us-west-2",
      "stats.ndv.enabled":"true"
    },
    "auth":{"scheme":"oauth2","properties":{"token":"..."}}
  }
  ```
- **Full reconciliation** – `ReconcilerService` enters full-rescan mode (`fullRescan=true`), so the
  connector lists every table in the namespace, creates missing namespaces in the destination
  catalog, updates `DestinationTarget` pointers, and ingests snapshot stats for each table.

## Cross-References
- SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Iceberg connector for contrast: [`docs/connectors-iceberg.md`](connectors-iceberg.md)
- Service & reconciler integration: [`docs/service.md`](service.md), [`docs/reconciler.md`](reconciler.md)
