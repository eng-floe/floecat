# Iceberg Connector

## Overview
`connectors/catalogs/iceberg/` implements the SPI for Iceberg catalogs exposed via the Iceberg REST API and
AWS Glue. It enables Floecat to ingest schema metadata, partition specs, snapshots, statistics, and
file manifests from Iceberg tables stored in S3.

The implementation centres on `IcebergConnector` (abstract base) with catalog-specific subclasses
for REST, Glue, and single-table filesystem-backed tables. Subclasses share NDV and stats collection while
specializing discovery and catalog wiring.

## Architecture & Responsibilities
- **`IcebergConnector`** – Abstract `FloecatConnector` implementation. Defines shared snapshot,
  stats, and NDV logic.
- **`IcebergRestConnector`** – REST catalog discovery via Iceberg `RESTCatalog`.
- **`IcebergGlueConnector`** – REST catalog + Glue discovery via `GlueIcebergFilter`.
- **`IcebergFilesystemConnector`** – Single-table connector that opens directly from the connector
  `uri` when `iceberg.source=filesystem`.
- **`IcebergConnectorProvider`** – Exposes the connector via CDI so the service can instantiate it
  using URIs from `ConnectorSpec`.
- **`IcebergPlanner`** – Implements `connector/common/Planner`, translating Iceberg `TableScan`
  results into `ScanFile` entries (data/delete files, column stats, formats).
- **`IcebergTypeMapper`** – Converts Iceberg Types into Floecat logical types when reporting stats
  or planner column metadata.
- **`GlueIcebergFilter`** – Uses AWS Glue to quickly enumerate only those tables registered as
  Iceberg, preventing needless REST calls.
- **`PuffinNdvProvider`** – Leverages Puffin NDV files when available.

## Public API / Surface Area
`IcebergConnector` and subclasses satisfy the SPI:
- `listNamespaces()` – Uses REST or Glue discovery depending on the selected source.
- `listTables(namespace)` – Uses REST or Glue discovery depending on the selected source.
- `describe(namespace, table)` – Loads the table, serialises the Iceberg schema to JSON via
  `SchemaParser.toJson`, captures partition keys, and returns table properties.
- `enumerateSnapshots(...)` – Iterates Iceberg snapshots and emits `SnapshotBundle`s with upstream
  timestamps, parent IDs, sequence numbers, manifest lists, and summary maps.
  When invoked with incremental `SnapshotEnumerationOptions`, the connector walks back from the
  current snapshot through the parent chain and stops once it reaches a snapshot already known to
  Floecat, returning only the newly discovered lineage.
- `captureSnapshotTargetStats(...)` – Captures table/column/file stats for one snapshot (optionally
  selector-scoped), using the configured `StatsEngine` and NDV providers.

## Important Internal Details
- **Authentication** – The connector supports multiple schemes: `aws-sigv4` (default), OAuth2 token,
  or none. SigV4 configuration controls signing names and regions, uses `s3.region` when
  `rest.signing-region` is unset, and injects request headers via Iceberg REST properties.
- **NDV** – NDV collection is optional. Controlled by `stats.ndv.enabled`, `stats.ndv.sample_fraction`,
  and `stats.ndv.max_files` connector options. NDV providers combine sampling, Puffin sketches, and
  Parquet footer data.
- **S3 IO** – Uses `org.apache.iceberg.aws.s3.S3FileIO` unless `io-impl` is specified in
  connector options. Header hints (`rest.header.*`) propagate custom headers to REST calls.
- **Metadata capture** – `IcebergConnector` embeds the serialized `IcebergMetadata` protobuf in the
  `SnapshotBundle.metadata` map so the reconciler can persist schemas/specs/refs/logs without
  leaking Iceberg-specific types into the core SPI.
- **Constraint mapping** – Snapshot constraints currently emit only metadata that is reliably
  exposed by core Iceberg tables:
  - `CT_PRIMARY_KEY` from Iceberg `identifier-field-ids` (advisory, emitted as not-enforced).
  - `CT_NOT_NULL` from required primitive schema fields (including nested struct leaves).
  - `CT_FOREIGN_KEY`, `CT_UNIQUE`, and `CT_CHECK` are not emitted from core Iceberg metadata
    because no portable source is defined for them.
  - Source-specific extraction path:
    - **REST**: snapshot/schema-derived constraints + source-specific additions (currently none).
    - **Glue**: snapshot/schema-derived constraints + source-specific additions (currently none).
    - **Filesystem**: snapshot/schema-derived constraints + source-specific additions (currently none).
  - Connector matrix (current behavior):
    - **REST**: `CT_PRIMARY_KEY`, `CT_NOT_NULL`.
    - **Glue**: `CT_PRIMARY_KEY`, `CT_NOT_NULL`.
    - **Filesystem**: `CT_PRIMARY_KEY`, `CT_NOT_NULL`.

## Data Flow & Lifecycle
```
ConnectorFactory.create(cfg)
  → IcebergConnectorFactory.create(uri, options, authScheme, authProps, headerHints)
      → Build REST properties, configure SigV4 or token auth
      → Choose REST/Glue/filesystem connector based on options
      → Initialize RESTCatalog (and Glue filter when needed)
  → listNamespaces/listTables/describe
  → enumerateSnapshots
      → For each snapshot load Table metadata lineage
  → captureSnapshotTargetStats
      → StatsEngine pulls Parquet stats (table/column + per-file), NDV provider merges sketches
      → plan
      → Build TableScan, collect FileScanTask -> ScanBundle
```
Resources (RESTCatalog, GlueClient) are closed when the connector is closed.

## Configuration & Extensibility
Connector options (part of `ConnectorSpec.properties`):
- `iceberg.source` – Selects discovery backend (`glue`, `rest`, `filesystem`). Defaults to `glue`.
- `rest.signing-region`, `s3.region`, `rest.auth.type`, `rest.signing-name` – control SigV4.
- `io-impl` – override Iceberg IO implementation.
- `stats.ndv.*` – enable NDV estimation (boolean), sample fraction (0−1], max Parquet files to scan.
- `header.<name>` – send custom headers to the REST endpoint.
- For `iceberg.source=filesystem`, the connector `uri` must point at a single Iceberg metadata JSON
  file. `external.namespace` and `external.table-name` remain optional logical-name overrides.

Auth credential types (`--cred-type`) are documented in [`docs/cli-reference.md`](cli-reference.md).
For Iceberg, the relevant types are `bearer`, `client`, `cli` (provider=aws), `token-exchange`,
`token-exchange-entra`, `token-exchange-gcp`, `aws`, `aws-web-identity`, and `aws-assume-role`.

To extend behavior:
- Provide a custom NDV provider by plugging into `GenericStatsEngine`.
- Wrap alternative auth schemes by implementing `AuthProvider` and mapping new `auth.scheme` values.
- Add new planner logic by extending `IcebergPlanner` (for example to emit positional deletes).

## Examples & Scenarios
- **Connector creation** – A connector spec referencing Iceberg REST looks like:
  ```json
  {
    "display_name":"glue-iceberg",
    "kind":"CK_ICEBERG",
    "uri":"https://iceberg.example.com",
    "properties":{"s3.region":"us-east-1","stats.ndv.enabled":"true"},
    "auth":{"scheme":"aws-sigv4","properties":{"signing-name":"glue"}}
  }
  ```
  `ConnectorsImpl` validates it by creating an `IcebergConnector` and calling `listNamespaces()`.
- **Glue (AWS SigV4)** – CLI example using Glue discovery:
  ```bash
  connector create "Glue Iceberg" ICEBERG \
    "https://glue.us-east-1.amazonaws.com/iceberg/" \
    tpcds_iceberg demo \
    --auth-scheme aws-sigv4 \
    --dest-ns tpcds_iceberg \
    --props iceberg.source=glue
  ```
- **Nessie (REST catalog)** – CLI example using a Nessie REST endpoint:
  ```bash
  connector create "Nessie Iceberg" ICEBERG \
    "http://localhost:19120/iceberg" \
    tpch demo \
    --auth-scheme none \
    --dest-ns tpch \
    --props iceberg.source=rest \
    --props warehouse=s3://warehouse
  ```
  For `iceberg.source=rest`, Floecat only requests delegated storage access when the caller
  explicitly sets `X-Iceberg-Access-Delegation`. This is intended for REST catalogs such as
  Polaris that support returning `storage-credentials` on `loadTable`.
  If the upstream REST catalog ignores that header, normal REST behavior still applies and success
  depends on some other valid storage credential path. If the upstream catalog rejects delegated
  access requests, connector planning or table load will fail on the REST call.
- **Filesystem (single table)** – CLI example using the metadata JSON as the connector URI:
  ```bash
  connector create "Filesystem Iceberg" ICEBERG \
    "s3://warehouse/metadata/00001.metadata.json" \
    fixtures demo \
    --auth-scheme none \
    --dest-ns fixtures \
    --props iceberg.source=filesystem \
    --props external.namespace=fixtures.simple \
    --props external.table-name=trino_test
  ```
- **Reconciliation** – `ReconcilerService` iterates Iceberg tables, uses `describe` to create or
  update Floecat Table records (storing schema JSON + upstream ref), ingests snapshot lineage via
  `enumerateSnapshots`, then routes stats capture through the stats control plane (native engine
  uses `captureSnapshotTargetStats`).

## Cross-References
- SPI contract: [`docs/connectors-spi.md`](connectors-spi.md)
- Delta connector for comparison: [`docs/connectors-delta.md`](connectors-delta.md)
- Service connector management: [`docs/service.md`](service.md)
