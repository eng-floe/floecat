# Delta / Unity Catalog Connector

## Overview

`connectors/catalogs/delta/` implements a connector targeting Databricks Unity Catalog-powered Delta Lake
warehouses. It uses the Delta Kernel, Unity Catalog REST APIs, Databricks SQL endpoints, and AWS S3
(through the v2 client) to enumerate tables, collect statistics, and plan files.

The primary implementation is `DeltaConnector` (abstract) with source-specific subclasses for Unity
Catalog, AWS Glue, and filesystem-backed tables, exposed via `DeltaConnectorProvider`. Supporting classes manage
OAuth2 bearer token usage (including CLI, service principal, and WIF flows resolved upstream),
Databricks SQL execution, and custom file readers for S3.

## Architecture & Responsibilities

- **`DeltaConnector`** – Abstract `FloecatConnector` that centralizes snapshot/stat logic.
- **`UnityDeltaConnector`** – Unity Catalog-backed connector that:
  - Talks to Unity Catalog REST (`UcHttp`) to list catalogs/schemas/tables.
  - Uses Delta Kernel (`io.delta.kernel.Table`) for schema and snapshot access.
  - Executes Databricks SQL statements via `SqlStmtClient` if a warehouse is configured.
  - Reads Parquet data with `S3V2FileSystemClient` and `ParquetS3V2InputFile` for NDV/statistics.
  - Plans files using `DeltaPlanner`, emitting `ScanFile`s for data/delete manifests.
- **`DeltaFilesystemConnector`** – Single-table connector for `delta.table-root` plus optional
  `external.namespace` / `external.table-name` overrides.
- **`DeltaGlueConnector`** – AWS Glue-backed connector that:
  - Lists databases and Delta-registered tables from Glue.
  - Resolves table `storage_location` from Glue metadata and reads table snapshots via Delta Kernel.
- **`DeltaConnectorFactory`** – Selects Unity, Glue, or filesystem sources and wires engine/auth/IO.
- **`UcBaseSupport` / `UcHttp`** – HTTP helpers for constructing API URLs, encoding parameters, and
  handling retries/timeouts.
- **`DeltaTypeMapper`** – Maps Delta/Parquet logical types into Floecat logical types for stats.

## Public API / Surface Area

`DeltaConnector` and subclasses implement the SPI methods:

- `listNamespaces()` – Fetches catalogs via `/api/2.1/unity-catalog/catalogs`, then enumerates
  schemas per catalog, returning `catalog.schema` pairs.
- `listTables(namespace)` – Calls `/api/2.1/unity-catalog/tables` filtered by catalog/schema, then
  filters to `data_source_format == DELTA`.
- `describe(namespace, table)` – Fetches table metadata from Unity Catalog, reads the Delta schema via
  Delta Kernel, and returns a `TableDescriptor` containing location, partition keys, and properties.
- `enumerateSnapshots(...)` – Iterates Delta snapshots and emits `SnapshotBundle`s for snapshot
  lineage/metadata. In incremental mode, the
  connector enumerates all Delta versions that Floecat has not already ingested. When
  `SnapshotEnumerationOptions.targetSnapshotIds` is supplied, enumeration is limited to that
  explicit version set even when `fullRescan=true`.
- `captureSnapshotTargetStats(...)` – Captures table/column/file stats for one snapshot and optional
  selector scope, optionally sampling Parquet files for NDV (`SamplingNdvProvider`,
  `ParquetNdvProvider`).

## Important Internal Details

- **Authentication** – Uses an OAuth2 bearer token supplied in the resolved connector config or
  the Databricks CLI cache. Token exchange and secret handling happen earlier in the service layer,
  except for CLI cache refresh which is handled in the connector.
- **HTTP & SQL clients** – `UcHttp` centralises base URI, connect/read timeouts, and error mapping.
  `SqlStmtClient` optionally executes SQL statements (for example to inspect statistics tables) via
  Databricks SQL warehouses.
- **S3 integration** – Uses AWS SDK v2 (`S3Client`) with region from connector properties to read
  data files. `S3RangeReader` provides efficient range reads for Parquet file access.
- **NDV sampling** – Controlled by `stats.ndv.enabled`, `stats.ndv.sample_fraction`, and
  `stats.ndv.max_files`. Samples combine streaming NDV with Parquet footers for accuracy.
- **Type mapping** – `DeltaTypeMapper` ensures nested Delta/Parquet types are faithfully represented
  when computing stats, aligning with `types/` definitions.
- **Constraint mapping** – Snapshot constraints currently emit metadata that is reliably exposed by
  Delta snapshots/table metadata:
  - `CT_NOT_NULL` from non-nullable schema fields (including nested struct leaves).
  - `CT_CHECK` from table properties using `delta.constraints.<name>=<sql_expression>`.
  - `CT_PRIMARY_KEY`, `CT_FOREIGN_KEY`, and `CT_UNIQUE` are not emitted from core Delta metadata
    because no portable source is defined for them.
  - Source-specific extraction path:
    - **Unity Catalog**: merge of snapshot metadata + UC table properties from
      `/api/2.1/unity-catalog/tables/{full_name}`. Snapshot metadata wins on key collisions.
    - **Glue**: merge of snapshot metadata + Glue table parameters. Snapshot metadata wins on key
      collisions.
    - **Filesystem**: snapshot metadata only.
  - Connector matrix (current behavior):
    - **Unity**: `CT_NOT_NULL`, `CT_CHECK` (`delta.constraints.*`) from merged snapshot + UC metadata.
    - **Glue**: `CT_NOT_NULL`, `CT_CHECK` (`delta.constraints.*`) from merged snapshot + Glue metadata.
    - **Filesystem**: `CT_NOT_NULL`, `CT_CHECK` (`delta.constraints.*`) from snapshot metadata only.

## Data Flow & Lifecycle

```
ConnectorFactory.create(cfg)
  → DeltaConnectorFactory.create(uri, options, authProvider)
      → Select Unity vs filesystem source
      → Instantiate S3 client + Delta Kernel engine
      → Configure Unity Catalog HTTP and optional SQL client when needed
  → listNamespaces/listTables via Unity Catalog REST
  → describe via REST + Delta Kernel schema inspection
  → enumerateSnapshots
      → Delta Kernel snapshot lineage
  → captureSnapshotTargetStats
      → Delta Kernel Snapshot → Parquet stats engine → TargetStatsRecord (table/column/file stats)
  → plan
      → DeltaPlanner traverses _delta_log → ScanBundle entries for data/delete files
```

Connector resources (HTTP clients, S3 client, Delta engine) are closed when `close()` is invoked.

## Configuration & Extensibility

Important connector properties:

- `delta.source` – Selects backend (`unity`, `glue`, `filesystem`). Defaults to `unity`.
- `delta.table-root` – Required for `delta.source=filesystem`, pointing at a Delta table root.
- `external.namespace`, `external.table-name` – Optional overrides for filesystem connector naming.
- `http.connect.ms`, `http.read.ms` – Timeout controls for Unity Catalog HTTP calls.
- `databricks.sql.warehouse_id` – Enables SQL statement execution when set.
- `s3.region` / `aws.region` – Region for the S3 client used to read Parquet files.
- `stats.ndv.*` – Sampling knobs identical to the Iceberg connector.
- Authentication-specific options (`auth.scheme`, `auth.properties`) – `auth.scheme=oauth2`
  works with resolved bearer-style credentials or `oauth.mode=cli` (to read the Databricks CLI
  cache). Secret-bearing auth values must be supplied via `AuthCredentials`, not persisted in
  `auth.properties`. Service principal and WIF are expressed as `AuthCredentials` and resolved
  upstream. For `delta.source=glue`, use resolved AWS credentials or non-secret
  `auth.properties` profile settings and set `auth.scheme=aws-sigv4` or `none`.

Auth credential types (`--cred-type`) are documented in [`docs/cli-reference.md`](cli-reference.md).
For `delta.source=unity`, the relevant types are `bearer`, `client` (SP), `cli`,
`token-exchange` (WIF), `token-exchange-entra`, and `token-exchange-gcp`. Entra/GCP exchanges only
work if the Databricks workspace is configured to trust those IdPs. Use the Databricks workspace
host for `uri` (for example `https://dbc-<workspace-id>.cloud.databricks.com`); for Databricks
Unity Catalog, token exchange endpoints typically use `https://<workspace-host>/oidc/v1/token`.

For `delta.source=glue` and `delta.source=filesystem`, this Databricks OIDC token endpoint pattern
does not apply. Shared outbound token endpoint validation behavior is documented in
[`docs/operations.md`](operations.md).

Extensibility points:

- Implement new auth schemes by extending `AuthProvider` and wiring them in the connector provider.
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
    "auth":{
      "scheme":"oauth2",
      "credentials":{"bearer":{"token":"<access-token>"}},
      "properties":{}
    }
  }
  ```

- **CLI examples**
  - **Service principal (SP)** – For `delta.source=unity`, use `client` credentials. Resolve via
    client credentials exchange (service layer), connector sees a bearer token. Token endpoint is
    the workspace OIDC URL:
    `https://<workspace-host>/oidc/v1/token`.

    ```bash
    connector create "Unity Delta SP" DELTA https://dbc-d382c535-b2a9.cloud.databricks.com \
      "cusack.ext_tpcds" tpcds --dest-ns federated --source-table store_sales \
      --auth-scheme oauth2 \
      --cred-type client \
      --cred endpoint=https://dbc-d382c535-b2a9.cloud.databricks.com/oidc/v1/token \
      --cred client_id=3d9b2f0f-7f1a-4b6e-9f0a-2f1b6c9a1234 \
      --cred client_secret=ddbsp-9f1c2a3b4c5d6e7f8a9b \
      --auth scope=all-apis
    ```

  - **WIF (token exchange)** – For `delta.source=unity`, use `token-exchange`. Resolve via RFC 8693
    exchange (service layer), connector sees a bearer token. Token endpoint is the workspace OIDC
    URL:
    `https://<workspace-host>/oidc/v1/token`.

    ```bash
    connector create "Unity Delta WIF" DELTA https://dbc-d382c535-b2a9.cloud.databricks.com \
      "cusack.ext_tpcds" tpcds --dest-ns federated --source-table store_sales \
      --auth-scheme oauth2 \
      --cred-type token-exchange \
      --cred endpoint=https://dbc-d382c535-b2a9.cloud.databricks.com/oidc/v1/token \
      --cred client_id=3d9b2f0f-7f1a-4b6e-9f0a-2f1b6c9a1234 \
      --cred client_secret=ddbsp-9f1c2a3b4c5d6e7f8a9b \
      --cred subject_token_type=urn:ietf:params:oauth:token-type:jwt \
      --cred requested_token_type=urn:ietf:params:oauth:token-type:access_token \
      --cred scope="all-apis offline_access"
    ```

  - **CLI cache** – Connector reads the Databricks CLI cache directly:

    ```bash
    connector create "Unity Delta CLI" DELTA https://dbc-d382c535-b2a9.cloud.databricks.com \
      "cusack.ext_tpcds" tpcds --dest-ns federated --source-table store_sales \
      --auth-scheme oauth2 \
      --cred-type cli \
      --cred cache_path=~/.databricks/token-cache.json
    ```

  - **Bearer token (PAT)** – Using the `connector` CLI with a resolved token or Databricks personal access token:

  ```bash
  connector create "Unity Delta Token" DELTA https://dbc-d382c535-b2a9.cloud.databricks.com \
    "cusack.ext_tpcds" tpcds --dest-ns federated --source-table store_sales \
    --auth-scheme oauth2 --cred-type bearer --cred token=<access-token>
  ```

- **Full reconciliation** – `ReconcilerService` enters full-rescan mode (`fullRescan=true`), so the
  connector lists every table in the namespace, creates missing namespaces in the destination
  catalog, updates `DestinationTarget` pointers, and ingests snapshot stats for each table.

## Cross-References

- SPI details: [`docs/connectors-spi.md`](connectors-spi.md)
- Iceberg connector for contrast: [`docs/connectors-iceberg.md`](connectors-iceberg.md)
- Service & reconciler integration: [`docs/service.md`](service.md), [`docs/reconciler.md`](reconciler.md)
