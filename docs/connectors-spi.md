# Connector SPI

## Overview
`core/connectors/spi/` defines the contract that every upstream metadata connector must implement. The
SPI abstracts discovery of namespaces/tables, enumeration of snapshots with statistics (table,
column, and file-level), enumeration of physical files for a snapshot, and authentication adapters. It
also packages shared tooling for column statistics, file statistics, and NDV estimation.

Connectors implement `FloecatConnector` and typically wrap an upstream catalog API (Iceberg REST,
Unity Catalog, etc.), translating its schemas, snapshots, and metrics into Floecat protobufs.

## Architecture & Responsibilities
- **`FloecatConnector`** – Primary interface (extends `Closeable`). Methods:
  - `id()` → stable connector identifier.
  - `format()` → `ConnectorFormat` (ICEBERG, DELTA, etc.).
  - `listNamespaces()`.
  - `listTables(namespaceFq)`.
  - `describe(namespaceFq, tableName)` → `TableDescriptor` with location, schema JSON, partition
    keys, and properties.
  - `enumerateSnapshotsWithStats(...)` → `SnapshotBundle`s containing per-snapshot table/column/file stats.
- **`ConnectorFactory`** – Instantiates connectors given a `ConnectorConfig` (URI, options,
  authentication). The service uses it to validate specs and the reconciler uses it during runs.
- **`ConnectorConfigMapper`** – Bidirectional conversion between RPC `Connector` protobufs and the
  SPI’s `ConnectorConfig` records.
- **Snapshot bundle attachments** – `SnapshotBundle.metadata` exposes a `map<string, bytes>` that
  connectors can use to attach format-specific payloads (for example, Iceberg metadata snapshots)
  without changing the SPI surface.
- **Auth providers** – `AuthProvider` + concrete implementations such as `NoAuthProvider` and
  `AwsSigV4AuthProvider` supply credentials or headers per connector.
- **Stats helpers** – `StatsEngine`, `GenericStatsEngine`, `ProtoStatsBuilder`, and NDV utilities
  (`NdvProvider`, `SamplingNdvProvider`, `ParquetNdvProvider`, `FilteringNdvProvider`,
  `StaticOnceNdvProvider`, `NdvApprox`, `NdvSketch`). These components interpret Parquet footers,
  combine NDV approximations, and emit `TableStats`/`ColumnStats` protobufs.

### Column bounds encoding

When connectors emit `ColumnStats.min`/`max`, they must use the canonical string format documented in
`floecat/catalog/stats.proto`. Each of these bounds is optional—`hasMin()`/`hasMax()` indicate the
field was populated (even when the string itself is empty). In brief:

  * Bounds are UTF-8 strings reflecting the logical ordering (not engine collation) and should be
    left unset when unknown.
  * Encodings follow the logical_type:
    * Boolean → `"true"`/`"false"` (lowercase).
    * Integer → base-10 digits with optional `-`, no leading `+` or zero padding.
    * Float → Java `Float.toString`/`Double.toString` output, plus `NaN`, `Infinity`, `-Infinity`.
      Normalizing `-0` → `0` improves stability.
    * Decimal → plain base-10 string with optional `-`, no exponent, normalized by trimming leading
      zeros in the integer part and trailing zeros in the fractional part; `ValueEncoders.encodeToString(lt, value)`
      already follows this normalization routine and collapses `-0` → `0`.
    * Date/Time/Timestamp → ISO-8601 (`YYYY-MM-DD`, `HH:MM:SS[.fffffffff]`, `YYYY-MM-DDTHH:MM:SS[.fffffffff]`
      for `TIMESTAMP`, `YYYY-MM-DDTHH:MM:SS[.fffffffff]Z` for `TIMESTAMPTZ`). If the logical type
      includes a temporal precision suffix (e.g. `TIMESTAMP(3)`), emit exactly that many fractional
      digits (0..6). Otherwise Floecat defaults to microsecond precision with ISO formatting.
    * UUID → lowercase 8-4-4-4-12 hex.
    * String → literal UTF-8 content.
    * Binary → base64 (RFC 4648) without line breaks (padding `=` is OK).
  * Null/NAN counts are optional (`null_count`, `nan_count`); set them only when the connector can
    report a value so downstream planners can distinguish “unknown” from zero.
  * Non-orderable types (`INTERVAL`, `JSON`, `ARRAY`, `MAP`, `STRUCT`, `VARIANT`) should leave
    `min`/`max` unset. Floecat treats `INTERVAL` as non‑stats‑orderable; if you still emit bounds,
    encode them as ISO‑8601 duration strings and expect them to be stored but ignored by pruning
    comparisons.

Helpers such as `ValueEncoders.encodeToString` already follow these rules; reuse them when converting
native column values to strings so stats stay portable across languages.

### Temporal values (no numeric heuristics)

Floecat does not guess time units based on numeric magnitude. Connectors must supply typed temporal
values (e.g., `LocalTime`, `LocalDateTime`, `Instant`) or ISO‑8601 strings with the correct
precision. Numeric epoch values are rejected for `TIME`, `TIMESTAMP`, and `TIMESTAMPTZ`. If your
connector reads Parquet/Delta/Iceberg stats, convert numeric values using the source metadata’s
explicit unit before calling `ValueEncoders.encodeToString`.

Schema mappers should normalize temporal types at ingest time and emit canonical logical types.

If your connector provides zoned timestamp strings, either map them to `TIMESTAMPTZ` or enable
conversion for `TIMESTAMP` by setting `floecat.timestamp_no_tz.policy=CONVERT_TO_SESSION_ZONE` and
`floecat.session.timezone=<IANA zone>` (or the corresponding `FLOECAT_*` env vars).

`DATE` continues to accept numeric epoch-day values; fractional values are rejected.

For Parquet `TIMESTAMP(isAdjustedToUTC=false)` stats, Floecat interprets the epoch counts as UTC
wall-clock when constructing a `LocalDateTime` (i.e., no session-zone shift is applied). If you want
session-zone semantics, convert explicitly before encoding stats.

## Public API / Surface Area
The SPI is intentionally small:
```java
interface FloecatConnector extends Closeable {
  String id();
  ConnectorFormat format();
  List<String> listNamespaces();
  List<String> listTables(String namespaceFq);
  TableDescriptor describe(String namespaceFq, String tableName);
  List<SnapshotBundle> enumerateSnapshotsWithStats(...);
}
```
`TableDescriptor`, `SnapshotBundle`, and `ScanBundle` are immutable records; connectors populate them
with canonical metadata that the reconciler ingests. `SnapshotBundle.fileStats` is optional but
should be populated when Parquet footers or upstream metadata can provide per-file row counts, sizes,
and per-column stats. Snapshot bundles also carry manifest-list URIs, sequence numbers, and summary
maps so downstream APIs can mirror Iceberg’s REST contract.

`ConnectorConfig` encodes:
- Kind + source/destination selectors (`SourceSelector`, `DestinationTarget`).
- URI and arbitrary `properties` map.
- Authentication configuration (scheme, credentials, headers, properties).

`ConnectorProvider` is a lightweight registry allowing CDI discovery of connector factories.

## Important Internal Details
- **AuthCredentials resolution** – Connectors consume already-resolved auth props (for example a
  bearer token). The service handles secrets manager lookups and token exchange flows, so connector
  implementations should not perform credential exchanges themselves.
- **Auth redaction** – The service never stores `AuthCredentials` in connector records and masks
  sensitive auth fields in responses so callers cannot retrieve raw secrets.
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
  → FloecatConnector (opens upstream clients, auth providers)
      → listNamespaces/listTables → service repo ensures namespace/table existence
      → describe → Table specs persisted with upstream references
      → enumerateSnapshotsWithStats → StatsRepository writes Table/Column/File stats per snapshot
  ← close() cleans up HTTP/S3/DB connections
```
`ConnectorFactory.create` is invoked both in `ConnectorsImpl.validate` (short-lived) and in the
reconciler (long-running); connectors must tolerate repeated instantiation and release resources in
`close()`.

## Configuration & Extensibility
- To add a new connector type, implement `FloecatConnector` plus a factory and annotate it so CDI can
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
