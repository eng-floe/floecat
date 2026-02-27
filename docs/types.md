# Type System Utilities

## Overview
The `core/types/` module implements FloeCat's canonical logical type system. It bridges the logical
types declared in protobuf (`types/types.proto`) with Java helpers used by connectors, schema
mappers, statistics engines, and the execution scan bundle assembler.

Key classes: `LogicalType`, `LogicalKind`, `LogicalTypeProtoAdapter`, `LogicalComparators`,
`LogicalCoercions`, `ValueEncoders`, and `MinMaxCodec`.

## Canonical Type Kinds

`LogicalKind` defines the complete set of canonical types shared across all table formats (Iceberg,
Delta, etc.) and SQL-facing components.

| Canonical Kind   | Proto field number | Description                                       |
|------------------|--------------------|---------------------------------------------------|
| `BOOLEAN`        | 1                  | Boolean true/false                                |
| `INT`            | 2                  | 64-bit signed integer (all source sizes collapse) |
| `FLOAT`          | 4                  | 32-bit IEEE-754 single-precision float            |
| `DOUBLE`         | 5                  | 64-bit IEEE-754 double-precision float            |
| `DATE`           | 6                  | Calendar date (no time, no timezone)              |
| `TIME`           | 7                  | Time of day (no date, no timezone)                |
| `TIMESTAMP`      | 8                  | Timezone-naive timestamp (local time)             |
| `TIMESTAMPTZ`    | 13                 | UTC-normalised timestamp                          |
| `STRING`         | 9                  | UTF-8 text                                        |
| `BINARY`         | 10                 | Arbitrary byte sequence                           |
| `UUID`           | 11                 | 128-bit universally unique identifier             |
| `DECIMAL`        | 12                 | Fixed-precision decimal (precision, scale)        |
| `INTERVAL`       | 14                 | Duration / period                                 |
| `JSON`           | 15                 | Semi-structured JSON text                         |
| `ARRAY`          | 16                 | Ordered collection (non-parameterised in v1)      |
| `MAP`            | 17                 | Key-value map (non-parameterised in v1)           |
| `STRUCT`         | 18                 | Named-field record (non-parameterised in v1)      |
| `VARIANT`        | 19                 | Schema-flexible semi-structured value             |

### Integer collapsing
Every source-format integer size (TINYINT, SMALLINT, INT, INTEGER, BIGINT, LONG, INT8, INT4, INT2,
UINT8, UINT4, UINT2) collapses to canonical `INT` (64-bit signed). Source alias names can be
resolved via `LogicalKind.fromName(String)`.

### Timestamp semantics
* `TIMESTAMP` — stores local time without UTC normalisation (Iceberg `withoutZone()`, Delta
  `timestamp_ntz`).
* `TIMESTAMPTZ` — stores microseconds-since-epoch UTC (Iceberg `withZone()`, Delta `timestamp`).

> **Note:** The Floe spec decode matrix v1 has these two entries inverted. The implementation
> applies the semantically correct mapping and records the discrepancy in code comments.

#### Precision + parsing
- Canonical `TIME`, `TIMESTAMP`, and `TIMESTAMPTZ` are microsecond precision. Inputs with
  higher precision are truncated to micros for stats encoding/comparison.
- Numeric encodings are only accepted for `DATE` (epoch days). `TIME`, `TIMESTAMP`, and
  `TIMESTAMPTZ` require typed values or ISO‑8601 strings; numeric heuristics are not used.
- Temporal precisions can be carried in the logical type string (e.g. `TIME(3)`,
  `TIMESTAMP(6)`, `TIMESTAMPTZ(0)`, range 0..6). When present, encoders truncate and emit exactly that many
  fractional digits. When absent, FloeCat defaults to microsecond precision and ISO‑8601
  formatting (no fixed width).
- `TIMESTAMP` expects timezone‑naive inputs (no `Z` or offset). By default, zoned strings are
  rejected. You can opt into conversion by setting:
  - `floecat.timestamp_no_tz.policy=CONVERT_TO_SESSION_ZONE` (or env
    `FLOECAT_TIMESTAMP_NO_TZ_POLICY`)
  - `floecat.session.timezone=<IANA zone>` (or env `FLOECAT_SESSION_TIMEZONE`)
  When enabled, zoned timestamps are converted into that session zone and stored as local
  `TIMESTAMP` values.

### Complex types (v1)
`ARRAY`, `MAP`, `STRUCT`, and `VARIANT` are non-parameterised in v1. The logical kind captures
only the container category; element/value/field types are captured by child `SchemaColumn` rows
carrying their own paths (e.g. `address.city`, `items[]`, `tags{}`).

## Architecture & Responsibilities
- **`LogicalType` / `LogicalKind`** – Immutable representations of logical types. `LogicalType`
  stores `(kind, precision, scale, temporalPrecision)`. Canonical `DECIMAL` semantics are
  `1 ≤ precision ≤ 38` and `0 ≤ scale ≤ precision`. TIME/TIMESTAMP/TIMESTAMPTZ may carry a
  fractional‑second precision (0..6). Connectors and consumers are responsible for enforcing
  source-format compatibility while preserving the canonical max precision contract. All other
  kinds reject parameters.
- **`LogicalTypeProtoAdapter`** – Converts between the protobuf `ai.floedb.floecat.types.LogicalType`
  wire message and the JVM `LogicalType`, preserving kind/precision/scale.
- **`LogicalCoercions`** – Coerces raw stat values to the canonical Java type for a given kind (e.g.
  any `Number` → `Long` for `INT`, string → `LocalDateTime` for `TIMESTAMP` (timezone‑naive policy),
  string → `Instant` for `TIMESTAMPTZ`).
- **`LogicalComparators`** – Provides `Comparator` instances for ordering values encoded as strings
  or byte buffers (used when building column stats).
- **`ValueEncoders` / `MinMaxCodec`** – Encode scalar values into canonical strings/bytes, enabling
  deterministic min/max statistics across connectors.

## Type-Family Helpers

`LogicalType` exposes four predicate helpers for grouping kinds:

```java
// Scalar numeric: INT, FLOAT, DOUBLE, DECIMAL
boolean isNumeric()

// Temporal: DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL
boolean isTemporal()

// Container: ARRAY, MAP, STRUCT, VARIANT
boolean isComplex()

// Everything that is not a container (alias for !isComplex())
boolean isScalar()
```

Example:
```java
LogicalType t = LogicalType.decimal(10, 2);
t.isNumeric();   // true
t.isDecimal();   // true
t.isTemporal();  // false
t.isComplex();   // false
t.isScalar();    // true
```

## Public API / Surface Area
Most classes expose static helpers:
```java
LogicalType t = LogicalType.decimal(38, 4);
String logicalType = LogicalTypeFormat.format(t);
String encoded = MinMaxCodec.encode(t, BigDecimal.valueOf(42));
```
`LogicalTypeProtoAdapter.decodeLogicalType(String logicalType)` and
`.encodeLogicalType(LogicalType logicalType)` convert between canonical logical type strings and
runtime objects.

## Arrow Mapping Contract

`core/arrow` helpers (especially `ArrowSchemaUtil`) are defined over FloeCat logical types, not over
arbitrary engine-native type systems.

- Input is `SchemaColumn.logical_type` and should be a FloeCat canonical logical type string (or an
  accepted alias handled by `LogicalKind.fromName` semantics).
- Integer aliases (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `INT2/4/8`, `UINT2/4/8`) all map to
  Arrow signed 64-bit (`Int64`) to preserve collapsed canonical `INT` behavior.
- Unknown, null, or blank logical types fail fast with `IllegalArgumentException`; they are not
  silently coerced to `Utf8`.
- `JSON` maps to Arrow `Utf8`.
- `UUID` maps to Arrow `FixedSizeBinary(16)`; `BINARY` maps to Arrow `Binary`.
- `DECIMAL` maps to Arrow `Decimal128` with the declared precision/scale.
- `TIME` maps to Arrow `Time(MICROSECOND, 64)`, `TIMESTAMP` to `Timestamp(MICROSECOND, null)`,
  and `TIMESTAMPTZ` to `Timestamp(MICROSECOND, "UTC")`.
- `INTERVAL` and complex container types (`ARRAY`, `MAP`, `STRUCT`, `VARIANT`) are **not**
  supported in Arrow schema generation; they must be omitted or cast to `STRING`/`BINARY`.

If external Flight providers want to reuse `core/arrow`, they should first map their source type
surface into FloeCat logical types.

## Source-Format Alias Lookup

`LogicalKind.fromName(String candidate)` resolves source-format type names to canonical kinds:

```java
LogicalKind.fromName("bigint")               // → INT
LogicalKind.fromName("float4")               // → FLOAT
LogicalKind.fromName("double precision")     // → DOUBLE
LogicalKind.fromName("timestamp with time zone") // → TIMESTAMPTZ
LogicalKind.fromName("ARRAY")                // → ARRAY
```

The lookup is case-insensitive and collapses internal whitespace. Unknown names throw
`IllegalArgumentException`.

## Important Internal Details
- **Validation** – `LogicalType` constructor enforces: for `DECIMAL`, `precision ≥ 1` and
  `0 ≤ scale ≤ precision`. Canonical DECIMAL precision is capped at 38; enforcement happens at
  connector/consumer boundaries where source formats are interpreted. Non-decimal kinds reject
  precision/scale altogether.
- **Non-orderable types** – `INTERVAL`, `JSON`, and complex kinds (`ARRAY`, `MAP`, `STRUCT`,
  `VARIANT`) have no meaningful min/max statistics. `LogicalComparators.normalize()` returns
  `null` and `ValueEncoders.encodeToString` throws for JSON/complex kinds, so connectors should
  leave bounds unset.
- **Comparators** – `LogicalComparators` provides specialised comparators for lexical ordering of
  encoded min/max values so histogram builders can operate on encoded strings.
- **Encoders** – `ValueEncoders` normalises values before storing them in stats to guarantee
  consistent lexical ordering across connectors.

## Data Flow & Lifecycle
```
Connector reads Parquet/Delta/Iceberg schema
  → schema mapper emits canonical LogicalKind strings (e.g. "INT", "TIMESTAMPTZ", "ARRAY")
  → SchemaColumn.logical_type stores the canonical string
  → ValueEncoders encode per-column min/max/ndv bounds
  → StatsRepository stores encoded values (string or bytes)
  → Query lifecycle service converts stored logical type IDs into planner TypeSpecs via TypeRegistry
```

## Configuration & Extensibility
The module is pure Java; no configuration is required. Extending the type system involves:
1. Adding a new `LogicalKind` enum entry and proto `Kind` field number.
2. Registering source-format aliases in the `ALIASES` map inside `LogicalKind`.
3. Updating `LogicalCoercions`, `LogicalComparators`, and `ValueEncoders` for the new kind.
4. Updating connector schema mappers (`IcebergSchemaMapper`, `DeltaSchemaMapper`) to emit the new
   canonical name.
5. Ensuring downstream consumers (`FloeTypeMapper`, `DeltaManifestMaterializer`) handle the new
   canonical name.

## Examples & Scenarios
- **Iceberg schema parsing** – `IcebergSchemaMapper.toCanonical(Type)` converts Iceberg types to
  canonical strings (e.g. `TimestampType.withZone()` → `"TIMESTAMPTZ"`), storing them in
  `SchemaColumn.logical_type`. This avoids the historic ambiguity where `"timestamp"` meant
  UTC-stored in Delta but non-UTC in Iceberg.
- **Delta schema parsing** – `DeltaSchemaMapper.deltaTypeToCanonical(JsonNode)` applies Delta-
  specific semantics: `"timestamp"` → `"TIMESTAMPTZ"` (UTC-stored), `"timestamp_ntz"` → `"TIMESTAMP"`
  (timezone-naive).
- **Statistics ingestion** – NDV providers convert Parquet min/max values using `MinMaxCodec` before
  storing them in `ColumnStats`, ensuring planners can compare them without deserialising actual
  binary payloads.

## Cross-References
- Protobuf type definitions: [`docs/proto.md`](proto.md)
- Query lifecycle service: [`docs/service.md`](service.md#query-lifecycle-service)
