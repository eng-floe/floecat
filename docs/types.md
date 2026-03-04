# Type System Utilities

## Overview
The `core/types/` module implements Floecat’s logical type system. It bridges the logical types declared
in protobuf (`types/types.proto`) with Java helpers used by connectors, statistics engines, and the
execution scan bundle assembler.

Key classes: `LogicalType`, `LogicalKind`, `LogicalTypeProtoAdapter`, `LogicalComparators`,
`LogicalCoercions`, `ValueEncoders`, and `MinMaxCodec`.

## Architecture & Responsibilities
- **`LogicalType` / `LogicalKind`** – Immutable representations of logical types (boolean, integer,
  decimal with precision/scale, temporal, UUID, etc.). Validates that decimals include both precision
  and scale.
- **`LogicalTypeProtoAdapter`** – Converts between protobuf `ai.floedb.floecat.types.LogicalType` and
  JVM `LogicalType`, ensuring kind/precision/scale consistency.
- **`LogicalCoercions`** – Rules for widening or narrowing conversions between logical types (for
  example INT32 → INT64, DECIMAL precision adjustments).
- **`LogicalComparators`** – Provides `Comparator` instances for ordering values encoded as strings or
  byte buffers (used when building stats).
- **`ValueEncoders` / `MinMaxCodec`** – Encode scalar values into canonical strings/bytes, enabling
  deterministic min/max statistics. Handles signed numbers, decimals, binary, and temporals.

## Public API / Surface Area
Most classes expose static helpers:
```java
LogicalType t = LogicalType.decimal(38, 4);
boolean compatible = LogicalCoercions.canCoerce(t, LogicalType.of(LogicalKind.FLOAT64));
String encoded = MinMaxCodec.encodeDecimal(BigDecimal.valueOf(42), t);
```
`LogicalTypeProtoAdapter.fromProto(LogicalType rpcType)` and `.toProto(LogicalType jt)` convert
between wire format and runtime objects.

## Important Internal Details
- **Validation** – `LogicalType` constructor enforces decimal constraints (`precision >= 1`,
  `0 <= scale <= precision`). Non-decimals reject precision/scale settings altogether.
- **Comparators** – `LogicalComparators` provide specialised comparators for lexical ordering of
  encoded min/max values so histogram builders can operate on encoded strings.
- **Encoders** – `ValueEncoders` normalise values before storing them in stats to guarantee consistent
  lexical ordering across connectors (for example left-padding integers, standardising boolean
  casing).

## Data Flow & Lifecycle
```
Connector reads Parquet schema → maps physical types to LogicalType
  → ValueEncoders encode per-column min/max/ndv bounds
  → StatsRepository stores encoded values (string or bytes)
  → Query lifecycle service converts stored logical type IDs into planner TypeSpecs via TypeRegistry
```

## Configuration & Extensibility
The module is pure Java; no configuration is required. Extending the type system involves:
- Adding new `LogicalKind` enum entries.
- Updating `LogicalTypeProtoAdapter`, `LogicalCoercions`, comparators, and encoders to handle new
  kinds.
- Ensuring connectors map upstream physical types to the new logical kind, and planners understand
  the emitted `TypeSpec` (see [`docs/service.md`](service.md#query-lifecycle-service)).

## Examples & Scenarios
- **Iceberg schema parsing** – The query lifecycle service parses Iceberg schemas,
  constructs `TypeSpec`s, registers them with `TypeRegistry`, and uses `LogicalTypeProtoAdapter` to
  maintain consistency between stored schema JSON and bundle metadata.
- **Statistics ingestion** – NDV providers convert Parquet min/max values using `MinMaxCodec` before
  storing them in `ColumnStats`, ensuring planners can compare them without deserialising actual
  binary payloads.

## Cross-References
- Protobuf type definitions: [`docs/proto.md`](proto.md)
- Query lifecycle service: [`docs/service.md`](service.md#query-lifecycle-service)
