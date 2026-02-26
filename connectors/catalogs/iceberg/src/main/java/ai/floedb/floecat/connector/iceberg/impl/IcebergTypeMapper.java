/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.connector.iceberg.impl;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Maps Iceberg native types to FloeCat canonical {@link LogicalType}.
 *
 * <p><b>Module boundary note:</b> {@code IcebergSchemaMapper} (in {@code core/connectors/common})
 * also contains an inline string-returning copy of this mapping ({@code toCanonical(Type)}). The
 * duplication is intentional: {@code core/connectors/common} must not depend on {@code
 * connectors/catalogs/iceberg}, so the schema mapper inlines the mapping rather than calling this
 * class. Keep both in sync when adding new Iceberg types.
 *
 * <p><b>Timestamp semantics:</b> Iceberg {@code TimestampType.withZone()} is UTC-normalised →
 * {@link LogicalKind#TIMESTAMPTZ}. Iceberg {@code TimestampType.withoutZone()} is timezone-naive →
 * {@link LogicalKind#TIMESTAMP}.
 *
 * <p><b>Integer collapsing:</b> Both Iceberg {@code INTEGER} (32-bit) and {@code LONG} (64-bit)
 * collapse to canonical {@link LogicalKind#INT} (64-bit).
 *
 * <p><b>Complex types:</b> {@code LIST} → {@link LogicalKind#ARRAY}, {@code MAP} → {@link
 * LogicalKind#MAP}, {@code STRUCT} → {@link LogicalKind#STRUCT}. Element/value/field types are
 * captured by child {@code SchemaColumn} rows with their own paths.
 *
 * <p><b>Unrecognised/unsupported types</b> fail fast with {@link IllegalArgumentException}. This
 * keeps planner/stats behavior aligned with schema mapping, which also rejects unsupported Iceberg
 * types.
 */
final class IcebergTypeMapper {
  private static final int MAX_DECIMAL_PRECISION = 38;

  /**
   * Converts an Iceberg {@link Type} to a FloeCat canonical {@link LogicalType}.
   *
   * @param t an Iceberg type (never null)
   * @return the corresponding canonical {@link LogicalType}
   */
  static LogicalType toLogical(Type t) {
    return switch (t.typeId()) {
      case BOOLEAN -> LogicalType.of(LogicalKind.BOOLEAN);
      case INTEGER, LONG -> LogicalType.of(LogicalKind.INT);
      case FLOAT -> LogicalType.of(LogicalKind.FLOAT);
      case DOUBLE -> LogicalType.of(LogicalKind.DOUBLE);
      case DATE -> LogicalType.of(LogicalKind.DATE);
      case TIME -> LogicalType.of(LogicalKind.TIME);
      case TIMESTAMP -> {
        var ts = (Types.TimestampType) t;
        yield LogicalType.of(
            ts.shouldAdjustToUTC() ? LogicalKind.TIMESTAMPTZ : LogicalKind.TIMESTAMP);
      }
      case STRING -> LogicalType.of(LogicalKind.STRING);
      case FIXED, BINARY -> LogicalType.of(LogicalKind.BINARY);
      case UUID -> LogicalType.of(LogicalKind.UUID);
      case LIST -> LogicalType.of(LogicalKind.ARRAY);
      case MAP -> LogicalType.of(LogicalKind.MAP);
      case STRUCT -> LogicalType.of(LogicalKind.STRUCT);
      case DECIMAL -> {
        var d = (Types.DecimalType) t;
        if (d.precision() > MAX_DECIMAL_PRECISION) {
          throw new IllegalArgumentException(
              "Unsupported DECIMAL precision "
                  + d.precision()
                  + " for Iceberg declared type '"
                  + d
                  + "'; max supported precision is "
                  + MAX_DECIMAL_PRECISION);
        }
        yield LogicalType.decimal(d.precision(), d.scale());
      }
      default -> throw new IllegalArgumentException("Unrecognized Iceberg type: " + t.typeId());
    };
  }
}
