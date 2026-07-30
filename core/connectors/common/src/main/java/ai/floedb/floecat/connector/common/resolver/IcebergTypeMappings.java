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

package ai.floedb.floecat.connector.common.resolver;

import ai.floedb.floecat.types.LogicalField;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.util.List;
import java.util.Objects;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Shared Iceberg -> Floecat type mapping logic for schema and stats. */
public final class IcebergTypeMappings {
  private IcebergTypeMappings() {}

  /** Convert an Iceberg {@link Type} to a canonical {@link LogicalType}. */
  public static LogicalType toLogical(Type t) {
    Objects.requireNonNull(t, "Iceberg type");
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
      case TIMESTAMP_NANO -> {
        var ts = (Types.TimestampNanoType) t;
        yield LogicalType.of(
            ts.shouldAdjustToUTC() ? LogicalKind.TIMESTAMPTZ : LogicalKind.TIMESTAMP);
      }
      case STRING -> LogicalType.of(LogicalKind.STRING);
      case FIXED, BINARY -> LogicalType.of(LogicalKind.BINARY);
      case UUID -> LogicalType.of(LogicalKind.UUID);
      case LIST -> {
        Types.ListType list = t.asListType();
        yield LogicalType.array(toLogical(list.elementType()), list.isElementOptional());
      }
      case MAP -> {
        Types.MapType map = t.asMapType();
        yield LogicalType.map(
            toLogical(map.keyType()), toLogical(map.valueType()), map.isValueOptional());
      }
      case STRUCT -> {
        List<LogicalField> fields =
            t.asStructType().fields().stream()
                .map(f -> new LogicalField(f.name(), f.isOptional(), toLogical(f.type())))
                .toList();
        // An explicitly empty source struct is a known-empty shape, not the legacy tag.
        yield LogicalType.struct(fields);
      }
      case VARIANT -> LogicalType.of(LogicalKind.VARIANT);
      case DECIMAL -> {
        var d = (Types.DecimalType) t;
        LogicalType logicalType = LogicalType.decimal(d.precision(), d.scale());
        DecimalPrecisionConstraints.validateDecimalPrecision(
            logicalType, "Iceberg", d.toString(), MAX_DECIMAL_PRECISION);
        yield logicalType;
      }
      default -> throw new IllegalArgumentException("Unrecognized Iceberg type: " + t.typeId());
    };
  }

  private static final int MAX_DECIMAL_PRECISION = 38;
}
