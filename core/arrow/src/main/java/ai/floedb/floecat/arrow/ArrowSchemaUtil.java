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

package ai.floedb.floecat.arrow;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Utilities for converting {@link SchemaColumn} metadata into Arrow schemas. */
public final class ArrowSchemaUtil {

  private ArrowSchemaUtil() {}

  public static Schema toArrowSchema(List<SchemaColumn> columns) {
    Objects.requireNonNull(columns, "columns");
    List<Field> fields = new ArrayList<>(columns.size());
    for (SchemaColumn column : columns) {
      fields.add(toField(column));
    }
    return new Schema(fields);
  }

  private static Field toField(SchemaColumn column) {
    return new Field(column.getName(), fieldType(column), List.of());
  }

  private static FieldType fieldType(SchemaColumn column) {
    ArrowType arrowType = arrowType(column.getLogicalType());
    return new FieldType(column.getNullable(), arrowType, null);
  }

  private static ArrowType arrowType(String logicalType) {
    String normalized = requireNonBlank(logicalType);
    String upper = normalized.toUpperCase(Locale.ROOT);
    if (upper.endsWith("[]")) {
      return new ArrowType.Utf8();
    }
    if ("INTERVAL".equals(upper)) {
      throw new IllegalArgumentException(
          "INTERVAL has no stable Arrow representation; omit or cast to STRING/BINARY");
    }
    if (Set.of("ARRAY", "MAP", "STRUCT", "VARIANT").contains(upper)) {
      throw new IllegalArgumentException(
          "Complex logical types are not supported in Arrow schema mapping: " + upper);
    }
    if ("JSON".equals(upper)) {
      return new ArrowType.Utf8();
    }
    if ("FLOAT4".equals(upper)) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    }
    if ("FLOAT8".equals(upper)) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    }
    if ("TIMESTAMPTZ".equals(upper)) {
      return new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
    }
    LogicalType parsed = LogicalTypeFormat.parse(normalized);
    return arrowType(parsed);
  }

  private static ArrowType arrowType(LogicalType logicalType) {
    return switch (logicalType.kind()) {
      case BOOLEAN -> ArrowType.Bool.INSTANCE;
      case INT16 -> new ArrowType.Int(16, true);
      case INT32 -> new ArrowType.Int(32, true);
      case INT64 -> new ArrowType.Int(64, true);
      case FLOAT32 -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case FLOAT64 -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case DECIMAL -> decimalType(logicalType.precision(), logicalType.scale());
      case STRING -> new ArrowType.Utf8();
      case DATE -> new ArrowType.Date(DateUnit.DAY);
      case TIME -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case TIMESTAMP -> new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case UUID -> new ArrowType.FixedSizeBinary(16);
      case BINARY -> new ArrowType.Binary();
    };
  }

  private static String requireNonBlank(String logicalType) {
    if (logicalType == null) {
      throw new IllegalArgumentException("Logical type must not be null");
    }
    String normalized = logicalType.trim();
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("Logical type must not be blank");
    }
    return normalized;
  }

  private static ArrowType.Decimal decimalType(Integer precision, Integer scale) {
    int p = Objects.requireNonNull(precision, "DECIMAL precision must not be null");
    int s = Objects.requireNonNull(scale, "DECIMAL scale must not be null");
    if (p > 76) {
      throw new IllegalArgumentException("DECIMAL precision " + p + " exceeds Arrow max 76");
    }
    int bitWidth = p > 38 ? 256 : 128;
    return new ArrowType.Decimal(p, s, bitWidth);
  }

  public static String logicalType(Field field) {
    Objects.requireNonNull(field, "field");
    FieldType fieldType = field.getFieldType();
    if (fieldType == null) {
      throw new IllegalArgumentException("FieldType must not be null for " + field.getName());
    }
    return logicalType(fieldType.getType());
  }

  public static String logicalType(ArrowType arrowType) {
    Objects.requireNonNull(arrowType, "arrowType");
    LogicalType logical = logicalTypeFromArrowType(arrowType);
    return LogicalTypeFormat.format(logical);
  }

  private static LogicalType logicalTypeFromArrowType(ArrowType arrowType) {
    if (arrowType instanceof ArrowType.Bool) {
      return LogicalType.of(LogicalKind.BOOLEAN);
    }
    if (arrowType instanceof ArrowType.Int intType) {
      if (intType.getBitWidth() <= 16) {
        return LogicalType.of(LogicalKind.INT16);
      }
      if (intType.getBitWidth() <= 32) {
        return LogicalType.of(LogicalKind.INT32);
      }
      return LogicalType.of(LogicalKind.INT64);
    }
    if (arrowType instanceof ArrowType.FloatingPoint) {
      ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) arrowType;
      FloatingPointPrecision precision = fp.getPrecision();
      return LogicalType.of(
          precision == FloatingPointPrecision.SINGLE ? LogicalKind.FLOAT32 : LogicalKind.FLOAT64);
    }
    if (arrowType instanceof ArrowType.Decimal) {
      ArrowType.Decimal decimal = (ArrowType.Decimal) arrowType;
      return LogicalType.decimal(decimal.getPrecision(), decimal.getScale());
    }
    if (arrowType instanceof ArrowType.Utf8) {
      return LogicalType.of(LogicalKind.STRING);
    }
    if (arrowType instanceof ArrowType.Binary) {
      return LogicalType.of(LogicalKind.BINARY);
    }
    if (arrowType instanceof ArrowType.FixedSizeBinary fixed) {
      if (fixed.getByteWidth() == 16) {
        return LogicalType.of(LogicalKind.UUID);
      }
      return LogicalType.of(LogicalKind.BINARY);
    }
    if (arrowType instanceof ArrowType.Date) {
      return LogicalType.of(LogicalKind.DATE);
    }
    if (arrowType instanceof ArrowType.Time) {
      return LogicalType.of(LogicalKind.TIME);
    }
    if (arrowType instanceof ArrowType.Timestamp) {
      return LogicalType.of(LogicalKind.TIMESTAMP);
    }
    throw new IllegalArgumentException("Unsupported Arrow type: " + arrowType);
  }

  /** Normalize the user-requested projection list so it can be compared case-insensitively. */
  public static Set<String> normalizeRequiredColumns(List<String> requiredColumns) {
    if (requiredColumns.isEmpty()) {
      return Collections.emptySet();
    }
    Set<String> normalized = new HashSet<>(requiredColumns.size());
    for (String column : requiredColumns) {
      if (column == null) {
        continue;
      }
      String normalizedColumn = column.trim().toLowerCase(Locale.ROOT);
      if (normalizedColumn.isEmpty()) {
        continue;
      }
      normalized.add(normalizedColumn);
    }
    return normalized;
  }

  /** Returns {@code true} when the column should be populated given the normalized required set. */
  public static boolean shouldIncludeColumn(Set<String> normalizedRequired, String columnName) {
    String normalized = columnName.trim().toLowerCase(Locale.ROOT);
    return normalizedRequired.isEmpty() || normalizedRequired.contains(normalized);
  }
}
