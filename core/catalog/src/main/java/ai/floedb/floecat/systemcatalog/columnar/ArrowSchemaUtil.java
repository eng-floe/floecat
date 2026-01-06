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

package ai.floedb.floecat.systemcatalog.columnar;

import ai.floedb.floecat.query.rpc.SchemaColumn;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.apache.arrow.vector.types.FloatingPointPrecision;
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
    if (logicalType == null) {
      return new ArrowType.Utf8();
    }
    String normalized = logicalType.toUpperCase(Locale.ROOT).trim();
    if (normalized.endsWith("[]")) {
      return new ArrowType.Utf8();
    }
    return switch (normalized) {
      case "INT", "INTEGER" -> new ArrowType.Int(32, true);
      case "BIGINT" -> new ArrowType.Int(64, true);
      case "SMALLINT" -> new ArrowType.Int(16, true);
      case "FLOAT", "FLOAT4", "REAL" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "DOUBLE", "FLOAT8" -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "BOOLEAN", "BOOL" -> ArrowType.Bool.INSTANCE;
      default -> new ArrowType.Utf8();
    };
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
