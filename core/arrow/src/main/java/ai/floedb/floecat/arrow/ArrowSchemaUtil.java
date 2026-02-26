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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Utilities for converting {@link SchemaColumn} metadata into Arrow schemas. */
public final class ArrowSchemaUtil {

  private static final Pattern DECIMAL_TYPE =
      Pattern.compile("^(DECIMAL|NUMERIC)\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)$");

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
      throw new IllegalArgumentException("Logical type must not be null");
    }
    String normalized = logicalType.toUpperCase(Locale.ROOT).trim().replaceAll("\\s+", " ");
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException("Logical type must not be blank");
    }
    Matcher decimalMatcher = DECIMAL_TYPE.matcher(normalized);
    if (decimalMatcher.matches()) {
      int precision = Integer.parseInt(decimalMatcher.group(2));
      int scale = Integer.parseInt(decimalMatcher.group(3));
      return new ArrowType.Decimal(precision, scale, 128);
    }
    if ("DECIMAL".equals(normalized) || "NUMERIC".equals(normalized)) {
      return new ArrowType.Decimal(38, 0, 128);
    }
    if (normalized.endsWith("[]")) {
      return new ArrowType.Utf8();
    }
    if ("INTERVAL".equals(normalized) || normalized.startsWith("INTERVAL ")) {
      return new ArrowType.Interval(IntervalUnit.DAY_TIME);
    }
    return switch (normalized) {
      case "INT",
          "INTEGER",
          "BIGINT",
          "LONG",
          "SMALLINT",
          "TINYINT",
          "INT8",
          "INT4",
          "INT2",
          "UINT8",
          "UINT4",
          "UINT2" ->
          new ArrowType.Int(64, true);
      case "FLOAT", "FLOAT4", "REAL" -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case "DOUBLE", "FLOAT8", "FLOAT64", "DOUBLE PRECISION" ->
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case "BOOLEAN", "BOOL" -> ArrowType.Bool.INSTANCE;
      case "STRING", "VARCHAR", "CHAR", "CHARACTER", "TEXT", "NTEXT", "NVARCHAR" ->
          new ArrowType.Utf8();
      case "DATE" -> new ArrowType.Date(DateUnit.DAY);
      case "TIME" -> new ArrowType.Time(TimeUnit.MICROSECOND, 64);
      case "TIMESTAMP", "DATETIME", "TIMESTAMP_NTZ" ->
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
      case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE" ->
          new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC");
      case "UUID" -> new ArrowType.FixedSizeBinary(16);
      case "BINARY", "VARBINARY", "BYTEA", "BLOB", "IMAGE" -> new ArrowType.Binary();
      case "JSON", "JSONB", "ARRAY", "MAP", "STRUCT", "VARIANT" -> new ArrowType.Binary();
      default ->
          throw new IllegalArgumentException(
              "Unsupported logical type for Arrow schema mapping: " + logicalType);
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
