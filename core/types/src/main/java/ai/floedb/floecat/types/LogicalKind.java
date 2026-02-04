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

package ai.floedb.floecat.types;

import java.util.Locale;
import java.util.Map;

/**
 * Canonical logical type kinds shared across table formats (e.g. Iceberg, Delta) and SQL-facing
 * components.
 *
 * <p>This enum represents only the logical category. Type parameters (precision, scale, timezone,
 * length, etc.) are handled outside of this enum.
 */
public enum LogicalKind {
  BOOLEAN,
  INT16,
  INT32,
  INT64,
  FLOAT32,
  FLOAT64,
  DATE,
  TIME,
  TIMESTAMP,
  STRING,
  BINARY,
  UUID,
  DECIMAL;

  private static final Map<String, LogicalKind> ALIASES =
      Map.ofEntries(
          Map.entry("INTEGER", INT32),
          Map.entry("INT", INT32),
          Map.entry("SMALLINT", INT16),
          Map.entry("BIGINT", INT64),
          Map.entry("FLOAT", FLOAT32),
          Map.entry("REAL", FLOAT32),
          Map.entry("DOUBLE", FLOAT64),
          Map.entry("BOOLEAN", BOOLEAN),
          Map.entry("BOOL", BOOLEAN),
          Map.entry("VARCHAR", STRING),
          Map.entry("CHAR", STRING),
          Map.entry("CHARACTER", STRING),
          Map.entry("TEXT", STRING),
          Map.entry("STRING", STRING),
          Map.entry("VARBINARY", BINARY),
          Map.entry("BINARY", BINARY),
          Map.entry("UUID", UUID),
          Map.entry("DECIMAL", DECIMAL),
          Map.entry("NUMERIC", DECIMAL));

  public static LogicalKind fromName(String candidate) {
    if (candidate == null) {
      throw new IllegalArgumentException("Logical kind must not be null");
    }

    String normalized = normalize(candidate);

    // First try exact enum match (INT32, FLOAT64, ...)
    try {
      return LogicalKind.valueOf(normalized);
    } catch (IllegalArgumentException ignore) {
      // fall through
    }

    LogicalKind alias = ALIASES.get(normalized);
    if (alias != null) {
      return alias;
    }

    throw new IllegalArgumentException("Unknown logical kind: " + candidate);
  }

  private static String normalize(String s) {
    String out = s.trim().toUpperCase(Locale.ROOT);
    if (out.isEmpty()) {
      throw new IllegalArgumentException("Logical kind must not be blank");
    }
    // Collapse internal whitespace so inputs like "double   precision" normalize sensibly
    return out.replaceAll("\\s+", " ");
  }
}
