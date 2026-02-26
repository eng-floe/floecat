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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Canonical logical type kinds shared across table formats (e.g. Iceberg, Delta) and SQL-facing
 * components.
 *
 * <p>This enum represents only the logical category. Type parameters (precision, scale, timezone,
 * length, etc.) are handled outside of this enum.
 *
 * <p>Every integer size from source formats (TINYINT, SMALLINT, INT, BIGINT) collapses to {@link
 * #INT} (64-bit). Complex types ({@link #ARRAY}, {@link #MAP}, {@link #STRUCT}, {@link #VARIANT})
 * are non-parameterised here; nested structure is captured by child {@code SchemaColumn} rows with
 * their own paths.
 */
public enum LogicalKind {
  // Scalar numeric
  BOOLEAN,
  INT,
  FLOAT,
  DOUBLE,
  DECIMAL,

  // Scalar string / binary
  STRING,
  BINARY,
  UUID,

  // Scalar temporal
  DATE,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  INTERVAL,

  // Semi-structured
  JSON,

  // Complex / container (non-parameterised)
  ARRAY,
  MAP,
  STRUCT,
  VARIANT;

  private static final Map<String, LogicalKind> ALIASES;

  static {
    Map<String, LogicalKind> m = new HashMap<>();

    // INT — all integer sizes from source formats collapse to 64-bit INT
    m.put("INT", INT);
    m.put("INTEGER", INT);
    m.put("BIGINT", INT);
    m.put("LONG", INT);
    m.put("SMALLINT", INT);
    m.put("TINYINT", INT);
    m.put("INT8", INT);
    m.put("INT4", INT);
    m.put("INT2", INT);
    m.put("UINT8", INT);
    m.put("UINT4", INT);
    m.put("UINT2", INT);

    // FLOAT — 32-bit IEEE-754
    m.put("FLOAT", FLOAT);
    m.put("FLOAT4", FLOAT);
    m.put("FLOAT32", FLOAT);
    m.put("REAL", FLOAT);

    // DOUBLE — 64-bit IEEE-754
    m.put("DOUBLE", DOUBLE);
    m.put("FLOAT8", DOUBLE);
    m.put("FLOAT64", DOUBLE);
    m.put("DOUBLE PRECISION", DOUBLE);

    // STRING
    m.put("STRING", STRING);
    m.put("VARCHAR", STRING);
    m.put("CHAR", STRING);
    m.put("CHARACTER", STRING);
    m.put("TEXT", STRING);
    m.put("NTEXT", STRING);
    m.put("NVARCHAR", STRING);

    // BINARY
    m.put("BINARY", BINARY);
    m.put("VARBINARY", BINARY);
    m.put("BYTEA", BINARY);
    m.put("BLOB", BINARY);
    m.put("IMAGE", BINARY);

    // BOOLEAN
    m.put("BOOLEAN", BOOLEAN);
    m.put("BOOL", BOOLEAN);
    m.put("BIT", BOOLEAN);

    // UUID
    m.put("UUID", UUID);

    // DECIMAL
    m.put("DECIMAL", DECIMAL);
    m.put("NUMERIC", DECIMAL);

    // TIMESTAMPTZ
    m.put("TIMESTAMPTZ", TIMESTAMPTZ);
    m.put("TIMESTAMP WITH TIME ZONE", TIMESTAMPTZ);

    // TIMESTAMP
    m.put("TIMESTAMP", TIMESTAMP);
    m.put("DATETIME", TIMESTAMP);

    // INTERVAL
    m.put("INTERVAL", INTERVAL);

    // JSON
    m.put("JSON", JSON);
    m.put("JSONB", JSON);

    // DATE / TIME
    m.put("DATE", DATE);
    m.put("TIME", TIME);

    // Complex types
    m.put("ARRAY", ARRAY);
    m.put("MAP", MAP);
    m.put("STRUCT", STRUCT);
    m.put("VARIANT", VARIANT);

    ALIASES = Map.copyOf(m);
  }

  /**
   * Resolves a type name (canonical or aliased) to a {@link LogicalKind}.
   *
   * <p>The lookup is case-insensitive and collapses internal whitespace, so {@code "BIGINT"},
   * {@code "bigint"}, and {@code "BigInt"} all resolve to {@link #INT}, and {@code "double
   * precision"} resolves to {@link #DOUBLE}.
   *
   * <p>Canonical enum names are tried first ({@link #INT}, {@link #FLOAT}, {@link #TIMESTAMPTZ},
   * …); if no exact match is found, the ALIASES map is consulted. The ALIASES map covers all
   * source-format synonyms documented in the Floe Data Types spec, including integer sizes ({@code
   * TINYINT} … {@code BIGINT}), float aliases ({@code FLOAT4}, {@code FLOAT32}, {@code REAL}),
   * string aliases ({@code VARCHAR}, {@code CHAR}, …), and multi-word aliases ({@code "TIMESTAMP
   * WITH TIME ZONE"}, {@code "DOUBLE PRECISION"}).
   *
   * @param candidate type name to resolve (may be null, blank, or unknown — all throw)
   * @return the matching {@link LogicalKind}
   * @throws IllegalArgumentException if {@code candidate} is null, blank, or not recognised
   */
  public static LogicalKind fromName(String candidate) {
    if (candidate == null) {
      throw new IllegalArgumentException("Logical kind must not be null");
    }

    String normalized = normalize(candidate);

    // First try exact enum match (INT, FLOAT, DOUBLE, TIMESTAMPTZ, ...)
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
