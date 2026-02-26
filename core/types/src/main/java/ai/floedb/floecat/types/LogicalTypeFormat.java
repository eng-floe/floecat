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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * String formatting and parsing for FloeCat canonical {@link LogicalType} values.
 *
 * <p>Two complementary operations:
 *
 * <ul>
 *   <li>{@link #format(LogicalType)} — converts a {@code LogicalType} to its canonical string. For
 *       non-decimal kinds this is the enum name (e.g. {@code "INT"}); for DECIMAL: {@code
 *       "DECIMAL(p,s)"}.
 *   <li>{@link #parse(String)} — parses a type string (canonical name, SQL alias, or parameterised
 *       form) back to a {@code LogicalType}. Case-insensitive, collapses internal whitespace,
 *       strips optional length parameters from non-DECIMAL types (e.g. {@code VARCHAR(10) →
 *       STRING}), and resolves aliases via {@link LogicalKind#fromName(String)}.
 * </ul>
 *
 * <p><b>DECIMAL special case:</b> A bare {@code DECIMAL} or {@code NUMERIC} without explicit
 * precision and scale parameters is rejected by {@link #parse} — both precision and scale are
 * required by {@link LogicalType#decimal(int, int)}.
 *
 * @see LogicalType
 * @see LogicalKind
 */
public final class LogicalTypeFormat {
  // Accept common SQL spellings for decimals (e.g., DECIMAL(10,2), NUMERIC(10,2)).
  private static final Pattern DECIMAL_RE =
      Pattern.compile(
          "^\\s*(DECIMAL|NUMERIC)\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)\\s*$",
          Pattern.CASE_INSENSITIVE);

  // Strip optional type parameters like VARCHAR(10), TIMESTAMP(6), DECIMAL(10,2).
  // Keeps the base type name (including spaces, e.g., "DOUBLE PRECISION").
  private static final Pattern BASE_TYPE_WITH_PARAMS_RE =
      Pattern.compile("^\\s*([A-Z0-9_ ]+?)\\s*(?:\\(.*\\))?\\s*$");

  public static String format(LogicalType t) {
    Objects.requireNonNull(t, "LogicalType");
    if (t.isDecimal()) {
      return "DECIMAL(" + t.precision() + "," + t.scale() + ")";
    }
    return t.kind().name();
  }

  public static LogicalType parse(String s) {
    Objects.requireNonNull(s, "logical type string");
    String trimmed = s.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Unrecognized logical type: \"\" ");
    }

    // Normalize: uppercase + collapse internal whitespace so inputs like "double   precision" work.
    String normalized = trimmed.toUpperCase(Locale.ROOT).replaceAll("\\s+", " ");

    // First, handle DECIMAL/NUMERIC with precision+scale (LogicalType needs parameters).
    Matcher m = DECIMAL_RE.matcher(normalized);
    if (m.matches()) {
      int p = Integer.parseInt(m.group(2));
      int sc = Integer.parseInt(m.group(3));
      return LogicalType.decimal(p, sc);
    }

    // For all other types, strip optional parameters like VARCHAR(10), TIMESTAMP(6), etc.
    Matcher base = BASE_TYPE_WITH_PARAMS_RE.matcher(normalized);
    String baseName = normalized;
    if (base.matches()) {
      baseName = base.group(1).trim();
    }

    LogicalKind k;
    try {
      k = LogicalKind.fromName(baseName);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Unrecognized logical type: \"" + s + "\"", e);
    }

    // A bare DECIMAL without parameters is not a fully specified logical type in this model.
    if (k == LogicalKind.DECIMAL) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \"" + s + "\" (DECIMAL requires precision and scale)");
    }

    return LogicalType.of(k);
  }
}
