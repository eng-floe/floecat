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
 * String formatting and parsing for Floecat canonical {@link LogicalType} values.
 *
 * <p>Two complementary operations:
 *
 * <ul>
 *   <li>{@link #format(LogicalType)} — converts a {@code LogicalType} to its canonical string. For
 *       non-decimal kinds this is the enum name (e.g. {@code "INT"}), optionally with a temporal
 *       precision suffix (e.g. {@code "TIMESTAMP(3)"}). INTERVAL uses ANSI-style spellings (e.g.
 *       {@code "INTERVAL YEAR TO MONTH"}, {@code "INTERVAL DAY TO SECOND(3)"}) when range or
 *       precision is present; for DECIMAL: {@code "DECIMAL(p,s)"}.
 *   <li>{@link #parse(String)} — parses a type string (canonical name, SQL alias, or parameterised
 *       form) back to a {@code LogicalType}. Case-insensitive and whitespace-normalised.
 *       Parameterised non-DECIMAL forms are accepted only for known SQL spellings where parameters
 *       are meaningful in source systems (e.g. {@code VARCHAR(10)} → {@code STRING}, {@code
 *       TIMESTAMP(6)} → {@code TIMESTAMP}, {@code INTERVAL DAY TO SECOND(3)} → {@code INTERVAL});
 *       other parameterised forms fail fast.
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

  // Generic "TYPE(params)" splitter for non-DECIMAL parameter validation.
  private static final Pattern TYPE_WITH_PARAMS_RE =
      Pattern.compile("^\\s*([A-Z0-9_ ]+?)\\s*\\(([^)]*)\\)\\s*$");

  private static final Pattern INTEGER_PARAM_RE = Pattern.compile("^\\s*\\d+\\s*$");
  private static final Pattern STRING_LEN_PARAM_RE = Pattern.compile("^\\s*(\\d+|MAX)\\s*$");
  private static final Pattern INTERVAL_SHORTHAND_RE =
      Pattern.compile("^INTERVAL\\s*\\(\\s*(\\d+)\\s*\\)$");
  private static final Pattern INTERVAL_YEAR_MONTH_RE =
      Pattern.compile("^INTERVAL\\s+YEAR(?:\\s*\\(\\s*(\\d+)\\s*\\))?\\s+TO\\s+MONTH$");
  private static final Pattern INTERVAL_DAY_SECOND_RE =
      Pattern.compile(
          "^INTERVAL\\s+DAY(?:\\s*\\(\\s*(\\d+)\\s*\\))?\\s+TO\\s+SECOND(?:\\s*\\(\\s*(\\d+)\\s*\\))?$");

  public static String format(LogicalType t) {
    Objects.requireNonNull(t, "LogicalType");
    if (t.isDecimal()) {
      return "DECIMAL(" + t.precision() + "," + t.scale() + ")";
    }
    if (t.kind() == LogicalKind.INTERVAL) {
      return formatInterval(t);
    }
    Integer temporalPrecision = t.temporalPrecision();
    if (temporalPrecision != null
        && (t.kind() == LogicalKind.TIME
            || t.kind() == LogicalKind.TIMESTAMP
            || t.kind() == LogicalKind.TIMESTAMPTZ)) {
      return t.kind().name() + "(" + temporalPrecision + ")";
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

    if ("INTERVAL".equals(normalized)) {
      return LogicalType.of(LogicalKind.INTERVAL);
    }

    Matcher intervalShorthand = INTERVAL_SHORTHAND_RE.matcher(normalized);
    if (intervalShorthand.matches()) {
      Integer fractional = parseOptionalPrecision(intervalShorthand.group(1), normalized, true);
      return LogicalType.interval(IntervalRange.DAY_TO_SECOND, null, fractional);
    }

    Matcher intervalYm = INTERVAL_YEAR_MONTH_RE.matcher(normalized);
    if (intervalYm.matches()) {
      Integer leading = parseOptionalPrecision(intervalYm.group(1), normalized, false);
      return LogicalType.interval(IntervalRange.YEAR_TO_MONTH, leading, null);
    }

    Matcher intervalDs = INTERVAL_DAY_SECOND_RE.matcher(normalized);
    if (intervalDs.matches()) {
      Integer leading = parseOptionalPrecision(intervalDs.group(1), normalized, false);
      Integer fractional = parseOptionalPrecision(intervalDs.group(2), normalized, true);
      return LogicalType.interval(IntervalRange.DAY_TO_SECOND, leading, fractional);
    }

    String baseName = normalized;
    Matcher withParams = TYPE_WITH_PARAMS_RE.matcher(normalized);
    if (withParams.matches()) {
      String candidateBase = withParams.group(1).trim();
      String params = withParams.group(2).trim();
      LogicalKind candidateKind = null;
      try {
        candidateKind = LogicalKind.fromName(candidateBase);
      } catch (IllegalArgumentException ignored) {
        // Defer unknown-type handling to the main resolution path below.
      }
      Integer temporalPrecision = parseTemporalPrecision(candidateKind, candidateBase, params);
      if (temporalPrecision != null) {
        return LogicalType.temporal(candidateKind, temporalPrecision);
      }
      validateNonDecimalParameters(s, candidateBase, params);
      baseName = candidateBase;
    } else if (normalized.indexOf('(') >= 0 || normalized.indexOf(')') >= 0) {
      throw new IllegalArgumentException("Unrecognized logical type: \"" + s + "\"");
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

  private static void validateNonDecimalParameters(String raw, String baseName, String params) {
    if (params.isEmpty()) {
      throw new IllegalArgumentException("Unrecognized logical type: \"" + raw + "\"");
    }
    switch (baseName) {
      case "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR" -> {
        if (!STRING_LEN_PARAM_RE.matcher(params).matches()) {
          throw new IllegalArgumentException(
              "Unrecognized logical type: \"" + raw + "\" (invalid string length parameter)");
        }
      }
      case "TIME", "TIMESTAMP", "TIMESTAMPTZ", "INTERVAL" -> {
        if (!INTEGER_PARAM_RE.matcher(params).matches()) {
          throw new IllegalArgumentException(
              "Unrecognized logical type: \"" + raw + "\" (invalid temporal precision parameter)");
        }
      }
      default ->
          throw new IllegalArgumentException(
              "Unrecognized logical type: \""
                  + raw
                  + "\" (type does not accept parameters: "
                  + baseName
                  + ")");
    }
  }

  private static Integer parseTemporalPrecision(
      LogicalKind candidateKind, String baseName, String params) {
    if (candidateKind == null
        || (candidateKind != LogicalKind.TIME
            && candidateKind != LogicalKind.TIMESTAMP
            && candidateKind != LogicalKind.TIMESTAMPTZ)) {
      return null;
    }
    if (!INTEGER_PARAM_RE.matcher(params).matches()) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \""
              + baseName
              + "("
              + params
              + ")\""
              + " (invalid temporal precision parameter)");
    }
    int precision = Integer.parseInt(params.trim());
    if (precision < 0 || precision > LogicalType.MAX_TEMPORAL_PRECISION) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \""
              + baseName
              + "("
              + params
              + ")\" (temporal precision must be 0.."
              + LogicalType.MAX_TEMPORAL_PRECISION
              + ")");
    }
    return precision;
  }

  private static Integer parseOptionalPrecision(
      String raw, String fullType, boolean enforceTemporalPrecision) {
    if (raw == null) {
      return null;
    }
    String trimmed = raw.trim();
    if (trimmed.isEmpty()) {
      return null;
    }
    if (!INTEGER_PARAM_RE.matcher(trimmed).matches()) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \"" + fullType + "\" (invalid precision parameter)");
    }
    int precision = Integer.parseInt(trimmed);
    if (precision < 0) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \"" + fullType + "\" (precision must be >= 0)");
    }
    if (enforceTemporalPrecision && precision > LogicalType.MAX_TEMPORAL_PRECISION) {
      throw new IllegalArgumentException(
          "Unrecognized logical type: \""
              + fullType
              + "\" (precision must be 0.."
              + LogicalType.MAX_TEMPORAL_PRECISION
              + ")");
    }
    return precision;
  }

  private static String formatInterval(LogicalType t) {
    IntervalRange range = t.intervalRange();
    if (range == null || range == IntervalRange.UNSPECIFIED) {
      return "INTERVAL";
    }
    Integer leading = t.intervalLeadingPrecision();
    Integer fractional = t.intervalFractionalPrecision();
    return switch (range) {
      case YEAR_TO_MONTH ->
          (leading == null) ? "INTERVAL YEAR TO MONTH" : "INTERVAL YEAR(" + leading + ") TO MONTH";
      case DAY_TO_SECOND -> {
        StringBuilder sb = new StringBuilder("INTERVAL DAY");
        if (leading != null) {
          sb.append('(').append(leading).append(')');
        }
        sb.append(" TO SECOND");
        if (fractional != null) {
          sb.append('(').append(fractional).append(')');
        }
        yield sb.toString();
      }
      case UNSPECIFIED -> "INTERVAL";
    };
  }
}
