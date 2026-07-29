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
 * <p><b>Nested type grammar:</b> parameterised complex types use angle brackets:
 *
 * <pre>{@code
 * type   ::= scalar
 *          | ARRAY "<" type ">"
 *          | STRUCT "<" field ("," field)* ">"
 *          | MAP "<" type "," type ">"
 * field  ::= name ":" type
 * }</pre>
 *
 * e.g. {@code ARRAY<INT>}, {@code MAP<STRING, DOUBLE>}, {@code ARRAY<STRUCT<sku: STRING,
 * quantities: ARRAY<INT>>>}. Struct field names are case-preserved; names that are not simple
 * identifiers are double-quoted with {@code ""} escaping. Bare {@code ARRAY}/{@code MAP}/{@code
 * STRUCT} (no arguments) remain valid and parse to the legacy non-parameterised container tag.
 * Nullability of elements/values/fields is not part of the grammar; parsing defaults it to
 * nullable.
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

  private static final Pattern BARE_FIELD_NAME_RE = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  private static final Pattern NESTED_START_RE =
      Pattern.compile("^(ARRAY|MAP|STRUCT)\\s*<", Pattern.CASE_INSENSITIVE);

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
    if (t.hasTypeTree()) {
      return switch (t.kind()) {
        case ARRAY -> "ARRAY<" + format(t.element()) + ">";
        case MAP -> "MAP<" + format(t.key()) + ", " + format(t.value()) + ">";
        case STRUCT -> {
          StringBuilder sb = new StringBuilder("STRUCT<");
          for (int i = 0; i < t.fields().size(); i++) {
            LogicalField f = t.fields().get(i);
            if (i > 0) {
              sb.append(", ");
            }
            sb.append(formatFieldName(f.name())).append(": ").append(format(f.type()));
          }
          yield sb.append('>').toString();
        }
        default -> throw new IllegalStateException("type tree on non-container kind: " + t.kind());
      };
    }
    return t.kind().name();
  }

  /**
   * Formats only the container tag for complex types ({@code "ARRAY"}, {@code "MAP"}, {@code
   * "STRUCT"}, {@code "VARIANT"}), regardless of any nested type tree; scalar types format exactly
   * as {@link #format(LogicalType)}. This is the legacy flat spelling used where non-parameterised
   * consumers still read the type string.
   */
  public static String formatTag(LogicalType t) {
    Objects.requireNonNull(t, "LogicalType");
    return t.isComplex() ? t.kind().name() : format(t);
  }

  private static String formatFieldName(String name) {
    if (BARE_FIELD_NAME_RE.matcher(name).matches()) {
      return name;
    }
    return '"' + name.replace("\"", "\"\"") + '"';
  }

  public static LogicalType parse(String s) {
    Objects.requireNonNull(s, "logical type string");
    String trimmed = s.trim();
    if (trimmed.isEmpty()) {
      throw new IllegalArgumentException("Unrecognized logical type: \"\" ");
    }
    if (NESTED_START_RE.matcher(trimmed).find()) {
      Cursor c = new Cursor(trimmed, s);
      LogicalType t = parseTree(c);
      c.skipWhitespace();
      if (!c.atEnd()) {
        throw c.unrecognized("trailing characters after type");
      }
      return t;
    }
    return parseScalar(s);
  }

  private static LogicalType parseScalar(String s) {
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

  // ---------------------------------------------------------------------
  // Recursive descent parsing for the nested type grammar
  // ---------------------------------------------------------------------

  /** Mutable position over the original (case-preserved) input. */
  private static final class Cursor {
    final String src;
    final String raw;
    int pos;

    Cursor(String src, String raw) {
      this.src = src;
      this.raw = raw;
    }

    void skipWhitespace() {
      while (pos < src.length() && Character.isWhitespace(src.charAt(pos))) {
        pos++;
      }
    }

    boolean atEnd() {
      return pos >= src.length();
    }

    char peek() {
      if (atEnd()) {
        throw unrecognized("unexpected end of type");
      }
      return src.charAt(pos);
    }

    void expect(char c) {
      skipWhitespace();
      if (atEnd() || src.charAt(pos) != c) {
        throw unrecognized("expected '" + c + "' at position " + pos);
      }
      pos++;
    }

    IllegalArgumentException unrecognized(String detail) {
      return new IllegalArgumentException(
          "Unrecognized logical type: \"" + raw + "\" (" + detail + ")");
    }
  }

  private static LogicalType parseTree(Cursor c) {
    c.skipWhitespace();
    int start = c.pos;

    // Try to read an identifier and see if it opens a container argument list.
    int identEnd = start;
    while (identEnd < c.src.length() && Character.isLetter(c.src.charAt(identEnd))) {
      identEnd++;
    }
    String ident = c.src.substring(start, identEnd).toUpperCase(Locale.ROOT);
    int afterIdent = identEnd;
    while (afterIdent < c.src.length() && Character.isWhitespace(c.src.charAt(afterIdent))) {
      afterIdent++;
    }
    boolean opensArgs = afterIdent < c.src.length() && c.src.charAt(afterIdent) == '<';

    if (opensArgs) {
      switch (ident) {
        case "ARRAY" -> {
          c.pos = afterIdent + 1;
          LogicalType element = parseTree(c);
          c.expect('>');
          return LogicalType.array(element, true);
        }
        case "MAP" -> {
          c.pos = afterIdent + 1;
          LogicalType key = parseTree(c);
          c.expect(',');
          LogicalType value = parseTree(c);
          c.expect('>');
          return LogicalType.map(key, value, true);
        }
        case "STRUCT" -> {
          c.pos = afterIdent + 1;
          java.util.List<LogicalField> fields = new java.util.ArrayList<>();
          while (true) {
            String name = parseFieldName(c);
            c.expect(':');
            LogicalType type = parseTree(c);
            fields.add(new LogicalField(name, true, type));
            c.skipWhitespace();
            if (!c.atEnd() && c.peek() == ',') {
              c.pos++;
              continue;
            }
            break;
          }
          c.expect('>');
          return LogicalType.struct(fields);
        }
        default -> throw c.unrecognized("unknown container type: " + ident);
      }
    }

    // Scalar (or bare container tag): consume until a top-level ',' or '>', respecting parens.
    int parenDepth = 0;
    int end = c.pos;
    while (end < c.src.length()) {
      char ch = c.src.charAt(end);
      if (ch == '(') {
        parenDepth++;
      } else if (ch == ')') {
        parenDepth--;
      } else if (parenDepth == 0 && (ch == ',' || ch == '>' || ch == '<')) {
        break;
      }
      end++;
    }
    String segment = c.src.substring(c.pos, end).trim();
    if (segment.isEmpty()) {
      throw c.unrecognized("missing type at position " + c.pos);
    }
    c.pos = end;
    return parseScalar(segment);
  }

  private static String parseFieldName(Cursor c) {
    c.skipWhitespace();
    if (c.atEnd()) {
      throw c.unrecognized("missing struct field name");
    }
    if (c.peek() == '"') {
      c.pos++;
      StringBuilder sb = new StringBuilder();
      while (true) {
        if (c.atEnd()) {
          throw c.unrecognized("unterminated quoted field name");
        }
        char ch = c.src.charAt(c.pos);
        if (ch == '"') {
          if (c.pos + 1 < c.src.length() && c.src.charAt(c.pos + 1) == '"') {
            sb.append('"');
            c.pos += 2;
            continue;
          }
          c.pos++;
          break;
        }
        sb.append(ch);
        c.pos++;
      }
      if (sb.isEmpty()) {
        throw c.unrecognized("empty struct field name");
      }
      return sb.toString();
    }
    int start = c.pos;
    while (c.pos < c.src.length()) {
      char ch = c.src.charAt(c.pos);
      if (ch == ':' || ch == ',' || ch == '<' || ch == '>' || ch == '"') {
        break;
      }
      c.pos++;
    }
    String name = c.src.substring(start, c.pos).trim();
    if (name.isEmpty()) {
      throw c.unrecognized("missing struct field name at position " + start);
    }
    return name;
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
