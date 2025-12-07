package ai.floedb.floecat.types;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class LogicalTypeFormat {
  private static final Pattern DECIMAL_RE =
      Pattern.compile(
          "^\\s*DECIMAL\\s*\\(\\s*(\\d+)\\s*,\\s*(\\d+)\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);

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

    Matcher m = DECIMAL_RE.matcher(trimmed);
    if (m.matches()) {
      int p = Integer.parseInt(m.group(1));
      int sc = Integer.parseInt(m.group(2));
      return LogicalType.decimal(p, sc);
    }

    String upper = trimmed.toUpperCase(Locale.ROOT);
    try {
      LogicalKind k = LogicalKind.valueOf(upper);
      return LogicalType.of(k);
    } catch (IllegalArgumentException ignore) {
      // ignore
    }

    throw new IllegalArgumentException("Unrecognized logical type: \"" + s + "\"");
  }
}
