package ai.floedb.floecat.types;

import java.util.Objects;

public final class LogicalType {
  public final LogicalKind kind;
  public final Integer precision;
  public final Integer scale;

  private LogicalType(LogicalKind kind, Integer precision, Integer scale) {
    this.kind = Objects.requireNonNull(kind, "kind");
    if (kind == LogicalKind.DECIMAL) {
      if (precision == null || scale == null || precision < 1 || scale < 0 || scale > precision) {
        throw new IllegalArgumentException(
            "Invalid DECIMAL(precision, scale): " + precision + ", " + scale);
      }
    } else {
      if (precision != null || scale != null) {
        throw new IllegalArgumentException("precision/scale only allowed for DECIMAL");
      }
    }
    this.precision = precision;
    this.scale = scale;
  }

  public static LogicalType of(LogicalKind kind) {
    return new LogicalType(kind, null, null);
  }

  public static LogicalType decimal(int precision, int scale) {
    return new LogicalType(LogicalKind.DECIMAL, precision, scale);
  }

  public LogicalKind kind() {
    return kind;
  }

  public Integer precision() {
    return precision;
  }

  public Integer scale() {
    return scale;
  }

  public boolean isDecimal() {
    return kind == LogicalKind.DECIMAL;
  }

  @Override
  public String toString() {
    return isDecimal() ? "DECIMAL(" + precision + "," + scale + ")" : kind.name();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof LogicalType that)) {
      return false;
    }

    return kind == that.kind
        && Objects.equals(precision, that.precision)
        && Objects.equals(scale, that.scale);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kind, precision, scale);
  }
}
