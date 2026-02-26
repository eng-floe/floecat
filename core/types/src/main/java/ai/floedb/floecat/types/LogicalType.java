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
            "Invalid DECIMAL(precision, scale): "
                + precision
                + ", "
                + scale
                + " (precision must be ≥ 1, scale must be 0–precision)");
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

  /** Returns true iff this is a DECIMAL type. */
  public boolean isDecimal() {
    return kind == LogicalKind.DECIMAL;
  }

  /** Returns true iff this is a numeric type: INT, FLOAT, DOUBLE, or DECIMAL. */
  public boolean isNumeric() {
    return kind == LogicalKind.INT
        || kind == LogicalKind.FLOAT
        || kind == LogicalKind.DOUBLE
        || kind == LogicalKind.DECIMAL;
  }

  /** Returns true iff this is a temporal type: DATE, TIME, TIMESTAMP, TIMESTAMPTZ, or INTERVAL. */
  public boolean isTemporal() {
    return kind == LogicalKind.DATE
        || kind == LogicalKind.TIME
        || kind == LogicalKind.TIMESTAMP
        || kind == LogicalKind.TIMESTAMPTZ
        || kind == LogicalKind.INTERVAL;
  }

  /** Returns true iff this is a complex/container type: ARRAY, MAP, STRUCT, or VARIANT. */
  public boolean isComplex() {
    return kind == LogicalKind.ARRAY
        || kind == LogicalKind.MAP
        || kind == LogicalKind.STRUCT
        || kind == LogicalKind.VARIANT;
  }

  /** Returns true iff this is not a complex type (i.e., a scalar or semi-structured type). */
  public boolean isScalar() {
    return !isComplex();
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
