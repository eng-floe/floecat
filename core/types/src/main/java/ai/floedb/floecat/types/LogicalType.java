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

/**
 * Immutable representation of a FloeCat canonical logical type.
 *
 * <p>A {@code LogicalType} carries a {@link LogicalKind} plus optional {@code precision} and {@code
 * scale} parameters. Only {@link LogicalKind#DECIMAL} uses precision/scale; all other kinds reject
 * them. Precision/scale are absent (null) for non-decimal kinds.
 *
 * <p><b>DECIMAL precision contract:</b> {@code LogicalType} enforces only {@code precision ≥ 1} and
 * {@code 0 ≤ scale ≤ precision}. No upper bound on precision is enforced here — that is a
 * connector-layer concern. For example, Iceberg and Delta naturally produce max precision 38;
 * FloeDB's execution engine imposes a hard stop at 38 via {@code FloeTypeMapper}; BigQuery supports
 * up to 76. Connectors must enforce their own format ceiling while preserving this canonical
 * contract.
 *
 * <p><b>Complex types</b> ({@link LogicalKind#ARRAY}, {@link LogicalKind#MAP}, {@link
 * LogicalKind#STRUCT}, {@link LogicalKind#VARIANT}) carry no meaningful min/max statistics. Use
 * {@link #isComplex()} to detect them before attempting stat coercions.
 *
 * <p>Factory methods:
 *
 * <ul>
 *   <li>{@link #of(LogicalKind)} — for all non-decimal kinds
 *   <li>{@link #decimal(int, int)} — for DECIMAL
 * </ul>
 *
 * @see LogicalKind
 */
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

  /**
   * Creates a non-decimal logical type.
   *
   * @param kind the logical kind (must not be {@link LogicalKind#DECIMAL})
   * @return a new {@code LogicalType} with no precision or scale
   * @throws IllegalArgumentException if {@code kind} is DECIMAL (use {@link #decimal} instead)
   */
  public static LogicalType of(LogicalKind kind) {
    return new LogicalType(kind, null, null);
  }

  /**
   * Creates a DECIMAL logical type with the given precision and scale.
   *
   * @param precision number of significant decimal digits; must be ≥ 1
   * @param scale number of digits after the decimal point; must be ≥ 0 and ≤ precision
   * @return a new DECIMAL {@code LogicalType}
   * @throws IllegalArgumentException if the precision/scale constraints are violated
   */
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
