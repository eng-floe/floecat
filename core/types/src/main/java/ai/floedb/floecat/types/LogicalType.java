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
 * <p>A {@code LogicalType} carries a {@link LogicalKind} plus optional {@code precision}/{@code
 * scale} (for DECIMAL), {@code temporalPrecision} (for TIME/TIMESTAMP/TIMESTAMPTZ), or interval
 * range/precision fields (for INTERVAL). Other kinds reject parameters. Parameters are absent
 * (null) for kinds that do not accept them.
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
 *   <li>{@link #of(LogicalKind)} — for all non-decimal kinds without temporal precision
 *   <li>{@link #temporal(LogicalKind, Integer)} — for TIME/TIMESTAMP/TIMESTAMPTZ with precision
 *   <li>{@link #interval(IntervalRange, Integer, Integer)} — for INTERVAL with optional range and
 *       precisions
 *   <li>{@link #decimal(int, int)} — for DECIMAL
 * </ul>
 *
 * @see LogicalKind
 */
public final class LogicalType {
  public static final int DEFAULT_TEMPORAL_PRECISION = 6;
  public static final int MAX_TEMPORAL_PRECISION = 6;

  public final LogicalKind kind;
  public final Integer precision;
  public final Integer scale;
  public final Integer temporalPrecision;
  public final IntervalRange intervalRange;
  public final Integer intervalLeadingPrecision;
  public final Integer intervalFractionalPrecision;

  private LogicalType(
      LogicalKind kind,
      Integer precision,
      Integer scale,
      Integer temporalPrecision,
      IntervalRange intervalRange,
      Integer intervalLeadingPrecision,
      Integer intervalFractionalPrecision) {
    this.kind = Objects.requireNonNull(kind, "kind");
    IntervalRange normalizedIntervalRange = intervalRange;
    if (kind == LogicalKind.DECIMAL) {
      if (precision == null || scale == null || precision < 1 || scale < 0 || scale > precision) {
        throw new IllegalArgumentException(
            "Invalid DECIMAL(precision, scale): "
                + precision
                + ", "
                + scale
                + " (precision must be ≥ 1, scale must be 0–precision)");
      }
      if (temporalPrecision != null) {
        throw new IllegalArgumentException("temporal precision is not allowed for DECIMAL");
      }
      if (intervalRange != null
          || intervalLeadingPrecision != null
          || intervalFractionalPrecision != null) {
        throw new IllegalArgumentException("interval parameters are not allowed for DECIMAL");
      }
    } else if (kind == LogicalKind.TIME
        || kind == LogicalKind.TIMESTAMP
        || kind == LogicalKind.TIMESTAMPTZ) {
      if (precision != null || scale != null) {
        throw new IllegalArgumentException("precision/scale only allowed for DECIMAL");
      }
      if (temporalPrecision != null
          && (temporalPrecision < 0 || temporalPrecision > MAX_TEMPORAL_PRECISION)) {
        throw new IllegalArgumentException(
            "Invalid temporal precision: "
                + temporalPrecision
                + " (must be between 0 and "
                + MAX_TEMPORAL_PRECISION
                + ")");
      }
      if (intervalRange != null
          || intervalLeadingPrecision != null
          || intervalFractionalPrecision != null) {
        throw new IllegalArgumentException(
            "interval parameters are not allowed for " + kind.name());
      }
    } else if (kind == LogicalKind.INTERVAL) {
      if (normalizedIntervalRange == null) {
        normalizedIntervalRange = IntervalRange.UNSPECIFIED;
      }
      if (precision != null || scale != null || temporalPrecision != null) {
        throw new IllegalArgumentException(
            "precision, scale, and temporalPrecision are not supported for INTERVAL");
      }
      if (intervalLeadingPrecision != null && intervalLeadingPrecision < 0) {
        throw new IllegalArgumentException(
            "Invalid interval leading precision: " + intervalLeadingPrecision);
      }
      if (intervalFractionalPrecision != null
          && (intervalFractionalPrecision < 0
              || intervalFractionalPrecision > MAX_TEMPORAL_PRECISION)) {
        throw new IllegalArgumentException(
            "Invalid interval fractional precision: "
                + intervalFractionalPrecision
                + " (must be between 0 and "
                + MAX_TEMPORAL_PRECISION
                + ")");
      }
      if ((intervalLeadingPrecision != null || intervalFractionalPrecision != null)
          && normalizedIntervalRange == IntervalRange.UNSPECIFIED) {
        throw new IllegalArgumentException(
            "Interval precision requires an explicit interval range");
      }
      if (normalizedIntervalRange == IntervalRange.YEAR_TO_MONTH
          && intervalFractionalPrecision != null) {
        throw new IllegalArgumentException(
            "Interval fractional precision is not allowed for YEAR TO MONTH");
      }
    } else {
      if (precision != null
          || scale != null
          || temporalPrecision != null
          || intervalRange != null
          || intervalLeadingPrecision != null
          || intervalFractionalPrecision != null) {
        throw new IllegalArgumentException("type parameters are not supported for " + kind.name());
      }
    }
    this.precision = precision;
    this.scale = scale;
    this.temporalPrecision = temporalPrecision;
    this.intervalRange = normalizedIntervalRange;
    this.intervalLeadingPrecision = intervalLeadingPrecision;
    this.intervalFractionalPrecision = intervalFractionalPrecision;
  }

  /**
   * Creates a non-decimal logical type.
   *
   * @param kind the logical kind (must not be {@link LogicalKind#DECIMAL})
   * @return a new {@code LogicalType} with no precision or scale
   * @throws IllegalArgumentException if {@code kind} is DECIMAL (use {@link #decimal} instead)
   */
  public static LogicalType of(LogicalKind kind) {
    if (kind == LogicalKind.INTERVAL) {
      return interval(IntervalRange.UNSPECIFIED);
    }
    return new LogicalType(kind, null, null, null, null, null, null);
  }

  /**
   * Creates a temporal logical type with an optional precision (fractional seconds digits).
   *
   * @param kind TIME, TIMESTAMP, or TIMESTAMPTZ
   * @param temporalPrecision fractional second precision (0..6) or null if unspecified
   * @return a new temporal {@code LogicalType}
   * @throws IllegalArgumentException if the kind is not temporal or precision is out of range
   */
  public static LogicalType temporal(LogicalKind kind, Integer temporalPrecision) {
    return new LogicalType(kind, null, null, temporalPrecision, null, null, null);
  }

  /**
   * Creates an INTERVAL logical type with optional range and precisions.
   *
   * @param range interval range (null means UNSPECIFIED)
   * @param leadingPrecision leading field precision (optional)
   * @param fractionalPrecision fractional seconds precision (optional)
   * @return a new INTERVAL {@code LogicalType}
   */
  public static LogicalType interval(
      IntervalRange range, Integer leadingPrecision, Integer fractionalPrecision) {
    return new LogicalType(
        LogicalKind.INTERVAL, null, null, null, range, leadingPrecision, fractionalPrecision);
  }

  /** Convenience overload for an INTERVAL with just a range. */
  public static LogicalType interval(IntervalRange range) {
    return interval(range, null, null);
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
    return new LogicalType(LogicalKind.DECIMAL, precision, scale, null, null, null, null);
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

  public Integer temporalPrecision() {
    return temporalPrecision;
  }

  public IntervalRange intervalRange() {
    return intervalRange;
  }

  public Integer intervalLeadingPrecision() {
    return intervalLeadingPrecision;
  }

  public Integer intervalFractionalPrecision() {
    return intervalFractionalPrecision;
  }

  public int temporalPrecisionOrDefault() {
    return temporalPrecision == null ? DEFAULT_TEMPORAL_PRECISION : temporalPrecision;
  }

  /** Returns true iff this is a DECIMAL type. */
  public boolean isDecimal() {
    return kind == LogicalKind.DECIMAL;
  }

  /** Returns true iff this is a numeric type: INT, FLOAT, DOUBLE, or DECIMAL. */
  public boolean isNumeric() {
    return kind.isNumeric();
  }

  /** Returns true iff this is a temporal type: DATE, TIME, TIMESTAMP, TIMESTAMPTZ, or INTERVAL. */
  public boolean isTemporal() {
    return kind.isTemporal();
  }

  /** Returns true iff this is a complex/container type: ARRAY, MAP, STRUCT, or VARIANT. */
  public boolean isComplex() {
    return kind.isComplex();
  }

  /** Returns true iff this is not a complex type (i.e., a scalar or semi-structured type). */
  public boolean isScalar() {
    return kind.isScalar();
  }

  @Override
  public String toString() {
    return LogicalTypeFormat.format(this);
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
        && Objects.equals(scale, that.scale)
        && Objects.equals(temporalPrecision, that.temporalPrecision)
        && intervalRange == that.intervalRange
        && Objects.equals(intervalLeadingPrecision, that.intervalLeadingPrecision)
        && Objects.equals(intervalFractionalPrecision, that.intervalFractionalPrecision);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        kind,
        precision,
        scale,
        temporalPrecision,
        intervalRange,
        intervalLeadingPrecision,
        intervalFractionalPrecision);
  }
}
