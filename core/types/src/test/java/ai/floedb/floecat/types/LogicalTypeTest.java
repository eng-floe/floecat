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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link LogicalType}.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>DECIMAL factory guards (precision ≥ 1, scale 0–precision; upper bound is connector-level)
 *   <li>Type-family helpers: {@code isNumeric()}, {@code isTemporal()}, {@code isComplex()}, {@code
 *       isScalar()}
 *   <li>{@code toString()} format
 *   <li>{@code equals} / {@code hashCode} consistency
 * </ul>
 */
class LogicalTypeTest {

  // ---------------------------------------------------------------------------
  // DECIMAL factory — valid bounds
  // ---------------------------------------------------------------------------

  @Test
  void decimalMinPrecisionSucceeds() {
    LogicalType t = LogicalType.decimal(1, 0);
    assertThat(t.kind()).isEqualTo(LogicalKind.DECIMAL);
    assertThat(t.precision()).isEqualTo(1);
    assertThat(t.scale()).isEqualTo(0);
  }

  @Test
  void decimalIcebergMaxPrecisionSucceeds() {
    // 38 is the Iceberg/Delta/Decimal128 ceiling — still valid in LogicalType
    LogicalType t = LogicalType.decimal(38, 38);
    assertThat(t.precision()).isEqualTo(38);
    assertThat(t.scale()).isEqualTo(38);
  }

  @Test
  void decimalAbove38PrecisionSucceeds() {
    // LogicalType imposes no upper bound; precision > 38 is valid for sources
    // like PostgreSQL numeric(100,10) or BigQuery BIGNUMERIC. Connector-layer
    // validation is responsible for enforcing format-specific ceilings.
    LogicalType t = LogicalType.decimal(76, 10);
    assertThat(t.precision()).isEqualTo(76);
    assertThat(t.scale()).isEqualTo(10);
  }

  @Test
  void decimalTypicalValueSucceeds() {
    LogicalType t = LogicalType.decimal(10, 2);
    assertThat(t.precision()).isEqualTo(10);
    assertThat(t.scale()).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------
  // DECIMAL factory — invalid bounds
  // ---------------------------------------------------------------------------

  @Test
  void decimalPrecisionZeroThrows() {
    assertThatThrownBy(() -> LogicalType.decimal(0, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decimalNegativePrecisionThrows() {
    assertThatThrownBy(() -> LogicalType.decimal(-1, 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decimalNegativeScaleThrows() {
    assertThatThrownBy(() -> LogicalType.decimal(10, -1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void decimalScaleExceedsPrecisionThrows() {
    assertThatThrownBy(() -> LogicalType.decimal(5, 6))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // isNumeric()
  // ---------------------------------------------------------------------------

  @Test
  void isNumericTrueForNumericKinds() {
    assertThat(LogicalType.of(LogicalKind.INT).isNumeric()).isTrue();
    assertThat(LogicalType.of(LogicalKind.FLOAT).isNumeric()).isTrue();
    assertThat(LogicalType.of(LogicalKind.DOUBLE).isNumeric()).isTrue();
    assertThat(LogicalType.decimal(10, 2).isNumeric()).isTrue();
  }

  @Test
  void isNumericFalseForNonNumericKinds() {
    assertThat(LogicalType.of(LogicalKind.STRING).isNumeric()).isFalse();
    assertThat(LogicalType.of(LogicalKind.BOOLEAN).isNumeric()).isFalse();
    assertThat(LogicalType.of(LogicalKind.TIMESTAMP).isNumeric()).isFalse();
    assertThat(LogicalType.of(LogicalKind.ARRAY).isNumeric()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // isTemporal()
  // ---------------------------------------------------------------------------

  @Test
  void isTemporalTrueForTemporalKinds() {
    assertThat(LogicalType.of(LogicalKind.DATE).isTemporal()).isTrue();
    assertThat(LogicalType.of(LogicalKind.TIME).isTemporal()).isTrue();
    assertThat(LogicalType.of(LogicalKind.TIMESTAMP).isTemporal()).isTrue();
    assertThat(LogicalType.of(LogicalKind.TIMESTAMPTZ).isTemporal()).isTrue();
    assertThat(LogicalType.of(LogicalKind.INTERVAL).isTemporal()).isTrue();
  }

  @Test
  void isTemporalFalseForNonTemporalKinds() {
    assertThat(LogicalType.of(LogicalKind.INT).isTemporal()).isFalse();
    assertThat(LogicalType.of(LogicalKind.STRING).isTemporal()).isFalse();
    assertThat(LogicalType.of(LogicalKind.ARRAY).isTemporal()).isFalse();
    assertThat(LogicalType.of(LogicalKind.JSON).isTemporal()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // isComplex()
  // ---------------------------------------------------------------------------

  @Test
  void isComplexTrueForComplexKinds() {
    assertThat(LogicalType.of(LogicalKind.ARRAY).isComplex()).isTrue();
    assertThat(LogicalType.of(LogicalKind.MAP).isComplex()).isTrue();
    assertThat(LogicalType.of(LogicalKind.STRUCT).isComplex()).isTrue();
    assertThat(LogicalType.of(LogicalKind.VARIANT).isComplex()).isTrue();
  }

  @Test
  void isComplexFalseForScalarKinds() {
    assertThat(LogicalType.of(LogicalKind.INT).isComplex()).isFalse();
    assertThat(LogicalType.of(LogicalKind.STRING).isComplex()).isFalse();
    assertThat(LogicalType.of(LogicalKind.JSON).isComplex()).isFalse();
    assertThat(LogicalType.decimal(5, 2).isComplex()).isFalse();
  }

  // ---------------------------------------------------------------------------
  // isScalar() — inverse of isComplex() for every kind
  // ---------------------------------------------------------------------------

  @Test
  void isScalarIsInverseOfIsComplexForEveryKind() {
    for (LogicalKind kind : LogicalKind.values()) {
      LogicalType t =
          kind == LogicalKind.DECIMAL ? LogicalType.decimal(10, 2) : LogicalType.of(kind);
      assertThat(t.isScalar())
          .as("isScalar() must be inverse of isComplex() for kind %s", kind)
          .isNotEqualTo(t.isComplex());
    }
  }

  // ---------------------------------------------------------------------------
  // toString()
  // ---------------------------------------------------------------------------

  @Test
  void toStringForSimpleKindIsKindName() {
    assertThat(LogicalType.of(LogicalKind.INT).toString()).isEqualTo("INT");
    assertThat(LogicalType.of(LogicalKind.TIMESTAMPTZ).toString()).isEqualTo("TIMESTAMPTZ");
    assertThat(LogicalType.of(LogicalKind.ARRAY).toString()).isEqualTo("ARRAY");
    assertThat(LogicalType.of(LogicalKind.JSON).toString()).isEqualTo("JSON");
    assertThat(LogicalType.of(LogicalKind.INTERVAL).toString()).isEqualTo("INTERVAL");
  }

  @Test
  void toStringForDecimalIncludesPrecisionAndScale() {
    assertThat(LogicalType.decimal(10, 2).toString()).isEqualTo("DECIMAL(10,2)");
    assertThat(LogicalType.decimal(38, 0).toString()).isEqualTo("DECIMAL(38,0)");
  }

  @Test
  void temporalPrecisionIsRenderedWhenPresent() {
    LogicalType t = LogicalType.temporal(LogicalKind.TIMESTAMP, 3);
    assertThat(t.temporalPrecision()).isEqualTo(3);
    assertThat(t.toString()).isEqualTo("TIMESTAMP(3)");
  }

  @Test
  void intervalRangeIsRenderedWhenPresent() {
    LogicalType t = LogicalType.interval(IntervalRange.YEAR_TO_MONTH, 3, null);
    assertThat(t.intervalRange()).isEqualTo(IntervalRange.YEAR_TO_MONTH);
    assertThat(t.intervalLeadingPrecision()).isEqualTo(3);
    assertThat(t.toString()).isEqualTo("INTERVAL YEAR(3) TO MONTH");
  }

  @Test
  void intervalRangePresenceIsPreserved() {
    LogicalType unset = LogicalType.of(LogicalKind.INTERVAL);
    assertThat(unset.intervalRange()).isEqualTo(IntervalRange.UNSPECIFIED);

    LogicalType explicit = LogicalType.interval(IntervalRange.UNSPECIFIED);
    assertThat(explicit.intervalRange()).isEqualTo(IntervalRange.UNSPECIFIED);
    assertThat(unset).isEqualTo(explicit);
  }

  @Test
  void intervalPrecisionsRequireExplicitRange() {
    assertThatThrownBy(() -> LogicalType.interval(null, 2, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LogicalType.interval(IntervalRange.UNSPECIFIED, null, 3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void intervalFractionalPrecisionRequiresDayToSecondRange() {
    assertThatThrownBy(() -> LogicalType.interval(IntervalRange.YEAR_TO_MONTH, null, 3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void temporalPrecisionRejectsNonTemporalKinds() {
    assertThatThrownBy(() -> LogicalType.temporal(LogicalKind.INT, 3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void temporalPrecisionRejectsOutOfRange() {
    assertThatThrownBy(() -> LogicalType.temporal(LogicalKind.TIME, -1))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LogicalType.temporal(LogicalKind.TIME, 7))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // equals() / hashCode()
  // ---------------------------------------------------------------------------

  @Test
  void equalsAndHashCodeAreConsistentForDecimal() {
    LogicalType a = LogicalType.decimal(10, 2);
    LogicalType b = LogicalType.decimal(10, 2);
    assertThat(a).isEqualTo(b);
    assertThat(a.hashCode()).isEqualTo(b.hashCode());

    // Different precision/scale must not be equal
    assertThat(a).isNotEqualTo(LogicalType.decimal(10, 3));
    assertThat(a).isNotEqualTo(LogicalType.decimal(11, 2));
  }

  @Test
  void equalsAndHashCodeAreConsistentForSimpleKinds() {
    LogicalType c = LogicalType.of(LogicalKind.INT);
    LogicalType d = LogicalType.of(LogicalKind.INT);
    assertThat(c).isEqualTo(d);
    assertThat(c.hashCode()).isEqualTo(d.hashCode());

    assertThat(c).isNotEqualTo(LogicalType.of(LogicalKind.DOUBLE));
  }

  @Test
  void temporalPrecisionAffectsEquality() {
    LogicalType a = LogicalType.temporal(LogicalKind.TIMESTAMP, 3);
    LogicalType b = LogicalType.temporal(LogicalKind.TIMESTAMP, 6);
    assertThat(a).isNotEqualTo(b);
  }

  @Test
  void intervalRangeAffectsEquality() {
    LogicalType a = LogicalType.interval(IntervalRange.YEAR_TO_MONTH, 2, null);
    LogicalType b = LogicalType.interval(IntervalRange.DAY_TO_SECOND, null, 3);
    assertThat(a).isNotEqualTo(b);
  }
}
