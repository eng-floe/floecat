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

package ai.floedb.floecat.extensions.floedb.hints;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link FloeHintResolver#passByValue(LogicalType)}.
 *
 * <p>{@code passByValue()} maps a canonical logical type to PostgreSQL {@code typbyval} semantics.
 * This test verifies correctness against the builtins catalog ({@code 20_types.pbtxt}).
 *
 * <p><b>Critical regressions:</b>
 *
 * <ul>
 *   <li>{@link LogicalKind#INTERVAL} must return {@code false}. {@code interval} is a 16-byte
 *       composite struct ({@code typbyval=false}, {@code typlen=16} per the pg catalog). It was
 *       incorrectly added to the pass-by-value list during the type-system refactor.
 *   <li>{@link LogicalKind#UUID} must return {@code false}. {@code uuid} is a 16-byte fixed type
 *       ({@code typbyval=false}, {@code typlen=16} per the pg catalog). It was incorrectly listed
 *       as pass-by-value alongside the word-sized temporals.
 * </ul>
 */
class FloeHintResolverTypesTest {

  // ---------------------------------------------------------------------------
  // Regression tests: 16-byte types that must NOT be pass-by-value
  // ---------------------------------------------------------------------------

  @Test
  void intervalIsNotPassByValue() {
    // interval: typbyval=false, typlen=16 (composite struct: microseconds+days+months)
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.INTERVAL))).isFalse();
  }

  @Test
  void uuidIsNotPassByValue() {
    // uuid: typbyval=false, typlen=16 (128-bit fixed; stored as char array, not a machine word)
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.UUID))).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Pass-by-value types (fixed-size, machine-word-sized)
  // ---------------------------------------------------------------------------

  @Test
  void booleanIsPassByValue() {
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.BOOLEAN))).isTrue();
  }

  @Test
  void intIsPassByValue() {
    // INT → pg int8 (8 bytes, typbyval=true on 64-bit)
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.INT))).isTrue();
  }

  @Test
  void floatIsPassByValue() {
    // float4: typbyval=true
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.FLOAT))).isTrue();
  }

  @Test
  void doubleIsPassByValue() {
    // float8: typbyval=true
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.DOUBLE))).isTrue();
  }

  @Test
  void dateIsPassByValue() {
    // date: typbyval=true, typlen=4
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.DATE))).isTrue();
  }

  @Test
  void timeIsPassByValue() {
    // time: typbyval=true, typlen=8
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.TIME))).isTrue();
  }

  @Test
  void timestampIsPassByValue() {
    // timestamp: typbyval=true, typlen=8
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.TIMESTAMP))).isTrue();
  }

  @Test
  void timestamptzIsPassByValue() {
    // timestamptz: typbyval=true, typlen=8
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.TIMESTAMPTZ))).isTrue();
  }

  // ---------------------------------------------------------------------------
  // Not pass-by-value types (variable or large fixed-size)
  // ---------------------------------------------------------------------------

  @Test
  void stringIsNotPassByValue() {
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.STRING))).isFalse();
  }

  @Test
  void binaryIsNotPassByValue() {
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.BINARY))).isFalse();
  }

  @Test
  void decimalIsNotPassByValue() {
    assertThat(FloeHintResolver.passByValue(LogicalType.decimal(10, 2))).isFalse();
  }

  @Test
  void jsonIsNotPassByValue() {
    assertThat(FloeHintResolver.passByValue(LogicalType.of(LogicalKind.JSON))).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Complex types — all not pass-by-value
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "{0} is not pass-by-value")
  @EnumSource(
      value = LogicalKind.class,
      names = {"ARRAY", "MAP", "STRUCT", "VARIANT"})
  void complexTypesAreNotPassByValue(LogicalKind kind) {
    assertThat(FloeHintResolver.passByValue(LogicalType.of(kind))).isFalse();
  }

  // ---------------------------------------------------------------------------
  // Null guard
  // ---------------------------------------------------------------------------

  @Test
  void nullLogicalTypeReturnsFalse() {
    assertThat(FloeHintResolver.passByValue(null)).isFalse();
  }

  // ---------------------------------------------------------------------------
  // deriveTypmod
  // ---------------------------------------------------------------------------

  @Test
  void deriveTypmodEncodesDecimalPrecisionAndScale() throws Exception {
    int typmod = FloeHintResolver.deriveTypmod(LogicalType.decimal(10, 2));
    assertThat(typmod).isEqualTo((10 << 16) | 2);
  }

  @Test
  void deriveTypmodEncodesTemporalPrecision() throws Exception {
    int typmod = FloeHintResolver.deriveTypmod(LogicalType.temporal(LogicalKind.TIMESTAMP, 3));
    assertThat(typmod).isEqualTo(FloeHintResolver.VARHDRSZ + 3);
  }

  @Test
  void deriveTypmodReturnsMinusOneWhenUnavailable() throws Exception {
    assertThat(FloeHintResolver.deriveTypmod(null)).isEqualTo(-1);
    assertThat(FloeHintResolver.deriveTypmod(LogicalType.of(LogicalKind.STRING))).isEqualTo(-1);
    assertThat(FloeHintResolver.deriveTypmod(LogicalType.of(LogicalKind.TIMESTAMP))).isEqualTo(-1);
  }
}
