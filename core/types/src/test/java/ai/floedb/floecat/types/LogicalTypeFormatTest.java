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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Unit tests for {@link LogicalTypeFormat#format} and {@link LogicalTypeFormat#parse}.
 *
 * <p>Covers:
 *
 * <ul>
 *   <li>Round-trip: {@code parse(format(t)) == t} for all canonical kinds
 *   <li>DECIMAL format and parse with precision and scale
 *   <li>Bare {@code DECIMAL} without parameters is rejected
 *   <li>Parameterised aliases ({@code VARCHAR(10)}) are stripped to their canonical base type;
 *       temporal precisions ({@code TIMESTAMP(6)}) are preserved
 *   <li>Case-insensitive and whitespace-normalised parsing
 *   <li>Unknown type strings are rejected with a clear error
 * </ul>
 */
class LogicalTypeFormatTest {

  // ---------------------------------------------------------------------------
  // format() — canonical string representation
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "format({0}) == kind name")
  @EnumSource(
      value = LogicalKind.class,
      names = {"DECIMAL"},
      mode = EnumSource.Mode.EXCLUDE)
  void formatNonDecimalKindIsKindName(LogicalKind kind) {
    String formatted = LogicalTypeFormat.format(LogicalType.of(kind));
    assertThat(formatted).isEqualTo(kind.name());
  }

  @Test
  void formatDecimalIncludesPrecisionAndScale() {
    assertThat(LogicalTypeFormat.format(LogicalType.decimal(10, 2))).isEqualTo("DECIMAL(10,2)");
    assertThat(LogicalTypeFormat.format(LogicalType.decimal(38, 0))).isEqualTo("DECIMAL(38,0)");
    assertThat(LogicalTypeFormat.format(LogicalType.decimal(1, 1))).isEqualTo("DECIMAL(1,1)");
  }

  @Test
  void formatTemporalIncludesPrecisionWhenPresent() {
    assertThat(LogicalTypeFormat.format(LogicalType.temporal(LogicalKind.TIME, 3)))
        .isEqualTo("TIME(3)");
    assertThat(LogicalTypeFormat.format(LogicalType.temporal(LogicalKind.TIMESTAMP, 6)))
        .isEqualTo("TIMESTAMP(6)");
    assertThat(LogicalTypeFormat.format(LogicalType.temporal(LogicalKind.TIMESTAMPTZ, 0)))
        .isEqualTo("TIMESTAMPTZ(0)");
  }

  // ---------------------------------------------------------------------------
  // parse() — canonical names round-trip
  // ---------------------------------------------------------------------------

  @Test
  void parseCanonicalNamesRoundTrip() {
    for (LogicalKind kind : LogicalKind.values()) {
      if (kind == LogicalKind.DECIMAL) continue;
      LogicalType original = LogicalType.of(kind);
      LogicalType parsed = LogicalTypeFormat.parse(original.toString());
      assertThat(parsed).isEqualTo(original);
    }
  }

  @Test
  void parseDecimalWithPrecisionAndScaleRoundTrips() {
    LogicalType t = LogicalType.decimal(18, 4);
    assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(t))).isEqualTo(t);
  }

  @Test
  void parseDecimalIsCaseInsensitive() {
    assertThat(LogicalTypeFormat.parse("decimal(10,2)")).isEqualTo(LogicalType.decimal(10, 2));
    assertThat(LogicalTypeFormat.parse("NUMERIC(18,6)")).isEqualTo(LogicalType.decimal(18, 6));
  }

  @Test
  void parseDecimalWithSpacesAroundParameters() {
    // SQL often has spaces: DECIMAL( 10 , 2 )
    assertThat(LogicalTypeFormat.parse("DECIMAL( 10 , 2 )")).isEqualTo(LogicalType.decimal(10, 2));
  }

  // ---------------------------------------------------------------------------
  // parse() — aliased type names
  // ---------------------------------------------------------------------------

  @Test
  void parseAliasesResolveToCanonicalKinds() {
    assertThat(LogicalTypeFormat.parse("BIGINT").kind()).isEqualTo(LogicalKind.INT);
    assertThat(LogicalTypeFormat.parse("SMALLINT").kind()).isEqualTo(LogicalKind.INT);
    assertThat(LogicalTypeFormat.parse("DOUBLE PRECISION").kind()).isEqualTo(LogicalKind.DOUBLE);
    assertThat(LogicalTypeFormat.parse("REAL").kind()).isEqualTo(LogicalKind.FLOAT);
    assertThat(LogicalTypeFormat.parse("TEXT").kind()).isEqualTo(LogicalKind.STRING);
    assertThat(LogicalTypeFormat.parse("BYTEA").kind()).isEqualTo(LogicalKind.BINARY);
    assertThat(LogicalTypeFormat.parse("JSONB").kind()).isEqualTo(LogicalKind.JSON);
    assertThat(LogicalTypeFormat.parse("DATETIME").kind()).isEqualTo(LogicalKind.TIMESTAMP);
  }

  // ---------------------------------------------------------------------------
  // parse() — parameterised alias stripping
  // ---------------------------------------------------------------------------

  @Test
  void parseStripsParametersFromKnownBaseTypes() {
    // VARCHAR(255) → STRING; CHAR(10) → STRING
    assertThat(LogicalTypeFormat.parse("VARCHAR(255)").kind()).isEqualTo(LogicalKind.STRING);
    assertThat(LogicalTypeFormat.parse("VARCHAR(MAX)").kind()).isEqualTo(LogicalKind.STRING);
    assertThat(LogicalTypeFormat.parse("CHAR(10)").kind()).isEqualTo(LogicalKind.STRING);
    assertThat(LogicalTypeFormat.parse("TIMESTAMP(6)").temporalPrecision()).isEqualTo(6);
    assertThat(LogicalTypeFormat.parse("TIME(6)").temporalPrecision()).isEqualTo(6);
    LogicalType timestamptz = LogicalTypeFormat.parse("TIMESTAMP WITH TIME ZONE(3)");
    assertThat(timestamptz.kind()).isEqualTo(LogicalKind.TIMESTAMPTZ);
    assertThat(timestamptz.temporalPrecision()).isEqualTo(3);
  }

  @Test
  void parseTemporalPrecisionRoundTrips() {
    LogicalType t = LogicalType.temporal(LogicalKind.TIMESTAMP, 3);
    assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(t))).isEqualTo(t);
  }

  @Test
  void parseIntervalQualifiers() {
    LogicalType ym = LogicalTypeFormat.parse("INTERVAL YEAR TO MONTH");
    assertThat(ym.kind()).isEqualTo(LogicalKind.INTERVAL);
    assertThat(ym.intervalQualifier()).isEqualTo(IntervalQualifier.YEAR_MONTH);

    LogicalType dt = LogicalTypeFormat.parse("INTERVAL DAY TO SECOND");
    assertThat(dt.kind()).isEqualTo(LogicalKind.INTERVAL);
    assertThat(dt.intervalQualifier()).isEqualTo(IntervalQualifier.DAY_TIME);
  }

  @Test
  void intervalQualifierRoundTrips() {
    for (IntervalQualifier qualifier : IntervalQualifier.values()) {
      if (qualifier == IntervalQualifier.UNSPECIFIED) {
        continue;
      }
      LogicalType t = LogicalType.interval(qualifier);
      assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(t))).isEqualTo(t);
    }
    LogicalType unset = LogicalType.of(LogicalKind.INTERVAL);
    assertThat(LogicalTypeFormat.parse(LogicalTypeFormat.format(unset))).isEqualTo(unset);
  }

  // ---------------------------------------------------------------------------
  // parse() — case and whitespace normalisation
  // ---------------------------------------------------------------------------

  @Test
  void parseIsCaseInsensitive() {
    assertThat(LogicalTypeFormat.parse("int").kind()).isEqualTo(LogicalKind.INT);
    assertThat(LogicalTypeFormat.parse("Boolean").kind()).isEqualTo(LogicalKind.BOOLEAN);
    assertThat(LogicalTypeFormat.parse("timestamptz").kind()).isEqualTo(LogicalKind.TIMESTAMPTZ);
  }

  @Test
  void parseNormalisesWhitespace() {
    assertThat(LogicalTypeFormat.parse("  INT  ").kind()).isEqualTo(LogicalKind.INT);
    assertThat(LogicalTypeFormat.parse("double   precision").kind()).isEqualTo(LogicalKind.DOUBLE);
    assertThat(LogicalTypeFormat.parse("timestamp with time zone").kind())
        .isEqualTo(LogicalKind.TIMESTAMPTZ);
  }

  // ---------------------------------------------------------------------------
  // parse() — error cases
  // ---------------------------------------------------------------------------

  @Test
  void parseDecimalWithoutParametersThrows() {
    // A bare DECIMAL/NUMERIC without precision+scale is not a fully-specified type.
    assertThatThrownBy(() -> LogicalTypeFormat.parse("DECIMAL"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DECIMAL requires precision and scale");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("NUMERIC"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DECIMAL requires precision and scale");
  }

  @Test
  void parseUnknownTypeThrows() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse("GEOGRAPHY"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("GEOGRAPHY");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("INT96"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("INT96");
  }

  @Test
  void parseRejectsParametersForUnsupportedBaseTypes() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse("INT(10)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not accept parameters");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("BOOLEAN(1)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not accept parameters");
  }

  @Test
  void parseRejectsMalformedParametersOnSupportedBaseTypes() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse("INT(bad)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not accept parameters");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("VARCHAR(foo)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid string length parameter");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("TIMESTAMP(bad)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid temporal precision parameter");
    assertThatThrownBy(() -> LogicalTypeFormat.parse("TIME(9)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("temporal precision must be 0..");
  }

  @Test
  void parseNullThrows() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void parseBlankThrows() {
    assertThatThrownBy(() -> LogicalTypeFormat.parse(""))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> LogicalTypeFormat.parse("   "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  // ---------------------------------------------------------------------------
  // format() — null guard
  // ---------------------------------------------------------------------------

  @Test
  void formatNullThrows() {
    assertThatThrownBy(() -> LogicalTypeFormat.format(null))
        .isInstanceOf(NullPointerException.class);
  }
}
