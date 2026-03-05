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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link LogicalKind#fromName(String)}.
 *
 * <p>Verifies:
 *
 * <ul>
 *   <li>All canonical kind names resolve to themselves
 *   <li>All documented source-format aliases collapse to the correct canonical kind
 *   <li>Lookup is case-insensitive and trims/normalises whitespace
 *   <li>null, blank, and unknown inputs throw {@link IllegalArgumentException}
 * </ul>
 */
class LogicalKindTest {

  // ---------------------------------------------------------------------------
  // Canonical names
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "{0} -> {1}")
  @CsvSource({
    "BOOLEAN,   BOOLEAN",
    "INT,       INT",
    "FLOAT,     FLOAT",
    "DOUBLE,    DOUBLE",
    "DECIMAL,   DECIMAL",
    "STRING,    STRING",
    "BINARY,    BINARY",
    "UUID,      UUID",
    "DATE,      DATE",
    "TIME,      TIME",
    "TIMESTAMP, TIMESTAMP",
    "TIMESTAMPTZ, TIMESTAMPTZ",
    "INTERVAL,  INTERVAL",
    "JSON,      JSON",
    "ARRAY,     ARRAY",
    "MAP,       MAP",
    "STRUCT,    STRUCT",
    "VARIANT,   VARIANT",
  })
  void canonicalNamesResolve(String name, LogicalKind expected) {
    assertThat(LogicalKind.fromName(name.trim())).isEqualTo(expected);
  }

  // ---------------------------------------------------------------------------
  // Integer aliases (all collapse to INT)
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "''{0}'' -> INT")
  @ValueSource(
      strings = {
        "INT", "INTEGER", "BIGINT", "LONG", "SMALLINT", "TINYINT",
        "INT8", "INT4", "INT2", "UINT8", "UINT4", "UINT2"
      })
  void integerAliasesResolveToInt(String alias) {
    assertThat(LogicalKind.fromName(alias)).isEqualTo(LogicalKind.INT);
  }

  // ---------------------------------------------------------------------------
  // Float/Double aliases
  // ---------------------------------------------------------------------------

  @Test
  void floatAliasesResolveToFloat() {
    assertThat(LogicalKind.fromName("FLOAT4")).isEqualTo(LogicalKind.FLOAT);
    assertThat(LogicalKind.fromName("FLOAT32")).isEqualTo(LogicalKind.FLOAT);
    assertThat(LogicalKind.fromName("REAL")).isEqualTo(LogicalKind.FLOAT);
  }

  @Test
  void doubleAliasesResolveToDouble() {
    assertThat(LogicalKind.fromName("FLOAT8")).isEqualTo(LogicalKind.DOUBLE);
    assertThat(LogicalKind.fromName("FLOAT64")).isEqualTo(LogicalKind.DOUBLE);
    assertThat(LogicalKind.fromName("DOUBLE PRECISION")).isEqualTo(LogicalKind.DOUBLE);
  }

  // ---------------------------------------------------------------------------
  // String aliases
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "''{0}'' -> STRING")
  @ValueSource(strings = {"VARCHAR", "CHAR", "CHARACTER", "TEXT", "NTEXT", "NVARCHAR"})
  void stringAliasesResolveToString(String alias) {
    assertThat(LogicalKind.fromName(alias)).isEqualTo(LogicalKind.STRING);
  }

  // ---------------------------------------------------------------------------
  // Binary aliases
  // ---------------------------------------------------------------------------

  @ParameterizedTest(name = "''{0}'' -> BINARY")
  @ValueSource(strings = {"VARBINARY", "BYTEA", "BLOB", "IMAGE"})
  void binaryAliasesResolveToBinary(String alias) {
    assertThat(LogicalKind.fromName(alias)).isEqualTo(LogicalKind.BINARY);
  }

  // ---------------------------------------------------------------------------
  // Boolean aliases
  // ---------------------------------------------------------------------------

  @Test
  void booleanAliasesResolveToBoolean() {
    assertThat(LogicalKind.fromName("BOOL")).isEqualTo(LogicalKind.BOOLEAN);
    assertThat(LogicalKind.fromName("BIT")).isEqualTo(LogicalKind.BOOLEAN);
  }

  // ---------------------------------------------------------------------------
  // Remaining aliases
  // ---------------------------------------------------------------------------

  @Test
  void numericResolvesToDecimal() {
    assertThat(LogicalKind.fromName("NUMERIC")).isEqualTo(LogicalKind.DECIMAL);
  }

  @Test
  void datetimeResolvesToTimestamp() {
    assertThat(LogicalKind.fromName("DATETIME")).isEqualTo(LogicalKind.TIMESTAMP);
  }

  @Test
  void timestampWithTimeZoneResolvesToTimestamptz() {
    assertThat(LogicalKind.fromName("TIMESTAMP WITH TIME ZONE")).isEqualTo(LogicalKind.TIMESTAMPTZ);
  }

  @Test
  void jsonbResolvesToJson() {
    assertThat(LogicalKind.fromName("JSONB")).isEqualTo(LogicalKind.JSON);
  }

  // ---------------------------------------------------------------------------
  // Case-insensitivity
  // ---------------------------------------------------------------------------

  @Test
  void lookupIsCaseInsensitive() {
    assertThat(LogicalKind.fromName("bigint")).isEqualTo(LogicalKind.INT);
    assertThat(LogicalKind.fromName("varchar")).isEqualTo(LogicalKind.STRING);
    assertThat(LogicalKind.fromName("timestamptz")).isEqualTo(LogicalKind.TIMESTAMPTZ);
    assertThat(LogicalKind.fromName("Boolean")).isEqualTo(LogicalKind.BOOLEAN);
    assertThat(LogicalKind.fromName("jsonb")).isEqualTo(LogicalKind.JSON);
  }

  // ---------------------------------------------------------------------------
  // Whitespace normalisation
  // ---------------------------------------------------------------------------

  @Test
  void leadingTrailingWhitespaceIsStripped() {
    assertThat(LogicalKind.fromName("  INT  ")).isEqualTo(LogicalKind.INT);
    assertThat(LogicalKind.fromName("\tSTRING\t")).isEqualTo(LogicalKind.STRING);
  }

  @Test
  void internalWhitespaceInMultiWordAliasIsNormalized() {
    // "double   precision" has extra internal spaces — must still resolve
    assertThat(LogicalKind.fromName("double   precision")).isEqualTo(LogicalKind.DOUBLE);
    assertThat(LogicalKind.fromName("timestamp  with  time  zone"))
        .isEqualTo(LogicalKind.TIMESTAMPTZ);
  }

  // ---------------------------------------------------------------------------
  // Error cases
  // ---------------------------------------------------------------------------

  @Test
  void nullThrowsIllegalArgument() {
    assertThatThrownBy(() -> LogicalKind.fromName(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null");
  }

  @Test
  void blankThrowsIllegalArgument() {
    assertThatThrownBy(() -> LogicalKind.fromName("   "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("blank");
  }

  @Test
  void unknownNameThrowsIllegalArgument() {
    assertThatThrownBy(() -> LogicalKind.fromName("UNKNOWN_TYPE_XYZ"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("UNKNOWN_TYPE_XYZ");
  }
}
