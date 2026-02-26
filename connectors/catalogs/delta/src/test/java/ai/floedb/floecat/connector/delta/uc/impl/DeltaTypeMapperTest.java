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

package ai.floedb.floecat.connector.delta.uc.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import io.delta.kernel.types.VariantType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the package-private {@link DeltaTypeMapper}.
 *
 * <p>Validates:
 *
 * <ul>
 *   <li>Timestamp semantics: {@link TimestampType} (UTC) → {@code TIMESTAMPTZ}; {@link
 *       TimestampNTZType} (naive) → {@code TIMESTAMP}
 *   <li>Integer collapsing: Byte/Short/Integer/Long all → {@code INT}
 *   <li>Complex types map to their respective kinds
 *   <li>Unknown types return {@code BINARY} — not null (null-bug regression guard)
 * </ul>
 */
class DeltaTypeMapperTest {

  private static LogicalKind kind(DataType dt) {
    return DeltaTypeMapper.toLogical(dt).kind();
  }

  // ---------------------------------------------------------------------------
  // Timestamp semantics (critical correctness tests)
  // ---------------------------------------------------------------------------

  @Test
  void deltaTimestampMapsToTimestamptz() {
    // Delta TimestampType is UTC-stored → canonical TIMESTAMPTZ (adjusted to UTC)
    assertThat(kind(TimestampType.TIMESTAMP)).isEqualTo(LogicalKind.TIMESTAMPTZ);
  }

  @Test
  void deltaTimestampNtzMapsToTimestamp() {
    // Delta TimestampNTZType is timezone-naive → canonical TIMESTAMP (no UTC normalisation)
    assertThat(kind(TimestampNTZType.TIMESTAMP_NTZ)).isEqualTo(LogicalKind.TIMESTAMP);
  }

  // ---------------------------------------------------------------------------
  // Integer collapsing — all sizes → INT
  // ---------------------------------------------------------------------------

  @Test
  void byteTypeCollapsesToInt() {
    assertThat(kind(ByteType.BYTE)).isEqualTo(LogicalKind.INT);
  }

  @Test
  void shortTypeCollapsesToInt() {
    assertThat(kind(ShortType.SHORT)).isEqualTo(LogicalKind.INT);
  }

  @Test
  void integerTypeCollapsesToInt() {
    assertThat(kind(IntegerType.INTEGER)).isEqualTo(LogicalKind.INT);
  }

  @Test
  void longTypeCollapsesToInt() {
    assertThat(kind(LongType.LONG)).isEqualTo(LogicalKind.INT);
  }

  // ---------------------------------------------------------------------------
  // Float / Double
  // ---------------------------------------------------------------------------

  @Test
  void floatTypeMapsToFloat() {
    assertThat(kind(FloatType.FLOAT)).isEqualTo(LogicalKind.FLOAT);
  }

  @Test
  void doubleTypeMapsToDouble() {
    assertThat(kind(DoubleType.DOUBLE)).isEqualTo(LogicalKind.DOUBLE);
  }

  // ---------------------------------------------------------------------------
  // Other scalars
  // ---------------------------------------------------------------------------

  @Test
  void booleanTypeMapsToBoolean() {
    assertThat(kind(BooleanType.BOOLEAN)).isEqualTo(LogicalKind.BOOLEAN);
  }

  @Test
  void stringTypeMapsToString() {
    assertThat(kind(StringType.STRING)).isEqualTo(LogicalKind.STRING);
  }

  @Test
  void binaryTypeMappsToBinary() {
    assertThat(kind(BinaryType.BINARY)).isEqualTo(LogicalKind.BINARY);
  }

  @Test
  void dateTypeMapsToDate() {
    assertThat(kind(DateType.DATE)).isEqualTo(LogicalKind.DATE);
  }

  // ---------------------------------------------------------------------------
  // DECIMAL — includes precision and scale
  // ---------------------------------------------------------------------------

  @Test
  void decimalTypeMapsToDecimalWithParameters() {
    LogicalType t = DeltaTypeMapper.toLogical(new DecimalType(10, 2));
    assertThat(t.kind()).isEqualTo(LogicalKind.DECIMAL);
    assertThat(t.precision()).isEqualTo(10);
    assertThat(t.scale()).isEqualTo(2);
  }

  // ---------------------------------------------------------------------------
  // Complex / container types
  // ---------------------------------------------------------------------------

  @Test
  void arrayTypeMapsToArray() {
    assertThat(kind(new ArrayType(StringType.STRING, true))).isEqualTo(LogicalKind.ARRAY);
  }

  @Test
  void mapTypeMapsToMap() {
    assertThat(kind(new MapType(StringType.STRING, StringType.STRING, true)))
        .isEqualTo(LogicalKind.MAP);
  }

  @Test
  void structTypeMapsToStruct() {
    assertThat(kind(new StructType())).isEqualTo(LogicalKind.STRUCT);
  }

  @Test
  void variantTypeMapsToVariant() {
    assertThat(kind(VariantType.VARIANT)).isEqualTo(LogicalKind.VARIANT);
  }

  // ---------------------------------------------------------------------------
  // Null-bug regression: unknown types must return BINARY, never null
  // ---------------------------------------------------------------------------

  @Test
  void unknownTypeReturnsBinaryNotNull() {
    // Use an anonymous DataType subclass to simulate an unrecognised type
    DataType unknownType =
        new DataType() {
          @Override
          public boolean equivalent(DataType dataType) {
            return false;
          }

          @Override
          public String toString() {
            return "UnknownType";
          }

          @Override
          public boolean equals(Object o) {
            return this == o;
          }

          @Override
          public int hashCode() {
            return System.identityHashCode(this);
          }
        };
    LogicalType result = DeltaTypeMapper.toLogical(unknownType);
    assertThat(result).isNotNull();
    assertThat(result.kind()).isEqualTo(LogicalKind.BINARY);
  }
}
