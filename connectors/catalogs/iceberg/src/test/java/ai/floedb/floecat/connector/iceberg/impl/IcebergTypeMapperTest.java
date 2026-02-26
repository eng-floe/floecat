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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link IcebergTypeMapper#toLogical(Type)}.
 *
 * <p>Validates that every Iceberg type maps to the correct canonical {@link LogicalKind}, with
 * particular focus on:
 *
 * <ul>
 *   <li>Timestamp UTC semantics ({@code withZone()} → TIMESTAMPTZ, {@code withoutZone()} →
 *       TIMESTAMP)
 *   <li>Integer collapsing (INTEGER and LONG both → INT)
 *   <li>FIXED and BINARY both → BINARY
 *   <li>Complex type container mapping
 *   <li>DECIMAL precision/scale round-trip
 * </ul>
 */
class IcebergTypeMapperTest {

  // ---------------------------------------------------------------------------
  // Scalar numeric
  // ---------------------------------------------------------------------------

  @Test
  void booleanMapsToBoolean() {
    assertKind(Types.BooleanType.get(), LogicalKind.BOOLEAN);
  }

  @Test
  void integerMapsToInt() {
    assertKind(Types.IntegerType.get(), LogicalKind.INT);
  }

  @Test
  void longMapsToInt() {
    // INTEGER and LONG both collapse to canonical 64-bit INT.
    assertKind(Types.LongType.get(), LogicalKind.INT);
  }

  @Test
  void floatMapsToFloat() {
    assertKind(Types.FloatType.get(), LogicalKind.FLOAT);
  }

  @Test
  void doubleMapsToDouble() {
    assertKind(Types.DoubleType.get(), LogicalKind.DOUBLE);
  }

  @Test
  void decimalPreservesPrecisionAndScale() {
    LogicalType result = IcebergTypeMapper.toLogical(Types.DecimalType.of(18, 4));
    assertThat(result.kind()).isEqualTo(LogicalKind.DECIMAL);
    assertThat(result.precision()).isEqualTo(18);
    assertThat(result.scale()).isEqualTo(4);
  }

  @Test
  void decimalPrecisionAbove38FailsFast() {
    assertThatThrownBy(
            () -> {
              // Iceberg currently enforces the precision cap at type construction time; either way
              // this path must fail fast for out-of-contract DECIMAL precision.
              IcebergTypeMapper.toLogical(Types.DecimalType.of(39, 0));
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("precision")
        .hasMessageContaining("38");
  }

  // ---------------------------------------------------------------------------
  // Scalar string / binary
  // ---------------------------------------------------------------------------

  @Test
  void stringMapsToString() {
    assertKind(Types.StringType.get(), LogicalKind.STRING);
  }

  @Test
  void binaryMapsToBinary() {
    assertKind(Types.BinaryType.get(), LogicalKind.BINARY);
  }

  @Test
  void fixedMapsToBinary() {
    // FIXED (fixed-length byte array) collapses to canonical BINARY.
    assertKind(Types.FixedType.ofLength(16), LogicalKind.BINARY);
  }

  @Test
  void uuidMapsToUuid() {
    assertKind(Types.UUIDType.get(), LogicalKind.UUID);
  }

  // ---------------------------------------------------------------------------
  // Temporal
  // ---------------------------------------------------------------------------

  @Test
  void dateMapsToDate() {
    assertKind(Types.DateType.get(), LogicalKind.DATE);
  }

  @Test
  void timeMapsToTime() {
    assertKind(Types.TimeType.get(), LogicalKind.TIME);
  }

  @Test
  void timestampWithZoneMapsToTimestamptz() {
    // Iceberg withZone() = UTC-adjusted → canonical TIMESTAMPTZ.
    assertKind(Types.TimestampType.withZone(), LogicalKind.TIMESTAMPTZ);
  }

  @Test
  void timestampWithoutZoneMapsToTimestamp() {
    // Iceberg withoutZone() = timezone-naive → canonical TIMESTAMP (no UTC normalisation).
    assertKind(Types.TimestampType.withoutZone(), LogicalKind.TIMESTAMP);
  }

  // ---------------------------------------------------------------------------
  // Complex / container types
  // ---------------------------------------------------------------------------

  @Test
  void listMapsToArray() {
    assertKind(Types.ListType.ofOptional(1, Types.StringType.get()), LogicalKind.ARRAY);
  }

  @Test
  void mapMapsToMap() {
    assertKind(
        Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.LongType.get()),
        LogicalKind.MAP);
  }

  @Test
  void structMapsToStruct() {
    assertKind(
        Types.StructType.of(Types.NestedField.optional(1, "x", Types.StringType.get())),
        LogicalKind.STRUCT);
  }

  @Test
  void unsupportedIcebergKindsFailFast() {
    List<Type> unsupported =
        List.of(
            Types.TimestampNanoType.withoutZone(),
            Types.GeometryType.crs84(),
            Types.GeographyType.crs84(),
            Types.VariantType.get(),
            Types.UnknownType.get());

    for (Type icebergType : unsupported) {
      assertThatThrownBy(() -> IcebergTypeMapper.toLogical(icebergType))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Unrecognized Iceberg type");
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void assertKind(Type icebergType, LogicalKind expectedKind) {
    LogicalType result = IcebergTypeMapper.toLogical(icebergType);
    assertThat(result.kind())
        .as("Iceberg %s should map to %s", icebergType, expectedKind)
        .isEqualTo(expectedKind);
  }
}
