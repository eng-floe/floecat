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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.types.rpc.ScalarValue;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class LogicalTypeProtoAdapterTest {

  @Test
  void decodeLogicalType_parsesCanonicalAndAliases() {
    assertEquals(LogicalType.of(LogicalKind.INT), LogicalTypeProtoAdapter.decodeLogicalType("INT"));
    assertEquals(
        LogicalType.of(LogicalKind.INT), LogicalTypeProtoAdapter.decodeLogicalType("BIGINT"));
    assertEquals(
        LogicalType.decimal(12, 3), LogicalTypeProtoAdapter.decodeLogicalType("DECIMAL(12,3)"));
  }

  @Test
  void decodeLogicalType_rejectsNullBlankAndUnknown() {
    assertThrows(
        IllegalArgumentException.class, () -> LogicalTypeProtoAdapter.decodeLogicalType(null));
    assertThrows(
        IllegalArgumentException.class, () -> LogicalTypeProtoAdapter.decodeLogicalType(" "));
    assertThrows(
        IllegalArgumentException.class,
        () -> LogicalTypeProtoAdapter.decodeLogicalType("NOT_A_REAL_TYPE"));
  }

  @Test
  void scalarStats_acceptsTypedAndLegacyLogicalTypes() {
    ScalarStats typed =
        ScalarStats.newBuilder()
            .setType(LogicalTypeProtoAdapter.parseToProto("DECIMAL(12,3)"))
            .build();
    assertEquals(LogicalType.decimal(12, 3), LogicalTypeProtoAdapter.columnLogicalType(typed));

    ScalarStats legacy = ScalarStats.newBuilder().setLogicalType("BIGINT").build();
    assertEquals(
        LogicalType.of(LogicalKind.INT), LogicalTypeProtoAdapter.columnLogicalType(legacy));
    assertEquals(
        LogicalTypeProtoAdapter.parseToProto("BIGINT"),
        LogicalTypeProtoAdapter.upgradeLegacyScalarStats(legacy).getType());
  }

  @Test
  void scalarBounds_preferTypedValues_andPreserveNanoseconds() {
    LogicalType type = LogicalType.temporal(LogicalKind.TIMESTAMPTZ, 9);
    Instant instant = Instant.parse("2026-10-07T12:34:56.123456789Z");
    ScalarStats stats =
        ScalarStats.newBuilder()
            .setType(LogicalTypeProtoAdapter.toProto(type))
            .setMin("not-used")
            .setMinValue(LogicalTypeProtoAdapter.encodeTypedValue(type, instant))
            .build();

    assertEquals(instant, LogicalTypeProtoAdapter.columnMinValue(stats));
    assertEquals(ScalarValue.VCase.TSTZ, stats.getMinValue().getVCase());
  }

  @Test
  void scalarBounds_fallBackToLegacyStrings() {
    ScalarStats stats =
        ScalarStats.newBuilder().setLogicalType("INT").setMin("42").setMax("99").build();
    assertEquals(42L, LogicalTypeProtoAdapter.columnMinValue(stats));
    assertEquals(99L, LogicalTypeProtoAdapter.columnMaxValue(stats));
  }
}
