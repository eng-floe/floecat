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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.types.rpc.LogicalType;
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
  void protoTemporalPrecisionPresenceIsPreserved() {
    LogicalType unset = LogicalType.newBuilder().setKind(LogicalType.Kind.TK_TIMESTAMP).build();
    assertFalse(unset.hasTemporalPrecision());

    LogicalType zero =
        LogicalType.newBuilder()
            .setKind(LogicalType.Kind.TK_TIMESTAMP)
            .setTemporalPrecision(0)
            .build();
    assertTrue(zero.hasTemporalPrecision());
    assertEquals(0, zero.getTemporalPrecision());
  }
}
