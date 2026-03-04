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
import java.time.Instant;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class DeltaPlannerTest {

  @Test
  void canonicalizeStatValueConvertsTimestampMicrosToLocalDateTime() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(
            LogicalType.of(LogicalKind.TIMESTAMP), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeStatValueConvertsTimestamptzMicrosToInstant() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(
            LogicalType.of(LogicalKind.TIMESTAMPTZ), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456Z"));
  }

  @Test
  void canonicalizeStatValueLeavesNonTemporalValuesUntouched() {
    Object canonical =
        DeltaPlanner.canonicalizeStatValue(LogicalType.of(LogicalKind.STRING), "already-canonical");

    assertThat(canonical).isEqualTo("already-canonical");
  }
}
