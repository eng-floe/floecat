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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergPlannerTest {

  @Test
  void canonicalizeDecodedBoundConvertsTimeMicrosToLocalTime() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.TimeType.get(), 45_296_123_456L);

    assertThat(canonical).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeDecodedBoundDropsOutOfRangeTimeMicros() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.TimeType.get(), 86_400_000_000L);

    assertThat(canonical).isNull();
  }

  @Test
  void canonicalizeDecodedBoundLeavesNonTimeValuesUntouched() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.StringType.get(), "already-canonical");

    assertThat(canonical).isEqualTo("already-canonical");
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestampMicrosToLocalDateTime() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampType.withoutZone(), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestamptzMicrosToInstant() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampType.withZone(), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456Z"));
  }
}
