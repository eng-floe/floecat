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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

class TemporalCoercionsTest {

  @Test
  void truncatePrecisionZeroRemovesFractionalSeconds() {
    LocalTime t = LocalTime.of(12, 34, 56, 123_456_789);
    assertThat(TemporalCoercions.truncateToTemporalPrecision(t, 0))
        .isEqualTo(LocalTime.of(12, 34, 56));

    LocalDateTime ts = LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_789);
    assertThat(TemporalCoercions.truncateToTemporalPrecision(ts, 0))
        .isEqualTo(LocalDateTime.of(2026, 2, 26, 12, 34, 56));
  }

  @Test
  void truncatePrecisionThreeKeepsMilliseconds() {
    LocalTime t = LocalTime.of(12, 34, 56, 123_456_789);
    assertThat(TemporalCoercions.truncateToTemporalPrecision(t, 3))
        .isEqualTo(LocalTime.of(12, 34, 56, 123_000_000));

    Instant instant = Instant.parse("2026-02-26T12:34:56.123456789Z");
    assertThat(TemporalCoercions.truncateToTemporalPrecision(instant, 3))
        .isEqualTo(Instant.parse("2026-02-26T12:34:56.123Z"));
  }

  @Test
  void nullPrecisionDefaultsToMicros() {
    LocalDateTime ts = LocalDateTime.of(2026, 2, 26, 12, 34, 56, 123_456_789);
    assertThat(TemporalCoercions.formatLocalDateTime(ts, null))
        .isEqualTo("2026-02-26T12:34:56.123456");
  }

  @Test
  void parseTimestampNoTzRejectsZonedStrings() {
    assertThatThrownBy(() -> TemporalCoercions.parseTimestampNoTz("2026-02-26T12:34:56Z"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("timezone-naive");
  }

  @Test
  void formatInstantUtcPrecisionZeroOmitsFractionalPart() {
    Instant instant = Instant.parse("2026-02-26T12:34:56.123456789Z");
    assertThat(TemporalCoercions.formatInstantUtc(instant, 0)).isEqualTo("2026-02-26T12:34:56Z");
  }
}
