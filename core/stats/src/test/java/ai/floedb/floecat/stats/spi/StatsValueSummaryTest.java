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

package ai.floedb.floecat.stats.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsValueSummaryTest {

  @Test
  void equalsAndHashCodeUseByteContentForSketches() {
    StatsValueSummary left =
        new StatsValueSummary(
            10L,
            Optional.of(1L),
            Optional.empty(),
            Optional.empty(),
            Optional.of("a"),
            Optional.of("z"),
            Optional.of(new byte[] {1, 2, 3}),
            Optional.of(new byte[] {9, 8}),
            Optional.empty(),
            Map.of("k", "v"));
    StatsValueSummary right =
        new StatsValueSummary(
            10L,
            Optional.of(1L),
            Optional.empty(),
            Optional.empty(),
            Optional.of("a"),
            Optional.of("z"),
            Optional.of(new byte[] {1, 2, 3}),
            Optional.of(new byte[] {9, 8}),
            Optional.empty(),
            Map.of("k", "v"));

    assertThat(left).isEqualTo(right);
    assertThat(left.hashCode()).isEqualTo(right.hashCode());
  }
}
