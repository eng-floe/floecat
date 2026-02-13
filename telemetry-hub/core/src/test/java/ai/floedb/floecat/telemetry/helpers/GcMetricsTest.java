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

package ai.floedb.floecat.telemetry.helpers;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TestObservability;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class GcMetricsTest {
  @Test
  void recordsCollectionsAndPauses() {
    TestObservability observability = new TestObservability();
    GcMetrics metrics = new GcMetrics(observability, "svc", "op", "gc1");

    metrics.recordCollection(1);
    metrics.recordPause(Duration.ofMillis(10));

    assertThat(observability.counterValue(Telemetry.Metrics.GC_COLLECTIONS)).isEqualTo(1d);
    assertThat(observability.timerValues(Telemetry.Metrics.GC_PAUSE)).isNotEmpty();
    assertThat(observability.timerTagHistory(Telemetry.Metrics.GC_PAUSE))
        .anySatisfy(
            tags ->
                assertThat(tags)
                    .contains(Tag.of(TagKey.GC_NAME, "gc1"))
                    .anyMatch(tag -> TagKey.COMPONENT.equals(tag.key()))
                    .anyMatch(tag -> TagKey.OPERATION.equals(tag.key()))
                    .anyMatch(tag -> TagKey.RESULT.equals(tag.key())));
  }
}
