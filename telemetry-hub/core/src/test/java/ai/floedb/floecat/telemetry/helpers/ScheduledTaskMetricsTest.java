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
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class ScheduledTaskMetricsTest {
  @Test
  void registersTaskGaugesWithCanonicalTags() {
    TestObservability observability = new TestObservability();
    ScheduledTaskMetrics metrics = new ScheduledTaskMetrics(observability, "svc", "job", "daily");

    metrics.gaugeEnabled(() -> 1, "enabled");
    metrics.gaugeRunning(() -> 2, "running");
    metrics.gaugeLastTickStart(() -> 3, "start");
    metrics.gaugeLastTickEnd(() -> 4, "end");

    assertGaugeValue(observability.gauge(Telemetry.Metrics.TASK_ENABLED), 1);
    assertGaugeValue(observability.gauge(Telemetry.Metrics.TASK_RUNNING), 2);
    assertGaugeValue(observability.gauge(Telemetry.Metrics.TASK_LAST_TICK_START), 3);
    assertGaugeValue(observability.gauge(Telemetry.Metrics.TASK_LAST_TICK_END), 4);

    assertGaugeTags(observability.gaugeTags(Telemetry.Metrics.TASK_ENABLED));
    assertGaugeTags(observability.gaugeTags(Telemetry.Metrics.TASK_RUNNING));
    assertGaugeTags(observability.gaugeTags(Telemetry.Metrics.TASK_LAST_TICK_START));
    assertGaugeTags(observability.gaugeTags(Telemetry.Metrics.TASK_LAST_TICK_END));
  }

  private static void assertGaugeValue(Supplier<? extends Number> supplier, Number expected) {
    assertThat(supplier).isNotNull();
    assertThat(supplier.get()).isEqualTo(expected);
  }

  private static void assertGaugeTags(List<Tag> tags) {
    assertThat(tags).contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "job"));
    assertThat(tags).contains(Tag.of(TagKey.TASK, "daily"));
  }
}
