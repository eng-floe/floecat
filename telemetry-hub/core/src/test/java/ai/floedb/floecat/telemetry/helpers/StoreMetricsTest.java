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
import java.util.List;
import org.junit.jupiter.api.Test;

class StoreMetricsTest {
  @Test
  void recordsStoreCountersAndTimers() {
    TestObservability observability = new TestObservability();
    StoreMetrics metrics = new StoreMetrics(observability, "svc", "op");

    metrics.recordRequest("success", Tag.of(TagKey.ACCOUNT, "acct"));
    metrics.recordLatency(Duration.ofMillis(5), "success", Tag.of(TagKey.ACCOUNT, "acct"));
    metrics.recordBytes(123, "success", Tag.of(TagKey.ACCOUNT, "acct"));

    assertThat(observability.counterValue(Telemetry.Metrics.STORE_REQUESTS)).isEqualTo(1d);
    assertThat(observability.counterValue(Telemetry.Metrics.STORE_BYTES)).isEqualTo(123d);
    List<Tag> requestTags =
        observability.counterTagHistory(Telemetry.Metrics.STORE_REQUESTS).get(0);
    assertThat(requestTags)
        .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
        .contains(Tag.of(TagKey.RESULT, "success"));

    assertThat(observability.timerValues(Telemetry.Metrics.STORE_LATENCY)).isNotEmpty();
    assertThat(observability.timerTagHistory(Telemetry.Metrics.STORE_LATENCY))
        .anySatisfy(
            tags ->
                assertThat(tags)
                    .contains(Tag.of(TagKey.COMPONENT, "svc"), Tag.of(TagKey.OPERATION, "op"))
                    .contains(Tag.of(TagKey.RESULT, "success")));
  }
}
