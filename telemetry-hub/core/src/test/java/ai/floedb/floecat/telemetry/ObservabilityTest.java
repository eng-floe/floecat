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

package ai.floedb.floecat.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class ObservabilityTest {

  @Test
  void noopObservabilityIsSafe() {
    Observability obs = new NoopObservability();
    obs.counter(Telemetry.Metrics.RPC_REQUESTS, 1);
    obs.summary(Telemetry.Metrics.RPC_ERRORS, 2);
    obs.timer(Telemetry.Metrics.RPC_LATENCY, Duration.ofMillis(5));
    obs.gauge(Telemetry.Metrics.RPC_ACTIVE, () -> 1, "desc");
    obs.observe(Observability.Category.RPC, "svc", "op").close();
  }

  @Test
  void testObservabilityRecordsCountersAndScopes() {
    TestObservability obs = new TestObservability();
    obs.counter(Telemetry.Metrics.RPC_REQUESTS, 5);
    assertThat(obs.counterValue(Telemetry.Metrics.RPC_REQUESTS)).isEqualTo(5d);

    ObservationScope scope =
        obs.observe(Observability.Category.RPC, "svc", "operation", Tag.of("account", "A"));
    scope.retry();
    scope.error(new IllegalStateException("boom"));
    scope.close();

    assertThat(obs.scopes()).containsKey("RPC");
    TestObservability.TestObservationScope recorded = obs.scopes().get("RPC").get(0);
    assertThat(recorded.retries()).isEqualTo(1);
    assertThat(recorded.error()).isInstanceOf(IllegalStateException.class);
    assertThat(recorded.component()).isEqualTo("svc");
    assertThat(recorded.operation()).isEqualTo("operation");
    assertThat(recorded.tags()).contains(Tag.of("account", "A"));
  }

  @Test
  void testObservabilityGauge() {
    TestObservability obs = new TestObservability();
    Supplier<Double> supplier = () -> 42.0;
    obs.gauge(Telemetry.Metrics.RPC_ACTIVE, supplier, "desc");
    assertThat(obs.gauge(Telemetry.Metrics.RPC_ACTIVE)).isSameAs(supplier);
  }
}
