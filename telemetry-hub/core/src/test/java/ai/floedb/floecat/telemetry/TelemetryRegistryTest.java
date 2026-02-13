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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TelemetryRegistryTest {

  private TelemetryRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new TelemetryRegistry();
  }

  @Test
  void rejectsDuplicateMetrics() {
    MetricDef def = sampleDef("metric.one");
    registry.register(def);
    assertThatThrownBy(() -> registry.register(def)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void requiredSubsetChecked() {
    assertThatThrownBy(
            () -> new MetricDef(sampleId("metric.two"), Set.of("required"), Set.of("other")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("allowedTags");
  }

  @Test
  void metricsReturnsCopy() {
    MetricDef def = sampleDef("metric.three");
    registry.register(def);
    var snapshot = registry.metrics();
    assertThat(snapshot).containsKey("metric.three");
    registry.register(new MetricDef(sampleId("metric.four"), Set.of(), Set.of()));
    assertThat(snapshot).doesNotContainKey("metric.four");
  }

  @Test
  void contributorRegistrationIsAtomic() {
    registry.register(new CoreTelemetryContributor());
    String rpcErrorsName = Telemetry.Metrics.RPC_ERRORS.name();
    TelemetryContributor badContributor =
        target -> target.register(new MetricDef(sampleId(rpcErrorsName), Set.of(), Set.of()));

    assertThatThrownBy(() -> registry.register(badContributor))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(registry.metrics()).containsKey(Telemetry.Metrics.RPC_ERRORS.name());
  }

  @Test
  void nullContributorThrows() {
    assertThatThrownBy(() -> registry.register((TelemetryContributor) null))
        .isInstanceOf(NullPointerException.class);
  }

  private MetricDef sampleDef(String name) {
    return new MetricDef(sampleId(name), Set.of(), Set.of());
  }

  private MetricId sampleId(String name) {
    return new MetricId(name, MetricType.COUNTER, "count", "v1", "core");
  }
}
