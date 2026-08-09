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

package ai.floedb.floecat.service.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Step 5 of docs/telemetry/overview.md "Adding new metrics": prove the contributor actually loads
 * through {@code ServiceLoader} and that the contract entries exist, before anything runs.
 *
 * <p>The docgen compares names only, so the declared type is asserted here: the backend validates
 * it against the {@code Observability} method used to publish, throwing under the strict policy
 * tests run with and dropping the meter otherwise.
 */
class MetadataIoMetricsContractTest {

  private static final List<MetricId> ADMISSION_METRICS =
      List.of(
          ServiceMetrics.MetadataIo.PERMITS_CAPACITY,
          ServiceMetrics.MetadataIo.PERMITS_IN_USE,
          ServiceMetrics.MetadataIo.ADMISSION_WAITERS,
          ServiceMetrics.MetadataIo.ADMISSION_SATURATED_WAITS);

  @Test
  void everyAdmissionMetricIsRegisteredByTheServiceContributor() {
    TelemetryRegistry registry = Telemetry.newRegistryWithCore();

    for (MetricId metric : ADMISSION_METRICS) {
      MetricDef def = Telemetry.requireMetricDef(registry, metric);
      assertTrue(
          def.description() != null && !def.description().isBlank(),
          metric.name() + " must carry a description for the published contract");
      assertTrue(
          def.requiredTags().contains(TagKey.COMPONENT)
              && def.requiredTags().contains(TagKey.OPERATION),
          metric.name() + " must require the component/operation tags the bean supplies");
    }
  }

  @Test
  void theRegisteredDescriptionIsTheOneTheContractPublishes() {
    // Observability.gauge exports its description argument verbatim as Prometheus HELP, unlike
    // counter/timer which read it from the registry. Both paths must share the same constants.
    TelemetryRegistry registry = Telemetry.newRegistryWithCore();
    for (var pair :
        List.of(
            Map.entry(
                ServiceMetrics.MetadataIo.PERMITS_CAPACITY,
                ServiceMetrics.MetadataIo.CAPACITY_DESC),
            Map.entry(
                ServiceMetrics.MetadataIo.PERMITS_IN_USE, ServiceMetrics.MetadataIo.IN_USE_DESC),
            Map.entry(
                ServiceMetrics.MetadataIo.ADMISSION_WAITERS,
                ServiceMetrics.MetadataIo.WAITERS_DESC),
            Map.entry(
                ServiceMetrics.MetadataIo.ADMISSION_SATURATED_WAITS,
                ServiceMetrics.MetadataIo.SATURATED_DESC))) {
      assertEquals(
          pair.getValue(),
          Telemetry.requireMetricDef(registry, pair.getKey()).description(),
          pair.getKey().name() + " must publish exactly the contract description");
    }
  }

  @Test
  void everyAdmissionMetricIsDeclaredAsTheKindItIsPublishedAs() {
    // Observability validates the declared type against the method used to publish, on every
    // emission. Nothing checks this at compile time.
    for (MetricId gauge :
        List.of(
            ServiceMetrics.MetadataIo.PERMITS_CAPACITY,
            ServiceMetrics.MetadataIo.PERMITS_IN_USE,
            ServiceMetrics.MetadataIo.ADMISSION_WAITERS)) {
      assertEquals(
          MetricType.GAUGE, gauge.type(), gauge.name() + " is published via Observability.gauge");
    }
    assertEquals(
        MetricType.COUNTER,
        ServiceMetrics.MetadataIo.ADMISSION_SATURATED_WAITS.type(),
        "saturated waits are published via Observability.counter from the saturation sink");
  }
}
