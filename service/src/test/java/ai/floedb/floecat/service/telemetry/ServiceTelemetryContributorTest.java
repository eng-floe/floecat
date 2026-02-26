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

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.telemetry.MetricDef;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import ai.floedb.floecat.telemetry.TelemetryRegistry;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ServiceTelemetryContributorTest {

  @Test
  void registersFlightMetricContractsWithExpectedTags() {
    TelemetryRegistry registry = new TelemetryRegistry();
    registry.register(new ServiceTelemetryContributor());

    MetricDef requests = registry.metric(ServiceMetrics.Flight.REQUESTS.name());
    MetricDef latency = registry.metric(ServiceMetrics.Flight.LATENCY.name());
    MetricDef errors = registry.metric(ServiceMetrics.Flight.ERRORS.name());
    MetricDef cancelled = registry.metric(ServiceMetrics.Flight.CANCELLED.name());
    MetricDef inflight = registry.metric(ServiceMetrics.Flight.INFLIGHT.name());
    MetricDef syncCapture = registry.metric(ServiceMetrics.Reconcile.SYNC_CAPTURE.name());
    MetricDef triggerReconcile = registry.metric(ServiceMetrics.Reconcile.TRIGGER.name());

    assertThat(requests).isNotNull();
    assertThat(requests.requiredTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION, TagKey.STATUS);
    assertThat(requests.allowedTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.STATUS, TagKey.RESOURCE, TagKey.REASON);

    assertThat(latency).isNotNull();
    assertThat(latency.requiredTags()).isEqualTo(requests.requiredTags());
    assertThat(latency.allowedTags()).isEqualTo(requests.allowedTags());

    assertThat(errors).isNotNull();
    assertThat(errors.requiredTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION, TagKey.REASON);
    assertThat(errors.allowedTags()).isEqualTo(requests.allowedTags());

    assertThat(cancelled).isNotNull();
    assertThat(cancelled.requiredTags()).isEqualTo(errors.requiredTags());
    assertThat(cancelled.allowedTags()).isEqualTo(errors.allowedTags());

    assertThat(inflight).isNotNull();
    assertThat(inflight.requiredTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION);
    assertThat(inflight.allowedTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESOURCE);

    assertThat(syncCapture).isNotNull();
    assertThat(syncCapture.requiredTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER);
    assertThat(syncCapture.allowedTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER, TagKey.REASON);

    assertThat(triggerReconcile).isNotNull();
    assertThat(triggerReconcile.requiredTags()).isEqualTo(syncCapture.requiredTags());
    assertThat(triggerReconcile.allowedTags()).isEqualTo(syncCapture.allowedTags());

    // Ensure the new Flight metric IDs are all present in the registry.
    assertThat(registry.metrics().keySet())
        .containsAll(
            Set.of(
                ServiceMetrics.Flight.REQUESTS.name(),
                ServiceMetrics.Flight.LATENCY.name(),
                ServiceMetrics.Flight.ERRORS.name(),
                ServiceMetrics.Flight.CANCELLED.name(),
                ServiceMetrics.Flight.INFLIGHT.name(),
                ServiceMetrics.Reconcile.SYNC_CAPTURE.name(),
                ServiceMetrics.Reconcile.TRIGGER.name()));
  }
}
