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

package ai.floedb.floecat.service.statistics.scheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.NoopObservability;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.ObservationScope;
import ai.floedb.floecat.telemetry.Tag;
import jakarta.enterprise.inject.Instance;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link SchedulerPolicyRegistry#init()} emits the {@code POLICY_PROFILE} info gauge
 * with the correct {@code profile_name} tag.
 *
 * <p>Uses Mockito to stub CDI {@link Instance} types so no CDI container is needed.
 */
@SuppressWarnings("unchecked")
class SchedulerPolicyRegistryProfileGaugeTest {

  /** Verify that {@code init()} registers the POLICY_PROFILE gauge with the configured name. */
  @Test
  void initEmitsPolicyProfileGaugeWithProfileName() {
    // ---- Recording Observability ----
    List<GaugeRegistration> registrations = new ArrayList<>();
    var recordingObs = new CapturingGaugeObservability(registrations);

    // ---- Stub CDI Instance<> types ----
    var profile =
        new DefaultSchedulerProfile(3, 2, 1, 86_400_000L, 0.15, 50L, new NoopObservability());
    Instance<SchedulerPriorityPolicy> priorityInstance = mock(Instance.class);
    Instance<SchedulerAdmissionPolicy> admissionInstance = mock(Instance.class);
    Instance<SchedulerPreemptionPolicy> preemptionInstance = mock(Instance.class);
    ReconcileJobStore jobs = mock(ReconcileJobStore.class);

    when(priorityInstance.select(
            org.mockito.ArgumentMatchers
                .<jakarta.enterprise.util.AnnotationLiteral<SchedulerProfile>>any()))
        .thenReturn(priorityInstance);
    when(admissionInstance.select(
            org.mockito.ArgumentMatchers
                .<jakarta.enterprise.util.AnnotationLiteral<SchedulerProfile>>any()))
        .thenReturn(admissionInstance);
    when(preemptionInstance.select(
            org.mockito.ArgumentMatchers
                .<jakarta.enterprise.util.AnnotationLiteral<SchedulerProfile>>any()))
        .thenReturn(preemptionInstance);

    when(priorityInstance.isUnsatisfied()).thenReturn(false);
    when(admissionInstance.isUnsatisfied()).thenReturn(false);
    when(preemptionInstance.isUnsatisfied()).thenReturn(true); // no preemption policy

    when(priorityInstance.get()).thenReturn(profile);
    when(admissionInstance.get()).thenReturn(profile);

    var queueStats = new ReconcileJobStore.QueueStats(0L, 0L, 0L, 0L);
    when(jobs.queueStats()).thenReturn(queueStats);

    var registry =
        new SchedulerPolicyRegistry(
            "my-test-profile",
            priorityInstance,
            admissionInstance,
            preemptionInstance,
            jobs,
            recordingObs,
            null);

    // ---- Trigger @PostConstruct ----
    registry.init();

    // ---- Verify POLICY_PROFILE gauge was registered ----
    List<GaugeRegistration> profileGauges =
        registrations.stream()
            .filter(r -> ServiceMetrics.Reconcile.POLICY_PROFILE.equals(r.metric))
            .toList();

    assertFalse(profileGauges.isEmpty(), "POLICY_PROFILE gauge must be registered during init()");
    assertEquals(1, profileGauges.size(), "Exactly one POLICY_PROFILE gauge should be registered");

    GaugeRegistration reg = profileGauges.get(0);

    // Value must be 1 (info gauge convention).
    assertEquals(1L, reg.supplier.get().longValue(), "POLICY_PROFILE gauge value must be 1");

    // profile_name tag must carry the configured profile name.
    boolean hasProfileNameTag = false;
    for (Tag tag : reg.tags) {
      if ("profile_name".equals(tag.key()) && "my-test-profile".equals(tag.value())) {
        hasProfileNameTag = true;
        break;
      }
    }
    assertTrue(
        hasProfileNameTag, "POLICY_PROFILE gauge must have profile_name=my-test-profile tag");
  }

  // ---------------------------------------------------------------------------
  // Helper types
  // ---------------------------------------------------------------------------

  private record GaugeRegistration(
      MetricId metric, Supplier<? extends Number> supplier, Tag[] tags) {}

  /**
   * Minimal {@link Observability} that captures {@code gauge()} registrations. All other methods
   * are no-ops.
   */
  private static final class CapturingGaugeObservability implements Observability {

    private static final ObservationScope NOOP_SCOPE =
        new ObservationScope() {
          @Override
          public void success() {}

          @Override
          public void error(Throwable t) {}

          @Override
          public void retry() {}
        };

    private final List<GaugeRegistration> registrations;

    CapturingGaugeObservability(List<GaugeRegistration> registrations) {
      this.registrations = registrations;
    }

    @Override
    public void counter(MetricId metric, double amount, Tag... tags) {}

    @Override
    public void summary(MetricId metric, double value, Tag... tags) {}

    @Override
    public void timer(MetricId metric, Duration duration, Tag... tags) {}

    @Override
    public <T extends Number> void gauge(
        MetricId metric, Supplier<T> supplier, String description, Tag... tags) {
      registrations.add(new GaugeRegistration(metric, supplier, tags));
    }

    @Override
    public ObservationScope observe(
        Category category, String component, String operation, Tag... tags) {
      return NOOP_SCOPE;
    }
  }
}
