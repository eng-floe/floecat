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
    MetricDef captureNow = registry.metric(ServiceMetrics.Reconcile.CAPTURE_NOW.name());
    MetricDef startCapture = registry.metric(ServiceMetrics.Reconcile.START_CAPTURE.name());
    MetricDef getJob = registry.metric(ServiceMetrics.Reconcile.GET_JOB.name());
    MetricDef listJobs = registry.metric(ServiceMetrics.Reconcile.LIST_JOBS.name());
    MetricDef cancelJob = registry.metric(ServiceMetrics.Reconcile.CANCEL_JOB.name());
    MetricDef getSettings = registry.metric(ServiceMetrics.Reconcile.GET_SETTINGS.name());
    MetricDef updateSettings = registry.metric(ServiceMetrics.Reconcile.UPDATE_SETTINGS.name());
    MetricDef reconcileJobs = registry.metric(ServiceMetrics.Reconcile.JOBS.name());
    MetricDef reconcileJobLatency = registry.metric(ServiceMetrics.Reconcile.JOB_LATENCY.name());
    MetricDef viewsScanned = registry.metric(ServiceMetrics.Reconcile.VIEWS_SCANNED.name());
    MetricDef viewsChanged = registry.metric(ServiceMetrics.Reconcile.VIEWS_CHANGED.name());
    MetricDef queued = registry.metric(ServiceMetrics.Reconcile.JOBS_QUEUED.name());
    MetricDef oldestAge = registry.metric(ServiceMetrics.Reconcile.QUEUE_OLDEST_AGE.name());
    MetricDef plannerTicks = registry.metric(ServiceMetrics.Reconcile.PLANNER_TICKS.name());
    MetricDef plannerLatency =
        registry.metric(ServiceMetrics.Reconcile.PLANNER_TICK_LATENCY.name());
    MetricDef plannerEnqueue = registry.metric(ServiceMetrics.Reconcile.PLANNER_ENQUEUE.name());

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

    assertThat(captureNow).isNotNull();
    assertThat(captureNow.requiredTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER);
    assertThat(captureNow.allowedTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.TRIGGER, TagKey.REASON);

    assertThat(startCapture).isNotNull();
    assertThat(startCapture.requiredTags()).isEqualTo(captureNow.requiredTags());
    assertThat(startCapture.allowedTags()).isEqualTo(captureNow.allowedTags());

    assertThat(getJob).isNotNull();
    assertThat(getJob.requiredTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT);
    assertThat(getJob.allowedTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.REASON);
    assertThat(listJobs.requiredTags()).isEqualTo(getJob.requiredTags());
    assertThat(cancelJob.requiredTags()).isEqualTo(getJob.requiredTags());
    assertThat(getSettings.requiredTags()).isEqualTo(getJob.requiredTags());
    assertThat(updateSettings.requiredTags()).isEqualTo(getJob.requiredTags());

    assertThat(reconcileJobs).isNotNull();
    assertThat(reconcileJobs.requiredTags())
        .containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.MODE);
    assertThat(reconcileJobs.allowedTags())
        .containsExactlyInAnyOrder(
            TagKey.COMPONENT, TagKey.OPERATION, TagKey.RESULT, TagKey.MODE, TagKey.REASON);
    assertThat(reconcileJobLatency.requiredTags()).isEqualTo(reconcileJobs.requiredTags());
    assertThat(viewsScanned).isNotNull();
    assertThat(viewsScanned.requiredTags()).isEqualTo(reconcileJobs.requiredTags());
    assertThat(viewsScanned.allowedTags()).isEqualTo(reconcileJobs.allowedTags());
    assertThat(viewsChanged).isNotNull();
    assertThat(viewsChanged.requiredTags()).isEqualTo(reconcileJobs.requiredTags());
    assertThat(viewsChanged.allowedTags()).isEqualTo(reconcileJobs.allowedTags());

    assertThat(queued).isNotNull();
    assertThat(queued.requiredTags()).containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION);
    assertThat(queued.allowedTags()).containsExactlyInAnyOrder(TagKey.COMPONENT, TagKey.OPERATION);
    assertThat(oldestAge.requiredTags()).isEqualTo(queued.requiredTags());

    assertThat(plannerTicks).isNotNull();
    assertThat(plannerTicks.requiredTags()).isEqualTo(getJob.requiredTags());
    assertThat(plannerTicks.allowedTags()).isEqualTo(getJob.allowedTags());
    assertThat(plannerLatency.requiredTags()).isEqualTo(getJob.requiredTags());
    assertThat(plannerEnqueue.requiredTags()).isEqualTo(reconcileJobs.requiredTags());

    // Ensure the new Flight metric IDs are all present in the registry.
    assertThat(registry.metrics().keySet())
        .containsAll(
            Set.of(
                ServiceMetrics.Flight.REQUESTS.name(),
                ServiceMetrics.Flight.LATENCY.name(),
                ServiceMetrics.Flight.ERRORS.name(),
                ServiceMetrics.Flight.CANCELLED.name(),
                ServiceMetrics.Flight.INFLIGHT.name(),
                ServiceMetrics.Reconcile.CAPTURE_NOW.name(),
                ServiceMetrics.Reconcile.START_CAPTURE.name(),
                ServiceMetrics.Reconcile.GET_JOB.name(),
                ServiceMetrics.Reconcile.LIST_JOBS.name(),
                ServiceMetrics.Reconcile.CANCEL_JOB.name(),
                ServiceMetrics.Reconcile.GET_SETTINGS.name(),
                ServiceMetrics.Reconcile.UPDATE_SETTINGS.name(),
                ServiceMetrics.Reconcile.JOBS.name(),
                ServiceMetrics.Reconcile.JOB_LATENCY.name(),
                ServiceMetrics.Reconcile.SNAPSHOTS_PROCESSED.name(),
                ServiceMetrics.Reconcile.STATS_PROCESSED.name(),
                ServiceMetrics.Reconcile.TABLES_SCANNED.name(),
                ServiceMetrics.Reconcile.TABLES_CHANGED.name(),
                ServiceMetrics.Reconcile.VIEWS_SCANNED.name(),
                ServiceMetrics.Reconcile.VIEWS_CHANGED.name(),
                ServiceMetrics.Reconcile.ERRORS.name(),
                ServiceMetrics.Reconcile.JOBS_QUEUED.name(),
                ServiceMetrics.Reconcile.JOBS_RUNNING.name(),
                ServiceMetrics.Reconcile.JOBS_CANCELLING.name(),
                ServiceMetrics.Reconcile.QUEUE_OLDEST_AGE.name(),
                ServiceMetrics.Reconcile.PLANNER_TICKS.name(),
                ServiceMetrics.Reconcile.PLANNER_TICK_LATENCY.name(),
                ServiceMetrics.Reconcile.PLANNER_ENQUEUE.name()));
  }
}
