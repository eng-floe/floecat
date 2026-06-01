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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileQueueMetrics {
  private static final Logger LOG = Logger.getLogger(ReconcileQueueMetrics.class);

  @Inject ReconcileJobStore jobs;
  @Inject Observability observability;

  private final AtomicLong queued = new AtomicLong();
  private final AtomicLong running = new AtomicLong();
  private final AtomicLong cancelling = new AtomicLong();
  private final AtomicLong oldestAgeMs = new AtomicLong();
  private final AtomicLong healthBand = new AtomicLong();
  private final Map<StatsPriorityClass, AtomicLong> queuedByClass =
      new EnumMap<>(StatsPriorityClass.class);

  // Delta-tracking for counter metrics (emit only the increment since last refresh).
  private long lastAgingPromotionsTotal = 0L;
  private final Map<StatsPriorityClass, Long> lastAdmissionDeferred =
      new EnumMap<>(StatsPriorityClass.class);

  @PostConstruct
  void init() {
    Tag component = Tag.of(TagKey.COMPONENT, "service");
    Tag operation = Tag.of(TagKey.OPERATION, "job_queue");
    observability.gauge(
        ServiceMetrics.Reconcile.JOBS_QUEUED,
        queued::get,
        "Current number of queued reconcile jobs",
        component,
        operation);
    observability.gauge(
        ServiceMetrics.Reconcile.JOBS_RUNNING,
        running::get,
        "Current number of running reconcile jobs",
        component,
        operation);
    observability.gauge(
        ServiceMetrics.Reconcile.JOBS_CANCELLING,
        cancelling::get,
        "Current number of reconcile jobs waiting for cancellation",
        component,
        operation);
    observability.gauge(
        ServiceMetrics.Reconcile.QUEUE_OLDEST_AGE,
        oldestAgeMs::get,
        "Age in milliseconds of the oldest queued reconcile job",
        component,
        operation);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      AtomicLong counter = new AtomicLong();
      queuedByClass.put(cls, counter);
      observability.gauge(
          ServiceMetrics.Reconcile.QUEUE_DEPTH_BY_CLASS,
          counter::get,
          "Current number of queued reconcile jobs in priority class " + cls.name(),
          component,
          operation,
          Tag.of("priority_class", cls.name().toLowerCase()));
      lastAdmissionDeferred.put(cls, 0L);
    }
    // Value = SchedulerHealthBand.ordinal(): 0=GREEN 1=YELLOW 2=ORANGE 3=RED.
    // Autoscaler rules depend on this mapping — do not reorder SchedulerHealthBand without
    // updating dashboards and alert thresholds.
    observability.gauge(
        ServiceMetrics.Reconcile.HEALTH_BAND,
        healthBand::get,
        "Current scheduler health band (0=GREEN 1=YELLOW 2=ORANGE 3=RED)",
        component,
        operation);
    refresh();
  }

  @Scheduled(every = "{floecat.metrics.reconcile.refresh:15s}")
  void refresh() {
    try {
      var stats = jobs.queueStats();
      queued.set(stats.queued);
      running.set(stats.running);
      cancelling.set(stats.cancelling);
      long oldestCreatedAt = stats.oldestQueuedCreatedAtMs;
      long oldestAge =
          oldestCreatedAt > 0L ? Math.max(0L, System.currentTimeMillis() - oldestCreatedAt) : 0L;
      oldestAgeMs.set(oldestAge);
      for (StatsPriorityClass cls : StatsPriorityClass.values()) {
        AtomicLong counter = queuedByClass.get(cls);
        if (counter != null) {
          counter.set(stats.queuedByClass.getOrDefault(cls, 0L));
        }
      }
      healthBand.set(stats.healthBand.ordinal());

      // Delta-emit AGING_PROMOTIONS counter.
      long newPromotions = stats.agingPromotionsTotal;
      long promotionsDelta = newPromotions - lastAgingPromotionsTotal;
      if (promotionsDelta > 0) {
        observability.counter(
            ServiceMetrics.Reconcile.AGING_PROMOTIONS, promotionsDelta, COMPONENT, OPERATION);
        lastAgingPromotionsTotal = newPromotions;
      }

      // Delta-emit ADMISSION_DEFERRED counter per class.
      for (StatsPriorityClass cls : StatsPriorityClass.values()) {
        long newDeferred = stats.admissionDeferredByClass.getOrDefault(cls, 0L);
        long deferredDelta = newDeferred - lastAdmissionDeferred.getOrDefault(cls, 0L);
        if (deferredDelta > 0) {
          observability.counter(
              ServiceMetrics.Reconcile.ADMISSION_DEFERRED,
              deferredDelta,
              COMPONENT,
              OPERATION,
              Tag.of("priority_class", cls.name().toLowerCase()));
          lastAdmissionDeferred.put(cls, newDeferred);
        }
      }
    } catch (RuntimeException e) {
      // Warn rather than debug: a silent metrics failure leaves all gauges stale.
      LOG.warnf(e, "Failed to refresh reconcile queue metrics");
    }
  }

  private static final Tag COMPONENT = Tag.of(TagKey.COMPONENT, "service");
  private static final Tag OPERATION = Tag.of(TagKey.OPERATION, "job_queue");
}
