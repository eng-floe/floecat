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
import ai.floedb.floecat.service.statistics.scheduler.SchedulerSignalIndex;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileQueueMetrics {
  private static final Logger LOG = Logger.getLogger(ReconcileQueueMetrics.class);

  @Inject ReconcileJobStore jobs;
  @Inject Observability observability;
  @Inject Instance<SchedulerSignalIndex> signalIndexInstance;

  private final AtomicLong queued = new AtomicLong();
  private final AtomicLong running = new AtomicLong();
  private final AtomicLong cancelling = new AtomicLong();
  private final AtomicLong oldestAgeMs = new AtomicLong();
  private final AtomicLong healthBand = new AtomicLong();
  private final Map<StatsPriorityClass, AtomicLong> queuedByClass =
      new EnumMap<>(StatsPriorityClass.class);
  private final ConcurrentHashMap<String, AtomicLong> laneWaitAtomics = new ConcurrentHashMap<>();

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
      refreshLaneWaitGauges(stats.topLaneWaitMs);

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
      // Drain and emit signal counters.
      if (signalIndexInstance != null && !signalIndexInstance.isUnsatisfied()) {
        SchedulerSignalIndex si = signalIndexInstance.get();
        // Write-side counters: measure signal write throughput.
        drainAndEmitSignal(
            si.drainLastCaptureKnown(), si.drainLastCaptureUnknown(), "last_capture");
        drainAndEmitSignal(si.drainCoverageKnown(), si.drainCoverageUnknown(), "coverage");
        drainAndEmitSignal(si.drainDeltaKnown(), si.drainDeltaUnknown(), "delta");
        // Read-side miss counters: measure scoring-time signal availability.
        drainAndEmitLookupMiss(si.drainLastCaptureMiss(), "last_capture");
        drainAndEmitLookupMiss(si.drainCoverageMiss(), "coverage");
        drainAndEmitLookupMiss(si.drainDeltaMiss(), "delta");
      }
    } catch (RuntimeException e) {
      // Warn rather than debug: a silent metrics failure leaves all gauges stale.
      LOG.warnf(e, "Failed to refresh reconcile queue metrics");
    }
  }

  private void drainAndEmitSignal(long known, long unknown, String signalType) {
    Tag sigTag = Tag.of("signal_type", signalType);
    if (known > 0) {
      observability.counter(
          ServiceMetrics.Reconcile.SIGNAL_KNOWN, known, COMPONENT, OPERATION, sigTag);
    }
    if (unknown > 0) {
      observability.counter(
          ServiceMetrics.Reconcile.SIGNAL_UNKNOWN, unknown, COMPONENT, OPERATION, sigTag);
    }
  }

  private void drainAndEmitLookupMiss(long miss, String signalType) {
    if (miss > 0) {
      observability.counter(
          ServiceMetrics.Reconcile.SIGNAL_LOOKUP_MISS,
          miss,
          COMPONENT,
          OPERATION,
          Tag.of("signal_type", signalType));
    }
  }

  private static final Tag COMPONENT = Tag.of(TagKey.COMPONENT, "service");
  private static final Tag OPERATION = Tag.of(TagKey.OPERATION, "job_queue");
  private static final int MAX_LANE_WAIT_GAUGES = 200;

  private void refreshLaneWaitGauges(Map<String, Long> topLaneWaitMs) {
    if (topLaneWaitMs == null || topLaneWaitMs.isEmpty()) {
      for (AtomicLong value : laneWaitAtomics.values()) {
        value.set(0L);
      }
      return;
    }
    HashSet<String> active = new HashSet<>();
    for (Map.Entry<String, Long> entry : topLaneWaitMs.entrySet()) {
      String laneKey = normalizeLaneKey(entry.getKey());
      if (laneKey.isBlank()) {
        continue;
      }
      active.add(laneKey);
      AtomicLong gaugeValue = ensureLaneWaitGauge(laneKey);
      if (gaugeValue != null) {
        gaugeValue.set(Math.max(0L, entry.getValue() == null ? 0L : entry.getValue()));
      }
    }
    for (Map.Entry<String, AtomicLong> entry : laneWaitAtomics.entrySet()) {
      if (!active.contains(entry.getKey())) {
        entry.getValue().set(0L);
      }
    }
  }

  private AtomicLong ensureLaneWaitGauge(String laneKey) {
    AtomicLong existing = laneWaitAtomics.get(laneKey);
    if (existing != null) {
      return existing;
    }
    if (laneWaitAtomics.size() >= MAX_LANE_WAIT_GAUGES) {
      return null;
    }
    AtomicLong created = new AtomicLong();
    AtomicLong raced = laneWaitAtomics.putIfAbsent(laneKey, created);
    if (raced != null) {
      return raced;
    }
    observability.gauge(
        ServiceMetrics.Reconcile.LANE_WAIT_MS,
        created::get,
        "Oldest queued wait time in milliseconds for hot lanes",
        COMPONENT,
        OPERATION,
        Tag.of("lane_key", laneKey));
    return created;
  }

  private static String normalizeLaneKey(String laneKey) {
    return laneKey == null ? "" : laneKey.trim();
  }
}
