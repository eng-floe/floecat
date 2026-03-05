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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.telemetry.MetricId;
import ai.floedb.floecat.telemetry.MetricType;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import ai.floedb.floecat.telemetry.Telemetry.TagKey;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcilerScheduler {
  private static final MetricId RECONCILE_JOBS =
      new MetricId("floecat.service.reconcile.jobs.total", MetricType.COUNTER, "", "v1", "service");
  private static final MetricId RECONCILE_JOB_LATENCY =
      new MetricId(
          "floecat.service.reconcile.job.latency", MetricType.TIMER, "ms", "v1", "service");
  private static final MetricId RECONCILE_SNAPSHOTS_PROCESSED =
      new MetricId(
          "floecat.service.reconcile.snapshots_processed.total",
          MetricType.COUNTER,
          "",
          "v1",
          "service");
  private static final MetricId RECONCILE_STATS_PROCESSED =
      new MetricId(
          "floecat.service.reconcile.stats_processed.total",
          MetricType.COUNTER,
          "",
          "v1",
          "service");
  private static final MetricId RECONCILE_TABLES_SCANNED =
      new MetricId(
          "floecat.service.reconcile.tables_scanned.total",
          MetricType.COUNTER,
          "",
          "v1",
          "service");
  private static final MetricId RECONCILE_TABLES_CHANGED =
      new MetricId(
          "floecat.service.reconcile.tables_changed.total",
          MetricType.COUNTER,
          "",
          "v1",
          "service");
  private static final MetricId RECONCILE_ERRORS =
      new MetricId(
          "floecat.service.reconcile.errors.total", MetricType.COUNTER, "", "v1", "service");
  private static final long DEFAULT_LEASE_HEARTBEAT_MS = 2_000L;
  private static final long MIN_LEASE_HEARTBEAT_MS = 1_000L;
  private static final long MIN_CANCEL_CHECK_MS = 500L;
  private static final int DEFAULT_MAX_PARALLELISM = 1;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcilerService reconcilerService;
  @Inject ReconcileCancellationRegistry cancellations;
  @Inject Observability observability;
  @Inject Config config;
  private static final Logger LOG = Logger.getLogger(ReconcilerScheduler.class);

  private final AtomicBoolean polling = new AtomicBoolean(false);
  private final AtomicInteger inFlight = new AtomicInteger(0);
  private volatile int maxParallelism = DEFAULT_MAX_PARALLELISM;
  private volatile ExecutorService workers;

  @PostConstruct
  void init() {
    maxParallelism =
        Math.max(
            1,
            config
                .getOptionalValue("reconciler.max-parallelism", Integer.class)
                .orElse(DEFAULT_MAX_PARALLELISM));
    workers = Executors.newFixedThreadPool(maxParallelism);
  }

  @PreDestroy
  void destroy() {
    ExecutorService executor = workers;
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Scheduled(
      every = "{reconciler.pollEvery:1s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void pollOnce() {
    if (!polling.compareAndSet(false, true)) {
      return;
    }
    try {
      while (reserveWorkerSlot()) {
        var lease = jobs.leaseNext().orElse(null);
        if (lease == null) {
          inFlight.decrementAndGet();
          return;
        }
        submitLease(lease);
      }
    } finally {
      polling.set(false);
    }
  }

  private boolean reserveWorkerSlot() {
    while (true) {
      int current = inFlight.get();
      if (current >= maxParallelism) {
        return false;
      }
      if (inFlight.compareAndSet(current, current + 1)) {
        return true;
      }
    }
  }

  private void submitLease(ReconcileJobStore.LeasedJob lease) {
    ExecutorService executor = workers;
    if (executor == null) {
      inFlight.decrementAndGet();
      return;
    }
    try {
      executor.submit(
          () -> {
            try {
              runLease(lease);
            } finally {
              inFlight.decrementAndGet();
            }
          });
    } catch (RuntimeException e) {
      inFlight.decrementAndGet();
      throw e;
    }
  }

  private void runLease(ReconcileJobStore.LeasedJob lease) {
    cancellations.register(lease.jobId, Thread.currentThread());
    long started = System.currentTimeMillis();
    long leaseMs =
        Math.max(
            1_000L,
            config.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class).orElse(30_000L));
    long suggestedHeartbeatMs = Math.max(MIN_LEASE_HEARTBEAT_MS, leaseMs / 3L);
    long heartbeatEveryMs =
        Math.max(
            MIN_LEASE_HEARTBEAT_MS,
            config
                .getOptionalValue("reconciler.lease-heartbeat-ms", Long.class)
                .orElse(Math.max(DEFAULT_LEASE_HEARTBEAT_MS, suggestedHeartbeatMs)));
    long[] nextHeartbeatAtMs = {started + heartbeatEveryMs};
    long cancelCheckEveryMs = Math.max(MIN_CANCEL_CHECK_MS, heartbeatEveryMs / 2L);
    long[] nextCancelCheckAtMs = {started};
    boolean[] cancellationRequested = {false};
    AtomicBoolean leaseValid = new AtomicBoolean(true);
    BooleanSupplier heartbeat =
        () -> {
          if (!leaseValid.get()) {
            return false;
          }
          long now = System.currentTimeMillis();
          if (now < nextHeartbeatAtMs[0]) {
            return true;
          }
          boolean renewed = jobs.renewLease(lease.jobId, lease.leaseEpoch);
          if (!renewed) {
            leaseValid.set(false);
            LOG.warnf("Reconcile lease renewal failed for job %s; aborting worker path", lease.jobId);
            return false;
          }
          nextHeartbeatAtMs[0] = now + heartbeatEveryMs;
          return true;
        };

    jobs.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis());
    jobs.markProgress(lease.jobId, lease.leaseEpoch, 0, 0, 0, 0, 0, "Running reconcile");
    BooleanSupplier cancelRequested =
        () -> {
          if (!heartbeat.getAsBoolean() || Thread.currentThread().isInterrupted()) {
            return true;
          }
          long now = System.currentTimeMillis();
          if (now >= nextCancelCheckAtMs[0]) {
            cancellationRequested[0] = jobs.isCancellationRequested(lease.jobId);
            nextCancelCheckAtMs[0] = now + cancelCheckEveryMs;
          }
          return cancellationRequested[0];
        };

    try {
      var connectorId =
          ResourceId.newBuilder()
              .setAccountId(lease.accountId)
              .setId(lease.connectorId)
              .setKind(ResourceKind.RK_CONNECTOR)
              .build();

      var principal =
          PrincipalContext.newBuilder()
              .setAccountId(lease.accountId)
              .setSubject("reconciler.scheduler")
              .setCorrelationId("reconciler-job-" + lease.jobId)
              .build();
      if (cancelRequested.getAsBoolean()) {
        long now = System.currentTimeMillis();
        jobs.markCancelled(lease.jobId, lease.leaseEpoch, now, "Cancelled", 0, 0, 0, 0, 0);
        emitOutcome(lease, "cancelled", now - started, 0, 0, 0, 0, 0, null);
        return;
      }
      var result =
          reconcilerService.reconcile(
              principal,
              connectorId,
              lease.fullRescan,
              lease.scope,
              lease.captureMode,
              null,
              cancelRequested,
              (scanned, changed, errors, snapshotsProcessed, statsProcessed, message) -> {
                if (!heartbeat.getAsBoolean()) {
                  return;
                }
                jobs.markProgress(
                    lease.jobId,
                    lease.leaseEpoch,
                    scanned,
                    changed,
                    errors,
                    snapshotsProcessed,
                    statsProcessed,
                    message);
              });
      if (result.cancelled() || cancelRequested.getAsBoolean()) {
        long now = System.currentTimeMillis();
        jobs.markCancelled(
            lease.jobId,
            lease.leaseEpoch,
            now,
            result.message(),
            result.scanned,
            result.changed,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed);
        emitOutcome(
            lease,
            "cancelled",
            now - started,
            result.scanned,
            result.changed,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed,
            null);
        return;
      }
      if (!result.ok()) {
        long now = System.currentTimeMillis();
        jobs.markFailed(
            lease.jobId,
            lease.leaseEpoch,
            now,
            result.message(),
            result.scanned,
            result.changed,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed);
        emitOutcome(
            lease,
            "failed",
            now - started,
            result.scanned,
            result.changed,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed,
            null);
        return;
      }
      long finished = System.currentTimeMillis();
      long totalSnapshots = result.snapshotsProcessed;
      long totalStats = result.statsProcessed;
      if (result.cancelled() || cancelRequested.getAsBoolean()) {
        jobs.markCancelled(
            lease.jobId,
            lease.leaseEpoch,
            finished,
            result.message(),
            result.scanned,
            result.changed,
            result.errors,
            totalSnapshots,
            totalStats);
        emitOutcome(
            lease,
            "cancelled",
            finished - started,
            result.scanned,
            result.changed,
            result.errors,
            totalSnapshots,
            totalStats,
            null);
        return;
      }
      jobs.markSucceeded(
          lease.jobId,
          lease.leaseEpoch,
          finished,
          result.scanned,
          result.changed,
          totalSnapshots,
          totalStats);
      emitOutcome(
          lease,
          "succeeded",
          finished - started,
          result.scanned,
          result.changed,
          result.errors,
          totalSnapshots,
          totalStats,
          null);
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted() || jobs.isCancellationRequested(lease.jobId)) {
        long now = System.currentTimeMillis();
        jobs.markCancelled(lease.jobId, lease.leaseEpoch, now, "Cancelled", 0, 0, 0, 0, 0);
        emitOutcome(lease, "cancelled", now - started, 0, 0, 0, 0, 0, null);
        Thread.interrupted();
      } else {
        var msg = describeFailure(e);
        long now = System.currentTimeMillis();
        jobs.markFailed(lease.jobId, lease.leaseEpoch, now, msg, 0, 0, 1, 0, 0);
        emitOutcome(lease, "failed", now - started, 0, 0, 1, 0, 0, normalizeReason(e));
      }
    } finally {
      cancellations.unregister(lease.jobId, Thread.currentThread());
    }
  }

  private static String describeFailure(Throwable t) {
    if (t == null) {
      return "Unknown error";
    }
    var seen = new java.util.HashSet<Throwable>();
    var parts = new java.util.ArrayList<String>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      seen.add(cur);
      parts.add(renderThrowable(cur));
      cur = cur.getCause();
    }
    return String.join(" | caused by: ", parts);
  }

  private static String renderThrowable(Throwable t) {
    if (t instanceof io.grpc.StatusRuntimeException sre) {
      var status = sre.getStatus();
      String desc = status.getDescription();
      if (desc == null || desc.isBlank()) {
        desc = sre.getMessage();
      }
      if (desc == null || desc.isBlank()) {
        return "grpc=" + status.getCode();
      }
      return "grpc=" + status.getCode() + " desc=" + desc;
    }
    String msg = t.getMessage();
    String cls = t.getClass().getSimpleName();
    if (msg == null || msg.isBlank()) {
      return cls;
    }
    return cls + ": " + msg;
  }

  private void emitOutcome(
      ReconcileJobStore.LeasedJob lease,
      String result,
      long durationMs,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String reason) {
    observeOutcome(
        lease,
        result,
        durationMs,
        tablesScanned,
        tablesChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        reason);
    LOG.infof(
        "Reconcile job outcome account=%s connector=%s result=%s duration_ms=%d snapshots_processed=%d stats_processed=%d",
        lease.accountId,
        lease.connectorId,
        result,
        Math.max(0L, durationMs),
        Math.max(0L, snapshotsProcessed),
        Math.max(0L, statsProcessed));
  }

  private void observeOutcome(
      ReconcileJobStore.LeasedJob lease,
      String result,
      long durationMs,
      long tablesScanned,
      long tablesChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String reason) {
    if (observability == null) {
      return;
    }
    Tag[] tags = outcomeTags(lease, result, reason);
    observability.counter(RECONCILE_JOBS, 1.0, tags);
    observability.timer(RECONCILE_JOB_LATENCY, Duration.ofMillis(Math.max(0L, durationMs)), tags);
    observability.counter(RECONCILE_TABLES_SCANNED, Math.max(0L, tablesScanned), tags);
    observability.counter(RECONCILE_TABLES_CHANGED, Math.max(0L, tablesChanged), tags);
    observability.counter(RECONCILE_ERRORS, Math.max(0L, errors), tags);
    observability.counter(RECONCILE_SNAPSHOTS_PROCESSED, Math.max(0L, snapshotsProcessed), tags);
    observability.counter(RECONCILE_STATS_PROCESSED, Math.max(0L, statsProcessed), tags);
  }

  private static Tag[] outcomeTags(
      ReconcileJobStore.LeasedJob lease, String result, String reason) {
    String mode = lease != null && lease.fullRescan ? "full" : "incremental";
    if (reason == null || reason.isBlank()) {
      return new Tag[] {
        Tag.of(TagKey.COMPONENT, "service"),
        Tag.of(TagKey.OPERATION, "job_execute"),
        Tag.of(TagKey.RESULT, result),
        Tag.of(TagKey.MODE, mode)
      };
    }
    return new Tag[] {
      Tag.of(TagKey.COMPONENT, "service"),
      Tag.of(TagKey.OPERATION, "job_execute"),
      Tag.of(TagKey.RESULT, result),
      Tag.of(TagKey.MODE, mode),
      Tag.of(TagKey.REASON, reason)
    };
  }

  private static String normalizeReason(Throwable t) {
    if (t == null) {
      return "unknown";
    }
    String simple = t.getClass().getSimpleName();
    if (simple == null || simple.isBlank()) {
      return "runtime_exception";
    }
    return simple
        .replaceAll("([a-z])([A-Z])", "$1_$2")
        .replaceAll("[^A-Za-z0-9_]+", "_")
        .toLowerCase(java.util.Locale.ROOT);
  }
}
