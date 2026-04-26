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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
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
  private static final MetricId RECONCILE_VIEWS_SCANNED =
      new MetricId(
          "floecat.service.reconcile.views_scanned.total", MetricType.COUNTER, "", "v1", "service");
  private static final MetricId RECONCILE_VIEWS_CHANGED =
      new MetricId(
          "floecat.service.reconcile.views_changed.total", MetricType.COUNTER, "", "v1", "service");
  private static final MetricId RECONCILE_ERRORS =
      new MetricId(
          "floecat.service.reconcile.errors.total", MetricType.COUNTER, "", "v1", "service");
  private static final long DEFAULT_LEASE_HEARTBEAT_MS = 2_000L;
  private static final long MIN_LEASE_HEARTBEAT_MS = 1_000L;
  private static final long MIN_CANCEL_CHECK_MS = 500L;
  private static final int DEFAULT_MAX_PARALLELISM = 1;

  @Inject ReconcileJobStore jobs;
  @Inject ReconcileExecutorRegistry executorRegistry;
  @Inject ReconcileCancellationRegistry cancellations;
  @Inject Observability observability;
  @Inject Config config;

  @ConfigProperty(name = "floecat.reconciler.scheduler.enabled", defaultValue = "true")
  boolean schedulerEnabled;

  private static final Logger LOG = Logger.getLogger(ReconcilerScheduler.class);

  private final AtomicBoolean polling = new AtomicBoolean(false);
  private final AtomicInteger inFlight = new AtomicInteger(0);
  private volatile int maxParallelism = DEFAULT_MAX_PARALLELISM;
  private volatile ExecutorService workers;

  @PostConstruct
  void init() {
    if (!schedulerEnabled) {
      return;
    }
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
    if (!schedulerEnabled || executorRegistry.orderedExecutors().isEmpty()) {
      return;
    }
    if (!polling.compareAndSet(false, true)) {
      return;
    }
    try {
      ReconcileJobStore.LeaseRequest leaseRequest = executorRegistry.leaseRequest();
      while (reserveWorkerSlot()) {
        Optional<ReconcileJobStore.LeasedJob> leaseOpt;
        try {
          leaseOpt = jobs.leaseNext(leaseRequest);
        } catch (Exception e) {
          inFlight.decrementAndGet();
          LOG.errorf(e, "leaseNext() threw unexpectedly; releasing worker slot");
          return;
        }
        var lease = leaseOpt.orElse(null);
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

  void runLease(ReconcileJobStore.LeasedJob lease) {
    long started = System.currentTimeMillis();
    long leaseMs =
        Math.max(
            1_000L,
            config
                .getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class)
                .orElse(30_000L));
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
    AtomicBoolean interrupted = new AtomicBoolean(false);
    ProgressSnapshot progress = new ProgressSnapshot();
    ReconcileExecutor executor = null;
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
            LOG.warnf(
                "Reconcile lease renewal failed for job %s; aborting worker path", lease.jobId);
            return false;
          }
          nextHeartbeatAtMs[0] = now + heartbeatEveryMs;
          return true;
        };

    BooleanSupplier cancellationRequestedInStore =
        () -> {
          long now = System.currentTimeMillis();
          if (now >= nextCancelCheckAtMs[0]) {
            cancellationRequested[0] = jobs.isCancellationRequested(lease.jobId);
            nextCancelCheckAtMs[0] = now + cancelCheckEveryMs;
          }
          return cancellationRequested[0];
        };
    BooleanSupplier cancelRequested =
        () -> {
          if (Thread.currentThread().isInterrupted()) {
            interrupted.set(true);
            return true;
          }
          return cancellationRequestedInStore.getAsBoolean();
        };
    if (!heartbeat.getAsBoolean()) {
      long now = System.currentTimeMillis();
      emitOutcome(lease, "lease_lost", now - started, 0, 0, 0, 0, 0, 0, 0, "lease_lost");
      return;
    }
    try {
      executor =
          executorRegistry
              .executorFor(lease)
              .or(
                  () ->
                      executorRegistry.orderedExecutors().stream()
                          .filter(candidate -> candidate.supportsJobKind(lease.jobKind))
                          .filter(
                              candidate ->
                                  candidate.supportsExecutionClass(
                                      lease.executionPolicy.executionClass()))
                          .filter(candidate -> candidate.supportsLane(lease.executionPolicy.lane()))
                          .filter(candidate -> candidate.supports(lease))
                          .findFirst())
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "No reconcile executor available for lease " + lease.jobId));
      cancellations.register(lease.jobId, Thread.currentThread());
      jobs.markRunning(lease.jobId, lease.leaseEpoch, System.currentTimeMillis(), executor.id());
      jobs.markProgress(lease.jobId, lease.leaseEpoch, 0, 0, 0, 0, 0, 0, 0, "Running reconcile");
      BooleanSupplier shouldStop =
          () -> {
            if (!leaseValid.get()) {
              return true;
            }
            if (Thread.currentThread().isInterrupted()) {
              interrupted.set(true);
              return true;
            }
            return cancelRequested.getAsBoolean();
          };
      if (cancellationRequestedInStore.getAsBoolean()) {
        long now = System.currentTimeMillis();
        jobs.markCancelled(lease.jobId, lease.leaseEpoch, now, "Cancelled", 0, 0, 0, 0, 0, 0, 0);
        emitOutcome(lease, "cancelled", now - started, 0, 0, 0, 0, 0, 0, 0, null);
        return;
      }
      var result =
          executor.execute(
              new ReconcileExecutor.ExecutionContext(
                  lease,
                  shouldStop,
                  (tablesScanned,
                      tablesChanged,
                      viewsScanned,
                      viewsChanged,
                      errors,
                      snapshotsProcessed,
                      statsProcessed,
                      message) -> {
                    if (!heartbeat.getAsBoolean()) {
                      return;
                    }
                    progress.update(
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed,
                        message);
                    jobs.markProgress(
                        lease.jobId,
                        lease.leaseEpoch,
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed,
                        message);
                  }));
      long finished = System.currentTimeMillis();
      long totalSnapshots = result.snapshotsProcessed;
      long totalStats = result.statsProcessed;
      // Single definitive lease gate before any terminal state write (may renew lease).
      if (!heartbeat.getAsBoolean()) {
        emitOutcome(
            lease,
            "lease_lost",
            finished - started,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats,
            "lease_lost");
        return;
      }
      boolean interruptedNow = Thread.currentThread().isInterrupted() || interrupted.get();
      boolean cancelledNow = cancellationRequestedInStore.getAsBoolean();
      if (result.cancelled || cancelledNow) {
        if (!cancelledNow && interruptedNow) {
          emitOutcome(
              lease,
              "lease_lost",
              finished - started,
              result.tablesScanned,
              result.tablesChanged,
              result.viewsScanned,
              result.viewsChanged,
              result.errors,
              totalSnapshots,
              totalStats,
              "interrupted");
          return;
        }
        jobs.markCancelled(
            lease.jobId,
            lease.leaseEpoch,
            finished,
            result.message,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats);
        cancelChildJobs(lease, result.message);
        emitOutcome(
            lease,
            "cancelled",
            finished - started,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats,
            null);
        return;
      }
      if (isObsoleteFailureKind(result.failureKind)) {
        jobs.markCancelled(
            lease.jobId,
            lease.leaseEpoch,
            finished,
            result.message,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats);
        cancelChildJobs(lease, result.message);
        emitOutcome(
            lease,
            "cancelled",
            finished - started,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats,
            obsoleteFailureReason(result.failureKind),
            result.message);
        return;
      }
      if (!result.ok()) {
        jobs.markFailed(
            lease.jobId,
            lease.leaseEpoch,
            finished,
            result.message,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats);
        cancelChildJobs(lease, result.message);
        emitOutcome(
            lease,
            "failed",
            finished - started,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            totalSnapshots,
            totalStats,
            null,
            result.message);
        return;
      }
      jobs.markSucceeded(
          lease.jobId,
          lease.leaseEpoch,
          finished,
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          totalSnapshots,
          totalStats);
      emitOutcome(
          lease,
          "succeeded",
          finished - started,
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          totalSnapshots,
          totalStats,
          null);
    } catch (Exception e) {
      if (!leaseValid.get()) {
        long now = System.currentTimeMillis();
        emitOutcome(
            lease,
            "lease_lost",
            now - started,
            progress.tablesScanned,
            progress.tablesChanged,
            progress.viewsScanned,
            progress.viewsChanged,
            progress.errors,
            progress.snapshotsProcessed,
            progress.statsProcessed,
            "lease_lost");
      } else {
        boolean interruptedNow = Thread.currentThread().isInterrupted() || interrupted.get();
        boolean cancelledOnError = cancellationRequestedInStore.getAsBoolean();
        if (cancelledOnError) {
          long now = System.currentTimeMillis();
          jobs.markCancelled(
              lease.jobId,
              lease.leaseEpoch,
              now,
              progress.message.isBlank() ? "Cancelled" : progress.message,
              progress.tablesScanned,
              progress.tablesChanged,
              progress.viewsScanned,
              progress.viewsChanged,
              progress.errors,
              progress.snapshotsProcessed,
              progress.statsProcessed);
          cancelChildJobs(lease, progress.message.isBlank() ? "Cancelled" : progress.message);
          emitOutcome(
              lease,
              "cancelled",
              now - started,
              progress.tablesScanned,
              progress.tablesChanged,
              progress.viewsScanned,
              progress.viewsChanged,
              progress.errors,
              progress.snapshotsProcessed,
              progress.statsProcessed,
              null);
        } else if (interruptedNow) {
          long now = System.currentTimeMillis();
          emitOutcome(
              lease,
              "lease_lost",
              now - started,
              progress.tablesScanned,
              progress.tablesChanged,
              progress.viewsScanned,
              progress.viewsChanged,
              progress.errors,
              progress.snapshotsProcessed,
              progress.statsProcessed,
              "interrupted");
        } else {
          var msg = describeFailure(e);
          long now = System.currentTimeMillis();
          var failureKind = failureKindOf(e);
          if (isObsoleteFailureKind(failureKind)) {
            long errorCount = Math.max(1L, progress.errors);
            jobs.markCancelled(
                lease.jobId,
                lease.leaseEpoch,
                now,
                msg,
                progress.tablesScanned,
                progress.tablesChanged,
                progress.viewsScanned,
                progress.viewsChanged,
                errorCount,
                progress.snapshotsProcessed,
                progress.statsProcessed);
            cancelChildJobs(lease, msg);
            emitOutcome(
                lease,
                "cancelled",
                now - started,
                progress.tablesScanned,
                progress.tablesChanged,
                progress.viewsScanned,
                progress.viewsChanged,
                errorCount,
                progress.snapshotsProcessed,
                progress.statsProcessed,
                obsoleteFailureReason(failureKind),
                msg);
          } else {
            long errorCount = Math.max(1L, progress.errors);
            jobs.markFailed(
                lease.jobId,
                lease.leaseEpoch,
                now,
                msg,
                progress.tablesScanned,
                progress.tablesChanged,
                progress.viewsScanned,
                progress.viewsChanged,
                errorCount,
                progress.snapshotsProcessed,
                progress.statsProcessed);
            cancelChildJobs(lease, msg);
            emitOutcome(
                lease,
                "failed",
                now - started,
                progress.tablesScanned,
                progress.tablesChanged,
                progress.viewsScanned,
                progress.viewsChanged,
                errorCount,
                progress.snapshotsProcessed,
                progress.statsProcessed,
                normalizeReason(e),
                msg);
          }
        }
      }
    } finally {
      cancellations.unregister(lease.jobId, Thread.currentThread());
      Thread.interrupted();
    }
  }

  private void cancelChildJobs(ReconcileJobStore.LeasedJob lease, String reason) {
    if (lease == null || lease.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return;
    }
    String message = (reason == null || reason.isBlank()) ? "Parent plan job terminated" : reason;
    for (var child : childJobsFor(lease)) {
      var cancelled = jobs.cancel(lease.accountId, child.jobId, message);
      if (cancelled.isPresent() && "JS_CANCELLING".equals(cancelled.get().state)) {
        cancellations.requestCancel(cancelled.get().jobId);
      }
    }
  }

  private List<ReconcileJobStore.ReconcileJob> childJobsFor(ReconcileJobStore.LeasedJob lease) {
    if (lease == null || lease.jobKind != ReconcileJobKind.PLAN_CONNECTOR) {
      return List.of();
    }
    return jobs.childJobs(lease.accountId, lease.jobId);
  }

  private static final class ProgressSnapshot {
    long tablesScanned;
    long tablesChanged;
    long viewsScanned;
    long viewsChanged;
    long errors;
    long snapshotsProcessed;
    long statsProcessed;
    String message = "";

    void update(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed,
        String message) {
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.viewsScanned = viewsScanned;
      this.viewsChanged = viewsChanged;
      this.errors = errors;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
      this.message = message == null ? "" : message;
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

  private static ReconcileExecutor.ExecutionResult.FailureKind failureKindOf(Throwable t) {
    var seen = new java.util.HashSet<Throwable>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      if (cur instanceof ReconcileFailureException rfe) {
        return rfe.failureKind();
      }
      seen.add(cur);
      cur = cur.getCause();
    }
    return ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL;
  }

  private static boolean isObsoleteFailureKind(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind) {
    return failureKind == ReconcileExecutor.ExecutionResult.FailureKind.CONNECTOR_MISSING
        || failureKind == ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING;
  }

  private static String obsoleteFailureReason(
      ReconcileExecutor.ExecutionResult.FailureKind failureKind) {
    return switch (failureKind) {
      case CONNECTOR_MISSING -> "connector_missing";
      case TABLE_MISSING -> "table_missing";
      default -> "stale_source";
    };
  }

  private void emitOutcome(
      ReconcileJobStore.LeasedJob lease,
      String result,
      long durationMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String reason) {
    emitOutcome(
        lease,
        result,
        durationMs,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
        errors,
        snapshotsProcessed,
        statsProcessed,
        reason,
        null);
  }

  private void emitOutcome(
      ReconcileJobStore.LeasedJob lease,
      String result,
      long durationMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String reason,
      String detail) {
    observeOutcome(
        lease,
        result,
        durationMs,
        tablesScanned,
        tablesChanged,
        viewsScanned,
        viewsChanged,
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
    if (detail != null && !detail.isBlank()) {
      LOG.infof(
          "Reconcile job detail account=%s connector=%s result=%s detail=%s",
          lease.accountId, lease.connectorId, result, detail.replace('\n', ' ').replace('\r', ' '));
    }
  }

  private void observeOutcome(
      ReconcileJobStore.LeasedJob lease,
      String result,
      long durationMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
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
    observability.counter(RECONCILE_VIEWS_SCANNED, Math.max(0L, viewsScanned), tags);
    observability.counter(RECONCILE_VIEWS_CHANGED, Math.max(0L, viewsChanged), tags);
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
