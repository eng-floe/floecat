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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.scheduler.Scheduled;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
public class RemoteReconcileExecutorPoller {
  private static final long DEFAULT_LEASE_HEARTBEAT_MS = 2_000L;
  private static final long MIN_LEASE_HEARTBEAT_MS = 1_000L;
  private static final long MIN_CANCEL_CHECK_MS = 500L;
  private static final int DEFAULT_MAX_PARALLELISM = 1;

  enum WorkerMode {
    LOCAL,
    REMOTE;

    static WorkerMode fromConfig(String value) {
      if (value == null) {
        return LOCAL;
      }
      try {
        return WorkerMode.valueOf(value.trim().toUpperCase());
      } catch (IllegalArgumentException ex) {
        throw new IllegalArgumentException(
            "Unsupported floecat.reconciler.worker.mode: " + value, ex);
      }
    }

    boolean runsWorkers() {
      return true;
    }
  }

  @Inject ReconcileExecutorRegistry executorRegistry;
  @Inject RemoteReconcileExecutorClient client;
  @Inject Config config;

  @ConfigProperty(name = "floecat.reconciler.worker.mode", defaultValue = "local")
  String workerModeValue;

  private static final Logger LOG = Logger.getLogger(RemoteReconcileExecutorPoller.class);

  private final AtomicBoolean polling = new AtomicBoolean(false);
  private final AtomicBoolean repollRequested = new AtomicBoolean(false);
  private final AtomicInteger inFlight = new AtomicInteger(0);
  private volatile WorkerMode workerMode = WorkerMode.LOCAL;
  private volatile int maxParallelism = DEFAULT_MAX_PARALLELISM;
  private volatile ExecutorService workers;

  @PostConstruct
  void init() {
    workerMode = WorkerMode.fromConfig(workerModeValue);
    maxParallelism =
        config
            .getOptionalValue("reconciler.max-parallelism", Integer.class)
            .orElse(DEFAULT_MAX_PARALLELISM);
    if (maxParallelism <= 0 || !workerMode.runsWorkers()) {
      maxParallelism = 0;
      return;
    }
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
    requestDrain();
  }

  private void requestDrain() {
    if (!workerMode.runsWorkers()
        || maxParallelism <= 0
        || executorRegistry.orderedExecutors().isEmpty()) {
      return;
    }
    repollRequested.set(true);
    if (!polling.compareAndSet(false, true)) {
      return;
    }
    try {
      while (true) {
        repollRequested.set(false);
        while (reserveWorkerSlot()) {
          try {
            Optional<LeaseAssignment> assignment = leaseNextAssignment();
            if (assignment.isEmpty()) {
              inFlight.decrementAndGet();
              return;
            }
            submitAssignment(assignment.get());
          } catch (RuntimeException e) {
            inFlight.decrementAndGet();
            throw e;
          }
        }
        if (!repollRequested.get()) {
          return;
        }
      }
    } finally {
      polling.set(false);
      if (repollRequested.get()) {
        requestDrain();
      }
    }
  }

  private Optional<LeaseAssignment> leaseNextAssignment() {
    ReconcileJobStore.LeaseRequest request = remoteLeaseRequest();
    Optional<RemoteLeasedJob> lease;
    try {
      lease = client.lease(request, workerLeaseSource());
    } catch (RuntimeException e) {
      if (shouldIgnoreStartupUnavailable(e)) {
        LOG.debugf(
            "Skipping local reconcile poll cycle until gRPC server is ready: %s", e.getMessage());
        return Optional.empty();
      }
      throw e;
    }
    if (lease.isEmpty()) {
      return Optional.empty();
    }
    Optional<ReconcileExecutor> executor = executorRegistry.executorFor(lease.get().lease());
    if (executor.isEmpty()) {
      LOG.warnf(
          "Leased reconcile job jobId=%s kind=%s but no eligible executor matched locally",
          lease.get().lease().jobId, lease.get().lease().jobKind);
      return Optional.empty();
    }
    return Optional.of(new LeaseAssignment(executor.get(), lease.get()));
  }

  private ReconcileJobStore.LeaseRequest remoteLeaseRequest() {
    return executorRegistry.leaseRequest();
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

  private String workerLeaseSource() {
    return switch (workerMode) {
      case LOCAL -> "local-poller";
      case REMOTE -> "remote-poller";
    };
  }

  private boolean shouldIgnoreStartupUnavailable(RuntimeException error) {
    if (workerMode != WorkerMode.LOCAL || !(error instanceof StatusRuntimeException statusError)) {
      return false;
    }
    return statusError.getStatus().getCode() == Status.Code.UNAVAILABLE;
  }

  private void submitAssignment(LeaseAssignment assignment) {
    ExecutorService executor = workers;
    if (executor == null) {
      inFlight.decrementAndGet();
      return;
    }
    try {
      executor.submit(
          () -> {
            try {
              runLease(assignment);
            } finally {
              releaseWorkerSlot();
            }
          });
    } catch (RuntimeException e) {
      releaseWorkerSlot();
      throw e;
    }
  }

  private void releaseWorkerSlot() {
    inFlight.decrementAndGet();
    ExecutorService executor = workers;
    if (workerMode.runsWorkers() && executor != null && !executor.isShutdown()) {
      requestDrain();
    }
  }

  void runLease(LeaseAssignment assignment) {
    ReconcileExecutor executor = assignment.executor();
    RemoteLeasedJob remoteLease = assignment.lease();
    var lease = remoteLease.lease();
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
    long[] nextHeartbeatAtMs = {started};
    long cancelCheckEveryMs = Math.max(MIN_CANCEL_CHECK_MS, heartbeatEveryMs / 2L);
    long[] nextCancelCheckAtMs = {started};
    AtomicBoolean leaseValid = new AtomicBoolean(true);
    AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    ProgressSnapshot progress = new ProgressSnapshot();

    BooleanSupplier heartbeat =
        () -> {
          if (!leaseValid.get()) {
            return false;
          }
          long now = System.currentTimeMillis();
          if (now < nextHeartbeatAtMs[0]) {
            return true;
          }
          RemoteReconcileExecutorClient.LeaseHeartbeat response = client.renew(remoteLease);
          leaseValid.set(response.leaseValid());
          cancellationRequested.set(response.cancellationRequested());
          nextHeartbeatAtMs[0] = now + heartbeatEveryMs;
          return response.leaseValid();
        };

    BooleanSupplier shouldStop =
        () -> {
          if (Thread.currentThread().isInterrupted()) {
            interrupted.set(true);
            return true;
          }
          if (!heartbeat.getAsBoolean()) {
            return true;
          }
          long now = System.currentTimeMillis();
          if (now >= nextCancelCheckAtMs[0]) {
            cancellationRequested.set(client.cancellationRequested(remoteLease));
            nextCancelCheckAtMs[0] = now + cancelCheckEveryMs;
          }
          return cancellationRequested.get();
        };

    try {
      client.start(remoteLease, executor.id());
      if (shouldStop.getAsBoolean()) {
        completeIfPossible(
            remoteLease,
            RemoteLeasedJob.CompletionState.CANCELLED,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "Cancelled");
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
                    if (!leaseValid.get()) {
                      return;
                    }
                    progress.update(
                        tablesScanned,
                        tablesChanged,
                        viewsScanned,
                        viewsChanged,
                        errors,
                        snapshotsProcessed,
                        statsProcessed);
                    RemoteReconcileExecutorClient.LeaseHeartbeat response =
                        client.reportProgress(
                            remoteLease,
                            tablesScanned,
                            tablesChanged,
                            viewsScanned,
                            viewsChanged,
                            errors,
                            snapshotsProcessed,
                            statsProcessed,
                            message);
                    leaseValid.set(response.leaseValid());
                    cancellationRequested.set(response.cancellationRequested());
                  }));
      if (!leaseValid.get()) {
        LOG.warnf("Remote reconcile lease lost for job %s executor=%s", lease.jobId, executor.id());
        return;
      }
      if (result.cancelled || cancellationRequested.get()) {
        completeIfPossible(
            remoteLease,
            RemoteLeasedJob.CompletionState.CANCELLED,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed,
            result.message);
        return;
      }
      if (isObsoleteFailureKind(result.failureKind)) {
        completeIfPossible(
            remoteLease,
            RemoteLeasedJob.CompletionState.CANCELLED,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed,
            result.message);
        return;
      }
      if (!result.ok()) {
        completeIfPossible(
            remoteLease,
            RemoteLeasedJob.CompletionState.FAILED,
            result.tablesScanned,
            result.tablesChanged,
            result.viewsScanned,
            result.viewsChanged,
            result.errors,
            result.snapshotsProcessed,
            result.statsProcessed,
            result.message);
        return;
      }
      completeIfPossible(
          remoteLease,
          RemoteLeasedJob.CompletionState.SUCCEEDED,
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message);
      LOG.infof(
          "Remote reconcile job outcome account=%s connector=%s executor=%s result=succeeded duration_ms=%d",
          lease.accountId,
          lease.connectorId,
          executor.id(),
          Math.max(0L, System.currentTimeMillis() - started));
    } catch (Exception e) {
      if (leaseValid.get() && !interrupted.get()) {
        long errorCount =
            cancellationRequested.get() ? progress.errors : Math.max(1L, progress.errors);
        completeIfPossible(
            remoteLease,
            cancellationRequested.get() || isObsoleteFailureKind(failureKindOf(e))
                ? RemoteLeasedJob.CompletionState.CANCELLED
                : RemoteLeasedJob.CompletionState.FAILED,
            progress.tablesScanned,
            progress.tablesChanged,
            progress.viewsScanned,
            progress.viewsChanged,
            errorCount,
            progress.snapshotsProcessed,
            progress.statsProcessed,
            describeFailure(e));
      }
      if (isObsoleteFailureKind(failureKindOf(e))) {
        LOG.infof(
            "Remote reconcile job became obsolete for job %s executor=%s reason=%s",
            lease.jobId, executor.id(), describeFailure(e));
      } else {
        LOG.errorf(
            e,
            "Remote reconcile execution failed for job %s executor=%s",
            lease.jobId,
            executor.id());
      }
    } finally {
      Thread.interrupted();
    }
  }

  private void completeIfPossible(
      RemoteLeasedJob lease,
      RemoteLeasedJob.CompletionState state,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    RemoteReconcileExecutorClient.CompletionResult result =
        client.complete(
            lease,
            state,
            tablesScanned,
            tablesChanged,
            viewsScanned,
            viewsChanged,
            errors,
            snapshotsProcessed,
            statsProcessed,
            message);
    if (!result.accepted()) {
      LOG.warnf("Remote reconcile completion rejected for job %s", lease.lease().jobId);
    }
  }

  private static String describeFailure(Throwable t) {
    if (t == null) {
      return "Unknown error";
    }
    String msg = t.getMessage();
    if (msg == null || msg.isBlank()) {
      return t.getClass().getSimpleName();
    }
    return t.getClass().getSimpleName() + ": " + msg;
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

  private static final class ProgressSnapshot {
    long tablesScanned;
    long tablesChanged;
    long viewsScanned;
    long viewsChanged;
    long errors;
    long snapshotsProcessed;
    long statsProcessed;

    void update(
        long tablesScanned,
        long tablesChanged,
        long viewsScanned,
        long viewsChanged,
        long errors,
        long snapshotsProcessed,
        long statsProcessed) {
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.viewsScanned = viewsScanned;
      this.viewsChanged = viewsChanged;
      this.errors = errors;
      this.snapshotsProcessed = snapshotsProcessed;
      this.statsProcessed = statsProcessed;
    }
  }

  record LeaseAssignment(ReconcileExecutor executor, RemoteLeasedJob lease) {}
}
