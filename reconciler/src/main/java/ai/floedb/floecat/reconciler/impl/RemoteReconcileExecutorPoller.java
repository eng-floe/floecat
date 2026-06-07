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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class RemoteReconcileExecutorPoller {
  private static final long DEFAULT_LEASE_HEARTBEAT_MS = 2_000L;
  private static final long MAX_DEFAULT_LEASE_HEARTBEAT_MS = 10_000L;
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
            submitAssignment();
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

  private void submitAssignment() {
    ExecutorService executor = workers;
    if (executor == null) {
      releaseWorkerSlot(false);
      return;
    }
    try {
      executor.submit(
          () -> {
            boolean ranLease = false;
            try {
              Optional<LeaseAssignment> assignment = leaseNextAssignment();
              if (assignment.isEmpty()) {
                return;
              }
              ranLease = true;
              runLease(assignment.get());
            } finally {
              releaseWorkerSlot(ranLease);
            }
          });
    } catch (RuntimeException e) {
      releaseWorkerSlot(false);
      throw e;
    }
  }

  private void releaseWorkerSlot(boolean requestDrain) {
    inFlight.decrementAndGet();
    ExecutorService executor = workers;
    if (requestDrain && workerMode.runsWorkers() && executor != null && !executor.isShutdown()) {
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
    long suggestedHeartbeatMs =
        Math.max(
            MIN_LEASE_HEARTBEAT_MS,
            Math.min(MAX_DEFAULT_LEASE_HEARTBEAT_MS, Math.max(1L, leaseMs / 4L)));
    long heartbeatEveryMs =
        Math.max(
            MIN_LEASE_HEARTBEAT_MS,
            config
                .getOptionalValue("reconciler.lease-heartbeat-ms", Long.class)
                .orElse(Math.max(DEFAULT_LEASE_HEARTBEAT_MS, suggestedHeartbeatMs)));
    long cancelCheckEveryMs = Math.max(MIN_CANCEL_CHECK_MS, heartbeatEveryMs / 2L);
    long leaseSafetyMarginMs =
        Math.max(
            MIN_LEASE_HEARTBEAT_MS,
            Math.min(heartbeatEveryMs * 2L, Math.max(1_000L, leaseMs / 3L)));
    long[] nextCancelCheckAtMs = {started};
    AtomicBoolean leaseInvalid = new AtomicBoolean(false);
    AtomicBoolean leaseStateUncertain = new AtomicBoolean(false);
    AtomicBoolean cancellationRequested = new AtomicBoolean(false);
    AtomicBoolean completionStarted = new AtomicBoolean(false);
    AtomicBoolean interrupted = new AtomicBoolean(false);
    AtomicLong lastLeaseConfirmedAtMs = new AtomicLong(started);
    ProgressSnapshot progress = new ProgressSnapshot();
    ScheduledExecutorService heartbeatExecutor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "reconcile-lease-heartbeat-" + lease.jobId);
              thread.setDaemon(true);
              return thread;
            });
    Runnable heartbeatTask =
        () -> {
          if (leaseInvalid.get() || completionStarted.get()) {
            return;
          }
          try {
            RemoteReconcileExecutorClient.LeaseHeartbeat response = client.renew(remoteLease);
            if (!response.leaseValid()) {
              if (!completionStarted.get()) {
                LOG.warnf(
                    "Remote reconcile lease heartbeat rejected for job %s executor=%s cancellationRequested=%s",
                    lease.jobId, executor.id(), response.cancellationRequested());
              }
              leaseInvalid.set(true);
            } else {
              lastLeaseConfirmedAtMs.set(System.currentTimeMillis());
              leaseStateUncertain.set(false);
            }
            cancellationRequested.set(response.cancellationRequested());
          } catch (RuntimeException e) {
            if (isTransportFailure(e)
                && !leaseDefinitelyExpired(
                    leaseMs,
                    leaseSafetyMarginMs,
                    lastLeaseConfirmedAtMs.get(),
                    System.currentTimeMillis())) {
              LOG.warnf(
                  e,
                  "Remote reconcile lease heartbeat transport failure for job %s executor=%s; continuing with last confirmed lease state",
                  lease.jobId,
                  executor.id());
              return;
            }
            if (isTransportFailure(e)) {
              leaseStateUncertain.set(true);
              LOG.warnf(
                  e,
                  "Remote reconcile lease heartbeat state is uncertain for job %s executor=%s",
                  lease.jobId,
                  executor.id());
              return;
            }
            leaseInvalid.set(true);
            LOG.warnf(
                e,
                "Remote reconcile lease heartbeat failed for job %s executor=%s",
                lease.jobId,
                executor.id());
          }
        };

    BooleanSupplier shouldStop =
        () -> {
          if (Thread.currentThread().isInterrupted()) {
            interrupted.set(true);
            return true;
          }
          if (leaseInvalid.get()) {
            return true;
          }
          if (leaseDefinitelyExpired(
              leaseMs,
              leaseSafetyMarginMs,
              lastLeaseConfirmedAtMs.get(),
              System.currentTimeMillis())) {
            leaseStateUncertain.set(true);
          }
          long now = System.currentTimeMillis();
          if (now >= nextCancelCheckAtMs[0]) {
            try {
              cancellationRequested.set(client.cancellationRequested(remoteLease));
            } catch (RuntimeException e) {
              if (isTransportFailure(e)
                  && !leaseDefinitelyExpired(
                      leaseMs, leaseSafetyMarginMs, lastLeaseConfirmedAtMs.get(), now)) {
                LOG.warnf(
                    e,
                    "Remote reconcile cancellation check transport failure for job %s executor=%s; continuing with last confirmed lease state",
                    lease.jobId,
                    executor.id());
              } else {
                if (isTransportFailure(e)) {
                  leaseStateUncertain.set(true);
                  LOG.warnf(
                      e,
                      "Remote reconcile cancellation state is uncertain for job %s executor=%s",
                      lease.jobId,
                      executor.id());
                  return false;
                }
                leaseInvalid.set(true);
                throw e;
              }
            }
            nextCancelCheckAtMs[0] = now + cancelCheckEveryMs;
          }
          return cancellationRequested.get();
        };

    try {
      try {
        client.start(remoteLease, executor.id());
      } catch (RuntimeException e) {
        if (isTransportFailure(e)) {
          throw lifecycleStateUncertain(
              "Remote reconcile start state is uncertain for job " + lease.jobId, e);
        }
        throw e;
      }
      heartbeatExecutor.scheduleAtFixedRate(
          heartbeatTask, heartbeatEveryMs, heartbeatEveryMs, TimeUnit.MILLISECONDS);
      if (!leaseStillCompletable(
          remoteLease,
          lease,
          executor.id(),
          leaseMs,
          leaseSafetyMarginMs,
          leaseInvalid,
          leaseStateUncertain,
          cancellationRequested,
          lastLeaseConfirmedAtMs)) {
        LOG.warnf("Remote reconcile lease lost for job %s executor=%s", lease.jobId, executor.id());
        return;
      }
      shouldStop.getAsBoolean();
      if (cancellationRequested.get()) {
        completeIfPossible(
            remoteLease,
            RemoteLeasedJob.CompletionState.CANCELLED,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            ReconcileExecutor.ExecutionResult.RetryClass.NONE,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            "Cancelled",
            completionStarted,
            heartbeatExecutor);
        return;
      }
      if (leaseInvalid.get() || interrupted.get()) {
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
                    if (leaseInvalid.get()) {
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
                    RemoteReconcileExecutorClient.LeaseHeartbeat response;
                    try {
                      response =
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
                    } catch (RuntimeException e) {
                      if (isTransportFailure(e)
                          && !leaseDefinitelyExpired(
                              leaseMs,
                              leaseSafetyMarginMs,
                              lastLeaseConfirmedAtMs.get(),
                              System.currentTimeMillis())) {
                        LOG.warnf(
                            e,
                            "Remote reconcile progress transport failure for job %s executor=%s; continuing with last confirmed lease state",
                            lease.jobId,
                            executor.id());
                        return;
                      }
                      if (isTransportFailure(e)) {
                        leaseStateUncertain.set(true);
                        LOG.warnf(
                            e,
                            "Remote reconcile progress state is uncertain for job %s executor=%s",
                            lease.jobId,
                            executor.id());
                        return;
                      }
                      leaseInvalid.set(true);
                      throw e;
                    }
                    if (!response.leaseValid()) {
                      LOG.warnf(
                          "Remote reconcile progress heartbeat rejected for job %s executor=%s cancellationRequested=%s",
                          lease.jobId, executor.id(), response.cancellationRequested());
                      leaseInvalid.set(true);
                      return;
                    }
                    lastLeaseConfirmedAtMs.set(System.currentTimeMillis());
                    leaseStateUncertain.set(false);
                    cancellationRequested.set(response.cancellationRequested());
                  },
                  () -> {}));
      if (result.completionHandled) {
        stopHeartbeatsForHandledCompletion(completionStarted, heartbeatExecutor);
        LOG.infof(
            "Remote reconcile job outcome account=%s connector=%s executor=%s result=succeeded duration_ms=%d",
            lease.accountId,
            lease.connectorId,
            executor.id(),
            Math.max(0L, System.currentTimeMillis() - started));
        return;
      }
      if (!leaseStillCompletable(
          remoteLease,
          lease,
          executor.id(),
          leaseMs,
          leaseSafetyMarginMs,
          leaseInvalid,
          leaseStateUncertain,
          cancellationRequested,
          lastLeaseConfirmedAtMs)) {
        LOG.warnf("Remote reconcile lease lost for job %s executor=%s", lease.jobId, executor.id());
        return;
      }
      completeForOutcome(
          remoteLease,
          lease,
          executor.id(),
          started,
          cancellationRequested.get()
              ? ReconcileExecutor.ExecutionResult.JobOutcome.OBSOLETE
              : classify(result),
          result.retryDisposition,
          result.retryClass,
          result.tablesScanned,
          result.tablesChanged,
          result.viewsScanned,
          result.viewsChanged,
          result.errors,
          result.snapshotsProcessed,
          result.statsProcessed,
          result.message,
          completionStarted,
          heartbeatExecutor);
    } catch (Exception e) {
      boolean stateUncertain =
          retryClassOf(e) == ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN;
      if (stateUncertain) {
        LOG.warnf(
            e,
            "Remote reconcile terminal state is uncertain for job %s executor=%s; leaving lease for retry after expiry",
            lease.jobId,
            executor.id());
      } else if (!interrupted.get()
          && leaseStillCompletable(
              remoteLease,
              lease,
              executor.id(),
              leaseMs,
              leaseSafetyMarginMs,
              leaseInvalid,
              leaseStateUncertain,
              cancellationRequested,
              lastLeaseConfirmedAtMs)) {
        long errorCount =
            cancellationRequested.get() ? progress.errors : Math.max(1L, progress.errors);
        completeForOutcome(
            remoteLease,
            lease,
            executor.id(),
            started,
            cancellationRequested.get()
                ? ReconcileExecutor.ExecutionResult.JobOutcome.OBSOLETE
                : outcomeOf(e),
            retryDispositionOf(e),
            retryClassOf(e),
            progress.tablesScanned,
            progress.tablesChanged,
            progress.viewsScanned,
            progress.viewsChanged,
            errorCount,
            progress.snapshotsProcessed,
            progress.statsProcessed,
            describeFailure(e),
            completionStarted,
            heartbeatExecutor);
      }
      if (stateUncertain) {
        return;
      }
      if (outcomeOf(e) == ReconcileExecutor.ExecutionResult.JobOutcome.OBSOLETE) {
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
      heartbeatExecutor.shutdownNow();
      Thread.interrupted();
    }
  }

  private boolean leaseStillCompletable(
      RemoteLeasedJob remoteLease,
      ReconcileJobStore.LeasedJob lease,
      String executorId,
      long leaseMs,
      long leaseSafetyMarginMs,
      AtomicBoolean leaseInvalid,
      AtomicBoolean leaseStateUncertain,
      AtomicBoolean cancellationRequested,
      AtomicLong lastLeaseConfirmedAtMs) {
    if (leaseInvalid.get()) {
      return false;
    }
    long now = System.currentTimeMillis();
    if (!leaseStateUncertain.get()
        && !leaseDefinitelyExpired(
            leaseMs, leaseSafetyMarginMs, lastLeaseConfirmedAtMs.get(), now)) {
      return true;
    }
    try {
      RemoteReconcileExecutorClient.LeaseHeartbeat response = client.renew(remoteLease);
      if (!response.leaseValid()) {
        leaseInvalid.set(true);
        cancellationRequested.set(response.cancellationRequested());
        LOG.warnf(
            "Remote reconcile final lease confirmation rejected for job %s executor=%s cancellationRequested=%s",
            lease.jobId, executorId, response.cancellationRequested());
        return false;
      }
      lastLeaseConfirmedAtMs.set(System.currentTimeMillis());
      leaseStateUncertain.set(false);
      cancellationRequested.set(response.cancellationRequested());
      return true;
    } catch (RuntimeException e) {
      if (isTransportFailure(e)) {
        throw lifecycleStateUncertain(
            "Remote reconcile final lease confirmation state is uncertain for job " + lease.jobId,
            e);
      }
      leaseInvalid.set(true);
      throw e;
    }
  }

  private void stopHeartbeatsForHandledCompletion(
      AtomicBoolean completionStarted, ScheduledExecutorService heartbeatExecutor) {
    completionStarted.set(true);
    heartbeatExecutor.shutdownNow();
  }

  private void completeForOutcome(
      RemoteLeasedJob remoteLease,
      ReconcileJobStore.LeasedJob lease,
      String executorId,
      long started,
      ReconcileExecutor.ExecutionResult.JobOutcome outcome,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message,
      AtomicBoolean completionStarted,
      ScheduledExecutorService heartbeatExecutor) {
    switch (outcome) {
      case SUCCESS -> {
        boolean accepted =
            completeIfPossible(
                remoteLease,
                RemoteLeasedJob.CompletionState.SUCCEEDED,
                ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                ReconcileExecutor.ExecutionResult.RetryClass.NONE,
                tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                snapshotsProcessed,
                statsProcessed,
                message,
                completionStarted,
                heartbeatExecutor);
        if (accepted) {
          LOG.infof(
              "Remote reconcile job outcome account=%s connector=%s executor=%s result=succeeded duration_ms=%d",
              lease.accountId,
              lease.connectorId,
              executorId,
              Math.max(0L, System.currentTimeMillis() - started));
        }
      }
      case OBSOLETE ->
          completeIfPossible(
              remoteLease,
              RemoteLeasedJob.CompletionState.CANCELLED,
              retryDisposition,
              retryClass,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed,
              message,
              completionStarted,
              heartbeatExecutor);
      case RETRYABLE_FAILURE, TERMINAL_FAILURE ->
          completeIfPossible(
              remoteLease,
              RemoteLeasedJob.CompletionState.FAILED,
              retryDisposition,
              retryClass,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed,
              message,
              completionStarted,
              heartbeatExecutor);
    }
  }

  private boolean completeIfPossible(
      RemoteLeasedJob lease,
      RemoteLeasedJob.CompletionState state,
      ReconcileExecutor.ExecutionResult.RetryDisposition retryDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass retryClass,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message,
      AtomicBoolean completionStarted,
      ScheduledExecutorService heartbeatExecutor) {
    stopHeartbeatsForHandledCompletion(completionStarted, heartbeatExecutor);
    RemoteReconcileExecutorClient.CompletionResult result;
    try {
      result =
          client.complete(
              lease,
              state,
              retryDisposition,
              retryClass,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              snapshotsProcessed,
              statsProcessed,
              message);
    } catch (RuntimeException e) {
      if (isTransportFailure(e)) {
        throw lifecycleStateUncertain(
            "Remote reconcile completion state is uncertain for job " + lease.lease().jobId, e);
      }
      throw e;
    }
    if (!result.accepted()) {
      LOG.warnf("Remote reconcile completion rejected for job %s", lease.lease().jobId);
    }
    return result.accepted();
  }

  private static ReconcileFailureException lifecycleStateUncertain(String message) {
    return new ReconcileFailureException(
        ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
        message,
        null);
  }

  private static ReconcileFailureException lifecycleStateUncertain(
      String message, RuntimeException cause) {
    return new ReconcileFailureException(
        ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
        message,
        cause);
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

  private static ReconcileExecutor.ExecutionResult.RetryDisposition retryDispositionOf(
      Throwable t) {
    var seen = new java.util.HashSet<Throwable>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      if (cur instanceof ReconcileFailureException rfe) {
        return rfe.retryDisposition();
      }
      seen.add(cur);
      cur = cur.getCause();
    }
    return ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE;
  }

  private static ReconcileExecutor.ExecutionResult.RetryClass retryClassOf(Throwable t) {
    var seen = new java.util.HashSet<Throwable>();
    Throwable cur = t;
    while (cur != null && !seen.contains(cur)) {
      if (cur instanceof ReconcileFailureException rfe) {
        return rfe.retryClass();
      }
      seen.add(cur);
      cur = cur.getCause();
    }
    return ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR;
  }

  private static ReconcileExecutor.ExecutionResult.JobOutcome classify(
      ReconcileExecutor.ExecutionResult result) {
    if (result == null) {
      return ReconcileExecutor.ExecutionResult.JobOutcome.TERMINAL_FAILURE;
    }
    if (result.cancelled) {
      return ReconcileExecutor.ExecutionResult.JobOutcome.OBSOLETE;
    }
    return result.outcome;
  }

  private static ReconcileExecutor.ExecutionResult.JobOutcome outcomeOf(Throwable error) {
    ReconcileExecutor.ExecutionResult.FailureKind failureKind = failureKindOf(error);
    if (failureKind == ReconcileExecutor.ExecutionResult.FailureKind.CONNECTOR_MISSING
        || failureKind == ReconcileExecutor.ExecutionResult.FailureKind.TABLE_MISSING
        || failureKind == ReconcileExecutor.ExecutionResult.FailureKind.VIEW_MISSING) {
      return ReconcileExecutor.ExecutionResult.JobOutcome.OBSOLETE;
    }
    return retryDispositionOf(error) == ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL
        ? ReconcileExecutor.ExecutionResult.JobOutcome.TERMINAL_FAILURE
        : ReconcileExecutor.ExecutionResult.JobOutcome.RETRYABLE_FAILURE;
  }

  private static boolean isTransportFailure(Throwable error) {
    Throwable current = error;
    var seen = new java.util.HashSet<Throwable>();
    while (current != null && seen.add(current)) {
      if (current instanceof StatusRuntimeException statusError) {
        return switch (statusError.getStatus().getCode()) {
          case UNAVAILABLE, INTERNAL, UNKNOWN, DEADLINE_EXCEEDED, CANCELLED -> true;
          default -> false;
        };
      }
      current = current.getCause();
    }
    return false;
  }

  private static boolean leaseDefinitelyExpired(
      long leaseMs, long safetyMarginMs, long lastLeaseConfirmedAtMs, long nowMs) {
    long expiryThresholdMs = Math.max(1_000L, leaseMs - safetyMarginMs);
    return nowMs > lastLeaseConfirmedAtMs + expiryThresholdMs;
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
