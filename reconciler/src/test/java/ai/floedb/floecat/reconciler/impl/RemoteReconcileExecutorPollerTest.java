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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RemoteReconcileExecutorPollerTest {
  private RemoteReconcileExecutorPoller poller;

  @AfterEach
  void tearDown() {
    if (poller != null) {
      poller.destroy();
    }
  }

  @Test
  void pollOnceLeasesAndExecutesRemoteJob() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            context.progressListener().onProgress(1, 1, 0, 0, 0, 2, 3, "working");
            return ExecutionResult.success(4, 2, 0, 2, 3, "done");
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.lease(any(), eq("local-poller")))
        .thenReturn(java.util.Optional.of(lease), java.util.Optional.empty());
    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.reportProgress(
            any(), eq(1L), eq(1L), eq(0L), eq(0L), eq(0L), eq(2L), eq(3L), eq("working")))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.complete(
            any(),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(4L),
            eq(2L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(2L),
            eq(3L),
            eq("done")))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.pollOnce();

    assertTrue(completed.await(5, TimeUnit.SECONDS));
    verify(client).start(lease, "default_reconciler");
  }

  @Test
  void pollOnceReleasesWorkerSlotWhenLeaseRpcThrows() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch firstLeaseAttempted = new CountDownLatch(1);
    CountDownLatch secondLeaseAttempted = new CountDownLatch(1);
    AtomicInteger leaseAttempts = new AtomicInteger(0);
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            return ExecutionResult.success(0, 0, 0, 0, 0, "done");
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(3_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(1_000L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    when(client.lease(any(), eq("local-poller")))
        .thenAnswer(
            ignored -> {
              if (leaseAttempts.incrementAndGet() == 1) {
                firstLeaseAttempted.countDown();
                throw new RuntimeException("lease failed");
              }
              secondLeaseAttempted.countDown();
              return java.util.Optional.empty();
            });

    poller.pollOnce();
    assertTrue(firstLeaseAttempted.await(5, TimeUnit.SECONDS));

    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    do {
      poller.pollOnce();
      if (secondLeaseAttempted.await(25L, TimeUnit.MILLISECONDS)) {
        return;
      }
    } while (System.nanoTime() < deadlineNanos);
    assertTrue(secondLeaseAttempted.await(1, TimeUnit.SECONDS));
  }

  @Test
  void runLeaseCompletesConnectorMissingFailureAsCancelled() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            return ExecutionResult.failure(
                0,
                0,
                1,
                0,
                0,
                ExecutionResult.FailureKind.CONNECTOR_MISSING,
                ExecutionResult.RetryDisposition.RETRYABLE,
                "connector missing",
                new ReconcileFailureException(
                    ExecutionResult.FailureKind.CONNECTOR_MISSING, "connector missing", null));
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.CANCELLED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq("connector missing")))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));

    assertTrue(completed.await(5, TimeUnit.SECONDS));
    verify(client).start(lease, "default_reconciler");
  }

  @Test
  void runLeaseCompletesObsoleteOutcomeAsSucceeded() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor executor =
        remoteExecutor(
            "snapshot_finalize",
            context ->
                ReconcileExecutor.ExecutionResult.obsolete(
                    0,
                    0,
                    0,
                    0,
                    0,
                    1,
                    7,
                    ReconcileExecutor.ExecutionResult.FailureKind.NONE,
                    "Snapshot 42 already finalized by job winner",
                    null));

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-obsolete", ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE);

    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.CANCELLED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(7L),
            eq("Snapshot 42 already finalized by job winner")))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));

    assertTrue(completed.await(5, TimeUnit.SECONDS));
    verify(client).start(lease, "snapshot_finalize");
  }

  @Test
  void runLeaseCompletesTableMissingFailureAsCancelled() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            return ExecutionResult.failure(
                0,
                0,
                1,
                0,
                0,
                ExecutionResult.FailureKind.TABLE_MISSING,
                ExecutionResult.RetryDisposition.RETRYABLE,
                "table missing",
                new ReconcileFailureException(
                    ExecutionResult.FailureKind.TABLE_MISSING, "table missing", null));
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.CANCELLED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq("table missing")))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));

    assertTrue(completed.await(5, TimeUnit.SECONDS));
  }

  @Test
  void runLeaseCompletesViewMissingFailureAsCancelled() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            return ExecutionResult.failure(
                0,
                0,
                1,
                0,
                0,
                ExecutionResult.FailureKind.VIEW_MISSING,
                ExecutionResult.RetryDisposition.RETRYABLE,
                "view missing",
                new ReconcileFailureException(
                    ExecutionResult.FailureKind.VIEW_MISSING, "view missing", null));
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.CANCELLED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(1L),
            eq(0L),
            eq(0L),
            eq("view missing")))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));

    assertTrue(completed.await(5, TimeUnit.SECONDS));
  }

  @Test
  void pollOnceSkipsPollingWhenNoExecutorsAreEnabled() {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of());
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    poller.pollOnce();

    verify(client, times(0)).lease(any(), any());
  }

  @Test
  void pollOnceLeasesThroughExecutorScopedRemoteCapabilities() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch completed = new CountDownLatch(1);
    ReconcileExecutor plannerExecutor =
        remoteExecutor(
            "planner",
            Set.of(ReconcileJobKind.PLAN_CONNECTOR),
            context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "planner"));
    ReconcileExecutor fileGroupExecutor =
        remoteExecutor(
            "file-group",
            Set.of(ReconcileJobKind.EXEC_FILE_GROUP),
            context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "file-group"));

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry =
        new ReconcileExecutorRegistry(List.of(plannerExecutor, fileGroupExecutor));
    Config pollerConfig = mock(Config.class);
    when(pollerConfig.getOptionalValue(eq("reconciler.max-parallelism"), eq(Integer.class)))
        .thenReturn(java.util.Optional.of(2));
    when(pollerConfig.getOptionalValue(anyString(), eq(Long.class)))
        .thenReturn(java.util.Optional.empty());
    poller.config = pollerConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob fileGroupLease = leasedJob("job-3", ReconcileJobKind.EXEC_FILE_GROUP);
    AtomicInteger fileGroupLeaseAttempts = new AtomicInteger();

    when(client.lease(
            argThat(
                request ->
                    request != null
                        && request.jobKinds.equals(Set.of(ReconcileJobKind.PLAN_CONNECTOR))
                        && request.executorIds.equals(Set.of("planner"))),
            eq("local-poller")))
        .thenReturn(java.util.Optional.empty());
    when(client.lease(
            argThat(
                request ->
                    request != null
                        && request.jobKinds.equals(Set.of(ReconcileJobKind.EXEC_FILE_GROUP))
                        && request.executorIds.equals(Set.of("file-group"))),
            eq("local-poller")))
        .thenAnswer(
            invocation -> {
              return fileGroupLeaseAttempts.getAndIncrement() == 0
                  ? java.util.Optional.of(fileGroupLease)
                  : java.util.Optional.empty();
            });
    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            any(),
            any(),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            any()))
        .thenAnswer(
            invocation -> {
              completed.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    poller.pollOnce();

    assertTrue(completed.await(5, TimeUnit.SECONDS));
    verify(client).start(fileGroupLease, "file-group");
  }

  @Test
  void pollOnceUsesRemoteLeaseSourceInRemoteMode() {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "planner"));
    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "remote";
    poller.init();

    when(client.lease(any(), eq("remote-poller"))).thenReturn(java.util.Optional.empty());

    poller.pollOnce();

    verify(client, org.mockito.Mockito.timeout(5_000L).atLeastOnce())
        .lease(any(), eq("remote-poller"));
  }

  @Test
  void runLeaseRenewsInBackgroundDuringLongExecution() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch executionStarted = new CountDownLatch(1);
    CountDownLatch finishExecution = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ReconcileExecutor executor =
        new ReconcileExecutor() {
          @Override
          public String id() {
            return "default_reconciler";
          }

          @Override
          public ExecutionResult execute(ExecutionContext context) {
            executionStarted.countDown();
            try {
              assertTrue(finishExecution.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            return ExecutionResult.success(0, 0, 0, 0, 0, "done");
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(3_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(1_000L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_CAPTURE,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.renew(any()))
        .thenAnswer(invocation -> new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.complete(
            any(),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done")))
        .thenReturn(new RemoteReconcileExecutorClient.CompletionResult(true));

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();

    assertTrue(executionStarted.await(2, TimeUnit.SECONDS));
    verify(client, org.mockito.Mockito.timeout(5_000L).atLeastOnce()).renew(lease);
    finishExecution.countDown();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client).start(lease, "default_reconciler");
  }

  @Test
  void pollOnceSwallowsUnavailableDuringLocalStartup() {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "planner"));
    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    when(client.lease(any(), eq("local-poller")))
        .thenThrow(new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE));

    poller.pollOnce();

    verify(client, org.mockito.Mockito.timeout(5_000L)).lease(any(), eq("local-poller"));
  }

  @Test
  void runLeaseStopsWhenHeartbeatReturnsLeaseInvalid() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch stopped = new CountDownLatch(1);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> {
              long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
              while (!context.shouldStop().getAsBoolean() && System.nanoTime() < deadline) {
                try {
                  Thread.sleep(25L);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                }
              }
              stopped.countDown();
              return ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done");
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(3_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(1_000L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-1", ReconcileJobKind.PLAN_SNAPSHOT);
    when(client.renew(lease))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(false, false));
    when(client.cancellationRequested(lease)).thenReturn(false);

    Thread worker =
        new Thread(
            () ->
                poller.runLease(
                    new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease)));
    worker.start();

    assertTrue(stopped.await(5, TimeUnit.SECONDS));
    worker.join(5_000L);

    assertTrue(!worker.isAlive());
    verify(client).start(lease, "planner");
    verify(client).renew(lease);
    verify(client, never())
        .complete(
            any(), any(), any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), any());
  }

  @Test
  void runLeaseContinuesAfterTransientHeartbeatTransportFailure() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    CountDownLatch transportFailureSeen = new CountDownLatch(1);
    CountDownLatch renewedAfterFailure = new CountDownLatch(1);
    CountDownLatch allowCompletion = new CountDownLatch(1);
    CountDownLatch completionSubmitted = new CountDownLatch(1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger renewCalls = new AtomicInteger();
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> {
              try {
                assertTrue(transportFailureSeen.await(5, TimeUnit.SECONDS));
                assertTrue(renewedAfterFailure.await(5, TimeUnit.SECONDS));
                allowCompletion.countDown();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              return ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done");
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(10_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(200L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-transport", ReconcileJobKind.PLAN_SNAPSHOT);
    when(client.renew(lease))
        .thenAnswer(
            invocation -> {
              int call = renewCalls.incrementAndGet();
              if (call == 1) {
                transportFailureSeen.countDown();
                throw new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE);
              }
              renewedAfterFailure.countDown();
              return new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false);
            });
    when(client.cancellationRequested(lease)).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done")))
        .thenAnswer(
            invocation -> {
              completionSubmitted.countDown();
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();

    assertTrue(allowCompletion.await(5, TimeUnit.SECONDS));
    assertTrue(completionSubmitted.await(5, TimeUnit.SECONDS));
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client).start(lease, "planner");
    verify(client, times(2)).renew(lease);
    verify(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done"));
  }

  @Test
  void runLeaseStopsHeartbeatsOnceCompletionStarts() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger renewCalls = new AtomicInteger();
    CountDownLatch sawHeartbeat = new CountDownLatch(1);
    CountDownLatch completionStarted = new CountDownLatch(1);
    CountDownLatch allowCompletion = new CountDownLatch(1);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> {
              try {
                assertTrue(sawHeartbeat.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              return ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done");
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(10_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(200L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-stop-heartbeats", ReconcileJobKind.PLAN_SNAPSHOT);
    when(client.renew(lease))
        .thenAnswer(
            invocation -> {
              renewCalls.incrementAndGet();
              sawHeartbeat.countDown();
              return new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false);
            });
    when(client.cancellationRequested(lease)).thenReturn(false);
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done")))
        .thenAnswer(
            invocation -> {
              completionStarted.countDown();
              assertTrue(allowCompletion.await(5, TimeUnit.SECONDS));
              return new RemoteReconcileExecutorClient.CompletionResult(true);
            });

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();

    assertTrue(sawHeartbeat.await(5, TimeUnit.SECONDS));
    assertTrue(completionStarted.await(5, TimeUnit.SECONDS));
    int renewCountAtCompletionStart = renewCalls.get();
    Thread.sleep(500L);
    assertTrue(renewCalls.get() == renewCountAtCompletionStart);
    allowCompletion.countDown();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
  }

  @Test
  void runLeaseStopsHeartbeatsWhenHandledCompletionStarts() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicInteger renewCalls = new AtomicInteger();
    CountDownLatch sawHeartbeat = new CountDownLatch(1);
    CountDownLatch handledCompletionStarted = new CountDownLatch(1);
    CountDownLatch allowHandledCompletion = new CountDownLatch(1);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> {
              try {
                assertTrue(sawHeartbeat.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              context.beforeHandledCompletion().run();
              handledCompletionStarted.countDown();
              try {
                assertTrue(allowHandledCompletion.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              return ReconcileExecutor.ExecutionResult.successHandled(0, 0, 0, 0, 0, 0, 0, "done");
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(10_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(200L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-handled-stop-heartbeats", ReconcileJobKind.PLAN_TABLE);
    when(client.renew(lease))
        .thenAnswer(
            invocation -> {
              renewCalls.incrementAndGet();
              sawHeartbeat.countDown();
              return new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false);
            });
    when(client.cancellationRequested(lease)).thenReturn(false);

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();

    assertTrue(sawHeartbeat.await(5, TimeUnit.SECONDS));
    assertTrue(handledCompletionStarted.await(5, TimeUnit.SECONDS));
    int renewCountAtHandledCompletion = renewCalls.get();
    Thread.sleep(500L);
    assertEquals(renewCountAtHandledCompletion, renewCalls.get());
    allowHandledCompletion.countDown();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client, never())
        .complete(
            any(), any(), any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), any());
  }

  @Test
  void runLeaseDoesNotRenewDuringHandledCompletionWithoutExplicitProgress() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicInteger renewCalls = new AtomicInteger();
    CountDownLatch handledCompletionStarted = new CountDownLatch(1);
    CountDownLatch allowHandledCompletion = new CountDownLatch(1);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner",
            context -> {
              context.beforeHandledCompletion().run();
              handledCompletionStarted.countDown();
              try {
                assertTrue(allowHandledCompletion.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
              return ReconcileExecutor.ExecutionResult.successHandled(0, 0, 0, 0, 0, 0, 0, "done");
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(10_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(200L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-no-final-confirm", ReconcileJobKind.PLAN_TABLE);
    when(client.renew(lease))
        .thenAnswer(
            invocation -> {
              renewCalls.incrementAndGet();
              return new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false);
            });
    when(client.cancellationRequested(lease)).thenReturn(false);
    Thread worker =
        new Thread(
            () ->
                poller.runLease(
                    new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease)));
    worker.start();

    assertTrue(handledCompletionStarted.await(5, TimeUnit.SECONDS));
    int renewCountAtHandledCompletion = renewCalls.get();
    Thread.sleep(500L);
    assertEquals(renewCountAtHandledCompletion, renewCalls.get());
    allowHandledCompletion.countDown();
    worker.join(5_000L);

    verify(client).start(lease, "planner");
    verify(client, never())
        .complete(
            any(), any(), any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), any());
  }

  @Test
  void runLeaseDoesNotCompleteWhenTerminalStateIsUncertain() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ReconcileExecutor executor =
        remoteExecutor(
            "file-group",
            context -> {
              throw new ReconcileFailureException(
                  ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
                  ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
                  ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
                  "terminal submit uncertain",
                  new RuntimeException("response lost"));
            });

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    Config heartbeatConfig = mock(Config.class);
    when(heartbeatConfig.getOptionalValue("reconciler.max-parallelism", Integer.class))
        .thenReturn(java.util.Optional.of(1));
    when(heartbeatConfig.getOptionalValue("floecat.reconciler.job-store.lease-ms", Long.class))
        .thenReturn(java.util.Optional.of(3_000L));
    when(heartbeatConfig.getOptionalValue("reconciler.lease-heartbeat-ms", Long.class))
        .thenReturn(java.util.Optional.of(1_000L));
    poller.config = heartbeatConfig;
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-uncertain", ReconcileJobKind.EXEC_FILE_GROUP);
    when(client.renew(lease))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(lease)).thenReturn(false);

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client).start(lease, "file-group");
    verify(client, never())
        .complete(
            any(), any(), any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), any());
  }

  @Test
  void runLeaseDoesNotCompleteWhenStartStateIsUncertain() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ReconcileExecutor executor =
        remoteExecutor(
            "planner", context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done"));

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-start-uncertain", ReconcileJobKind.PLAN_CONNECTOR);
    org.mockito.Mockito.doThrow(new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE))
        .when(client)
        .start(lease, "planner");

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client).start(lease, "planner");
    verify(client, never())
        .complete(
            any(), any(), any(), any(), anyLong(), anyLong(), anyLong(), anyLong(), anyLong(),
            anyLong(), anyLong(), any());
  }

  @Test
  void runLeaseDoesNotConvertSuccessfulCompleteTransportFailureIntoFailedComplete()
      throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    ReconcileExecutor executor =
        remoteExecutor(
            "planner", context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done"));

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-complete-uncertain", ReconcileJobKind.PLAN_CONNECTOR);
    when(client.renew(lease))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(lease)).thenReturn(false);
    org.mockito.Mockito.doThrow(new io.grpc.StatusRuntimeException(io.grpc.Status.UNAVAILABLE))
        .when(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done"));

    Thread worker =
        new Thread(
            () -> {
              try {
                poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));
              } catch (Throwable t) {
                failure.set(t);
              }
            });
    worker.start();
    worker.join(5_000L);

    if (failure.get() != null) {
      throw new AssertionError("runLease failed", failure.get());
    }
    assertTrue(!worker.isAlive());
    verify(client).start(lease, "planner");
    verify(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done"));
    verify(client, never())
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.FAILED),
            any(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            any());
  }

  @Test
  void runLeaseDoesNotClassifyNonTransportCompleteFailureAsStateUncertain() throws Exception {
    RemoteReconcileExecutorClient client = mock(RemoteReconcileExecutorClient.class);
    ReconcileExecutor executor =
        remoteExecutor(
            "planner", context -> ReconcileExecutor.ExecutionResult.success(0, 0, 0, 0, 0, "done"));

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.workerModeValue = "local";
    poller.init();

    RemoteLeasedJob lease = leasedJob("job-complete-precondition", ReconcileJobKind.PLAN_CONNECTOR);
    when(client.renew(lease))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(lease)).thenReturn(false);
    io.grpc.StatusRuntimeException precondition =
        new io.grpc.StatusRuntimeException(io.grpc.Status.FAILED_PRECONDITION);
    org.mockito.Mockito.doThrow(precondition)
        .when(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done"));
    when(client.complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.FAILED),
            any(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            any()))
        .thenReturn(new RemoteReconcileExecutorClient.CompletionResult(false));

    poller.runLease(new RemoteReconcileExecutorPoller.LeaseAssignment(executor, lease));

    verify(client).start(lease, "planner");
    verify(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq(0L),
            eq("done"));
    verify(client)
        .complete(
            eq(lease),
            eq(RemoteLeasedJob.CompletionState.FAILED),
            any(),
            any(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            anyLong(),
            any());
  }

  private static ReconcileExecutor remoteExecutor(
      String id,
      java.util.function.Function<
              ReconcileExecutor.ExecutionContext, ReconcileExecutor.ExecutionResult>
          fn) {
    return remoteExecutor(id, java.util.EnumSet.allOf(ReconcileJobKind.class), fn);
  }

  private static ReconcileExecutor remoteExecutor(
      String id,
      Set<ReconcileJobKind> jobKinds,
      java.util.function.Function<
              ReconcileExecutor.ExecutionContext, ReconcileExecutor.ExecutionResult>
          fn) {
    return new ReconcileExecutor() {
      @Override
      public String id() {
        return id;
      }

      @Override
      public Set<ReconcileJobKind> supportedJobKinds() {
        return jobKinds;
      }

      @Override
      public Set<String> supportedLanes() {
        return Set.of();
      }

      @Override
      public boolean supportsLane(String lane) {
        return true;
      }

      @Override
      public ExecutionResult execute(ExecutionContext context) {
        return fn.apply(context);
      }
    };
  }

  private static RemoteLeasedJob leasedJob(String jobId, ReconcileJobKind jobKind) {
    return new RemoteLeasedJob(
        new ReconcileJobStore.LeasedJob(
            jobId,
            "acct",
            "connector-1",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-" + jobId,
            "",
            "",
            jobKind,
            ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
            ""));
  }
}
