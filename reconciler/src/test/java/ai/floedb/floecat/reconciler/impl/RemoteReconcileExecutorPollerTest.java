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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.spi.ReconcileExecutor;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
            context.progressListener().onProgress(1, 1, 0, 2, 3, "working");
            return ExecutionResult.success(4, 2, 0, 2, 3, "done");
          }
        };

    poller = new RemoteReconcileExecutorPoller();
    poller.client = client;
    poller.executorRegistry = new ReconcileExecutorRegistry(List.of(executor));
    poller.config = ConfigProvider.getConfig();
    poller.remoteExecutorEnabled = true;
    poller.init();

    RemoteLeasedJob lease =
        new RemoteLeasedJob(
            new ReconcileJobStore.LeasedJob(
                "job-1",
                "acct",
                "connector-1",
                false,
                CaptureMode.METADATA_AND_STATS,
                ReconcileScope.empty(),
                ReconcileExecutionPolicy.defaults(),
                "lease-1",
                "",
                ""));

    when(client.lease(eq(executor)))
        .thenReturn(java.util.Optional.of(lease), java.util.Optional.empty());
    when(client.renew(any()))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.cancellationRequested(any())).thenReturn(false);
    when(client.reportProgress(any(), eq(1L), eq(1L), eq(0L), eq(2L), eq(3L), eq("working")))
        .thenReturn(new RemoteReconcileExecutorClient.LeaseHeartbeat(true, false));
    when(client.complete(
            any(),
            eq(RemoteLeasedJob.CompletionState.SUCCEEDED),
            eq(4L),
            eq(2L),
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
}
