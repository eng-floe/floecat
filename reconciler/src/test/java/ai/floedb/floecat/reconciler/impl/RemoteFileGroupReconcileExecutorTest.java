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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RemoteFileGroupReconcileExecutorTest {

  @Test
  void executePersistsFullFailureChainForFileGroupCaptureErrors() {
    RemoteFileGroupWorkerClient workerClient = mock(RemoteFileGroupWorkerClient.class);
    StandaloneJavaFileGroupExecutionRunner runner =
        mock(StandaloneJavaFileGroupExecutionRunner.class);
    RemoteFileGroupReconcileExecutor executor =
        new RemoteFileGroupReconcileExecutor(workerClient, runner, true);

    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            "job-1",
            "acct",
            "connector-1",
            false,
            ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
            ReconcileScope.empty(),
            ReconcileExecutionPolicy.defaults(),
            "lease-1",
            "",
            "",
            ReconcileJobKind.EXEC_FILE_GROUP,
            ReconcileTableTask.empty(),
            ReconcileViewTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask.empty(),
            ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.of(
                "plan-1", "group-1", "table-1", 41L, List.of("s3://bucket/file.parquet")),
            "");
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-1")
            .build();
    Connector connector =
        Connector.newBuilder()
            .setKind(ConnectorKind.CK_DELTA)
            .setAuth(AuthConfig.getDefaultInstance())
            .build();
    StandaloneFileGroupExecutionPayload payload =
        new StandaloneFileGroupExecutionPayload(
            "job-1",
            "lease-1",
            "parent-1",
            connector,
            "floedb.obs",
            "otel_spans_raw",
            tableId,
            41L,
            "plan-1",
            "group-1",
            List.of("s3://bucket/file.parquet"),
            ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS)));
    RuntimeException failure =
        new RuntimeException(
            "Delta stats compute failed (version 41)",
            new IllegalArgumentException("Unsupported parquet page type DICTIONARY_PAGE"));

    when(workerClient.getExecution(remoteLease)).thenReturn(payload);
    when(runner.execute(payload)).thenThrow(failure);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    String expectedDetail =
        "RuntimeException: Delta stats compute failed (version 41)"
            + " | caused by: IllegalArgumentException: Unsupported parquet page type"
            + " DICTIONARY_PAGE";
    assertFalse(result.ok());
    assertEquals("File-group capture failed: " + expectedDetail, result.message);
    verify(workerClient)
        .submitFailure(eq(remoteLease), eq("job-1:plan-1:group-1:failure"), eq(expectedDetail));
  }
}
