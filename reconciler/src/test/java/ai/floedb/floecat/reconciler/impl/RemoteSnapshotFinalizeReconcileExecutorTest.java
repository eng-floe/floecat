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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RemoteSnapshotFinalizeReconcileExecutorTest {

  @Test
  void finalizesExplicitEmptySnapshotRemotely() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, blobStore, snapshotPlanBlobStore, true);
    ReconcileScope scope =
        ReconcileScope.of(
            List.of(),
            "table-1",
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, scope);
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    StandaloneSnapshotFinalizeExecutionPayload input =
        new StandaloneSnapshotFinalizeExecutionPayload(
            "finalize-job",
            "lease-1",
            "snapshot-job",
            tableId(),
            55L,
            true,
            0,
            "",
            0,
            "/final-stats.pb",
            "/capture-manifest.pb");

    when(workerClient.getSnapshotFinalizeInput(remoteLease)).thenReturn(input);
    when(workerClient.submitSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), anyList(), anyList(), anyList(), anyList()))
        .thenReturn(true);

    assertTrue(executor.supports(lease));
    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.ok());
    assertEquals(1L, result.statsProcessed);
    verify(snapshotPlanBlobStore, never()).loadFileGroupsByUri(any());
    verify(workerClient, never()).listSnapshotFileGroupResults(any());
    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<TargetStatsRecord>> finalStats = ArgumentCaptor.forClass(List.class);
    verify(workerClient)
        .submitSnapshotFinalizeSuccess(
            any(),
            any(),
            any(),
            any(),
            anyInt(),
            anyList(),
            anyList(),
            finalStats.capture(),
            anyList());
    assertEquals(1, finalStats.getValue().size());
    assertTrue(finalStats.getValue().get(0).hasTable());
    assertEquals(0L, finalStats.getValue().get(0).getTable().getRowCount());
  }

  @Test
  void rejectsDescriptorThatWasNotInImmutableSnapshotPlan() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, blobStore, snapshotPlanBlobStore, true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob();
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    StandaloneSnapshotFinalizeExecutionPayload input =
        new StandaloneSnapshotFinalizeExecutionPayload(
            "finalize-job",
            "lease-1",
            "snapshot-job",
            tableId(),
            55L,
            true,
            0,
            "/snapshot-plan.json",
            2,
            "/final-stats.pb",
            "/capture-manifest.pb");

    when(workerClient.getSnapshotFinalizeInput(remoteLease)).thenReturn(input);
    when(snapshotPlanBlobStore.loadFileGroupsByUri("/snapshot-plan.json"))
        .thenReturn(List.of(group("plan-1", "group-a"), group("plan-1", "group-b")));
    when(workerClient.listSnapshotFileGroupResults(remoteLease))
        .thenReturn(List.of(descriptor("plan-1", "group-a"), descriptor("plan-1", "group-c")));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    assertTrue(result.message.contains("unexpected snapshot file-group descriptor plan-1/group-c"));
    verify(blobStore, never()).get(any());
    verify(workerClient)
        .submitSnapshotFinalizeFailure(
            any(), any(), contains("unexpected snapshot file-group descriptor plan-1/group-c"));
    verify(workerClient, never())
        .submitSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), any(), any(), any(), any());
  }

  private static ReconcileJobStore.LeasedJob leasedFinalizeJob() {
    return leasedFinalizeJob(2, ReconcileScope.empty());
  }

  private static ReconcileJobStore.LeasedJob leasedFinalizeJob(
      int fileGroupCount, ReconcileScope scope) {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
            "table-1",
            55L,
            "db",
            "events",
            List.of(),
            true,
            ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
            "/snapshot-plan.json",
            fileGroupCount);
    return new ReconcileJobStore.LeasedJob(
        "finalize-job",
        "acct",
        "connector-1",
        true,
        ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "lease-1",
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        "snapshot-job");
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId("table-1")
        .build();
  }

  private static ReconcileFileGroupTask group(String planId, String groupId) {
    return ReconcileFileGroupTask.of(
        planId, groupId, "table-1", 55L, List.of("s3://bucket/" + groupId + ".parquet"));
  }

  private static ReconcileFileGroupResultDescriptor descriptor(String planId, String groupId) {
    return new ReconcileFileGroupResultDescriptor(
        1,
        "acct",
        "connector-1",
        "snapshot-job",
        "file-group-job-" + groupId,
        planId,
        groupId,
        "table-1",
        55L,
        "file-group-lease-" + groupId,
        "result-" + groupId,
        "/results/" + groupId + ".pb",
        1L,
        "sha256",
        1,
        1,
        0,
        0,
        0,
        0,
        "",
        0L,
        "",
        0,
        1L);
  }
}
