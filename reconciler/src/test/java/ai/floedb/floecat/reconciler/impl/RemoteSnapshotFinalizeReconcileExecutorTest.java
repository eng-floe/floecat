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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RemoteSnapshotFinalizeReconcileExecutorTest {

  @Test
  void rejectsMissingRequestedIndexArtifactCoverage() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteSnapshotFinalizeReconcileExecutor.validateIndexArtifactCoverage(
                    true,
                    Set.of("s3://bucket/a.parquet", "s3://bucket/b.parquet"),
                    Set.of("s3://bucket/a.parquet")));

    assertTrue(error.getMessage().contains("do not cover successful files"));
  }

  @Test
  void rejectsUnrequestedIndexArtifacts() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteSnapshotFinalizeReconcileExecutor.validateIndexArtifactCoverage(
                    false, Set.of("s3://bucket/a.parquet"), Set.of("s3://bucket/a.parquet")));

    assertTrue(error.getMessage().contains("unrequested index artifacts"));
  }

  @Test
  void expectedFileStatsIncludeDeletesAttachedToSuccessfulDataFiles() {
    String dataPath = "s3://bucket/data.parquet";
    String deletePath = "s3://bucket/delete.parquet";
    ReconcileFileExecutionPlan executionPlan =
        ReconcileFileExecutionPlan.of(
            dataPath,
            100L,
            "",
            null,
            "PARQUET",
            0,
            List.of(
                new ReconcileFileExecutionPlan.IcebergDeleteFile(
                    deletePath,
                    10L,
                    ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
                    0,
                    List.of())));
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
            "plan",
            "group",
            tableId().getId(),
            55L,
            1,
            "",
            0,
            List.of(dataPath),
            List.of(),
            List.of(),
            "{}",
            List.of(executionPlan));

    Set<String> targets =
        RemoteSnapshotFinalizeReconcileExecutor.expectedFileStatsTargets(group, Set.of(dataPath));

    assertEquals(2, targets.size());
    assertTrue(
        targets.contains(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
                ai.floedb.floecat.stats.identity.StatsTargetIdentity.fileTarget(dataPath))));
    assertTrue(
        targets.contains(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
                ai.floedb.floecat.stats.identity.StatsTargetIdentity.fileTarget(deletePath))));
  }

  @Test
  void acceptsEmptyFileGroupWithoutStatsPartials() {
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
            1,
            "/final-stats.pb",
            "/capture-manifest.pb",
            null);

    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validatePartialAggregates(
                input,
                true,
                0,
                Set.of(
                    FloecatConnector.StatsTargetKind.TABLE,
                    FloecatConnector.StatsTargetKind.COLUMN),
                List.of()));
  }

  @Test
  void rejectsMissingRequestedPartialsForNonemptyFileGroup() {
    StandaloneSnapshotFinalizeExecutionPayload input =
        new StandaloneSnapshotFinalizeExecutionPayload(
            "finalize-job",
            "lease-1",
            "snapshot-job",
            tableId(),
            55L,
            true,
            1,
            "/snapshot-plan.json",
            1,
            "/final-stats.pb",
            "/capture-manifest.pb",
            null);

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteSnapshotFinalizeReconcileExecutor.validatePartialAggregates(
                    input,
                    true,
                    1,
                    Set.of(
                        FloecatConnector.StatsTargetKind.TABLE,
                        FloecatConnector.StatsTargetKind.COLUMN),
                    List.of()));
    assertTrue(error.getMessage().contains("table aggregate partial coverage"));
  }

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
                List.of(),
                Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, scope);
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    var predecessor =
        new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
            "generation-1", 7L, "/capture-1.pb", 9L);
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
            "/capture-manifest.pb",
            predecessor);

    when(workerClient.getSnapshotFinalizeInput(remoteLease)).thenReturn(input);
    RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess prepared =
        preparedSnapshotFinalizeSuccess();
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), anyList(), anyList(), anyList(), anyList(),
            any()))
        .thenReturn(prepared);
    when(workerClient.submitSnapshotFinalizeSuccess(any(), any())).thenReturn(true);

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
    ArgumentCaptor<ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor> indexPredecessor =
        ArgumentCaptor.forClass(
            ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor.class);
    verify(workerClient)
        .prepareSnapshotFinalizeSuccess(
            any(),
            any(),
            any(),
            any(),
            anyInt(),
            anyList(),
            anyList(),
            finalStats.capture(),
            anyList(),
            indexPredecessor.capture());
    verify(workerClient).submitSnapshotFinalizeSuccess(remoteLease, prepared);
    assertEquals(1, finalStats.getValue().size());
    assertTrue(finalStats.getValue().get(0).hasTable());
    assertEquals(0L, finalStats.getValue().get(0).getTable().getRowCount());
    assertEquals(predecessor, indexPredecessor.getValue());
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
            "/capture-manifest.pb",
            null);

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
        .prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), any(), any(), any(), any(), any());
    verify(workerClient, never()).submitSnapshotFinalizeSuccess(any(), any());
  }

  @Test
  void preflightValidationFailureIsTerminalBeforeSubmissionBoundary() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, mock(BlobStore.class), mock(SnapshotPlanBlobStore.class), true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, ReconcileScope.empty());
    StandaloneSnapshotFinalizeExecutionPayload input = emptyFinalizeInput();
    AtomicBoolean submissionStarted = new AtomicBoolean();

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(input);
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), anyList(), anyList(), anyList(), anyList(),
            any()))
        .thenThrow(new IllegalArgumentException("inconsistent predecessors"));
    when(workerClient.submitSnapshotFinalizeFailure(any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (a, b, c, d, e, f, g, h) -> {},
                () -> submissionStarted.set(true)));

    assertEquals(ReconcileExecutor.ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.NONE, result.retryClass);
    assertFalse(submissionStarted.get());
    verify(workerClient, never()).submitSnapshotFinalizeSuccess(any(), any());
  }

  @Test
  void definitiveSubmissionRejectionIsTerminal() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, mock(BlobStore.class), mock(SnapshotPlanBlobStore.class), true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, ReconcileScope.empty());
    AtomicBoolean submissionStarted = new AtomicBoolean();

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(emptyFinalizeInput());
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), anyList(), anyList(), anyList(), anyList(),
            any()))
        .thenReturn(preparedSnapshotFinalizeSuccess());
    when(workerClient.submitSnapshotFinalizeSuccess(any(), any())).thenReturn(false);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (a, b, c, d, e, f, g, h) -> {},
                () -> submissionStarted.set(true)));

    assertEquals(ReconcileExecutor.ExecutionResult.JobOutcome.TERMINAL_FAILURE, result.outcome);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.NONE, result.retryClass);
    assertTrue(submissionStarted.get());
  }

  @Test
  void uncertainRpcOutcomeRemainsRetryableStateUncertain() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, mock(BlobStore.class), mock(SnapshotPlanBlobStore.class), true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, ReconcileScope.empty());
    ReconcileFailureException uncertain =
        new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
            "outcome unknown",
            null);

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(emptyFinalizeInput());
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), anyInt(), anyList(), anyList(), anyList(), anyList(),
            any()))
        .thenReturn(preparedSnapshotFinalizeSuccess());
    when(workerClient.submitSnapshotFinalizeSuccess(any(), any())).thenThrow(uncertain);

    ReconcileFailureException thrown =
        assertThrows(
            ReconcileFailureException.class,
            () ->
                executor.execute(
                    new ReconcileExecutor.ExecutionContext(
                        lease, () -> false, (a, b, c, d, e, f, g, h) -> {})));

    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE, thrown.retryDisposition());
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN, thrown.retryClass());
  }

  private static ReconcileJobStore.LeasedJob leasedFinalizeJob() {
    return leasedFinalizeJob(2, ReconcileScope.empty());
  }

  private static StandaloneSnapshotFinalizeExecutionPayload emptyFinalizeInput() {
    return new StandaloneSnapshotFinalizeExecutionPayload(
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
        "/capture-manifest.pb",
        null);
  }

  private static RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess
      preparedSnapshotFinalizeSuccess() {
    return new RemoteSnapshotFinalizeWorkerClient.PreparedSnapshotFinalizeSuccess(
        "result-1", SnapshotCaptureManifestDescriptor.getDefaultInstance());
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
        0,
        ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest.sha256(
            java.util.List.of(), java.util.List.of()),
        null,
        1L);
  }
}
