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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.auth.ReconcileWorkerAuthProvider;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactBundleSelection;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RemoteSnapshotPlanningReconcileExecutorTest {

  @Test
  void reuseManifestIdentityRequiresAccountAndConnector() {
    SnapshotCaptureManifest valid =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(9001L)
            .setReusableArtifactBundlesComplete(true)
            .build();

    ReconcileFailureException accountFailure =
        assertThrows(
            ReconcileFailureException.class,
            () ->
                RemoteSnapshotPlanningReconcileExecutor.validateReuseManifestIdentity(
                    tableId().toBuilder().setAccountId("other-acct").build(),
                    9001L,
                    "connector-1",
                    valid,
                    "/reuse.pb"));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        accountFailure.retryDisposition());
    ReconcileFailureException connectorFailure =
        assertThrows(
            ReconcileFailureException.class,
            () ->
                RemoteSnapshotPlanningReconcileExecutor.validateReuseManifestIdentity(
                    tableId(), 9001L, "other-connector", valid, "/reuse.pb"));
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        connectorFailure.retryDisposition());
  }

  @Test
  void planningLoadsReuseManifestFromLatestReconciledSnapshotWithNonMonotonicIds()
      throws Exception {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(snapshotFile("file-1", 10L)), List.of())));
    long reuseBasisSnapshotId = 7L;
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(reuseBasisSnapshotId)
            .setReusableArtifactBundlesComplete(true)
            .build()
            .toByteArray();
    String uri = "/reuse/7.pb";
    byte[] manifestSha256 =
        java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(reuseBasisSnapshotId)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setUri(uri)
                            .setPayloadBytes(manifestBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(manifestSha256))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    when(blobStore.get(uri)).thenReturn(manifestBytes);
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    lease, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .success());
    verify(blobStore).get(uri);
    verify(backend, never()).existingSnapshotIds(any(), any());
  }

  @Test
  void planningFailsTerminalWhenLatestReuseManifestIntegrityIsInvalid() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(snapshotFile("file-1", 10L)), List.of())));
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(9001L)
            .setReusableArtifactBundlesComplete(true)
            .build()
            .toByteArray();
    String uri = "/reuse/corrupt-9001.pb";
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(9001L)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setUri(uri)
                            .setPayloadBytes(manifestBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    when(blobStore.get(uri)).thenReturn(manifestBytes);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(!result.success());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.NONE, result.retryClass);
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            argThat(detail -> detail.contains("snapshot reuse manifest metadata mismatch")));
    verify(workerClient, never()).submitPlanSnapshotSuccess(any(), any(), any(), any());
  }

  @Test
  void planningFailsTerminalWhenLatestReuseManifestUriIsBlank() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(snapshotFile("file-1", 10L)), List.of())));
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(9001L)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setPayloadBytes(1L)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.success());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.NONE, result.retryClass);
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.NONE),
            argThat(detail -> detail.contains("snapshot reuse manifest URI is missing")));
    verify(blobStore, never()).get(any());
    verify(workerClient, never()).submitPlanSnapshotSuccess(any(), any(), any(), any());
  }

  @Test
  void planningRegeneratesWhenNoReconciledSnapshotHasReusableArtifacts() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(snapshotFile("file-1", 10L)), List.of())));
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(Optional.empty());
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    lease, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .success());
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            any(),
            any(),
            argThat(
                fileGroupJobs ->
                    !fileGroupJobs.isEmpty()
                        && fileGroupJobs.stream()
                            .allMatch(job -> !job.fileGroupTask().fileExecutionPlans().isEmpty())
                        && fileGroupJobs.stream()
                            .flatMap(job -> job.fileGroupTask().fileExecutionPlans().stream())
                            .allMatch(
                                plan ->
                                    plan.reusableArtifactBundleSelections().isEmpty()
                                        && !plan.sourceFingerprint().isBlank()
                                        && !plan.statsCaptureSignature().isBlank())),
            any());
    verify(backend, never()).existingSnapshotIds(any(), any());
    verify(blobStore, never()).get(any());
  }

  @Test
  void planningFailsClosedWhenSelectedReuseManifestIsUnavailableOrInvalid() {
    assertPlanningFailsWhenManifestIsUnavailable(
        ManifestUnavailableMode.NULL_BLOB,
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
        "snapshot reuse manifest is unavailable");
    assertPlanningFailsWhenManifestIsUnavailable(
        ManifestUnavailableMode.NOT_FOUND,
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
        ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR,
        "snapshot reuse manifest is unavailable");
    assertPlanningFailsWhenManifestIsUnavailable(
        ManifestUnavailableMode.UNMARKED_MANIFEST,
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        ReconcileExecutor.ExecutionResult.RetryClass.NONE,
        "snapshot reuse manifest is incomplete");
    assertPlanningFailsWhenManifestIsUnavailable(
        ManifestUnavailableMode.MISSING_REFERENCE,
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL,
        ReconcileExecutor.ExecutionResult.RetryClass.NONE,
        "snapshot reuse manifest reference is missing");
  }

  private static void assertPlanningFailsWhenManifestIsUnavailable(
      ManifestUnavailableMode unavailableMode,
      ReconcileExecutor.ExecutionResult.RetryDisposition expectedDisposition,
      ReconcileExecutor.ExecutionResult.RetryClass expectedRetryClass,
      String expectedDetail) {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(snapshotFile("file-1", 10L)), List.of())));
    String uri = "/reuse/missing-9001.pb";
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(9001L)
            .build()
            .toByteArray();
    boolean unmarked = unavailableMode == ManifestUnavailableMode.UNMARKED_MANIFEST;
    Snapshot.Builder basis = Snapshot.newBuilder().setTableId(tableId()).setSnapshotId(9001L);
    if (unavailableMode != ManifestUnavailableMode.MISSING_REFERENCE) {
      basis.setReuseManifestRef(
          SnapshotReuseManifestRef.newBuilder()
              .setUri(uri)
              .setPayloadBytes(unmarked ? manifestBytes.length : 123L)
              .setPayloadSha256(
                  ByteString.copyFrom(unmarked ? sha256(manifestBytes) : new byte[32]))
              .setStatsGenerationManifestUri("/stats/generation.pb"));
    }
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(Optional.of(basis.build()));
    if (unavailableMode == ManifestUnavailableMode.NOT_FOUND) {
      when(blobStore.get(uri)).thenThrow(new StorageNotFoundException("missing"));
    } else if (unavailableMode == ManifestUnavailableMode.NULL_BLOB) {
      when(blobStore.get(uri)).thenReturn(null);
    } else if (unmarked) {
      when(blobStore.get(uri)).thenReturn(manifestBytes);
    }
    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.success());
    assertEquals(expectedDisposition, result.retryDisposition);
    assertEquals(expectedRetryClass, result.retryClass);
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(expectedDisposition),
            eq(expectedRetryClass),
            argThat(detail -> detail.contains(expectedDetail)));
    verify(workerClient, never()).submitPlanSnapshotSuccess(any(), any(), any(), any());
  }

  private enum ManifestUnavailableMode {
    NULL_BLOB,
    NOT_FOUND,
    UNMARKED_MANIFEST,
    MISSING_REFERENCE
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void executeUsesDirectStatsFastPathForStatsOnlySnapshot() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    ReconcileSnapshotTask task = snapshotTask();
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                task));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(
            Optional.of(
                FloecatConnector.DirectSnapshotStatsCapture.of(
                    List.of(
                        TargetStatsRecords.tableRecord(
                            tableId(),
                            55L,
                            TableValueStats.newBuilder().setRowCount(7L).build(),
                            null)),
                    5,
                    List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never()).fetchSnapshotFilePlan(any(), any(), anyLong());
    verify(backend, never()).putTargetStats(any(), any());
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                snapshotTask ->
                    snapshotTask != null
                        && snapshotTask.fileGroups().isEmpty()
                        && snapshotTask.fileGroupPlanRecorded()
                        && snapshotTask.sourceFileCount() == 5
                        && snapshotTask.directStatsRecordCount() == 1
                        && snapshotTask.completionMode()
                            == ReconcileSnapshotTask.CompletionMode.DIRECT_STATS),
            argThat(fileGroupJobs -> fileGroupJobs != null && fileGroupJobs.isEmpty()),
            argThat(stats -> stats != null && stats.size() == 1));
  }

  @Test
  void executeFallsBackToFileGroupsWhenPageIndexesAreRequested() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(pageIndexScope());
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                pageIndexScope(),
                snapshotTask()));
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/file-1.parquet",
                            "PARQUET",
                            10L,
                            1L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            List.of(),
                            null,
                            new FloecatConnector.SnapshotDeletionVector("i", "encoded", null, 7, 2),
                            List.of(
                                new FloecatConnector.SnapshotIcebergDeleteFile(
                                    "s3://bucket/delete-1.parquet",
                                    4L,
                                    ai.floedb.floecat.catalog.rpc.FileContent.FC_EQUALITY_DELETES,
                                    3,
                                    List.of(7),
                                    "iceberg-delete-v1:11:2")),
                            "iceberg-data-v1:12:1")),
                    List.of(),
                    "{\"type\":\"struct\",\"fields\":[]}")));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never())
        .captureSnapshotTargetStatsDirect(any(), any(), anyLong(), any(), any(), any());
    verify(backend).fetchSnapshotFilePlan(any(), any(), eq(55L));
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                snapshotTask ->
                    snapshotTask != null
                        && snapshotTask.completionMode()
                            == ReconcileSnapshotTask.CompletionMode.FILE_GROUPS
                        && snapshotTask.fileGroups().size() == 1),
            argThat(
                fileGroupJobs ->
                    fileGroupJobs != null
                        && fileGroupJobs.size() == 1
                        && fileGroupJobs.getFirst().fileGroupTask().fileExecutionPlans().size() == 1
                        && fileGroupJobs
                            .getFirst()
                            .fileGroupTask()
                            .executionSchemaJson()
                            .contains("struct")
                        && fileGroupJobs
                                .getFirst()
                                .fileGroupTask()
                                .fileExecutionPlans()
                                .getFirst()
                                .deletionVector()
                                .cardinality()
                            == 2
                        && fileGroupJobs
                            .getFirst()
                            .fileGroupTask()
                            .fileExecutionPlans()
                            .getFirst()
                            .contentIdentity()
                            .equals("iceberg-data-v1:12:1")
                        && fileGroupJobs
                            .getFirst()
                            .fileGroupTask()
                            .fileExecutionPlans()
                            .getFirst()
                            .icebergDeleteFiles()
                            .getFirst()
                            .equalityFieldIds()
                            .equals(List.of(7))
                        && fileGroupJobs
                            .getFirst()
                            .fileGroupTask()
                            .fileExecutionPlans()
                            .getFirst()
                            .icebergDeleteFiles()
                            .getFirst()
                            .contentIdentity()
                            .equals("iceberg-delete-v1:11:2")),
            argThat(stats -> stats != null && stats.isEmpty()));
  }

  @Test
  void executeFallsBackToFileGroupsWhenDirectStatsAreUnavailable() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFilePlan(
                    List.of(
                        new FloecatConnector.SnapshotFileEntry(
                            "s3://bucket/file-1.parquet",
                            "PARQUET",
                            10L,
                            1L,
                            ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
                            "",
                            0,
                            List.of(),
                            null,
                            null,
                            List.of(),
                            "test-file-v1:file-1")),
                    List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend).captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any());
    verify(backend).fetchSnapshotFilePlan(any(), any(), eq(55L));
  }

  @Test
  void executeTreatsInvalidLeaseDuringResultSubmissionAsCancellation() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(
            Optional.of(
                FloecatConnector.DirectSnapshotStatsCapture.of(
                    List.of(
                        TargetStatsRecords.tableRecord(
                            tableId(),
                            55L,
                            TableValueStats.newBuilder().setRowCount(7L).build(),
                            null)),
                    5,
                    List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any()))
        .thenThrow(
            new RemoteLeasePreconditionFailedException(
                "submitLeasedPlanSnapshotResult",
                Status.FAILED_PRECONDITION
                    .withDescription("some server-side precondition text")
                    .asRuntimeException()));
    AtomicBoolean beforeHandledCompletion = new AtomicBoolean();

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease,
                () -> false,
                (a, b, c, d, e, f, g, h) -> {},
                () -> beforeHandledCompletion.set(true)));

    assertThat(result.cancelled).isTrue();
    assertThat(beforeHandledCompletion.get()).isTrue();
    verify(workerClient, never()).submitPlanSnapshotFailure(any(), any(), any(), any(), any());
  }

  @Test
  void executePartitionsPlannedFileGroupsByConfiguredMaxFilesPerGroup() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    RemoteLeasedJob remoteLease = new RemoteLeasedJob(lease);
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(new FloecatConnector.SnapshotFilePlan(snapshotFiles(40), List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(workerClient)
        .submitPlanSnapshotSuccess(
            eq(remoteLease),
            argThat(
                plannedSnapshot ->
                    plannedSnapshot != null
                        && plannedSnapshot.sourceFileCount() == 40
                        && plannedSnapshot.fileGroups().size() == 20
                        && plannedSnapshot.fileGroups().stream()
                            .allMatch(group -> group.filePaths().size() <= 2)),
            argThat(fileGroupJobs -> fileGroupJobs != null && fileGroupJobs.size() == 20),
            argThat(stats -> stats != null && stats.isEmpty()));
  }

  @Test
  void partitionByEstimatedWorkBalancesSkewedFilesWithinConfiguredFileLimit() {
    List<FloecatConnector.SnapshotFileEntry> files =
        List.of(
            snapshotFile("large-a", 100L),
            snapshotFile("large-b", 90L),
            snapshotFile("large-c", 80L),
            snapshotFile("small-a", 3L),
            snapshotFile("small-b", 2L),
            snapshotFile("small-c", 1L));

    List<List<FloecatConnector.SnapshotFileEntry>> groups =
        RemoteSnapshotPlanningReconcileExecutor.partitionByEstimatedWork(files, 2);

    assertThat(groups)
        .extracting(
            group -> group.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList())
        .containsExactly(
            List.of("s3://bucket/large-a.parquet", "s3://bucket/small-c.parquet"),
            List.of("s3://bucket/large-b.parquet", "s3://bucket/small-b.parquet"),
            List.of("s3://bucket/large-c.parquet", "s3://bucket/small-a.parquet"));
    assertThat(groups).allMatch(group -> group.size() <= 2);
    assertThat(
            groups.stream().flatMap(List::stream).map(FloecatConnector.SnapshotFileEntry::filePath))
        .containsExactlyInAnyOrderElementsOf(
            files.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList());
  }

  @Test
  void partitionByEstimatedWorkIsDeterministicAcrossInputOrder() {
    List<FloecatConnector.SnapshotFileEntry> files =
        List.of(
            snapshotFile("a", 100L),
            snapshotFile("b", 90L),
            snapshotFile("c", 80L),
            snapshotFile("d", 3L),
            snapshotFile("e", 2L),
            snapshotFile("f", 1L));
    List<FloecatConnector.SnapshotFileEntry> reversed = new ArrayList<>(files);
    Collections.reverse(reversed);

    List<List<String>> forwardGroups =
        RemoteSnapshotPlanningReconcileExecutor.partitionByEstimatedWork(files, 2).stream()
            .map(group -> group.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList())
            .toList();
    List<List<String>> reversedGroups =
        RemoteSnapshotPlanningReconcileExecutor.partitionByEstimatedWork(reversed, 2).stream()
            .map(group -> group.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList())
            .toList();

    assertThat(reversedGroups).isEqualTo(forwardGroups);
  }

  @Test
  void regroupByReuseBundleAffinityReadsEachPredecessorBundleOnce() {
    List<ReconcileFileExecutionPlan> plans = new ArrayList<>();
    for (int index = 0; index < 6; index++) {
      plans.add(reusablePlan("old-a-" + index, "s3://reuse/bundle-a.pb"));
      plans.add(reusablePlan("old-b-" + index, "s3://reuse/bundle-b.pb"));
    }
    plans.add(executionPlan("new-a"));
    plans.add(executionPlan("new-b"));
    List<ReconcileFileGroupTask> original = new ArrayList<>();
    for (int start = 0; start < plans.size(); start += 4) {
      List<ReconcileFileExecutionPlan> groupPlans =
          plans.subList(start, Math.min(start + 4, plans.size()));
      original.add(fileGroup(original.size(), groupPlans));
    }

    List<ReconcileFileGroupTask> regrouped =
        RemoteSnapshotPlanningReconcileExecutor.regroupByReuseBundleAffinity(original, 4);

    assertThat(regrouped).hasSize(4).allMatch(group -> group.fileCount() <= 4);
    assertThat(regrouped.stream().flatMap(group -> group.filePaths().stream()))
        .containsExactlyInAnyOrderElementsOf(
            plans.stream().map(ReconcileFileExecutionPlan::filePath).toList());
    assertThat(
            regrouped.stream()
                .map(
                    group ->
                        group.fileExecutionPlans().stream()
                            .flatMap(plan -> plan.reusableArtifactBundleSelections().stream())
                            .map(ReusableArtifactBundleSelection::payloadUri)
                            .distinct()
                            .count())
                .mapToLong(Long::longValue)
                .sum())
        .isEqualTo(4L);
    assertThat(
            regrouped.stream()
                .filter(
                    group ->
                        group.fileExecutionPlans().stream()
                            .anyMatch(plan -> plan.filePath().contains("old-a")))
                .flatMap(group -> group.fileExecutionPlans().stream())
                .filter(plan -> !plan.reusableArtifactBundleSelections().isEmpty())
                .flatMap(plan -> plan.reusableArtifactBundleSelections().stream())
                .map(ReusableArtifactBundleSelection::payloadUri)
                .distinct())
        .containsExactly("s3://reuse/bundle-a.pb");
  }

  @Test
  void executeFailsTerminalWhenExpectedSnapshotIsMissing() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L))).thenReturn(Optional.empty());

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(!result.success());
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL, result.retryDisposition);
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            eq(ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL),
            eq(ReconcileExecutor.ExecutionResult.RetryDisposition.TERMINAL),
            eq(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR),
            eq(
                "ReconcileFailureException: Snapshot id does not exist: tableId=table-1 snapshotId=55"));
  }

  @Test
  void executePersistsGrpcFailureDetailsForSnapshotPlanningErrors() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenThrow(
            new StatusRuntimeException(
                Status.RESOURCE_EXHAUSTED.withDescription(
                    "grpc: received message larger than max")));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(!result.success());
    verify(workerClient)
        .submitPlanSnapshotFailure(
            any(),
            any(),
            any(),
            any(),
            eq("grpc=RESOURCE_EXHAUSTED desc=grpc: received message larger than max"));
  }

  @Test
  void executeDoesNotRequirePersistedSnapshotMetadataForCaptureOnlyPlanning() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider = ignored -> java.util.Optional.empty();
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob lease = lease(statsOnlyScope());
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                statsOnlyScope(),
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(
            Optional.of(FloecatConnector.DirectSnapshotStatsCapture.of(List.of(), 0, List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(backend, never()).fetchSnapshot(any(), any(), anyLong());
  }

  @Test
  void executeDoesNotLeakWorkerAuthorizationAcrossAccounts() {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    ReconcileWorkerAuthProvider authProvider =
        accountId -> java.util.Optional.of("Bearer worker-token-" + accountId);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);

    ReconcileJobStore.LeasedJob leaseOne = lease("job-1", "acct-a", statsOnlyScope());
    ReconcileJobStore.LeasedJob leaseTwo = lease("job-2", "acct-b", statsOnlyScope());

    when(workerClient.getPlanSnapshotInput(any()))
        .thenAnswer(
            invocation -> {
              RemoteLeasedJob remoteLease = invocation.getArgument(0);
              ReconcileJobStore.LeasedJob lease = remoteLease.lease();
              return new StandalonePlanSnapshotPayload(
                  lease.jobId,
                  lease.leaseEpoch,
                  "",
                  connectorId(lease.accountId),
                  ReconcilerService.CaptureMode.CAPTURE_ONLY,
                  false,
                  statsOnlyScope(),
                  snapshotTask());
            });
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(
            Optional.of(FloecatConnector.DirectSnapshotStatsCapture.of(List.of(), 0, List.of())));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    leaseOne, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .ok());
    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    leaseTwo, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .ok());

    ArgumentCaptor<ai.floedb.floecat.reconciler.spi.ReconcileContext> contextCaptor =
        ArgumentCaptor.forClass(ai.floedb.floecat.reconciler.spi.ReconcileContext.class);
    verify(backend, org.mockito.Mockito.times(2))
        .captureSnapshotTargetStatsDirect(
            contextCaptor.capture(), any(), eq(55L), any(), any(), any());
    assertThat(contextCaptor.getAllValues())
        .extracting(
            ctx ->
                ctx.principal().getAccountId() + "|" + ctx.authorizationToken().orElse("<missing>"))
        .containsExactly("acct-a|Bearer worker-token-acct-a", "acct-b|Bearer worker-token-acct-b");
  }

  private static ReconcileJobStore.LeasedJob lease(ReconcileScope scope) {
    return lease("job-1", "acct", scope);
  }

  private static List<FloecatConnector.SnapshotFileEntry> snapshotFiles(int count) {
    java.util.ArrayList<FloecatConnector.SnapshotFileEntry> out = new java.util.ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      out.add(
          new FloecatConnector.SnapshotFileEntry(
              "s3://bucket/file-" + i + ".parquet",
              "PARQUET",
              10L,
              1L,
              ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
              "",
              0,
              List.of(),
              null,
              null,
              List.of(),
              "test-file-v1:" + i));
    }
    return List.copyOf(out);
  }

  private static FloecatConnector.SnapshotFileEntry snapshotFile(String name, long sizeBytes) {
    return new FloecatConnector.SnapshotFileEntry(
        "s3://bucket/" + name + ".parquet",
        "PARQUET",
        sizeBytes,
        1L,
        ai.floedb.floecat.catalog.rpc.FileContent.FC_DATA,
        "",
        0,
        List.of(),
        null,
        null,
        List.of(),
        "test-file-v1:" + name);
  }

  private static ReconcileFileExecutionPlan reusablePlan(String name, String bundleUri) {
    ReconcileFileExecutionPlan plan = executionPlan(name);
    return plan.withReuseBundleSelections(
        "source",
        "index-source",
        "stats-signature",
        "index-signature",
        java.util.Map.of(),
        List.of(
            new ReusableArtifactBundleSelection(
                "bundle",
                bundleUri,
                100L,
                new byte[32],
                List.of(plan.filePath()),
                List.of(plan.filePath()))));
  }

  private static ReconcileFileExecutionPlan executionPlan(String name) {
    return ReconcileFileExecutionPlan.of(
        "s3://bucket/" + name + ".parquet", 10L, "", null, "PARQUET", 0, List.of(), "");
  }

  private static ReconcileFileGroupTask fileGroup(
      int index, List<ReconcileFileExecutionPlan> plans) {
    List<String> paths = plans.stream().map(ReconcileFileExecutionPlan::filePath).toList();
    return ReconcileFileGroupTask.of(
        "plan",
        "snapshot-55-group-" + index,
        "table-1",
        55L,
        paths.size(),
        "",
        0,
        paths,
        List.of(),
        List.of(),
        "schema",
        plans);
  }

  private static ReconcileJobStore.LeasedJob lease(
      String jobId, String accountId, ReconcileScope scope) {
    return new ReconcileJobStore.LeasedJob(
        jobId,
        accountId,
        "connector-1",
        false,
        ReconcilerService.CaptureMode.CAPTURE_ONLY,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "lease-" + jobId,
        "",
        "",
        ReconcileJobKind.PLAN_SNAPSHOT,
        ai.floedb.floecat.reconciler.jobs.ReconcileTableTask.empty(),
        ai.floedb.floecat.reconciler.jobs.ReconcileViewTask.empty(),
        snapshotTask(),
        ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask.empty(),
        "");
  }

  private static ReconcileSnapshotTask snapshotTask() {
    return ReconcileSnapshotTask.of("table-1", 55L, "db", "events");
  }

  private static ReconcileScope statsOnlyScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(), EnumSet.of(ReconcileCapturePolicy.Output.TABLE_STATS)));
  }

  private static ReconcileScope pageIndexScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(),
            EnumSet.of(
                ReconcileCapturePolicy.Output.TABLE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
  }

  private static ResourceId connectorId() {
    return connectorId("acct");
  }

  private static ResourceId connectorId(String accountId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setKind(ResourceKind.RK_CONNECTOR)
        .setId("connector-1")
        .build();
  }

  private static ResourceId tableId() {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId("table-1")
        .build();
  }
}
