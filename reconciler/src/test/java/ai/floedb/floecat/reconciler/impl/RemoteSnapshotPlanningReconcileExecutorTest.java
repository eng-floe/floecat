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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
  void verifiedReuseManifestLoadsOnceAcrossWarmChainLookups() throws Exception {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    var authProvider = mock(ReconcileWorkerAuthProvider.class);
    BlobStore blobStore = mock(BlobStore.class);
    RemoteSnapshotPlanningReconcileExecutor executor =
        new RemoteSnapshotPlanningReconcileExecutor(backend, workerClient, authProvider, 2, true);
    executor.blobStore = blobStore;
    String uri = "/reuse/manifest.pb";
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder().setFormatVersion(1).setSnapshotId(54L).build();
    byte[] bytes = manifest.toByteArray();
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(bytes);
    when(blobStore.get(uri)).thenReturn(bytes);

    assertEquals(manifest, executor.loadCachedReuseManifest(uri, bytes.length, digest));
    assertEquals(manifest, executor.loadCachedReuseManifest(uri, bytes.length, digest));

    verify(blobStore).get(uri);
  }

  @Test
  void appendOnlyEligibilityRejectsDataRemovalsAndDeleteArtifactChanges() {
    FloecatConnector.SnapshotFileEntry addition = snapshotFile("file-1", 10L);

    assertTrue(
        new FloecatConnector.SnapshotFileDelta(List.of(addition), List.of(), false, "schema")
            .appendOnly());
    assertFalse(
        new FloecatConnector.SnapshotFileDelta(
                List.of(addition), List.of("s3://bucket/removed.parquet"), false, "schema")
            .appendOnly());
    assertFalse(
        new FloecatConnector.SnapshotFileDelta(List.of(addition), List.of(), true, "schema")
            .appendOnly());
  }

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
  void ordinaryPlanningLoadsReuseManifestOnlyFromExplicitParent() throws Exception {
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
            .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
            .build()
            .toByteArray();
    String uri = "/reuse/7.pb";
    byte[] manifestSha256 =
        java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(55L)
                    .setParentSnapshotId(reuseBasisSnapshotId)
                    .build()));
    when(backend.fetchSnapshot(any(), any(), eq(reuseBasisSnapshotId)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(reuseBasisSnapshotId)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
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
  void planningFallsBackWhenLatestReuseManifestIntegrityIsInvalid() {
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
                            .setFormatVersion(1)
                            .setUri(uri)
                            .setPayloadBytes(manifestBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    when(blobStore.get(uri)).thenReturn(manifestBytes);
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(workerClient).submitPlanSnapshotSuccess(any(), any(), any(), any());
    verify(workerClient, never()).submitPlanSnapshotFailure(any(), any(), any(), any(), any());
  }

  @Test
  void planningFallsBackWhenLatestReuseManifestUriIsBlank() {
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
                            .setFormatVersion(1)
                            .setPayloadBytes(1L)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(workerClient).submitPlanSnapshotSuccess(any(), any(), any(), any());
    verify(workerClient, never()).submitPlanSnapshotFailure(any(), any(), any(), any(), any());
    verify(blobStore, never()).get(any());
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
  void planningFallsBackWhenSelectedReuseManifestIsUnavailable() {
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.NULL_BLOB);
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.NOT_FOUND);
  }

  @Test
  void planningFallsBackWhenSelectedReuseManifestIsInvalid() {
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.UNMARKED_MANIFEST);
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.MISSING_REFERENCE);
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.MALFORMED_BUNDLE);
    assertPlanningFallsBackWhenManifestIsUnavailable(ManifestUnavailableMode.LEGACY_REFERENCE);
  }

  @Test
  void appendOnlyPlanningSubmitsOnlyNewFiles() throws Exception {
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
    String schemaJson = "{\"type\":\"struct\",\"fields\":[]}";

    ReconcileFileExecutionPlan priorPlan =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/file-1.parquet",
            10L,
            "",
            null,
            "PARQUET",
            0,
            List.of(),
            "test-file-v1:file-1");
    String statsSignature =
        FileArtifactReuse.statsCaptureSignature(statsOnlyScope().capturePolicy());
    String baseStatsPrefix =
        "/accounts/acct/tables/table-1/snapshots/7/file-groups/snapshot-7-group-0/";
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:snapshot-7-group-0")
                    .setPayloadUri(zeroDigestBundleUri(baseStatsPrefix))
                    .setPayloadBytes(100)
                    .setPayloadSha256(ByteString.copyFrom(new byte[32])))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(priorPlan.filePath())
                    .setSourceFingerprint(FileArtifactReuse.sourceFingerprint(priorPlan, ""))
                    .setStatsCaptureSignature(statsSignature))
            .build();
    var artifactIndex = persistArtifactIndex(blobStore, List.of(bundle));
    long baseSnapshotId = 7L;
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(baseSnapshotId)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .setReusableArtifactBundlesComplete(true)
            .setReusableArtifactIndex(artifactIndex)
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS)
                    .setDefaultColumnScope(
                        ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_FIRST_N)
                    .setMaxDefaultColumns(ReconcileCapturePolicy.DEFAULT_MAX_COLUMNS))
            .addFileGroups(
                ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
                    .setGroupId("snapshot-7-group-0")
                    .setStatsObjectPrefix(baseStatsPrefix)
                    .setSucceededFileCount(1))
            .addReusableArtifactBundles(bundle)
            .build()
            .toByteArray();
    String uri = "/reuse/7.pb";
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(baseSnapshotId)
                    .setSchemaJson(schemaJson)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
                            .setUri(uri)
                            .setPayloadBytes(manifestBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(digest))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    when(backend.fetchSnapshotFileDelta(any(), any(), eq(baseSnapshotId), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFileDelta(
                    List.of(snapshotFile("file-2", 10L)), List.of(), false, schemaJson)));
    when(blobStore.get(uri)).thenReturn(manifestBytes);
    when(workerClient.submitAppendOnlyPlanSnapshotSuccess(any(), any(), any(), any(), any()))
        .thenReturn(true);

    assertTrue(
        executor
            .execute(
                new ReconcileExecutor.ExecutionContext(
                    lease, () -> false, (a, b, c, d, e, f, g, h) -> {}))
            .success());

    verify(workerClient)
        .submitAppendOnlyPlanSnapshotSuccess(
            any(),
            argThat(task -> task.sourceFileCount() == 2 && task.fileGroupCount() == 1),
            argThat(
                jobs ->
                    jobs.size() == 1
                        && jobs.getFirst()
                            .fileGroupTask()
                            .filePaths()
                            .equals(List.of("s3://bucket/file-2.parquet"))
                        && jobs.getFirst().fileGroupTask().fileExecutionPlans().stream()
                            .allMatch(
                                plan ->
                                    !plan.sourceFingerprint().isBlank()
                                        && !plan.indexSourceFingerprint().isBlank()
                                        && !plan.statsCaptureSignature().isBlank()
                                        && !plan.indexCaptureSignature().isBlank()
                                        && plan.reusableArtifactBundleSelections().isEmpty())),
            any(),
            argThat(
                base ->
                    base.snapshotId() == baseSnapshotId
                        && base.sourceFileCount() == 1
                        && base.reusableArtifactIndex().equals(artifactIndex)));
    verify(workerClient, never()).submitPlanSnapshotSuccess(any(), any(), any(), any());
    verify(backend, never()).fetchSnapshotFilePlan(any(), any(), anyLong());
  }

  @Test
  void pageIndexOnlyAppendPlanningSubmitsOnlyNewFilesWithZeroStatsBase() throws Exception {
    assertAppendOnlyCoverage(indexOnlyScope(), 0, 2, 1, true);
  }

  @Test
  void zeroDeltaPageIndexOnlyAppendPlanningSubmitsNoFileGroups() throws Exception {
    assertAppendOnlyCoverage(indexOnlyScope(), 0, 2, 0, true);
  }

  @Test
  void pageIndexOnlyAppendPlanningFallsBackWhenIndexCoverageIsIncomplete() throws Exception {
    assertAppendOnlyCoverage(indexOnlyScope(), 0, 1, 1, false);
  }

  @Test
  void statsAppendPlanningFallsBackWhenStatsCoverageIsIncomplete() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 1, 0, 1, false);
  }

  @Test
  void statsAndIndexAppendPlanningRequiresBothKindsOfCoverage() throws Exception {
    assertAppendOnlyCoverage(pageIndexScope(), 1, 2, 1, false);
    assertAppendOnlyCoverage(pageIndexScope(), 2, 1, 1, false);
    assertAppendOnlyCoverage(pageIndexScope(), 2, 2, 1, true);
  }

  @Test
  void appendOnlyPlanningFallsBackWhenAddedPathExistsInPersistentBase() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 2, 0, 1, false, true, false, false);
  }

  @Test
  void corruptPersistentArtifactIndexFallsBackToFullCapture() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 2, 0, 1, false, false, true, false);
  }

  @Test
  void missingPersistentArtifactIndexFallsBackToFullCapture() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 2, 0, 1, false, false, false, true);
  }

  @Test
  void preIndexReuseManifestFallsBackToFullCapture() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 2, 0, 1, false, false, false, false, true);
  }

  @Test
  void appendOnlyPlanningCheckpointsAtConfiguredDepth() throws Exception {
    assertAppendOnlyCoverage(
        statsOnlyScope(), 2, 0, 1, false, false, false, false, false, false, 0, false);
  }

  @Test
  void appendOnlyPlanningFallsBackWhenDeltaPlanningThrows() throws Exception {
    assertAppendOnlyCoverage(
        statsOnlyScope(), 2, 0, 1, false, false, false, false, false, false, 16, true);
  }

  @Test
  void prePackArtifactIndexFallsBackToFullCapture() throws Exception {
    assertAppendOnlyCoverage(statsOnlyScope(), 2, 0, 1, false, false, false, false, false, true);
  }

  @Test
  void compactionOrDeleteChangeLoadsReusableBundlesFromLatestCompleteManifest() throws Exception {
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
                    List.of(snapshotFile("file-1", 10L), snapshotFile("file-3", 10L)), List.of())));

    String statsSignature =
        FileArtifactReuse.statsCaptureSignature(statsOnlyScope().capturePolicy());
    String baseStatsPrefix = "/reuse/base-7/";
    var baseBundle = statsBundle("file-1", zeroDigestBundleUri(baseStatsPrefix), statsSignature);
    var baseIndex = persistArtifactIndex(blobStore, List.of(baseBundle));
    byte[] baseManifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setParentJobId("job-7")
            .setTableId("table-1")
            .setSnapshotId(7L)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .setReusableArtifactBundlesComplete(true)
            .setReusableArtifactIndex(baseIndex)
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS)
                    .setDefaultColumnScope(
                        ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_FIRST_N)
                    .setMaxDefaultColumns(ReconcileCapturePolicy.DEFAULT_MAX_COLUMNS))
            .addFileGroups(
                ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
                    .setGroupId("file-1")
                    .setStatsObjectPrefix(baseStatsPrefix)
                    .setSucceededFileCount(1))
            .addReusableArtifactBundles(baseBundle)
            .build()
            .toByteArray();
    byte[] baseDigest = java.security.MessageDigest.getInstance("SHA-256").digest(baseManifest);
    String baseUri = "/reuse/7.pb";
    String deltaStatsPrefix = "/reuse/current-8/";
    var deltaBundle = statsBundle("file-2", zeroDigestBundleUri(deltaStatsPrefix), statsSignature);
    var currentIndex = persistArtifactIndex(blobStore, List.of(baseBundle, deltaBundle));
    byte[] currentManifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setParentJobId("job-8")
            .setTableId("table-1")
            .setSnapshotId(8L)
            .setSourceFileCount(2)
            .setFileStatsRecordCount(2)
            .setReusableArtifactBundlesComplete(true)
            .setReusableArtifactIndex(currentIndex)
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS)
                    .setDefaultColumnScope(
                        ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_FIRST_N)
                    .setMaxDefaultColumns(ReconcileCapturePolicy.DEFAULT_MAX_COLUMNS))
            .addFileGroups(
                ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
                    .setGroupId("file-2")
                    .setStatsObjectPrefix(deltaStatsPrefix)
                    .setSucceededFileCount(1))
            .setAppendOnlyBase(
                ai.floedb.floecat.reconciler.rpc.AppendOnlySnapshotBase.newBuilder()
                    .setFormatVersion(1)
                    .setSnapshotId(7L)
                    .setManifestUri(baseUri)
                    .setManifestBytes(baseManifest.length)
                    .setManifestSha256(ByteString.copyFrom(baseDigest))
                    .setSourceFileCount(1)
                    .setFileStatsRecordCount(1)
                    .setStatsGenerationId("full-rescan-job-7")
                    .setReusableArtifactIndex(baseIndex))
            .addReusableArtifactBundles(deltaBundle)
            .build()
            .toByteArray();
    byte[] currentDigest =
        java.security.MessageDigest.getInstance("SHA-256").digest(currentManifest);
    String currentUri = "/reuse/8.pb";
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(8L)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
                            .setUri(currentUri)
                            .setPayloadBytes(currentManifest.length)
                            .setPayloadSha256(ByteString.copyFrom(currentDigest))
                            .setStatsGenerationManifestUri("/stats/generation-8.pb"))
                    .build()));
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(55L)
                    .setParentSnapshotId(8L)
                    .build()));
    when(backend.fetchSnapshot(any(), any(), eq(8L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(8L)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
                            .setUri(currentUri)
                            .setPayloadBytes(currentManifest.length)
                            .setPayloadSha256(ByteString.copyFrom(currentDigest))
                            .setStatsGenerationManifestUri("/stats/generation-8.pb"))
                    .build()));
    when(backend.fetchSnapshotFileDelta(any(), any(), eq(8L), eq(55L)))
        .thenReturn(
            Optional.of(
                new FloecatConnector.SnapshotFileDelta(
                    List.of(snapshotFile("file-3", 10L)),
                    List.of("s3://bucket/file-2.parquet"),
                    true,
                    "schema")));
    when(blobStore.get(currentUri)).thenReturn(currentManifest);
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
                jobs -> {
                  List<ReconcileFileExecutionPlan> plans =
                      jobs.stream()
                          .flatMap(job -> job.fileGroupTask().fileExecutionPlans().stream())
                          .toList();
                  return plans.stream()
                          .filter(plan -> plan.filePath().endsWith("file-1.parquet"))
                          .allMatch(plan -> !plan.reusableArtifactBundleSelections().isEmpty())
                      && plans.stream()
                          .filter(plan -> plan.filePath().endsWith("file-3.parquet"))
                          .allMatch(plan -> plan.reusableArtifactBundleSelections().isEmpty());
                }),
            any());
    verify(blobStore).get(currentUri);
    verify(blobStore, never()).get(baseUri);
  }

  @Test
  void planningFallsBackWhenExplicitParentReuseManifestObjectIsUnavailable() {
    assertPlanningRetriesWhenParentManifestIsUnavailable(ManifestUnavailableMode.PARENT_NULL_BLOB);
    assertPlanningRetriesWhenParentManifestIsUnavailable(ManifestUnavailableMode.PARENT_NOT_FOUND);
  }

  private static void assertPlanningFallsBackWhenManifestIsUnavailable(
      ManifestUnavailableMode unavailableMode) {
    assertPlanningOutcomeWhenManifestIsUnavailable(unavailableMode, true, null, null, "");
  }

  private static void assertPlanningOutcomeWhenManifestIsUnavailable(
      ManifestUnavailableMode unavailableMode,
      boolean expectFullCapture,
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
    when(workerClient.submitPlanSnapshotFailure(any(), any(), any(), any(), any()))
        .thenReturn(true);
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);
    String uri = "/reuse/missing-9001.pb";
    SnapshotCaptureManifest.Builder manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setTableId("table-1")
            .setSnapshotId(9001L);
    if (unavailableMode == ManifestUnavailableMode.MALFORMED_BUNDLE) {
      manifest
          .setReusableArtifactBundlesComplete(true)
          .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
          .addFileGroups(
              ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
                  .setGroupId("group-1")
                  .setStatsObjectPrefix("/stats/group-1/"))
          .addReusableArtifactBundles(
              ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder());
    }
    byte[] manifestBytes = manifest.build().toByteArray();
    boolean unmarked = unavailableMode == ManifestUnavailableMode.UNMARKED_MANIFEST;
    boolean malformed = unavailableMode == ManifestUnavailableMode.MALFORMED_BUNDLE;
    Snapshot.Builder basis = Snapshot.newBuilder().setTableId(tableId()).setSnapshotId(9001L);
    if (unavailableMode != ManifestUnavailableMode.MISSING_REFERENCE) {
      basis.setReuseManifestRef(
          SnapshotReuseManifestRef.newBuilder()
              .setFormatVersion(unavailableMode == ManifestUnavailableMode.LEGACY_REFERENCE ? 0 : 1)
              .setUri(uri)
              .setPayloadBytes(unmarked || malformed ? manifestBytes.length : 123L)
              .setPayloadSha256(
                  ByteString.copyFrom(unmarked || malformed ? sha256(manifestBytes) : new byte[32]))
              .setStatsGenerationManifestUri("/stats/generation.pb"));
    }
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(Optional.of(basis.build()));
    if (unavailableMode == ManifestUnavailableMode.NOT_FOUND) {
      when(blobStore.get(uri)).thenThrow(new StorageNotFoundException("missing"));
    } else if (unavailableMode == ManifestUnavailableMode.NULL_BLOB) {
      when(blobStore.get(uri)).thenReturn(null);
    } else if (unmarked || malformed) {
      when(blobStore.get(uri)).thenReturn(manifestBytes);
    }
    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    if (expectFullCapture) {
      assertTrue(result.success());
      verify(workerClient).submitPlanSnapshotSuccess(any(), any(), any(), any());
      verify(workerClient, never()).submitPlanSnapshotFailure(any(), any(), any(), any(), any());
      if (unavailableMode == ManifestUnavailableMode.LEGACY_REFERENCE) {
        verify(blobStore, never()).get(any());
      }
      return;
    }
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

  private static void assertPlanningRetriesWhenParentManifestIsUnavailable(
      ManifestUnavailableMode unavailableMode) {
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
    when(backend.fetchSnapshot(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(55L)
                    .setParentSnapshotId(9001L)
                    .build()));
    String missingUri = "/reuse/missing-9001.pb";
    when(backend.fetchSnapshot(any(), any(), eq(9001L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(9001L)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
                            .setUri(missingUri)
                            .setPayloadBytes(123L)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));
    if (unavailableMode.throwsNotFound()) {
      when(blobStore.get(missingUri)).thenThrow(new StorageNotFoundException("missing"));
    } else {
      when(blobStore.get(missingUri)).thenReturn(null);
    }

    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);
    when(workerClient.submitPlanSnapshotFailure(any(), any(), any(), any(), any()))
        .thenReturn(true);
    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));
    assertTrue(result.success());
    verify(workerClient).submitPlanSnapshotSuccess(any(), any(), any(), any());
    verify(workerClient, never()).submitPlanSnapshotFailure(any(), any(), any(), any(), any());
  }

  private enum ManifestUnavailableMode {
    NULL_BLOB,
    NOT_FOUND,
    UNMARKED_MANIFEST,
    MISSING_REFERENCE,
    MALFORMED_BUNDLE,
    LEGACY_REFERENCE,
    PARENT_NULL_BLOB(false),
    PARENT_NOT_FOUND(true);

    private final boolean throwsNotFound;

    ManifestUnavailableMode() {
      this(false);
    }

    ManifestUnavailableMode(boolean throwsNotFound) {
      this.throwsNotFound = throwsNotFound;
    }

    boolean throwsNotFound() {
      return throwsNotFound;
    }
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
  void regroupByReuseBundleAffinityDoesNotCreateOneGroupPerSmallBundle() {
    List<ReconcileFileExecutionPlan> plans = new ArrayList<>();
    for (int index = 0; index < 12; index++) {
      plans.add(reusablePlan("old-" + index, "s3://reuse/bundle-" + index + ".pb"));
    }
    List<ReconcileFileGroupTask> original =
        plans.stream().map(plan -> fileGroup(plans.indexOf(plan), List.of(plan))).toList();

    List<ReconcileFileGroupTask> regrouped =
        RemoteSnapshotPlanningReconcileExecutor.regroupByReuseBundleAffinity(original, 4);

    assertThat(regrouped).hasSize(3).allMatch(group -> group.fileCount() == 4);
    assertThat(regrouped.stream().flatMap(group -> group.filePaths().stream()))
        .containsExactlyInAnyOrderElementsOf(
            plans.stream().map(ReconcileFileExecutionPlan::filePath).toList());
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

  private static void assertAppendOnlyCoverage(
      ReconcileScope scope,
      int baseStatsCount,
      int baseIndexCount,
      int addedFileCount,
      boolean expectAppendOnly)
      throws Exception {
    assertAppendOnlyCoverage(
        scope,
        baseStatsCount,
        baseIndexCount,
        addedFileCount,
        expectAppendOnly,
        false,
        false,
        false);
  }

  private static void assertAppendOnlyCoverage(
      ReconcileScope scope,
      int baseStatsCount,
      int baseIndexCount,
      int addedFileCount,
      boolean expectAppendOnly,
      boolean overlapAddition,
      boolean corruptIndex,
      boolean missingIndex)
      throws Exception {
    assertAppendOnlyCoverage(
        scope,
        baseStatsCount,
        baseIndexCount,
        addedFileCount,
        expectAppendOnly,
        overlapAddition,
        corruptIndex,
        missingIndex,
        false);
  }

  private static void assertAppendOnlyCoverage(
      ReconcileScope scope,
      int baseStatsCount,
      int baseIndexCount,
      int addedFileCount,
      boolean expectAppendOnly,
      boolean overlapAddition,
      boolean corruptIndex,
      boolean missingIndex,
      boolean omitIndexContract)
      throws Exception {
    assertAppendOnlyCoverage(
        scope,
        baseStatsCount,
        baseIndexCount,
        addedFileCount,
        expectAppendOnly,
        overlapAddition,
        corruptIndex,
        missingIndex,
        omitIndexContract,
        false);
  }

  private static void assertAppendOnlyCoverage(
      ReconcileScope scope,
      int baseStatsCount,
      int baseIndexCount,
      int addedFileCount,
      boolean expectAppendOnly,
      boolean overlapAddition,
      boolean corruptIndex,
      boolean missingIndex,
      boolean omitIndexContract,
      boolean prePackIndex)
      throws Exception {
    assertAppendOnlyCoverage(
        scope,
        baseStatsCount,
        baseIndexCount,
        addedFileCount,
        expectAppendOnly,
        overlapAddition,
        corruptIndex,
        missingIndex,
        omitIndexContract,
        prePackIndex,
        16,
        false);
  }

  private static void assertAppendOnlyCoverage(
      ReconcileScope scope,
      int baseStatsCount,
      int baseIndexCount,
      int addedFileCount,
      boolean expectAppendOnly,
      boolean overlapAddition,
      boolean corruptIndex,
      boolean missingIndex,
      boolean omitIndexContract,
      boolean prePackIndex,
      int maxAppendOnlyChainDepth,
      boolean deltaPlanningFails)
      throws Exception {
    var backend = mock(ai.floedb.floecat.reconciler.spi.ReconcilerBackend.class);
    var workerClient = mock(RemotePlannerWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    var executor =
        new RemoteSnapshotPlanningReconcileExecutor(
            backend, workerClient, ignored -> Optional.empty(), 2, maxAppendOnlyChainDepth, true);
    executor.blobStore = blobStore;
    ReconcileJobStore.LeasedJob lease = lease(scope);
    when(workerClient.getPlanSnapshotInput(any()))
        .thenReturn(
            new StandalonePlanSnapshotPayload(
                lease.jobId,
                lease.leaseEpoch,
                "",
                connectorId(),
                ReconcilerService.CaptureMode.CAPTURE_ONLY,
                false,
                scope,
                snapshotTask()));
    when(backend.captureSnapshotTargetStatsDirect(any(), any(), eq(55L), any(), any(), any()))
        .thenReturn(Optional.empty());

    int baseSourceCount = 2;
    long baseSnapshotId = 7L;
    String schemaJson = "{\"type\":\"struct\",\"fields\":[]}";
    var baseBundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:base")
                    .setPayloadUri(zeroDigestBundleUri("/reuse/"))
                    .setPayloadBytes(100)
                    .setPayloadSha256(ByteString.copyFrom(new byte[32])));
    for (int index = 0; index < baseStatsCount; index++) {
      String filePath =
          overlapAddition && index == 0
              ? "s3://bucket/delta-0.parquet"
              : "s3://bucket/base-" + index + ".parquet";
      baseBundle.addFileStats(
          ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
              .setFilePath(filePath)
              .setSourceFingerprint("stats-source-" + index)
              .setStatsCaptureSignature("stats-signature"));
    }
    for (int index = 0; index < baseIndexCount; index++) {
      String filePath =
          overlapAddition && index == 0
              ? "s3://bucket/delta-0.parquet"
              : "s3://bucket/base-" + index + ".parquet";
      baseBundle.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
              .setFilePath(filePath)
              .setSourceFingerprint("index-source-" + index)
              .setIndexCaptureSignature("index-signature"));
    }
    var artifactIndex =
        persistArtifactIndex(
            blobStore,
            baseStatsCount + baseIndexCount == 0 ? List.of() : List.of(baseBundle.build()));
    if (corruptIndex && artifactIndex.getRunsCount() > 0) {
      var run = artifactIndex.getRuns(0);
      var corruptManifest =
          run.getManifest().toBuilder()
              .setPayloadBytes(1)
              .setInlinePayload(ByteString.copyFrom(new byte[] {1}));
      artifactIndex =
          artifactIndex.toBuilder()
              .setRuns(0, run.toBuilder().setManifest(corruptManifest))
              .build();
    } else if (missingIndex && artifactIndex.getRunsCount() > 0) {
      var run = artifactIndex.getRuns(0);
      var missingManifest =
          run.getManifest().toBuilder()
              .setUri("/reuse/missing-run-manifest.pb")
              .clearInlinePayload();
      artifactIndex =
          artifactIndex.toBuilder()
              .setRuns(0, run.toBuilder().setManifest(missingManifest))
              .build();
    } else if (prePackIndex && artifactIndex.getRunsCount() > 0) {
      var run = artifactIndex.getRuns(0);
      var runManifest =
          ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunManifest.parseFrom(
              run.getManifest().getInlinePayload());
      var legacyRunManifest = runManifest.toBuilder();
      for (int block = 0; block < runManifest.getBlocksCount(); block++) {
        legacyRunManifest.setBlocks(
            block, runManifest.getBlocks(block).toBuilder().clearLength().clearBlockSha256());
      }
      byte[] legacyBytes = legacyRunManifest.build().toByteArray();
      var legacyManifestReference =
          run.getManifest().toBuilder()
              .setPayloadBytes(legacyBytes.length)
              .setPayloadSha256(ByteString.copyFrom(sha256(legacyBytes)))
              .setInlinePayload(ByteString.copyFrom(legacyBytes));
      // Keep the current format version so the pre-pack block shape is what gets rejected.
      artifactIndex =
          artifactIndex.toBuilder()
              .setRuns(0, run.toBuilder().setManifest(legacyManifestReference))
              .build();
    }
    var manifestBuilder =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setParentJobId("job-7")
            .setTableId("table-1")
            .setSnapshotId(baseSnapshotId)
            .setSourceFileCount(baseSourceCount)
            .setFileStatsRecordCount(baseStatsCount)
            .setIndexArtifactCount(baseIndexCount)
            .setReusableArtifactBundlesComplete(true)
            .setCapturePolicy(toProtoCapturePolicy(scope.capturePolicy()));
    if (!omitIndexContract) {
      manifestBuilder.setReusableArtifactIndex(artifactIndex);
    }
    if (baseStatsCount + baseIndexCount > 0) {
      manifestBuilder.addFileGroups(
          ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
              .setGroupId("base")
              .setStatsObjectPrefix("/reuse/")
              .setSucceededFileCount(baseSourceCount));
      manifestBuilder.addReusableArtifactBundles(baseBundle);
    }
    byte[] manifestBytes = manifestBuilder.build().toByteArray();
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    String manifestUri = "/reuse/7.pb";
    when(blobStore.get(manifestUri)).thenReturn(manifestBytes);
    when(backend.latestReconciledSnapshotForReuse(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(
                Snapshot.newBuilder()
                    .setTableId(tableId())
                    .setSnapshotId(baseSnapshotId)
                    .setSchemaJson(schemaJson)
                    .setReuseManifestRef(
                        SnapshotReuseManifestRef.newBuilder()
                            .setFormatVersion(1)
                            .setUri(manifestUri)
                            .setPayloadBytes(manifestBytes.length)
                            .setPayloadSha256(ByteString.copyFrom(digest))
                            .setStatsGenerationManifestUri("/stats/generation.pb"))
                    .build()));

    List<FloecatConnector.SnapshotFileEntry> additions =
        java.util.stream.IntStream.range(0, addedFileCount)
            .mapToObj(index -> snapshotFile("delta-" + index, 10L))
            .toList();
    if (deltaPlanningFails) {
      when(backend.fetchSnapshotFileDelta(any(), any(), eq(baseSnapshotId), eq(55L)))
          .thenThrow(new RuntimeException("delta history unavailable"));
    } else {
      when(backend.fetchSnapshotFileDelta(any(), any(), eq(baseSnapshotId), eq(55L)))
          .thenReturn(
              Optional.of(
                  new FloecatConnector.SnapshotFileDelta(additions, List.of(), false, schemaJson)));
    }
    List<FloecatConnector.SnapshotFileEntry> fullFiles = new ArrayList<>();
    fullFiles.add(snapshotFile("base-0", 10L));
    fullFiles.add(snapshotFile("base-1", 10L));
    fullFiles.addAll(additions);
    when(backend.fetchSnapshotFilePlan(any(), any(), eq(55L)))
        .thenReturn(
            Optional.of(new FloecatConnector.SnapshotFilePlan(List.copyOf(fullFiles), List.of())));
    when(workerClient.submitAppendOnlyPlanSnapshotSuccess(any(), any(), any(), any(), any()))
        .thenReturn(true);
    when(workerClient.submitPlanSnapshotSuccess(any(), any(), any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());

    if (expectAppendOnly) {
      List<String> expectedPaths =
          additions.stream().map(FloecatConnector.SnapshotFileEntry::filePath).toList();
      verify(workerClient)
          .submitAppendOnlyPlanSnapshotSuccess(
              any(),
              argThat(
                  task ->
                      task.sourceFileCount() == baseSourceCount + addedFileCount
                          && task.fileGroupCount() == (addedFileCount == 0 ? 0 : 1)),
              argThat(
                  jobs ->
                      jobs.stream()
                          .flatMap(job -> job.fileGroupTask().filePaths().stream())
                          .toList()
                          .equals(expectedPaths)),
              any(),
              argThat(
                  base ->
                      base.sourceFileCount() == baseSourceCount
                          && base.fileStatsRecordCount() == baseStatsCount
                          && base.indexArtifactCount() == baseIndexCount));
      verify(workerClient, never()).submitPlanSnapshotSuccess(any(), any(), any(), any());
      verify(backend, never()).fetchSnapshotFilePlan(any(), any(), anyLong());
    } else {
      verify(workerClient, never())
          .submitAppendOnlyPlanSnapshotSuccess(any(), any(), any(), any(), any());
      verify(workerClient)
          .submitPlanSnapshotSuccess(
              any(),
              argThat(task -> task.sourceFileCount() == fullFiles.size()),
              argThat(
                  jobs ->
                      jobs.stream().flatMap(job -> job.fileGroupTask().filePaths().stream()).count()
                          == fullFiles.size()),
              any());
      verify(backend).fetchSnapshotFilePlan(any(), any(), eq(55L));
    }
  }

  private static ai.floedb.floecat.reconciler.rpc.CapturePolicy toProtoCapturePolicy(
      ReconcileCapturePolicy policy) {
    var builder =
        ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
            .setDefaultColumnScope(ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_FIRST_N)
            .setMaxDefaultColumns(policy.maxDefaultColumns());
    for (ReconcileCapturePolicy.Output output : policy.outputs()) {
      builder.addOutputs(
          switch (output) {
            case TABLE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_TABLE_STATS;
            case FILE_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_FILE_STATS;
            case COLUMN_STATS -> ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_COLUMN_STATS;
            case PARQUET_PAGE_INDEX ->
                ai.floedb.floecat.reconciler.rpc.CaptureOutput.CO_PARQUET_PAGE_INDEX;
          });
    }
    return builder.build();
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

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference statsBundle(
      String name, String bundleUri, String statsSignature) {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/" + name + ".parquet",
            10L,
            "",
            null,
            "PARQUET",
            0,
            List.of(),
            "test-file-v1:" + name);
    return ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
        .setArtifact(
            ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                .setTargetStorageId("reuse-bundle:" + name)
                .setPayloadUri(bundleUri)
                .setPayloadBytes(100)
                .setPayloadSha256(ByteString.copyFrom(new byte[32])))
        .addFileStats(
            ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                .setFilePath(plan.filePath())
                .setSourceFingerprint(FileArtifactReuse.sourceFingerprint(plan, ""))
                .setStatsCaptureSignature(statsSignature))
        .build();
  }

  private static String zeroDigestBundleUri(String statsPrefix) {
    return statsPrefix
        + (statsPrefix.endsWith("/") ? "" : "/")
        + "reuse-bundles/"
        + "0".repeat(64)
        + ".pb";
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference
      persistArtifactIndex(
          BlobStore blobStore,
          List<ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference> bundles) {
    if (bundles.isEmpty()) {
      return ReusableArtifactIndexStore.emptyReference();
    }
    java.util.Map<String, byte[]> objects = new java.util.LinkedHashMap<>();
    doAnswer(
            invocation -> {
              objects.put(invocation.getArgument(0), invocation.getArgument(1));
              return null;
            })
        .when(blobStore)
        .putImmutable(anyString(), any(byte[].class), anyString());
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    var reference =
        new ReusableArtifactIndexStore(blobStore)
            .append("/artifact-index/", ReusableArtifactIndexStore.emptyReference(), bundles);
    objects.forEach((uri, bytes) -> when(blobStore.get(uri)).thenReturn(bytes));
    doAnswer(
            invocation -> {
              java.util.Map<String, byte[]> loaded = new java.util.LinkedHashMap<>();
              for (String uri : invocation.<List<String>>getArgument(0)) {
                loaded.put(uri, blobStore.get(uri));
              }
              return loaded;
            })
        .when(blobStore)
        .getBatch(any());
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    return reference;
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

  private static ReconcileScope indexOnlyScope() {
    return ReconcileScope.of(
        List.of(),
        "table-1",
        List.of(),
        ReconcileCapturePolicy.of(
            List.of(), EnumSet.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX)));
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
