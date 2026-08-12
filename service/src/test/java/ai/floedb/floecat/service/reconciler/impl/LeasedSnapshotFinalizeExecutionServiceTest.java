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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotReuseManifestRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.FileArtifactReuse;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.jobs.ArtifactReferenceDigest;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotContentState;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.ReusableArtifactManifest;
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedSnapshotFinalizeExecutionServiceTest {

  @Test
  void reusableBundleTargetMappingsMustMatchTheStagedArtifactDigest() {
    StatsObjectDescriptor artifact =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri("/reuse/bundle.pb")
            .setPayloadBytes(12L)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    var bundle =
        ReusableArtifactBundleReference.newBuilder()
            .setArtifact(artifact)
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath("s3://bucket/data.parquet"))
            .build();
    String stagedDigest =
        ArtifactReferenceDigest.sha256(
            List.of(
                artifact.toBuilder()
                    .setTargetStorageId(
                        StatsTargetIdentity.storageId(
                            StatsTargetIdentity.fileTarget("s3://bucket/data.parquet")))
                    .build()),
            List.of());

    assertDoesNotThrow(
        () ->
            LeasedSnapshotFinalizeExecutionService.validateStagedArtifactMappings(
                bundle, stagedDigest));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LeasedSnapshotFinalizeExecutionService.validateStagedArtifactMappings(
                bundle.toBuilder()
                    .setFileStats(
                        0,
                        bundle.getFileStats(0).toBuilder()
                            .setFilePath("s3://bucket/forged.parquet"))
                    .build(),
                stagedDigest));
  }

  @Test
  void reusableBundleMetadataProvidesManifestCoverageWithoutReadingTheBundle() {
    String statsPrefix = "/stats/group-1/";
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setGroupId("group-1")
                    .setStatsObjectPrefix(statsPrefix))
            .setFileStatsRecordCount(2)
            .setIndexArtifactCount(1)
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:group-1")
                            .setPayloadUri(zeroDigestBundleUri(statsPrefix))
                            .setPayloadBytes(123)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32])))
                    .addFileStats(
                        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                            .setFilePath("s3://bucket/data-1.parquet")
                            .setSourceFingerprint("source-1")
                            .setStatsCaptureSignature("stats-signature"))
                    .addFileStats(
                        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                            .setFilePath("s3://bucket/data-2.parquet")
                            .setSourceFingerprint("source-2")
                            .setStatsCaptureSignature("stats-signature"))
                    .addIndexArtifacts(
                        ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                            .setFilePath("s3://bucket/data-1.parquet")
                            .setSourceFingerprint("index-source")
                            .setIndexCaptureSignature("index-signature")))
            .build();

    assertDoesNotThrow(() -> ReusableArtifactManifest.validate(manifest));
  }

  @Test
  void reusableBundleMetadataMustBeMarkedComplete() {
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder().setFormatVersion(1).build();

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> ReusableArtifactManifest.validate(manifest));

    assertTrue(failure.getMessage().contains("reuse bundle index is not complete"));
  }

  @Test
  void reusableBundleMetadataMustCoverDeclaredManifestArtifacts() {
    String statsPrefix = "/stats/group-1/";
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setGroupId("group-1")
                    .setStatsObjectPrefix(statsPrefix))
            .setFileStatsRecordCount(2)
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:group-1")
                            .setPayloadUri(zeroDigestBundleUri(statsPrefix))
                            .setPayloadBytes(123)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32])))
                    .addFileStats(
                        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                            .setFilePath("s3://bucket/data-1.parquet")
                            .setSourceFingerprint("source-1")
                            .setStatsCaptureSignature("stats-signature")))
            .build();

    assertThrows(IllegalArgumentException.class, () -> ReusableArtifactManifest.validate(manifest));
  }

  @Test
  void ordinaryManifestRequiresExactReusableBundleCardinality() {
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:extra")
                            .setPayloadUri("/stats/extra/reuse-bundles/bundle.pb")
                            .setPayloadBytes(1)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))))
            .build();

    assertThrows(IllegalArgumentException.class, () -> ReusableArtifactManifest.validate(manifest));
  }

  @Test
  void appendOnlyManifestRejectsInheritedBundlePayloads() {
    String inheritedBundleUri =
        zeroDigestBundleUri(
            Keys.tableBlobPrefix(ACCOUNT_ID, TABLE_ID) + "snapshots/54/file-groups/base/");
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setSourceFileCount(1)
            .setReusableArtifactBundlesComplete(true)
            .setAppendOnlyBase(
                ai.floedb.floecat.reconciler.rpc.AppendOnlySnapshotBase.newBuilder()
                    .setFormatVersion(1)
                    .setSnapshotId(SNAPSHOT_ID - 1)
                    .setManifestUri("/base-manifest.pb")
                    .setManifestBytes(1)
                    .setManifestSha256(ByteString.copyFrom(new byte[32]))
                    .setSourceFileCount(1)
                    .setStatsGenerationId("full-rescan-base")
                    .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference()))
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:base")
                            .setPayloadUri(inheritedBundleUri)
                            .setPayloadBytes(1)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))))
            .build();

    assertThrows(IllegalArgumentException.class, () -> ReusableArtifactManifest.validate(manifest));
  }

  @Test
  void reusableBundlesAreMatchedByFencedPrefixWhenGroupIdsRepeatAcrossPlans() {
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setPlanId("plan-1")
                    .setGroupId("group-1")
                    .setStatsObjectPrefix("/stats/job-1/"))
            .addFileGroups(
                FileGroupResultDescriptor.newBuilder()
                    .setPlanId("plan-2")
                    .setGroupId("group-1")
                    .setStatsObjectPrefix("/stats/job-2/"))
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:group-1")
                            .setPayloadUri(zeroDigestBundleUri("/stats/job-1/"))
                            .setPayloadBytes(1)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))))
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(
                        StatsObjectDescriptor.newBuilder()
                            .setTargetStorageId("reuse-bundle:group-1")
                            .setPayloadUri(zeroDigestBundleUri("/stats/job-2/"))
                            .setPayloadBytes(1)
                            .setPayloadSha256(ByteString.copyFrom(new byte[32]))))
            .build();

    assertDoesNotThrow(() -> ReusableArtifactManifest.validate(manifest));
  }

  private static final String ACCOUNT_ID = "acct";
  private static final String FINALIZE_JOB_ID = "finalize-job";
  private static final String LEASE_EPOCH = "lease-1";
  private static final String TABLE_ID = "table-1";
  private static final long SNAPSHOT_ID = 55L;
  private static final ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor
      INDEX_PREDECESSOR =
          new ReconcileFileGroupResultDescriptor.IndexGenerationPredecessor(
              "prior", 3L, "/prior-manifest.pb", 4L);

  private LeasedSnapshotFinalizeExecutionService service;
  private ReconcileJobStore jobs;
  private BlobStore blobs;
  private CurrentSnapshotPointerService currentSnapshotPointerService;
  private SnapshotFinalizePersistenceService persistence;
  private PrincipalContext principal;
  private AtomicReference<ReconcileJobStore.SnapshotFinalizeCommitIntent> acceptedIntent;

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeExecutionService();
    jobs = mock(ReconcileJobStore.class);
    blobs = mock(BlobStore.class);
    currentSnapshotPointerService = mock(CurrentSnapshotPointerService.class);
    persistence = mock(SnapshotFinalizePersistenceService.class);
    principal = mock(PrincipalContext.class);
    acceptedIntent = new AtomicReference<>();
    service.jobs = jobs;
    service.blobStore = blobs;
    service.childStateService = mock(SnapshotFinalizeChildStateService.class);
    service.currentSnapshotPointerService = currentSnapshotPointerService;
    service.persistence = persistence;
    service.indexArtifactRepository = mock(IndexArtifactRepository.class);
    service.statsStore = mock(ai.floedb.floecat.stats.spi.StatsStore.class);
    service.snapshotRepo = mock(ai.floedb.floecat.service.repo.impl.SnapshotRepository.class);
    service.reachabilityGuard = new TableBlobReachabilityGuard();
    service.idempotencyStore = mock(IdempotencyRepository.class);
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
    when(service.idempotencyStore.get(anyString())).thenReturn(Optional.empty());
    when(service.idempotencyStore.createPending(
            anyString(), anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(true);
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.beginSnapshotFinalizeCommit(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.beginSnapshotFinalizeCommit(
            eq(FINALIZE_JOB_ID),
            eq(LEASE_EPOCH),
            any(ReconcileJobStore.SnapshotFinalizeCommitIntent.class)))
        .thenAnswer(
            invocation -> {
              acceptedIntent.set(invocation.getArgument(2));
              return true;
            });
    when(jobs.snapshotFinalizeCommitIntent(FINALIZE_JOB_ID))
        .thenAnswer(invocation -> Optional.ofNullable(acceptedIntent.get()));
    when(service.statsStore.isPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(true);
    when(service.snapshotRepo.recordReuseManifest(
            any(), anyLong(), any(SnapshotReuseManifestRef.class)))
        .thenReturn(
            Snapshot.newBuilder()
                .setTableId(
                    ai.floedb.floecat.common.rpc.ResourceId.newBuilder()
                        .setAccountId(ACCOUNT_ID)
                        .setId(TABLE_ID))
                .setSnapshotId(SNAPSHOT_ID)
                .build());
    when(persistence.prepareStatsGenerationForPublication(
            any(), anyLong(), anyString(), anyBoolean()))
        .thenReturn(new StatsStore.StatsGenerationPredecessor("", 0L));
    when(persistence.publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), any()))
        .thenReturn(true);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID)).thenReturn(Optional.of(finalizeJobView()));
    when(jobs.getCompletionLeaseView(FINALIZE_JOB_ID, LEASE_EPOCH, true))
        .thenAnswer(
            ignored ->
                jobs.getCompactLeaseView(FINALIZE_JOB_ID)
                    .filter(job -> "JS_RUNNING".equals(job.state))
                    .map(LeasedSnapshotFinalizeExecutionServiceTest::publicationLeaseView));
    when(jobs.childFileGroupResultDescriptorsPage(anyString(), anyString(), anyInt(), anyString()))
        .thenReturn(new ReconcileJobStore.FileGroupResultDescriptorPage(List.of(), ""));
    when(jobs.completeSnapshotFinalizeSuccess(
            eq(FINALIZE_JOB_ID),
            eq(LEASE_EPOCH),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyInt(),
            anyInt(),
            anyLong(),
            anyLong(),
            any(),
            anyLong(),
            anyString()))
        .thenReturn(true);
  }

  @Test
  void successSubmissionDurablyQueuesPublicationWithoutBlobIo() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());

    service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(jobs)
        .beginSnapshotFinalizeCommit(
            eq(FINALIZE_JOB_ID),
            eq(LEASE_EPOCH),
            any(ReconcileJobStore.SnapshotFinalizeCommitIntent.class));
    verify(blobs, never()).get(anyString());
    verify(blobs, never()).put(anyString(), any(byte[].class), anyString());
    verify(jobs, never())
        .completeSnapshotFinalizeSuccess(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyInt(),
            anyInt(),
            anyLong(),
            anyLong(),
            any(),
            anyLong(),
            anyString());
  }

  @Test
  void incompatibleAppendOnlyFailureEnqueuesFullCaptureReconcile() {
    when(jobs.enqueue(
            eq(ACCOUNT_ID),
            eq("connector"),
            eq(true),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(ReconcileScope.class),
            any(ReconcileExecutionPolicy.class),
            eq("")))
        .thenReturn("full-capture-job");

    assertTrue(
        service.persistFailure(
            principal,
            FINALIZE_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            "append-only base is incompatible",
            ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
                .SFFK_APPEND_ONLY_BASE_INCOMPATIBLE));

    verify(jobs)
        .enqueue(
            eq(ACCOUNT_ID),
            eq("connector"),
            eq(true),
            eq(CaptureMode.METADATA_AND_CAPTURE),
            any(ReconcileScope.class),
            any(ReconcileExecutionPolicy.class),
            eq(""));
  }

  @Test
  void incompatibleAppendOnlyFailureEnqueuesOneReplacementWhenTheResultWriteIsRetried() {
    AtomicInteger finalizeAttempts = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (finalizeAttempts.getAndIncrement() == 0) {
                throw new StorageAbortRetryableException("idempotency write aborted");
              }
              return null;
            })
        .when(service.idempotencyStore)
        .finalizeSuccess(
            anyString(),
            anyString(),
            anyString(),
            anyString(),
            any(),
            any(),
            any(byte[].class),
            any(),
            any());

    assertTrue(
        service.persistFailure(
            principal,
            FINALIZE_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            "append-only base is incompatible",
            ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
                .SFFK_APPEND_ONLY_BASE_INCOMPATIBLE));

    assertEquals(2, finalizeAttempts.get());
    verify(jobs, times(1))
        .enqueue(
            anyString(),
            anyString(),
            anyBoolean(),
            any(CaptureMode.class),
            any(ReconcileScope.class),
            any(ReconcileExecutionPolicy.class),
            anyString());
  }

  @Test
  void ordinaryFinalizeFailureDoesNotEnqueueAFullCaptureReconcile() {
    assertTrue(
        service.persistFailure(
            principal,
            FINALIZE_JOB_ID,
            LEASE_EPOCH,
            "result-1",
            "descriptor count mismatch",
            ai.floedb.floecat.reconciler.rpc.SubmitLeasedSnapshotFinalizeResultRequest.FailureKind
                .SFFK_UNSPECIFIED));

    verify(jobs, never())
        .enqueue(
            anyString(),
            anyString(),
            anyBoolean(),
            any(CaptureMode.class),
            any(ReconcileScope.class),
            any(ReconcileExecutionPolicy.class),
            anyString());
  }

  @Test
  void acceptedPublicationUsesDurableCompletionViewWithoutRenewingWorkerLease() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());

    assertTrue(
        service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));
    assertTrue(service.publishAcceptedSnapshotFinalize(FINALIZE_JOB_ID));

    verify(jobs, times(1)).renewLease(FINALIZE_JOB_ID, LEASE_EPOCH);
    verify(jobs).getCompletionLeaseView(FINALIZE_JOB_ID, LEASE_EPOCH, true);
  }

  @Test
  void successVerifiesAndRegistersManifest() throws Exception {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());

    persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(blobs).get(manifestUri());
    byte[] durableManifestBytes = manifestBytes();
    String durableManifestUri = manifestUri();
    verify(blobs, never()).put(anyString(), any(byte[].class), anyString());
    verify(service.idempotencyStore, never())
        .createPending(anyString(), anyString(), anyString(), anyString(), any(), any());
    verify(service.idempotencyStore, never())
        .finalizeSuccess(
            anyString(), anyString(), anyString(), anyString(), any(), any(), any(), any(), any());
    verify(persistence)
        .publishPreparedStatsGeneration(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), eq(List.of()), any(), any());
    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
    verify(service.snapshotRepo)
        .recordReuseManifest(
            any(),
            eq(SNAPSHOT_ID),
            eq(
                SnapshotReuseManifestRef.newBuilder()
                    .setFormatVersion(1)
                    .setUri(durableManifestUri)
                    .setPayloadBytes(durableManifestBytes.length)
                    .setPayloadSha256(ByteString.copyFrom(sha256(durableManifestBytes)))
                    .setStatsGenerationManifestUri(
                        Keys.snapshotTargetStatsManifestBlobUri(
                            ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "full-rescan-parent-job"))
                    .build()));
    verify(currentSnapshotPointerService)
        .maybeAdvance(any(), any(Snapshot.class), eq(FINALIZE_JOB_ID));
  }

  @Test
  void deletedSnapshotFailsFinalizeWithoutAdvancingCurrent() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());
    when(service.snapshotRepo.recordReuseManifest(
            any(), anyLong(), any(SnapshotReuseManifestRef.class)))
        .thenThrow(new BaseResourceRepository.NotFoundException("snapshot deleted"));

    assertThrows(
        BaseResourceRepository.NotFoundException.class,
        () -> persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));

    verify(currentSnapshotPointerService, never())
        .maybeAdvance(any(), any(Snapshot.class), anyString());
    verify(jobs, never())
        .completeSnapshotFinalizeSuccess(
            eq(FINALIZE_JOB_ID),
            eq(LEASE_EPOCH),
            anyString(),
            anyString(),
            anyLong(),
            anyString(),
            anyInt(),
            anyInt(),
            anyLong(),
            anyLong(),
            any(),
            anyLong(),
            anyString());
  }

  @Test
  void successActivatesPreparedFileStatsAfterValidatingTheCompactBundle() {
    String childJobId = "file-group-job";
    String childLeaseEpoch = "child-lease";
    String filePath = "s3://bucket/data/file-1.parquet";
    String statsPrefix =
        Keys.reconcileFileGroupStatsObjectPrefix(
            ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "parent-job", childJobId, childLeaseEpoch);
    var statsRecord =
        ai.floedb.floecat.catalog.rpc.TargetStatsRecord.newBuilder()
            .setTarget(StatsTargetIdentity.fileTarget(filePath))
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, "source-fingerprint")
            .putProperties(FileArtifactReuse.STATS_SIGNATURE_PROPERTY, "stats-signature")
            .putProperties(FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, "[]")
            .build();
    byte[] bundleBytes =
        ReusableArtifactBundlePayload.newBuilder()
            .setFormatVersion(1)
            .addFileStats(statsRecord)
            .build()
            .toByteArray();
    byte[] statsSha256 = sha256(bundleBytes);
    String statsUri =
        statsPrefix + "reuse-bundles/" + HexFormat.of().formatHex(statsSha256) + ".pb";
    StatsObjectDescriptor bundleArtifact =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri(statsUri)
            .setPayloadBytes(bundleBytes.length)
            .setPayloadSha256(ByteString.copyFrom(statsSha256))
            .build();
    byte[] payloadBytes = new byte[] {7};
    String payloadUri = "/file-group-result.pb";
    FileGroupResultDescriptor fileGroup =
        FileGroupResultDescriptor.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFileGroupJobId(childJobId)
            .setPlanId("plan-1")
            .setGroupId("group-1")
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(childLeaseEpoch)
            .setResultId("child-result")
            .setPayloadUri(payloadUri)
            .setPayloadBytes(payloadBytes.length)
            .setPayloadSha256(ByteString.copyFrom(sha256(payloadBytes)))
            .setCreatedAt(com.google.protobuf.util.Timestamps.fromMillis(1234L))
            .setStatsObjectPrefix(statsPrefix)
            .setPlannedFileCount(1)
            .setSucceededFileCount(1)
            .setFileStatsRecordCount(1)
            .setArtifactReferencesSha256(
                ByteString.copyFrom(
                    HexFormat.of()
                        .parseHex(
                            ArtifactReferenceDigest.sha256(
                                List.of(
                                    bundleArtifact.toBuilder()
                                        .setTargetStorageId(
                                            StatsTargetIdentity.storageId(statsRecord.getTarget()))
                                        .build()),
                                List.of()))))
            .build();
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setReusableArtifactBundlesComplete(true)
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_FILE_STATS))
            .addFileGroups(fileGroup)
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(bundleArtifact)
                    .addFileStats(
                        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                            .setFilePath(filePath)
                            .setSourceFingerprint("source-fingerprint")
                            .setStatsCaptureSignature("stats-signature")))
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .setReusableArtifactIndex(testArtifactIndex(1, 0))
            .build();
    byte[] manifestBytes = manifest.toByteArray();
    SnapshotCaptureManifestDescriptor descriptor =
        descriptor(manifestUri(manifestBytes), manifestBytes, 1, 1, 1);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 1, 1, fileStatsScope())));
    when(service.childStateService.compactChildState(ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, 1))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                1, 1, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);
    when(jobs.childFileGroupResultDescriptorsPage(ACCOUNT_ID, "parent-job", 500, ""))
        .thenReturn(
            new ReconcileJobStore.FileGroupResultDescriptorPage(
                List.of(storedDescriptor(fileGroup)), ""));

    persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(blobs, times(1)).get(manifestUri(manifestBytes));
    verify(blobs, never()).get(payloadUri);
    verify(blobs, never()).get(statsUri);
    verify(persistence)
        .publishPreparedStatsGeneration(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), eq(List.of()), any(), any());
    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
  }

  @Test
  void successRetriesWhenAFileGroupHasNotFinishedMetadataOnlyStaging() {
    String childJobId = "file-group-job";
    String childLeaseEpoch = "child-lease";
    String filePath = "s3://bucket/data-1.parquet";
    String statsPrefix =
        Keys.reconcileFileGroupStatsObjectPrefix(
            ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "parent-job", childJobId, childLeaseEpoch);
    StatsObjectDescriptor bundleArtifact =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri(zeroDigestBundleUri(statsPrefix))
            .setPayloadBytes(123)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .build();
    FileGroupResultDescriptor fileGroup =
        FileGroupResultDescriptor.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFileGroupJobId(childJobId)
            .setPlanId("plan-1")
            .setGroupId("group-1")
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(childLeaseEpoch)
            .setResultId("child-result")
            .setPayloadUri("/file-group-result.pb")
            .setPayloadBytes(1)
            .setPayloadSha256(ByteString.copyFrom(new byte[32]))
            .setStatsObjectPrefix(statsPrefix)
            .setPlannedFileCount(1)
            .setSucceededFileCount(1)
            .setFileStatsRecordCount(1)
            .setArtifactReferencesSha256(
                ByteString.copyFrom(
                    HexFormat.of()
                        .parseHex(
                            ArtifactReferenceDigest.sha256(
                                List.of(
                                    bundleArtifact.toBuilder()
                                        .setTargetStorageId(
                                            StatsTargetIdentity.storageId(
                                                StatsTargetIdentity.fileTarget(filePath)))
                                        .build()),
                                List.of()))))
            .build();
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setReusableArtifactBundlesComplete(true)
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_FILE_STATS))
            .addFileGroups(fileGroup)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .setReusableArtifactIndex(testArtifactIndex(1, 0))
            .addReusableArtifactBundles(
                ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
                    .setArtifact(bundleArtifact)
                    .addFileStats(
                        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                            .setFilePath(filePath)
                            .setSourceFingerprint("source-1")
                            .setStatsCaptureSignature("stats-signature")))
            .build()
            .toByteArray();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 1, 1, fileStatsScope())));
    when(service.childStateService.compactChildState(ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, 1))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                1, 1, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);
    when(jobs.childFileGroupResultDescriptorsPage(ACCOUNT_ID, "parent-job", 500, ""))
        .thenReturn(
            new ReconcileJobStore.FileGroupResultDescriptorPage(
                List.of(storedDescriptor(fileGroup)), ""));
    when(service.statsStore.isPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(false);

    assertThrows(
        StorageAbortRetryableException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(manifestBytes), manifestBytes, 1, 1, 1)));

    verify(blobs, times(1)).get(manifestUri(manifestBytes));
    verify(blobs, never()).get("/file-group-result.pb");
    verify(persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
  }

  @Test
  void successRejectsIncompleteIndexCoverageBeforeActivation() {
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setReusableArtifactBundlesComplete(true)
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX))
            .setSourceFileCount(1)
            .setIndexArtifactCount(0)
            .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
            .build()
            .toByteArray();
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 1, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(manifestBytes), manifestBytes, 0, 1, 0)));

    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
  }

  @Test
  void successRejectsMissingRealizedStatsCoverageBeforePublication() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("customer_id", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setReusableArtifactBundlesComplete(true)
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_COLUMN_STATS)
                    .setDefaultColumnScope(
                        ai.floedb.floecat.reconciler.rpc.DefaultColumnScope.DCS_EXPLICIT_ONLY)
                    .setMaxDefaultColumns(32)
                    .addColumns(
                        ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy.newBuilder()
                            .setSelector("customer_id")
                            .setCaptureStats(true)))
            .setSourceFileCount(1)
            .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
            .build()
            .toByteArray();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 1, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(manifestBytes), manifestBytes, 0, 1, 0)));

    verify(jobs, never()).beginSnapshotFinalizeCommit(anyString(), anyString());
    verify(persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
  }

  @Test
  void incrementalFinalizeActivatesTheRemoteAdditiveIndexGeneration() {
    ReconcileCapturePolicy policy = indexCapturePolicy();
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes = indexCaptureManifestBytes();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);
    StatsStore.PublicationFence publicationFence = mock(StatsStore.PublicationFence.class);
    IndexArtifactRepository.PreparedActivation preparedActivation =
        new IndexArtifactRepository.PreparedActivation(
            new IndexArtifactRepository.ActivationFence("/active", "next", 4L),
            publicationFence,
            false);
    when(service.indexArtifactRepository.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(byte[].class), any(), anyBoolean()))
        .thenReturn(preparedActivation);

    persistAndPublish(
        principal,
        FINALIZE_JOB_ID,
        LEASE_EPOCH,
        "result-1",
        descriptor(manifestUri(manifestBytes), manifestBytes, 0, 0, 0));

    var publicationOrder = inOrder(service.indexArtifactRepository, persistence);
    publicationOrder
        .verify(service.indexArtifactRepository)
        .prepareGenerationActivation(
            any(),
            eq(SNAPSHOT_ID),
            eq("full-rescan-parent-job"),
            any(byte[].class),
            any(),
            eq(false));
    publicationOrder
        .verify(persistence)
        .publishPreparedStatsGeneration(
            any(),
            eq(SNAPSHOT_ID),
            eq("full-rescan-parent-job"),
            any(),
            any(),
            eq(publicationFence));
    publicationOrder
        .verify(service.indexArtifactRepository)
        .completePreparedGenerationActivation(any(), eq(SNAPSHOT_ID), eq(preparedActivation));
  }

  @Test
  void indexAndStatsPublicationConflictRepreparesAndPublishesOnce() {
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), indexCapturePolicy());
    byte[] manifestBytes = indexCaptureManifestBytes();
    StatsStore.PublicationFence firstFence = mock(StatsStore.PublicationFence.class);
    StatsStore.PublicationFence secondFence = mock(StatsStore.PublicationFence.class);
    IndexArtifactRepository.PreparedActivation first =
        new IndexArtifactRepository.PreparedActivation(
            new IndexArtifactRepository.ActivationFence("/active", "next", 4L), firstFence, false);
    IndexArtifactRepository.PreparedActivation second =
        new IndexArtifactRepository.PreparedActivation(
            new IndexArtifactRepository.ActivationFence("/active", "next", 4L), secondFence, false);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);
    when(service.indexArtifactRepository.prepareGenerationActivation(
            any(), anyLong(), anyString(), any(byte[].class), any(), eq(false)))
        .thenReturn(first, second);
    when(persistence.publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), any()))
        .thenReturn(false, true);

    persistAndPublish(
        principal,
        FINALIZE_JOB_ID,
        LEASE_EPOCH,
        "result-1",
        descriptor(manifestUri(manifestBytes), manifestBytes, 0, 0, 0));

    verify(service.indexArtifactRepository, times(2))
        .prepareGenerationActivation(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), any(), any(), eq(false));
    verify(persistence)
        .publishPreparedStatsGeneration(
            any(), eq(SNAPSHOT_ID), anyString(), any(), any(), eq(firstFence));
    verify(persistence)
        .publishPreparedStatsGeneration(
            any(), eq(SNAPSHOT_ID), anyString(), any(), any(), eq(secondFence));
    verify(service.indexArtifactRepository)
        .completePreparedGenerationActivation(any(), eq(SNAPSHOT_ID), eq(second));
  }

  @Test
  void sameRevisionIncrementalCaptureInheritsPriorStatsTargets() {
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());
    when(jobs.getFinalizedSnapshot(ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.FinalizedSnapshotEvent(
                    "event",
                    ACCOUNT_ID,
                    TABLE_ID,
                    SNAPSHOT_ID,
                    10L,
                    "prior-finalizer",
                    ReconcileSnapshotContentState.FORMAT_VERSION,
                    "connector",
                    "db",
                    "events",
                    "revision-1",
                    "metadata-1",
                    List.of())));

    persistAndPublish(
        principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor(manifestUri()));

    verify(persistence)
        .prepareStatsGenerationForPublication(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), eq(true));
  }

  @Test
  void changedRevisionIncrementalCaptureReplacesPriorStatsTargets() {
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());
    when(jobs.getFinalizedSnapshot(ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID))
        .thenReturn(
            Optional.of(
                new ReconcileJobStore.FinalizedSnapshotEvent(
                    "event",
                    ACCOUNT_ID,
                    TABLE_ID,
                    SNAPSHOT_ID,
                    10L,
                    "prior-finalizer",
                    ReconcileSnapshotContentState.FORMAT_VERSION,
                    "connector",
                    "db",
                    "events",
                    "revision-0",
                    "metadata-0",
                    List.of())));

    persistAndPublish(
        principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor(manifestUri()));

    verify(persistence)
        .prepareStatsGenerationForPublication(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), eq(false));
  }

  @Test
  void successRejectsManifestPredecessorThatDiffersFromSnapshotPin() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setReusableArtifactBundlesComplete(true)
            .setFormatVersion(1)
            .setReusableArtifactBundlesComplete(true)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .setIndexPredecessor(
                IndexGenerationPredecessor.newBuilder()
                    .setGenerationId("different")
                    .setActivePointerVersion(5L)
                    .setCaptureManifestUri("/different.pb")
                    .setCaptureManifestPointerVersion(6L))
            .setCapturePolicy(
                ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                    .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX))
            .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
            .build()
            .toByteArray();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(manifestBytes), manifestBytes, 0, 0, 0)));

    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class), any(), anyBoolean());
  }

  @Test
  void successRejectsManifestPolicyThatDiffersFromTrustedLease() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes = manifestBytes();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri(manifestBytes))).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(manifestBytes), manifestBytes, 0, 0, 0)));

    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class), any(), anyBoolean());
  }

  @Test
  void trustedLeaseRejectsMissingExplicitIndexSelectors() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#7", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    ReconcileJobStore.LeasedJob lease =
        new ReconcileJobStore.LeasedJob(
            FINALIZE_JOB_ID,
            ACCOUNT_ID,
            "connector",
            false,
            CaptureMode.METADATA_AND_CAPTURE,
            scope,
            ReconcileExecutionPolicy.defaults(),
            LEASE_EPOCH,
            "",
            "");
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder().setSourceFileCount(1).build();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            LeasedSnapshotFinalizeExecutionService.validateRealizedIndexSelectors(lease, manifest));
  }

  @Test
  void successRejectsManifestOutsideFencedLocation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            persistAndPublish(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor("s3://other/manifest.pb")));

    verify(blobs, never()).get(anyString());
    verify(currentSnapshotPointerService, never())
        .maybeAdvance(any(), any(Snapshot.class), anyString());
  }

  @Test
  void missingManifestReturnedAsNullIsRetryable() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(null);

    assertThrows(
        StorageAbortRetryableException.class,
        () -> persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));

    verify(currentSnapshotPointerService, never())
        .maybeAdvance(any(), any(Snapshot.class), anyString());
  }

  @Test
  void missingManifestExceptionIsRetryable() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri()))
        .thenThrow(new StorageNotFoundException("GET manifest: not found"));

    assertThrows(
        StorageAbortRetryableException.class,
        () -> persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));

    verify(currentSnapshotPointerService, never())
        .maybeAdvance(any(), any(Snapshot.class), anyString());
  }

  @Test
  void exactTerminalReplayUsesCanonicalResultWithoutLeaseOrPublicationReads() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_SUCCEEDED")));

    persistAndPublish(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(jobs, never()).renewLease(anyString(), anyString());
    verify(blobs, never()).head(anyString());
    verify(currentSnapshotPointerService, never())
        .maybeAdvance(any(), any(Snapshot.class), anyString());
  }

  private boolean persistAndPublish(
      PrincipalContext principalContext,
      String jobId,
      String leaseEpoch,
      String resultId,
      SnapshotCaptureManifestDescriptor descriptor) {
    boolean accepted =
        service.persistSuccess(principalContext, jobId, leaseEpoch, resultId, descriptor);
    if (acceptedIntent.get() != null) {
      service.publishAcceptedSnapshotFinalize(jobId);
    }
    return accepted;
  }

  private static SnapshotCaptureManifestDescriptor descriptor(String uri) {
    byte[] manifest = manifestBytes();
    return descriptor(uri, manifest, 0, 0, 0);
  }

  private static ReconcileFileGroupResultDescriptor storedDescriptor(
      FileGroupResultDescriptor descriptor) {
    return new ReconcileFileGroupResultDescriptor(
        descriptor.getFormatVersion(),
        descriptor.getAccountId(),
        descriptor.getConnectorId(),
        descriptor.getParentJobId(),
        descriptor.getFileGroupJobId(),
        descriptor.getPlanId(),
        descriptor.getGroupId(),
        descriptor.getTableId(),
        descriptor.getSnapshotId(),
        descriptor.getLeaseEpoch(),
        descriptor.getResultId(),
        descriptor.getPayloadUri(),
        descriptor.getPayloadBytes(),
        Base64.getEncoder().encodeToString(descriptor.getPayloadSha256().toByteArray()),
        descriptor.getPlannedFileCount(),
        descriptor.getSucceededFileCount(),
        descriptor.getFailedFileCount(),
        descriptor.getSkippedFileCount(),
        descriptor.getPartialAggregateRecordCount(),
        descriptor.getIndexArtifactCount(),
        descriptor.getStatsObjectPrefix(),
        descriptor.getFileStatsRecordCount(),
        HexFormat.of().formatHex(descriptor.getArtifactReferencesSha256().toByteArray()),
        null,
        descriptor.hasCreatedAt()
            ? com.google.protobuf.util.Timestamps.toMillis(descriptor.getCreatedAt())
            : 0L);
  }

  private static SnapshotCaptureManifestDescriptor descriptor(
      String uri, byte[] manifest, int fileGroupCount, int sourceFileCount, int statsRecordCount) {
    return SnapshotCaptureManifestDescriptor.newBuilder()
        .setFormatVersion(1)
        .setAccountId(ACCOUNT_ID)
        .setConnectorId("connector")
        .setParentJobId("parent-job")
        .setFinalizeJobId(FINALIZE_JOB_ID)
        .setTableId(TABLE_ID)
        .setSnapshotId(SNAPSHOT_ID)
        .setLeaseEpoch(LEASE_EPOCH)
        .setResultId("result-1")
        .setManifestUri(uri)
        .setManifestBytes(manifest.length)
        .setManifestSha256(ByteString.copyFrom(sha256(manifest)))
        .setFileGroupCount(fileGroupCount)
        .setSourceFileCount(sourceFileCount)
        .setStatsRecordCount(statsRecordCount)
        .build();
  }

  private static byte[] manifestBytes() {
    return SnapshotCaptureManifest.newBuilder()
        .setReusableArtifactBundlesComplete(true)
        .setFormatVersion(1)
        .setReusableArtifactBundlesComplete(true)
        .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
        .setAccountId(ACCOUNT_ID)
        .setConnectorId("connector")
        .setParentJobId("parent-job")
        .setFinalizeJobId(FINALIZE_JOB_ID)
        .setTableId(TABLE_ID)
        .setSnapshotId(SNAPSHOT_ID)
        .setLeaseEpoch(LEASE_EPOCH)
        .setResultId("result-1")
        .build()
        .toByteArray();
  }

  private static ReconcileCapturePolicy indexCapturePolicy() {
    return ReconcileCapturePolicy.of(
        List.of(new ReconcileCapturePolicy.Column("#7", true, true)),
        Set.of(
            ReconcileCapturePolicy.Output.COLUMN_STATS,
            ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
  }

  private static ReconcileScope fileStatsScope() {
    return ReconcileScope.of(
        List.of(),
        TABLE_ID,
        List.of(),
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS)));
  }

  private static ReconcileFileGroupTask plannedGroup(String filePath) {
    return ReconcileFileGroupTask.of("plan-1", "group-1", TABLE_ID, SNAPSHOT_ID, List.of(filePath))
        .withFileExecutionPlans(
            List.of(
                ReconcileFileExecutionPlan.of(
                    filePath, 1L, "", null, "PARQUET", 0, List.of(), "content-identity")));
  }

  private static byte[] indexCaptureManifestBytes() {
    return SnapshotCaptureManifest.newBuilder()
        .setReusableArtifactBundlesComplete(true)
        .setFormatVersion(1)
        .setReusableArtifactBundlesComplete(true)
        .setAccountId(ACCOUNT_ID)
        .setConnectorId("connector")
        .setParentJobId("parent-job")
        .setFinalizeJobId(FINALIZE_JOB_ID)
        .setTableId(TABLE_ID)
        .setSnapshotId(SNAPSHOT_ID)
        .setLeaseEpoch(LEASE_EPOCH)
        .setResultId("result-1")
        .setReusableArtifactIndex(ReusableArtifactIndexStore.emptyReference())
        .addRealizedIndexSelectors("#7")
        .addRealizedStatsSelectors("#7")
        .setIndexPredecessor(
            IndexGenerationPredecessor.newBuilder()
                .setGenerationId("prior")
                .setActivePointerVersion(3L)
                .setCaptureManifestUri("/prior-manifest.pb")
                .setCaptureManifestPointerVersion(4L))
        .setCapturePolicy(
            ai.floedb.floecat.reconciler.rpc.CapturePolicy.newBuilder()
                .addOutputs(CaptureOutput.CO_COLUMN_STATS)
                .addOutputs(CaptureOutput.CO_PARQUET_PAGE_INDEX)
                .addColumns(
                    ai.floedb.floecat.reconciler.rpc.CaptureColumnPolicy.newBuilder()
                        .setSelector("#7")
                        .setCaptureStats(true)
                        .setCaptureIndex(true)))
        .build()
        .toByteArray();
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String zeroDigestBundleUri(String statsPrefix) {
    return statsPrefix + "reuse-bundles/" + "00".repeat(32) + ".pb";
  }

  private ReusableArtifactIndexReference testArtifactIndex(int stats, int indexes) {
    if (stats + indexes == 0) {
      return ReusableArtifactIndexStore.emptyReference();
    }
    var bundle =
        ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:test-index")
                    .setPayloadUri("/bundles/test-index.pb")
                    .setPayloadBytes(1)
                    .setPayloadSha256(ByteString.copyFrom(new byte[32])));
    for (int index = 0; index < stats; index++) {
      bundle.addFileStats(
          ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
              .setFilePath("s3://bucket/stats-" + index + ".parquet")
              .setSourceFingerprint("stats-source-" + index)
              .setStatsCaptureSignature("stats-v1"));
    }
    for (int index = 0; index < indexes; index++) {
      bundle.addIndexArtifacts(
          ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
              .setFilePath("s3://bucket/index-" + index + ".parquet")
              .setSourceFingerprint("index-source-" + index)
              .setIndexCaptureSignature("index-v1"));
    }
    return new ReusableArtifactIndexStore(blobs)
        .append(
            Keys.tableReusableArtifactIndexObjectBlobPrefix(ACCOUNT_ID, TABLE_ID),
            ReusableArtifactIndexStore.emptyReference(),
            List.of(bundle.build()));
  }

  private static ReusableArtifactBundleReference artifactBundle(String id, String filePath) {
    return ReusableArtifactBundleReference.newBuilder()
        .setArtifact(
            StatsObjectDescriptor.newBuilder()
                .setTargetStorageId("reuse-bundle:" + id)
                .setPayloadUri("/bundles/" + id + ".pb")
                .setPayloadBytes(1)
                .setPayloadSha256(ByteString.copyFrom(new byte[32])))
        .addFileStats(
            ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                .setFilePath(filePath)
                .setSourceFingerprint("source:" + filePath)
                .setStatsCaptureSignature("stats-v1"))
        .build();
  }

  private static String manifestUri() {
    return manifestUri(manifestBytes());
  }

  private static String manifestUri(byte[] manifestBytes) {
    return Keys.reconcileSnapshotDurableCaptureManifestUri(
        ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "parent-job", sha256(manifestBytes));
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView() {
    return finalizeJobView("JS_RUNNING");
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView(String state) {
    return finalizeJobView(state, 0, 0);
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView(
      int fileGroupCount, int sourceFileCount) {
    return finalizeJobView("JS_RUNNING", fileGroupCount, sourceFileCount);
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView(
      String state, int fileGroupCount, int sourceFileCount) {
    return finalizeJobView(state, fileGroupCount, sourceFileCount, ReconcileScope.empty());
  }

  private static ReconcileJobStore.ReconcileJob finalizeJobView(
      String state, int fileGroupCount, int sourceFileCount, ReconcileScope scope) {
    ReconcileSnapshotTask snapshotTask =
        ReconcileSnapshotTask.of(
                TABLE_ID,
                SNAPSHOT_ID,
                "db",
                "events",
                List.of(),
                true,
                ReconcileSnapshotTask.CompletionMode.FILE_GROUPS,
                "/snapshot-plan.pb",
                fileGroupCount,
                sourceFileCount,
                "",
                0)
            .withContentState(
                "revision-1",
                "metadata-1",
                ReconcileSnapshotContentState.coverage(
                    CaptureMode.METADATA_AND_CAPTURE,
                    scope == null ? ReconcileScope.empty() : scope));
    if (scope != null && scope.capturePolicy().requestsIndexes()) {
      snapshotTask = snapshotTask.withIndexPredecessor(INDEX_PREDECESSOR);
    }
    return new ReconcileJobStore.ReconcileJob(
        FINALIZE_JOB_ID,
        ACCOUNT_ID,
        "connector",
        state,
        "",
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        0L,
        false,
        CaptureMode.METADATA_AND_CAPTURE,
        0L,
        0L,
        scope,
        ReconcileExecutionPolicy.defaults(),
        "",
        "",
        ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE,
        ReconcileTableTask.empty(),
        ReconcileViewTask.empty(),
        snapshotTask,
        ReconcileFileGroupTask.empty(),
        "parent-job");
  }

  private static ReconcileJobStore.LeasedJob publicationLeaseView(
      ReconcileJobStore.ReconcileJob job) {
    return new ReconcileJobStore.LeasedJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.fullRescan,
        job.captureMode,
        job.scope,
        job.executionPolicy,
        LEASE_EPOCH,
        "",
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId,
        "");
  }
}
