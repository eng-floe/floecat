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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
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
import ai.floedb.floecat.reconciler.rpc.CaptureOutput;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.IndexGenerationPredecessor;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.service.catalog.impl.CurrentSnapshotPointerService;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.IndexArtifactRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LeasedSnapshotFinalizeExecutionServiceTest {
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

  @BeforeEach
  void setUp() {
    service = new LeasedSnapshotFinalizeExecutionService();
    jobs = mock(ReconcileJobStore.class);
    blobs = mock(BlobStore.class);
    currentSnapshotPointerService = mock(CurrentSnapshotPointerService.class);
    persistence = mock(SnapshotFinalizePersistenceService.class);
    principal = mock(PrincipalContext.class);
    service.jobs = jobs;
    service.blobStore = blobs;
    service.childStateService = mock(SnapshotFinalizeChildStateService.class);
    service.currentSnapshotPointerService = currentSnapshotPointerService;
    service.persistence = persistence;
    service.indexArtifactRepository = mock(IndexArtifactRepository.class);
    service.statsStore = mock(ai.floedb.floecat.stats.spi.StatsStore.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    when(principal.getCorrelationId()).thenReturn("corr");
    when(principal.getAccountId()).thenReturn(ACCOUNT_ID);
    when(service.idempotencyStore.get(anyString())).thenReturn(Optional.empty());
    when(service.idempotencyStore.createPending(
            anyString(), anyString(), anyString(), anyString(), any(), any()))
        .thenReturn(true);
    when(jobs.renewLease(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(jobs.beginSnapshotFinalizeCommit(FINALIZE_JOB_ID, LEASE_EPOCH)).thenReturn(true);
    when(service.statsStore.isPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(true);
    when(persistence.prepareStatsGenerationForPublication(
            any(), anyLong(), anyString(), anyBoolean()))
        .thenReturn(new StatsStore.StatsGenerationPredecessor("", 0L));
    when(persistence.publishPreparedStatsGeneration(
            any(), anyLong(), anyString(), any(), any(), any()))
        .thenReturn(true);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID)).thenReturn(Optional.of(finalizeJobView()));
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
            anyLong(),
            anyString()))
        .thenReturn(true);
  }

  @Test
  void successVerifiesAndRegistersManifest() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(manifestBytes());

    service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(blobs).get(manifestUri());
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
    verify(currentSnapshotPointerService).maybeAdvance(any(), eq(SNAPSHOT_ID), eq(FINALIZE_JOB_ID));
  }

  @Test
  void successActivatesPreparedFileStatsWithoutReadingOrRepeatingTheirObjects() {
    String childJobId = "file-group-job";
    String childLeaseEpoch = "child-lease";
    String statsPrefix =
        Keys.reconcileFileGroupStatsObjectPrefix(
            ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "parent-job", childJobId, childLeaseEpoch);
    String targetStorageId = "file-" + "1".repeat(64);
    byte[] statsSha256 = sha256(new byte[] {1, 2, 3});
    String statsUri =
        statsPrefix
            + sha256Hex(targetStorageId)
            + "/"
            + HexFormat.of().formatHex(statsSha256)
            + ".pb";
    StatsObjectDescriptor statsObject =
        StatsObjectDescriptor.newBuilder()
            .setTargetStorageId(targetStorageId)
            .setPayloadUri(statsUri)
            .setPayloadBytes(3)
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
            .setStatsObjectPrefix(statsPrefix)
            .setPlannedFileCount(1)
            .setSucceededFileCount(1)
            .setFileStatsRecordCount(1)
            .setArtifactReferencesSha256(ByteString.copyFrom(new byte[32]))
            .build();
    SnapshotCaptureManifest manifest =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .addFileGroups(fileGroup)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .build();
    byte[] manifestBytes = manifest.toByteArray();
    SnapshotCaptureManifestDescriptor descriptor =
        descriptor(manifestUri(), manifestBytes, 1, 1, 1);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID)).thenReturn(Optional.of(finalizeJobView(1, 1)));
    when(service.childStateService.compactChildState(ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, 1))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                1, 1, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);

    service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(blobs, times(1)).get(manifestUri());
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
            .setStatsObjectPrefix(
                Keys.reconcileFileGroupStatsObjectPrefix(
                    ACCOUNT_ID, TABLE_ID, SNAPSHOT_ID, "parent-job", childJobId, childLeaseEpoch))
            .setPlannedFileCount(1)
            .setSucceededFileCount(1)
            .setFileStatsRecordCount(1)
            .setArtifactReferencesSha256(ByteString.copyFrom(new byte[32]))
            .build();
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId(ACCOUNT_ID)
            .setConnectorId("connector")
            .setParentJobId("parent-job")
            .setFinalizeJobId(FINALIZE_JOB_ID)
            .setTableId(TABLE_ID)
            .setSnapshotId(SNAPSHOT_ID)
            .setLeaseEpoch(LEASE_EPOCH)
            .setResultId("result-1")
            .addFileGroups(fileGroup)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .build()
            .toByteArray();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID)).thenReturn(Optional.of(finalizeJobView(1, 1)));
    when(service.childStateService.compactChildState(ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, 1))
        .thenReturn(
            new SnapshotFinalizeChildStateService.ChildState(
                1, 1, List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of()));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);
    when(service.statsStore.isPreparedFileGroup(
            any(), anyLong(), anyString(), anyString(), anyString(), anyString()))
        .thenReturn(false);

    assertThrows(
        StorageAbortRetryableException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(), manifestBytes, 1, 1, 1)));

    verify(blobs, times(1)).get(manifestUri());
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
            .setFormatVersion(1)
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
            .build()
            .toByteArray();
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 1, scope)));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(), manifestBytes, 0, 1, 0)));

    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
  }

  @Test
  void incrementalFinalizeActivatesTheRemoteAdditiveIndexGeneration() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#7", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
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
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);
    when(service.indexArtifactRepository.activateGeneration(
            any(), anyLong(), anyString(), any(byte[].class), any(), anyBoolean()))
        .thenReturn(new IndexArtifactRepository.ActivationFence("/active", "next", 4L));

    service.persistSuccess(
        principal,
        FINALIZE_JOB_ID,
        LEASE_EPOCH,
        "result-1",
        descriptor(manifestUri(), manifestBytes, 0, 0, 0));

    var publicationOrder = inOrder(service.indexArtifactRepository, persistence);
    publicationOrder
        .verify(service.indexArtifactRepository)
        .activateGeneration(
            any(),
            eq(SNAPSHOT_ID),
            eq("full-rescan-parent-job"),
            any(byte[].class),
            any(),
            eq(true));
    publicationOrder
        .verify(persistence)
        .publishPreparedStatsGeneration(
            any(), eq(SNAPSHOT_ID), eq("full-rescan-parent-job"), any(), any(), any());
  }

  @Test
  void successRejectsManifestPredecessorThatDiffersFromSnapshotPin() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
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
            .build()
            .toByteArray();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(), manifestBytes, 0, 0, 0)));

    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class), any(), anyBoolean());
  }

  @Test
  void successRejectsManifestPolicyThatDoesNotMatchLease() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileScope scope = ReconcileScope.of(List.of(), TABLE_ID, List.of(), policy);
    byte[] manifestBytes = manifestBytes();
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_RUNNING", 0, 0, scope)));
    when(blobs.get(manifestUri())).thenReturn(manifestBytes);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor(manifestUri(), manifestBytes, 0, 0, 0)));

    verify(persistence, never())
        .publishPreparedStatsGeneration(any(), anyLong(), anyString(), any(), any(), any());
    verify(service.indexArtifactRepository, never())
        .activateGeneration(any(), anyLong(), anyString(), any(byte[].class));
  }

  @Test
  void successRejectsManifestOutsideFencedLocation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            service.persistSuccess(
                principal,
                FINALIZE_JOB_ID,
                LEASE_EPOCH,
                "result-1",
                descriptor("s3://other/manifest.pb")));

    verify(blobs, never()).get(anyString());
    verify(currentSnapshotPointerService, never()).maybeAdvance(any(), anyLong(), anyString());
  }

  @Test
  void missingManifestReturnedAsNullIsRetryable() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri())).thenReturn(null);

    assertThrows(
        StorageAbortRetryableException.class,
        () ->
            service.persistSuccess(
                principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));

    verify(currentSnapshotPointerService, never()).maybeAdvance(any(), anyLong(), anyString());
  }

  @Test
  void missingManifestExceptionIsRetryable() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(blobs.get(manifestUri()))
        .thenThrow(new StorageNotFoundException("GET manifest: not found"));

    assertThrows(
        StorageAbortRetryableException.class,
        () ->
            service.persistSuccess(
                principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor));

    verify(currentSnapshotPointerService, never()).maybeAdvance(any(), anyLong(), anyString());
  }

  @Test
  void exactTerminalReplayUsesCanonicalResultWithoutLeaseOrPublicationReads() {
    SnapshotCaptureManifestDescriptor descriptor = descriptor(manifestUri());
    when(jobs.getCompactLeaseView(FINALIZE_JOB_ID))
        .thenReturn(Optional.of(finalizeJobView("JS_SUCCEEDED")));

    service.persistSuccess(principal, FINALIZE_JOB_ID, LEASE_EPOCH, "result-1", descriptor);

    verify(jobs, never()).renewLease(anyString(), anyString());
    verify(blobs, never()).head(anyString());
    verify(currentSnapshotPointerService, never()).maybeAdvance(any(), anyLong(), anyString());
  }

  private static SnapshotCaptureManifestDescriptor descriptor(String uri) {
    byte[] manifest = manifestBytes();
    return descriptor(uri, manifest, 0, 0, 0);
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
        .setFormatVersion(1)
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

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (java.security.NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String sha256Hex(String value) {
    return HexFormat.of()
        .formatHex(sha256(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }

  private static String manifestUri() {
    return Keys.reconcileSnapshotCaptureManifestUri(
        ACCOUNT_ID, "parent-job", FINALIZE_JOB_ID, LEASE_EPOCH);
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
            0);
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
}
