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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifestDescriptor;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RemoteSnapshotFinalizeReconcileExecutorTest {

  @Test
  void trustedFinalizerRejectsUnrequestedRealizedSelectors() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                ReconcileCapturePolicy.empty(), 1, Set.of(), Set.of("#1")));
  }

  @Test
  void trustedFinalizerEnforcesFirstNSelectorLimitsByFieldIdentity() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            2);

    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                policy,
                1,
                Set.of("#1", "#2", "customer_id", "customer_name"),
                Set.of("#1", "#2", "customer_id", "customer_name")));
    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                policy,
                1,
                Set.of("#1", "customer_id", "customer_name"),
                Set.of("#1", "customer_id", "customer_name")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                policy, 1, Set.of("#1", "#2", "#3"), Set.of("#1", "#2")));
  }

  @Test
  void trustedFinalizerRejectsMissingExplicitAndDefaultStatsSelectors() {
    ReconcileCapturePolicy explicit =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("#1", true, false),
                new ReconcileCapturePolicy.Column("#2", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    ReconcileCapturePolicy defaults =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS),
            ReconcileCapturePolicy.DefaultColumnScope.FIRST_N,
            2);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                explicit, 1, Set.of("#1"), Set.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateRealizedSelectors(
                defaults, 1, Set.of(), Set.of()));
  }

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
  void appendOnlyDefaultIndexSelectorsCompareByStableFieldIdentity() {
    Set<String> inherited =
        RemoteSnapshotFinalizeReconcileExecutor.defaultIndexSelectorIdentities(
            List.of("#1", "customer_id"));
    Set<String> delta =
        RemoteSnapshotFinalizeReconcileExecutor.defaultIndexSelectorIdentities(List.of("#1"));

    assertEquals(Set.of("#1"), inherited);
    assertEquals(inherited, delta);
    assertEquals(
        Set.of("#1"),
        RemoteSnapshotFinalizeReconcileExecutor.defaultIndexSelectorIdentities(
            List.of("#1", "customer_id", "id")));
  }

  @Test
  void reuseMetadataMustMatchImmutableFingerprintsSignaturesAndSelectors() {
    String filePath = "s3://bucket/data.parquet";
    String deletePath = "s3://bucket/delete.parquet";
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX),
            ReconcileCapturePolicy.DefaultColumnScope.EXPLICIT_ONLY,
            32);
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
                filePath, 10L, "{}", null, "parquet", 0, List.of(), "content-1")
            .withReuseBundleSelections(
                "stats-source",
                "index-source",
                "stats-signature",
                "index-signature",
                Map.of(deletePath, "delete-source"),
                List.of());
    var valid =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(statsDescriptor("reuse-bundle:g", "/bundle.pb", new byte[32]))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(filePath)
                    .setSourceFingerprint("stats-source")
                    .setStatsCaptureSignature("stats-signature")
                    .addRealizedStatsSelectors("#1"))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(deletePath)
                    .setSourceFingerprint("delete-source")
                    .setStatsCaptureSignature("stats-signature")
                    .addRealizedStatsSelectors("#1"))
            .addIndexArtifacts(
                ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                    .setFilePath(filePath)
                    .setSourceFingerprint("index-source")
                    .setIndexCaptureSignature("index-signature")
                    .addRealizedIndexSelectors("#1"))
            .build();

    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseMetadata(
                policy, List.of(plan), valid, List.of("#1"), List.of("#1")));
    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseMetadata(
                policy,
                List.of(plan),
                valid.toBuilder()
                    .setFileStats(
                        0,
                        valid.getFileStats(0).toBuilder().addRealizedStatsSelectors("customer_id"))
                    .build(),
                List.of("#1", "customer_id"),
                List.of("#1")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseMetadata(
                policy,
                List.of(plan),
                valid.toBuilder()
                    .setFileStats(
                        0, valid.getFileStats(0).toBuilder().setSourceFingerprint("forged"))
                    .build(),
                List.of("#1"),
                List.of("#1")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseMetadata(
                policy,
                List.of(plan),
                valid.toBuilder()
                    .setFileStats(
                        1, valid.getFileStats(1).toBuilder().setStatsCaptureSignature("forged"))
                    .build(),
                List.of("#1"),
                List.of("#1")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseMetadata(
                policy,
                List.of(plan),
                valid.toBuilder()
                    .setIndexArtifacts(
                        0, valid.getIndexArtifacts(0).toBuilder().clearRealizedIndexSelectors())
                    .build(),
                List.of("#1"),
                List.of("#1")));
  }

  @Test
  void reuseBundleArtifactMustMatchEveryCommittedDescriptorPayloadField() {
    byte[] digest = new byte[32];
    digest[0] = 7;
    StatsObjectDescriptor artifact =
        statsDescriptor("reuse-bundle:group-1", "/stats/reuse-bundles/bundle.pb", digest);
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(artifact)
            .build();
    StatsObjectDescriptor committed = artifact.toBuilder().setTargetStorageId("file:data").build();

    assertDoesNotThrow(
        () ->
            RemoteSnapshotFinalizeReconcileExecutor.validateReuseBundleArtifact(
                bundle, List.of(committed)));

    byte[] otherDigest = digest.clone();
    otherDigest[0] = 8;
    for (StatsObjectDescriptor mismatch :
        List.of(
            committed.toBuilder().setPayloadUri("/stats/reuse-bundles/other.pb").build(),
            committed.toBuilder().setPayloadBytes(committed.getPayloadBytes() + 1).build(),
            committed.toBuilder().setPayloadSha256(ByteString.copyFrom(otherDigest)).build())) {
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  RemoteSnapshotFinalizeReconcileExecutor.validateReuseBundleArtifact(
                      bundle, List.of(mismatch)));
      assertTrue(error.getMessage().contains("does not match committed artifact descriptors"));
    }
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
                    List.of(),
                    "iceberg-delete-v1:1:1")),
            "iceberg-data-v1:1:1");
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
  void expectedFileStatsIncludeOnDiskDeltaDeletionVectorAttachedToSuccessfulDataFile() {
    String dataPath = "s3://bucket/data.parquet";
    String deletionVectorPath = "s3://bucket/deletion-vector.bin";
    ReconcileFileExecutionPlan executionPlan =
        ReconcileFileExecutionPlan.of(
            dataPath,
            100L,
            "",
            new ReconcileFileExecutionPlan.DeltaDeletionVector("p", deletionVectorPath, 4, 16, 2),
            "PARQUET",
            0,
            List.of(),
            "delta-add-v1:1::");
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
                ai.floedb.floecat.stats.identity.StatsTargetIdentity.fileTarget(
                    deletionVectorPath))));
  }

  @Test
  void deduplicatesMatchingSnapshotFileStatsTargetsWithoutReadingObjects() {
    byte[] sha256 = new byte[32];
    sha256[0] = 1;
    StatsObjectDescriptor laterUri = statsDescriptor("file-delete", "/stats/z.pb", sha256);
    StatsObjectDescriptor earlierUri = statsDescriptor("file-delete", "/stats/a.pb", sha256);

    List<StatsObjectDescriptor> deduplicated =
        RemoteSnapshotFinalizeReconcileExecutor.deduplicateSnapshotFileStats(
            List.of(laterUri, earlierUri));

    assertEquals(1, deduplicated.size());
    assertEquals("/stats/a.pb", deduplicated.getFirst().getPayloadUri());
  }

  @Test
  void rejectsConflictingSnapshotFileStatsForTheSameTarget() {
    byte[] firstSha256 = new byte[32];
    firstSha256[0] = 1;
    byte[] secondSha256 = new byte[32];
    secondSha256[0] = 2;

    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteSnapshotFinalizeReconcileExecutor.deduplicateSnapshotFileStats(
                    List.of(
                        statsDescriptor("file-delete", "/stats/a.pb", firstSha256),
                        statsDescriptor("file-delete", "/stats/b.pb", secondSha256))));

    assertTrue(error.getMessage().contains("conflicting snapshot file stats"));
  }

  @Test
  void deduplicatesSharedAuxiliaryStatsAcrossDifferentBundlesByIdentityMetadata() {
    String path = "s3://bucket/delete.parquet";
    String target =
        ai.floedb.floecat.stats.identity.StatsTargetIdentity.storageId(
            ai.floedb.floecat.stats.identity.StatsTargetIdentity.fileTarget(path));
    byte[] firstSha256 = new byte[32];
    firstSha256[0] = 1;
    byte[] secondSha256 = new byte[32];
    secondSha256[0] = 2;
    StatsObjectDescriptor first = statsDescriptor(target, "/stats/z-bundle.pb", firstSha256);
    StatsObjectDescriptor second = statsDescriptor(target, "/stats/a-bundle.pb", secondSha256);
    var metadata =
        ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
            .setFilePath(path)
            .setSourceFingerprint("source-fingerprint")
            .setStatsCaptureSignature("stats-signature")
            .addRealizedStatsSelectors("#1")
            .build();
    var firstBundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(first.toBuilder().setTargetStorageId("reuse-bundle:z"))
            .addFileStats(metadata)
            .build();
    var secondBundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(second.toBuilder().setTargetStorageId("reuse-bundle:a"))
            .addFileStats(metadata)
            .build();

    var deduplicated =
        RemoteSnapshotFinalizeReconcileExecutor.deduplicateSnapshotArtifacts(
            List.of(first, second), List.of(firstBundle, secondBundle));

    assertEquals(1, deduplicated.fileStats().size());
    assertEquals("/stats/a-bundle.pb", deduplicated.fileStats().getFirst().getPayloadUri());
    assertEquals(
        1,
        deduplicated.reuseBundles().stream().mapToInt(bundle -> bundle.getFileStatsCount()).sum());
    assertEquals(0, deduplicated.reuseBundles().getFirst().getFileStatsCount());
    assertEquals(1, deduplicated.reuseBundles().get(1).getFileStatsCount());
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
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
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
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
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
  void doesNotSupportExplicitEmptySnapshot() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, blobStore, snapshotPlanBlobStore, true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(0, ReconcileScope.empty());

    assertFalse(executor.supports(lease));
    verifyNoInteractions(workerClient, blobStore, snapshotPlanBlobStore);
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
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
            null);

    when(workerClient.getSnapshotFinalizeInput(remoteLease)).thenReturn(input);
    when(snapshotPlanBlobStore.loadPlan("/snapshot-plan.json"))
        .thenReturn(
            SnapshotPlanBlobStore.SnapshotPlanBlob.of(
                List.of(
                    new PlannedFileGroupJob(ReconcileScope.empty(), group("plan-1", "group-a")),
                    new PlannedFileGroupJob(ReconcileScope.empty(), group("plan-1", "group-b")))));
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
            any(), any(), any(), any(), any(), any(), any(), anyInt(), any(), any(), any(), any(),
            any(), any(), any(), any());
    verify(workerClient, never()).submitSnapshotFinalizeSuccess(any(), any());
  }

  @Test
  void retriesWhenImmutableSnapshotPlanIsTemporarilyUnavailable() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, mock(BlobStore.class), snapshotPlanBlobStore, true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(1, ReconcileScope.empty());
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
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
            null);

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(input);
    when(snapshotPlanBlobStore.loadPlan("/snapshot-plan.json"))
        .thenThrow(new StorageAbortRetryableException("plan not yet visible"));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertEquals(ReconcileExecutor.ExecutionResult.JobOutcome.RETRYABLE_FAILURE, result.outcome);
    assertEquals(
        ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE, result.retryDisposition);
    assertEquals(ReconcileExecutor.ExecutionResult.RetryClass.TRANSIENT_ERROR, result.retryClass);
    verify(workerClient)
        .submitSnapshotFinalizeFailure(any(), any(), contains("plan not yet visible"));
    verify(workerClient, never()).submitSnapshotFinalizeSuccess(any(), any());
  }

  @Test
  void preflightValidationFailureIsTerminalBeforeSubmissionBoundary() {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, mock(BlobStore.class), mock(SnapshotPlanBlobStore.class), true);
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(1, ReconcileScope.empty());
    StandaloneSnapshotFinalizeExecutionPayload input = emptyFinalizeInput();
    AtomicBoolean submissionStarted = new AtomicBoolean();

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(input);
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), any(), any(), any(), anyInt(), anyList(), anyList(),
            anyList(), anyList(), anyList(), anyList(), anyList(), any()))
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
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(1, ReconcileScope.empty());
    AtomicBoolean submissionStarted = new AtomicBoolean();

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(emptyFinalizeInput());
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), any(), any(), any(), anyInt(), anyList(), anyList(),
            anyList(), anyList(), anyList(), anyList(), anyList(), any()))
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
    ReconcileJobStore.LeasedJob lease = leasedFinalizeJob(1, ReconcileScope.empty());
    ReconcileFailureException uncertain =
        new ReconcileFailureException(
            ReconcileExecutor.ExecutionResult.FailureKind.INTERNAL,
            ReconcileExecutor.ExecutionResult.RetryDisposition.RETRYABLE,
            ReconcileExecutor.ExecutionResult.RetryClass.STATE_UNCERTAIN,
            "outcome unknown",
            null);

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(emptyFinalizeInput());
    when(workerClient.prepareSnapshotFinalizeSuccess(
            any(), any(), any(), any(), any(), any(), any(), anyInt(), anyList(), anyList(),
            anyList(), anyList(), anyList(), anyList(), anyList(), any()))
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

  @Test
  void finalizesZeroDeltaAppendFromBaseManifestWithoutFileGroupResults() throws Exception {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, blobStore, snapshotPlanBlobStore, true);
    ReconcileJobStore.LeasedJob lease = leasedAppendFinalizeJob();
    String manifestUri = "/accounts/acct/tables/table-1/snapshots/54/manifest.pb";
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:snapshot-54-group-0")
                    .setPayloadUri(
                        "/accounts/acct/tables/table-1/reuse-bundles/"
                            + "0000000000000000000000000000000000000000000000000000000000000000.pb")
                    .setPayloadBytes(100)
                    .setPayloadSha256(ByteString.copyFrom(new byte[32])))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath("s3://bucket/base.parquet")
                    .setSourceFingerprint("source-v1")
                    .setStatsCaptureSignature("stats-v1"))
            .build();
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setParentJobId("snapshot-job")
            .setTableId("table-1")
            .setSnapshotId(54L)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .addFileGroups(baseFileGroup())
            .setReusableArtifactBundlesComplete(true)
            .addReusableArtifactBundles(bundle)
            .setReusableArtifactIndex(testArtifactIndex(1, 0))
            .build()
            .toByteArray();
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    SnapshotPlanBlobStore.AppendOnlyBase base =
        new SnapshotPlanBlobStore.AppendOnlyBase(
            54L,
            manifestUri,
            manifestBytes.length,
            java.util.HexFormat.of().formatHex(digest),
            1,
            1,
            0,
            "full-rescan-snapshot-job",
            "",
            testArtifactIndex(1, 0));
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
            0,
            "/final-stats.pb",
            "/capture-manifest.pb",
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
            null);

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(input);
    when(snapshotPlanBlobStore.loadPlan("/snapshot-plan.json"))
        .thenReturn(SnapshotPlanBlobStore.SnapshotPlanBlob.of(List.of(), base));
    when(blobStore.get(manifestUri)).thenReturn(manifestBytes);
    when(workerClient.prepareAppendOnlySnapshotFinalizeSuccess(
            any(), any(), any(), any(), any(), any(), any(), anyInt(), anyList(), anyList(),
            anyList(), anyList(), anyList(), anyList(), anyList(), any(), any()))
        .thenReturn(preparedSnapshotFinalizeSuccess());
    when(workerClient.submitSnapshotFinalizeSuccess(any(), any())).thenReturn(true);

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertTrue(result.success());
    verify(workerClient, never()).listSnapshotFileGroupResults(any());
    verify(workerClient)
        .prepareAppendOnlySnapshotFinalizeSuccess(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            eq(1),
            eq(List.of()),
            eq(List.of()),
            eq(List.of()),
            eq(List.of()),
            eq(List.of()),
            eq(List.of()),
            eq(List.of()),
            any(),
            eq(base));
  }

  @Test
  void appendOnlyFinalizeLoadsDeltaGroupDescriptors() throws Exception {
    RemoteSnapshotFinalizeWorkerClient workerClient =
        mock(RemoteSnapshotFinalizeWorkerClient.class);
    BlobStore blobStore = mock(BlobStore.class);
    SnapshotPlanBlobStore snapshotPlanBlobStore = mock(SnapshotPlanBlobStore.class);
    RemoteSnapshotFinalizeReconcileExecutor executor =
        new RemoteSnapshotFinalizeReconcileExecutor(
            workerClient, blobStore, snapshotPlanBlobStore, true);
    ReconcileJobStore.LeasedJob lease = leasedAppendFinalizeJob(1, 2);
    String manifestUri = "/accounts/acct/tables/table-1/snapshots/54/manifest.pb";
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:snapshot-54-group-0")
                    .setPayloadUri(
                        "/accounts/acct/tables/table-1/reuse-bundles/"
                            + "0000000000000000000000000000000000000000000000000000000000000000.pb")
                    .setPayloadBytes(100)
                    .setPayloadSha256(ByteString.copyFrom(new byte[32])))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath("s3://bucket/base.parquet")
                    .setSourceFingerprint("source-v1")
                    .setStatsCaptureSignature("stats-v1"))
            .build();
    byte[] manifestBytes =
        SnapshotCaptureManifest.newBuilder()
            .setFormatVersion(1)
            .setAccountId("acct")
            .setConnectorId("connector-1")
            .setParentJobId("snapshot-job")
            .setTableId("table-1")
            .setSnapshotId(54L)
            .setSourceFileCount(1)
            .setFileStatsRecordCount(1)
            .addFileGroups(baseFileGroup())
            .setReusableArtifactBundlesComplete(true)
            .addReusableArtifactBundles(bundle)
            .setReusableArtifactIndex(testArtifactIndex(1, 0))
            .build()
            .toByteArray();
    byte[] digest = java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes);
    SnapshotPlanBlobStore.AppendOnlyBase base =
        new SnapshotPlanBlobStore.AppendOnlyBase(
            54L,
            manifestUri,
            manifestBytes.length,
            java.util.HexFormat.of().formatHex(digest),
            1,
            1,
            0,
            "full-rescan-snapshot-job",
            "",
            testArtifactIndex(1, 0));
    StandaloneSnapshotFinalizeExecutionPayload input =
        new StandaloneSnapshotFinalizeExecutionPayload(
            "finalize-job",
            "lease-1",
            "snapshot-job",
            tableId(),
            55L,
            true,
            2,
            "/snapshot-plan.json",
            1,
            "/final-stats.pb",
            "/capture-manifest.pb",
            "/reusable-index/",
            "/stats-generation.pb",
            "/index-capture-manifests/",
            null);

    when(workerClient.getSnapshotFinalizeInput(any())).thenReturn(input);
    when(snapshotPlanBlobStore.loadPlan("/snapshot-plan.json"))
        .thenReturn(
            SnapshotPlanBlobStore.SnapshotPlanBlob.of(
                List.of(
                    new PlannedFileGroupJob(ReconcileScope.empty(), group("plan-1", "group-a"))),
                base));
    when(blobStore.get(manifestUri)).thenReturn(manifestBytes);
    when(workerClient.listSnapshotFileGroupResults(any()))
        .thenReturn(List.of(descriptor("plan-1", "group-c")));

    ReconcileExecutor.ExecutionResult result =
        executor.execute(
            new ReconcileExecutor.ExecutionContext(
                lease, () -> false, (a, b, c, d, e, f, g, h) -> {}));

    assertFalse(result.ok());
    verify(workerClient).listSnapshotFileGroupResults(any());
    assertTrue(result.message.contains("unexpected snapshot file-group descriptor"));
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
        "/reusable-index/",
        "/stats-generation.pb",
        "/index-capture-manifests/",
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

  private static ReconcileJobStore.LeasedJob leasedAppendFinalizeJob() {
    return leasedAppendFinalizeJob(0, 1);
  }

  private static ReconcileJobStore.LeasedJob leasedAppendFinalizeJob(
      int fileGroupCount, int sourceFileCount) {
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
            fileGroupCount,
            sourceFileCount,
            "",
            0);
    return new ReconcileJobStore.LeasedJob(
        "finalize-job",
        "acct",
        "connector-1",
        true,
        ReconcilerService.CaptureMode.METADATA_AND_CAPTURE,
        ReconcileScope.empty(),
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

  private static ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor baseFileGroup() {
    return ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor.newBuilder()
        .setGroupId("snapshot-54-group-0")
        .setStatsObjectPrefix("/accounts/acct/tables/table-1/")
        .setSucceededFileCount(1)
        .build();
  }

  private static StatsObjectDescriptor statsDescriptor(
      String targetStorageId, String payloadUri, byte[] sha256) {
    return StatsObjectDescriptor.newBuilder()
        .setTargetStorageId(targetStorageId)
        .setPayloadUri(payloadUri)
        .setPayloadBytes(12L)
        .setPayloadSha256(ByteString.copyFrom(sha256))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference testArtifactIndex(
      int stats, int indexes) {
    int entries = stats + indexes;
    var index =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference.newBuilder()
            .setFormatVersion(1)
            .setFileStatsRecordCount(stats)
            .setIndexArtifactCount(indexes);
    if (entries > 0) {
      var object =
          ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexObjectReference.newBuilder()
              .setPayloadBytes(1L)
              .setPayloadSha256(ByteString.copyFrom(new byte[32]));
      index.addRuns(
          ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunReference.newBuilder()
              .setManifest(object.clone().setUri("/artifact-index/manifest.pb"))
              .setFilter(object.clone().setUri("/artifact-index/filter.bf"))
              .setEntryCount(entries)
              .setFileStatsRecordCount(stats)
              .setIndexArtifactCount(indexes));
    }
    return index.build();
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
