/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexArtifactState;
import ai.floedb.floecat.catalog.rpc.IndexFileTarget;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RemoteFileArtifactReusePlannerTest {
  private static final String PATH = "s3://bucket/file.parquet";
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setKind(ResourceKind.RK_TABLE)
          .setId("table")
          .build();

  @Test
  void bindsCompatibleStatsAndIndexIntoTheRemoteExecutionPlan() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    String sourceFingerprint = FileArtifactReuse.sourceFingerprint(plan, "{}");
    String indexSourceFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
    String statsSignature = FileArtifactReuse.statsCaptureSignature(policy);
    String indexSignature = FileArtifactReuse.indexCaptureSignature(policy);
    TargetStatsRecord stats =
        stats(41L, 10L).toBuilder()
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, sourceFingerprint)
            .putProperties(FileArtifactReuse.STATS_SIGNATURE_PROPERTY, statsSignature)
            .putProperties(FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, "#1")
            .build();
    IndexArtifactRecord index =
        IndexArtifactRecord.newBuilder()
            .setTableId(TABLE)
            .setSnapshotId(41L)
            .setTarget(
                IndexTarget.newBuilder().setFile(IndexFileTarget.newBuilder().setFilePath(PATH)))
            .setState(IndexArtifactState.IAS_READY)
            .setArtifactUri("s3://bucket/index.parquet")
            .putProperties(FileArtifactReuse.SOURCE_FINGERPRINT_PROPERTY, indexSourceFingerprint)
            .putProperties(FileArtifactReuse.INDEX_SIGNATURE_PROPERTY, indexSignature)
            .putProperties("indexed_columns", "#1")
            .build();

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrich(
                TABLE,
                42L,
                "{}",
                List.of(plan),
                policy,
                false,
                List.of(stats),
                List.of(index),
                (snapshotId, path) -> "")
            .getFirst();

    assertEquals(42L, enriched.reusableFileStats().getSnapshotId());
    assertEquals(42L, enriched.reusableIndexArtifact().getSnapshotId());
    assertFalse(enriched.sourceFingerprint().isBlank());
    assertFalse(enriched.indexSourceFingerprint().isBlank());
  }

  @Test
  void retainsLegacyDeltaFallbackInTheRemotePlanner() {
    String identity = "delta-add-v1:42:::10";
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(PATH, 123L, "{}", null, "PARQUET", 0, List.of(), identity);
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS));

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrich(
                TABLE,
                42L,
                "{}",
                List.of(plan),
                policy,
                false,
                List.of(stats(41L, 10L)),
                List.of(),
                (snapshotId, path) -> identity)
            .getFirst();

    assertEquals(42L, enriched.reusableFileStats().getSnapshotId());
    assertEquals(10L, enriched.reusableFileStats().getFile().getRowCount());
  }

  @Test
  void selectsOneGroupBundleWithoutMaterializingArtifactRecords() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    String statsFingerprint = FileArtifactReuse.sourceFingerprint(plan, "{}");
    String indexFingerprint = FileArtifactReuse.indexSourceFingerprint(plan);
    var descriptor =
        ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri("s3://bucket/reuse-bundle.pb")
            .setPayloadBytes(1024)
            .setPayloadSha256(com.google.protobuf.ByteString.copyFrom(new byte[32]))
            .build();
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(descriptor)
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(PATH)
                    .setSourceFingerprint(statsFingerprint)
                    .setStatsCaptureSignature(FileArtifactReuse.statsCaptureSignature(policy)))
            .addIndexArtifacts(
                ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                    .setFilePath(PATH)
                    .setSourceFingerprint(indexFingerprint)
                    .setIndexCaptureSignature(FileArtifactReuse.indexCaptureSignature(policy)))
            .build();

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(bundle))
            .getFirst();

    assertTrue(enriched.reusesFileStats());
    assertTrue(enriched.reusesIndexArtifact());
    assertEquals(1, enriched.reusableArtifactBundleSelections().size());
    assertEquals(
        List.of(PATH), enriched.reusableArtifactBundleSelections().getFirst().statsFilePaths());
    assertEquals(
        List.of(PATH), enriched.reusableArtifactBundleSelections().getFirst().indexFilePaths());
    assertEquals(TargetStatsRecord.getDefaultInstance(), enriched.reusableFileStats());
    assertEquals(IndexArtifactRecord.getDefaultInstance(), enriched.reusableIndexArtifact());
  }

  private static TargetStatsRecord stats(long snapshotId, long rows) {
    return TargetStatsRecord.newBuilder()
        .setTableId(TABLE)
        .setSnapshotId(snapshotId)
        .setTarget(StatsTarget.newBuilder().setFile(FileStatsTarget.newBuilder().setFilePath(PATH)))
        .setFile(
            FileTargetStats.newBuilder()
                .setTableId(TABLE)
                .setSnapshotId(snapshotId)
                .setFilePath(PATH)
                .setRowCount(rows)
                .setSizeBytes(123L))
        .build();
  }
}
