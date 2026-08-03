/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.catalog.rpc.UpstreamStamp;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FileArtifactReuseTest {
  @Test
  void statsAndIndexFingerprintsSeparateDataFromDeletionContext() {
    var base =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/data.parquet", 100L, "{}", null, "PARQUET", 0, List.of(), "content-a");
    var changedContent =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/data.parquet", 100L, "{}", null, "PARQUET", 0, List.of(), "content-b");
    var withDv =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/data.parquet",
            100L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector("u", "dv.bin", 4, 8, 2),
            "PARQUET",
            0,
            List.of(),
            "content-a");

    assertThat(FileArtifactReuse.sourceFingerprint(base, "schema-a"))
        .isNotEqualTo(FileArtifactReuse.sourceFingerprint(withDv, "schema-a"))
        .isNotEqualTo(FileArtifactReuse.sourceFingerprint(changedContent, "schema-a"))
        .isNotEqualTo(FileArtifactReuse.sourceFingerprint(base, "schema-b"));
    assertThat(FileArtifactReuse.indexSourceFingerprint(base))
        .isEqualTo(FileArtifactReuse.indexSourceFingerprint(withDv))
        .isNotEqualTo(FileArtifactReuse.indexSourceFingerprint(changedContent));
  }

  @Test
  void statsFingerprintIncludesIcebergDeleteContextIndependentOfPlanOrder() {
    var firstDelete =
        new ReconcileFileExecutionPlan.IcebergDeleteFile(
            "s3://bucket/delete-a.parquet",
            10L,
            ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
            1,
            List.of(),
            "iceberg-delete-v1:1:2");
    var secondDelete =
        new ReconcileFileExecutionPlan.IcebergDeleteFile(
            "s3://bucket/delete-b.parquet",
            20L,
            ReconcileFileExecutionPlan.IcebergDeleteContent.EQUALITY,
            1,
            List.of(3),
            "iceberg-delete-v1:2:3");
    var base =
        ReconcileFileExecutionPlan.of(
            "s3://bucket/data.parquet",
            100L,
            "{}",
            null,
            "PARQUET",
            1,
            List.of(),
            "iceberg-data-v1:1:10");
    var ordered =
        ReconcileFileExecutionPlan.of(
            base.filePath(),
            base.fileSizeInBytes(),
            base.partitionDataJson(),
            null,
            base.fileFormat(),
            base.partitionSpecId(),
            List.of(firstDelete, secondDelete),
            base.contentIdentity());
    var reversed =
        ReconcileFileExecutionPlan.of(
            base.filePath(),
            base.fileSizeInBytes(),
            base.partitionDataJson(),
            null,
            base.fileFormat(),
            base.partitionSpecId(),
            List.of(secondDelete, firstDelete),
            base.contentIdentity());

    assertThat(FileArtifactReuse.sourceFingerprint(ordered, "schema"))
        .isEqualTo(FileArtifactReuse.sourceFingerprint(reversed, "schema"))
        .isNotEqualTo(FileArtifactReuse.sourceFingerprint(base, "schema"));
    assertThat(FileArtifactReuse.indexSourceFingerprint(ordered))
        .isEqualTo(FileArtifactReuse.indexSourceFingerprint(base));
  }

  @Test
  void statsAndIndexPoliciesHaveIndependentSignatures() {
    var statsOnly =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS));
    var statsAndIndex =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.COLUMN_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));

    assertThat(FileArtifactReuse.statsCaptureSignature(statsOnly))
        .isEqualTo(FileArtifactReuse.statsCaptureSignature(statsAndIndex));
    assertThat(FileArtifactReuse.indexCaptureSignature(statsOnly, "schema"))
        .isNotEqualTo(FileArtifactReuse.indexCaptureSignature(statsAndIndex, "schema"));
  }

  @Test
  void defaultIndexSignatureIncludesExecutionSchemaButExplicitSelectionDoesNot() {
    var defaultIndex =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    var explicitIndex =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));

    assertThat(FileArtifactReuse.indexCaptureSignature(defaultIndex, "schema-a"))
        .isNotEqualTo(FileArtifactReuse.indexCaptureSignature(defaultIndex, "schema-b"));
    assertThat(FileArtifactReuse.indexCaptureSignature(explicitIndex, "schema-a"))
        .isEqualTo(FileArtifactReuse.indexCaptureSignature(explicitIndex, "schema-b"));
  }

  @Test
  void captureSignatureSeparatesSelectorsFromOutputs() {
    var explicitFileStatsColumn =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("FILE_STATS", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS));
    var defaultColumnsAndFileStats =
        ReconcileCapturePolicy.of(
            List.of(),
            Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.COLUMN_STATS));

    assertThat(FileArtifactReuse.statsCaptureSignature(explicitFileStatsColumn))
        .isNotEqualTo(FileArtifactReuse.statsCaptureSignature(defaultColumnsAndFileStats));
  }

  @Test
  void bindingStatsUpdatesEverySnapshotEnvelope() {
    ResourceId table =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table")
            .build();
    TargetStatsRecord prior =
        TargetStatsRecord.newBuilder()
            .setTableId(table)
            .setSnapshotId(1L)
            .setTarget(
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath("data.parquet")))
            .setFile(
                FileTargetStats.newBuilder()
                    .setTableId(table)
                    .setSnapshotId(1L)
                    .setFilePath("data.parquet")
                    .addColumns(
                        FileColumnStats.newBuilder()
                            .setColumnId(1L)
                            .setScalar(
                                ScalarStats.newBuilder()
                                    .setUpstream(UpstreamStamp.newBuilder().setCommitRef("1")))))
            .putProperties(FileArtifactReuse.REALIZED_STATS_SELECTORS_PROPERTY, "#1")
            .build();

    TargetStatsRecord rebound =
        FileArtifactReuse.bindStatsToSnapshot(prior, table, 2L, "source", "stats");

    assertThat(rebound.getSnapshotId()).isEqualTo(2L);
    assertThat(rebound.getFile().getSnapshotId()).isEqualTo(2L);
    assertThat(rebound.getFile().getColumns(0).getScalar().getUpstream().getCommitRef())
        .isEqualTo("2");
    assertThat(FileArtifactReuse.compatibleStats(rebound, "data.parquet", "source", "stats"))
        .isTrue();
  }
}
