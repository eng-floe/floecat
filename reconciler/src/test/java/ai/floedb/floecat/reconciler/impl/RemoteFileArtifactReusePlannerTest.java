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

import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileExecutionPlan;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RemoteFileArtifactReusePlannerTest {
  private static final String PATH = "s3://bucket/file.parquet";

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
                    .setStatsCaptureSignature(FileArtifactReuse.statsCaptureSignature(policy))
                    .addRealizedStatsSelectors("#1"))
            .addIndexArtifacts(
                ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                    .setFilePath(PATH)
                    .setSourceFingerprint(indexFingerprint)
                    .setIndexCaptureSignature(FileArtifactReuse.indexCaptureSignature(policy, "{}"))
                    .addRealizedIndexSelectors("#1"))
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
  }

  @Test
  void reusesStatsOnlyWhenMainAndAllAuxiliaryTargetsAreCompatible() {
    String deletionVectorPath = "s3://bucket/deletion-vector.bin";
    String deleteFilePath = "s3://bucket/delete-file.parquet";
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector("u", deletionVectorPath, 0, 12, 3L),
            "PARQUET",
            0,
            List.of(
                new ReconcileFileExecutionPlan.IcebergDeleteFile(
                    deleteFilePath,
                    45L,
                    ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
                    0,
                    List.of(),
                    "iceberg-delete-v1:8:11")),
            "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS));
    String statsSignature = FileArtifactReuse.statsCaptureSignature(policy);
    String mainFingerprint = FileArtifactReuse.sourceFingerprint(plan, "{}");
    var auxiliaryFingerprints = FileArtifactReuse.auxiliaryStatsFingerprints(plan);
    var descriptor =
        ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
            .setTargetStorageId("reuse-bundle:group-1")
            .setPayloadUri("s3://bucket/reuse-bundle.pb")
            .setPayloadBytes(1024)
            .setPayloadSha256(com.google.protobuf.ByteString.copyFrom(new byte[32]))
            .build();
    var incompleteBundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(descriptor)
            .addFileStats(statsMetadata(PATH, mainFingerprint, statsSignature))
            .addFileStats(
                statsMetadata(
                    deletionVectorPath,
                    auxiliaryFingerprints.get(deletionVectorPath),
                    statsSignature))
            .build();

    ReconcileFileExecutionPlan incomplete =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(incompleteBundle))
            .getFirst();

    assertFalse(incomplete.reusesFileStats());
    assertTrue(incomplete.reusableArtifactBundleSelections().isEmpty());

    var completeBundle =
        incompleteBundle.toBuilder()
            .addFileStats(
                statsMetadata(
                    deleteFilePath, auxiliaryFingerprints.get(deleteFilePath), statsSignature))
            .build();
    ReconcileFileExecutionPlan complete =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(completeBundle))
            .getFirst();

    assertTrue(complete.reusesFileStats());
    assertEquals(
        Set.of(PATH, deletionVectorPath, deleteFilePath),
        Set.copyOf(complete.reusableArtifactBundleSelections().getFirst().statsFilePaths()));
  }

  @Test
  void rejectsBundleIndexThatDoesNotCoverExplicitSelectors() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#2", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    var bundle = indexBundle(plan, policy, "{}", "#1");

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(bundle))
            .getFirst();

    assertFalse(enriched.reusesIndexArtifact());
  }

  @Test
  void rejectsBundleStatsThatDoNotCoverExplicitSelectors() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#2", true, false)),
            Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS));
    String statsFingerprint = FileArtifactReuse.sourceFingerprint(plan, "{}");
    var bundle =
        ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
            .setArtifact(
                ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                    .setTargetStorageId("reuse-bundle:group-1")
                    .setPayloadUri("s3://bucket/reuse-bundle.pb")
                    .setPayloadBytes(1024)
                    .setPayloadSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
            .addFileStats(
                ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                    .setFilePath(PATH)
                    .setSourceFingerprint(statsFingerprint)
                    .setStatsCaptureSignature(FileArtifactReuse.statsCaptureSignature(policy))
                    .addRealizedStatsSelectors("#1"))
            .build();

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(bundle))
            .getFirst();

    assertFalse(enriched.reusesFileStats());
  }

  @Test
  void rejectsBundleIndexWhenNamedRequirementIsMissingDespiteMatchingStableIdentity() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(
                new ReconcileCapturePolicy.Column("#1", false, true),
                new ReconcileCapturePolicy.Column("logical_customer_id", false, true)),
            Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    var bundle = indexBundle(plan, policy, "{}", "#1", "physical_customer_id");

    assertEquals(
        FileArtifactReuse.selectorIdentities(policy.selectorsForIndex()),
        FileArtifactReuse.selectorIdentities(
            bundle.getIndexArtifacts(0).getRealizedIndexSelectorsList()));

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(plan), policy, false, List.of(bundle))
            .getFirst();

    assertFalse(enriched.reusesIndexArtifact());
  }

  @Test
  void rejectsDefaultBundleIndexWhenExecutionSchemaChanges() {
    ReconcileFileExecutionPlan plan =
        ReconcileFileExecutionPlan.of(
            PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "iceberg-data-v1:7:10");
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(), Set.of(ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    var bundle = indexBundle(plan, policy, "schema-a", "#1");

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "schema-b", List.of(plan), policy, false, List.of(bundle))
            .getFirst();

    assertFalse(enriched.reusesIndexArtifact());
  }

  @Test
  void changedOrRemovedDeleteContextDoesNotReuseFileStats() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.FILE_STATS));
    ReconcileFileExecutionPlan withIcebergDelete =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            null,
            "PARQUET",
            0,
            List.of(
                new ReconcileFileExecutionPlan.IcebergDeleteFile(
                    "s3://bucket/delete.parquet",
                    10L,
                    ReconcileFileExecutionPlan.IcebergDeleteContent.POSITION,
                    0,
                    List.of(),
                    "delete-v1")),
            "data-v1");
    ReconcileFileExecutionPlan withoutIcebergDelete =
        ReconcileFileExecutionPlan.of(PATH, 123L, "{}", null, "PARQUET", 0, List.of(), "data-v1");
    assertFalse(
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}",
                List.of(withoutIcebergDelete),
                policy,
                false,
                List.of(statsBundle(withIcebergDelete, policy)))
            .getFirst()
            .reusesFileStats());

    ReconcileFileExecutionPlan priorDv =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector(
                "u", "s3://bucket/dv.bin", 4, 20, 3L),
            "PARQUET",
            0,
            List.of(),
            "data-v1");
    ReconcileFileExecutionPlan changedDv =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector(
                "u", "s3://bucket/dv.bin", 4, 20, 4L),
            "PARQUET",
            0,
            List.of(),
            "data-v1");
    assertFalse(
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(changedDv), policy, false, List.of(statsBundle(priorDv, policy)))
            .getFirst()
            .reusesFileStats());
  }

  @Test
  void changedDeleteContextCanReusePhysicalIndexButNotLogicalStats() {
    ReconcileCapturePolicy policy =
        ReconcileCapturePolicy.of(
            List.of(new ReconcileCapturePolicy.Column("#1", true, true)),
            Set.of(
                ReconcileCapturePolicy.Output.FILE_STATS,
                ReconcileCapturePolicy.Output.PARQUET_PAGE_INDEX));
    ReconcileFileExecutionPlan prior =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector(
                "u", "s3://bucket/dv.bin", 4, 20, 3L),
            "PARQUET",
            0,
            List.of(),
            "data-v1");
    ReconcileFileExecutionPlan current =
        ReconcileFileExecutionPlan.of(
            PATH,
            123L,
            "{}",
            new ReconcileFileExecutionPlan.DeltaDeletionVector(
                "u", "s3://bucket/dv.bin", 4, 20, 4L),
            "PARQUET",
            0,
            List.of(),
            "data-v1");
    var priorBundle =
        statsBundle(prior, policy).toBuilder()
            .addIndexArtifacts(
                ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                    .setFilePath(PATH)
                    .setSourceFingerprint(FileArtifactReuse.indexSourceFingerprint(prior))
                    .setIndexCaptureSignature(FileArtifactReuse.indexCaptureSignature(policy, "{}"))
                    .addRealizedIndexSelectors("#1"))
            .build();

    ReconcileFileExecutionPlan enriched =
        RemoteFileArtifactReusePlanner.enrichFromBundles(
                "{}", List.of(current), policy, false, List.of(priorBundle))
            .getFirst();

    assertFalse(enriched.reusesFileStats());
    assertTrue(enriched.reusesIndexArtifact());
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference indexBundle(
      ReconcileFileExecutionPlan plan,
      ReconcileCapturePolicy policy,
      String executionSchemaJson,
      String... realizedSelectors) {
    return ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
        .setArtifact(
            ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                .setTargetStorageId("reuse-bundle:group-1")
                .setPayloadUri("s3://bucket/reuse-bundle.pb")
                .setPayloadBytes(1024)
                .setPayloadSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
        .addIndexArtifacts(
            ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                .setFilePath(PATH)
                .setSourceFingerprint(FileArtifactReuse.indexSourceFingerprint(plan))
                .setIndexCaptureSignature(
                    FileArtifactReuse.indexCaptureSignature(policy, executionSchemaJson))
                .addAllRealizedIndexSelectors(List.of(realizedSelectors)))
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata statsMetadata(
      String filePath, String sourceFingerprint, String statsCaptureSignature) {
    return ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
        .setFilePath(filePath)
        .setSourceFingerprint(sourceFingerprint)
        .setStatsCaptureSignature(statsCaptureSignature)
        .build();
  }

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference statsBundle(
      ReconcileFileExecutionPlan plan, ReconcileCapturePolicy policy) {
    return ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
        .setArtifact(
            ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                .setTargetStorageId("reuse-bundle:prior")
                .setPayloadUri("s3://bucket/prior-reuse-bundle.pb")
                .setPayloadBytes(100L)
                .setPayloadSha256(com.google.protobuf.ByteString.copyFrom(new byte[32])))
        .addFileStats(
            ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata.newBuilder()
                .setFilePath(PATH)
                .setSourceFingerprint(FileArtifactReuse.sourceFingerprint(plan, "{}"))
                .setStatsCaptureSignature(FileArtifactReuse.statsCaptureSignature(policy)))
        .build();
  }
}
