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
                    .setStatsCaptureSignature(FileArtifactReuse.statsCaptureSignature(policy)))
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

  private static ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference indexBundle(
      ReconcileFileExecutionPlan plan,
      ReconcileCapturePolicy policy,
      String executionSchemaJson,
      String realizedSelector) {
    return ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference.newBuilder()
        .setArtifact(
            ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor.newBuilder()
                .setPayloadUri("s3://bucket/reuse-bundle.pb"))
        .addIndexArtifacts(
            ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata.newBuilder()
                .setFilePath(PATH)
                .setSourceFingerprint(FileArtifactReuse.indexSourceFingerprint(plan))
                .setIndexCaptureSignature(
                    FileArtifactReuse.indexCaptureSignature(policy, executionSchemaJson))
                .addRealizedIndexSelectors(realizedSelector))
        .build();
  }
}
