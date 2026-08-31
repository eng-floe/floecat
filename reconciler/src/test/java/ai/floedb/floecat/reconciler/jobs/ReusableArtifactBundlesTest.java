/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ReusableArtifactBundlesTest {
  @Test
  void acceptsCurrentFormatAndRejectsOtherVersions() throws Exception {
    byte[] current =
        ReusableArtifactBundlePayload.newBuilder().setFormatVersion(1).build().toByteArray();
    byte[] unsupported =
        ReusableArtifactBundlePayload.newBuilder().setFormatVersion(2).build().toByteArray();

    assertEquals(1, ReusableArtifactBundles.parse(current).getFormatVersion());
    assertThrows(IllegalArgumentException.class, () -> ReusableArtifactBundles.parse(unsupported));
  }

  @Test
  void mergesSelectedPathsWhenSeveralPlansReuseTheSameInheritedBundle() throws Exception {
    byte[] digest = MessageDigest.getInstance("SHA-256").digest("bundle".getBytes());
    String uri =
        "/accounts/acct/tables/table/target-stats/snapshots/1/generations/"
            + "full-rescan-prior/worker-uploads/group/lease/reuse-bundles/"
            + java.util.HexFormat.of().formatHex(digest)
            + ".pb";
    ReconcileFileGroupTask group =
        ReconcileFileGroupTask.of(
                "plan", "group", "table", 2L, List.of("s3://bucket/a", "s3://bucket/b"))
            .withFileExecutionPlans(
                List.of(
                    planWithIndexSelection("s3://bucket/a", uri, digest),
                    planWithIndexSelection("s3://bucket/b", uri, digest)));

    List<ReusableArtifactBundleSelection> selections =
        ReusableArtifactBundles.inheritedIndexArtifactBundleSelections(List.of(group));

    assertEquals(1, selections.size());
    assertEquals(List.of("s3://bucket/a", "s3://bucket/b"), selections.getFirst().indexFilePaths());
    assertEquals(1, ReusableArtifactBundles.inheritedIndexArtifactBundles(List.of(group)).size());
  }

  private static ReconcileFileExecutionPlan planWithIndexSelection(
      String filePath, String uri, byte[] digest) {
    return ReconcileFileExecutionPlan.of(filePath, 1L, "", null, "PARQUET", 0, List.of(), "content")
        .withReuseBundleSelections(
            "source",
            "index-source",
            "stats",
            "index",
            Map.of(),
            List.of(
                new ReusableArtifactBundleSelection(
                    "reuse-bundle:prior", uri, 6L, digest, List.of(), List.of(filePath))));
  }
}
