/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.floedb.floecat.reconciler.rpc.AppendOnlySnapshotBase;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import org.junit.jupiter.api.Test;

class ReusableArtifactManifestTest {
  @Test
  void fullCaptureHasZeroChainDepth() {
    assertEquals(
        0,
        ReusableArtifactManifest.chainDepth(
            SnapshotCaptureManifest.newBuilder()
                .setFormatVersion(ReusableArtifactManifest.FORMAT_VERSION)
                .build()));
  }

  @Test
  void appendCaptureIncrementsAuthenticatedBaseDepth() {
    assertEquals(
        5,
        ReusableArtifactManifest.chainDepth(
            SnapshotCaptureManifest.newBuilder()
                .setFormatVersion(ReusableArtifactManifest.FORMAT_VERSION)
                .setAppendOnlyBase(AppendOnlySnapshotBase.newBuilder().setChainDepth(4))
                .build()));
  }
}
