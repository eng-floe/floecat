/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import org.junit.jupiter.api.Test;

class AppendOnlySnapshotBaseLoaderTest {
  @Test
  void rejectsSameSnapshotBeforeReadingItsManifest() {
    BlobStore blobStore = mock(BlobStore.class);
    var loader = new AppendOnlySnapshotBaseLoader(blobStore);

    assertThrows(IllegalArgumentException.class, () -> loader.load(null, input(10L), base(10L)));

    verify(blobStore, never()).get("/base-manifest.pb");
  }

  @Test
  void reportsMissingManifestAsStorageNotFound() {
    BlobStore blobStore = mock(BlobStore.class);
    when(blobStore.get("/base-manifest.pb")).thenReturn(null);
    var loader = new AppendOnlySnapshotBaseLoader(blobStore);

    assertThrows(StorageNotFoundException.class, () -> loader.load(null, input(10L), base(9L)));
  }

  private static StandaloneSnapshotFinalizeExecutionPayload input(long snapshotId) {
    return new StandaloneSnapshotFinalizeExecutionPayload(
        "job",
        "lease",
        "parent",
        ResourceId.newBuilder().setAccountId("acct").setId("table").build(),
        snapshotId,
        true,
        1,
        "/plan.json",
        0,
        "/stats/",
        "/manifests/",
        "/index/",
        "/stats-generation.pb",
        "/index-manifests/",
        null);
  }

  private static SnapshotPlanBlobStore.AppendOnlyBase base(long snapshotId) {
    return new SnapshotPlanBlobStore.AppendOnlyBase(
        snapshotId,
        "/base-manifest.pb",
        1,
        "00".repeat(32),
        1,
        0,
        0,
        0,
        "full-rescan-parent",
        "",
        ReusableArtifactIndexStore.emptyReference());
  }
}
