/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.reconciler.impl;

/** Snapshot summary keys pointing at the immutable, finalized artifact-reuse manifest. */
public final class SnapshotReuseManifestMetadata {
  public static final String URI = "floedb.reconcile.reuse-manifest-uri";
  public static final String BYTES = "floedb.reconcile.reuse-manifest-bytes";
  public static final String SHA256 = "floedb.reconcile.reuse-manifest-sha256";

  private SnapshotReuseManifestMetadata() {}
}
