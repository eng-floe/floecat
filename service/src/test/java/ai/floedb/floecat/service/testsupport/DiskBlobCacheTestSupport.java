/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.cache.BlobCacheEvents;
import ai.floedb.floecat.cache.DiskBlobCache;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import java.nio.file.Path;
import java.time.Duration;

/** Production-shaped disk-cache wiring for repository tests. */
public final class DiskBlobCacheTestSupport {
  private DiskBlobCacheTestSupport() {}

  public static BlobCacheAccess create(Path directory) {
    return new BlobCacheAccess(
        new DiskBlobCache(
            directory, 64L * 1024L * 1024L, 256 * 1024, Duration.ZERO, BlobCacheEvents.none()));
  }
}
