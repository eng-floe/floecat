/*
 * Copyright 2026 Yellowbrick Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package ai.floedb.floecat.service.statistics.impl;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.CaffeineMemoryCache;
import ai.floedb.floecat.cache.MemoryCache;
import ai.floedb.floecat.reconciler.rpc.ExternalManifestCommitmentIndex;
import java.util.function.Supplier;

/** Process-local cache of fully validated, content-addressed external-manifest indexes. */
public final class ExternalManifestCommitmentCache {
  private final MemoryCache<ExternalManifestCommitments.CacheKey, ExternalManifestCommitmentIndex>
      entries;
  private final boolean enabled;

  public ExternalManifestCommitmentCache(long maxBytes, CacheEvents events, boolean enabled) {
    entries =
        new CaffeineMemoryCache<>(
            CacheFamily.MANIFEST_COMMITMENT,
            maxBytes,
            ExternalManifestCommitmentCache::estimatedKeyBytes,
            events);
    this.enabled = enabled;
  }

  public static ExternalManifestCommitmentCache forTesting() {
    return new ExternalManifestCommitmentCache(64L * 1024L * 1024L, CacheEvents.none(), true);
  }

  ExternalManifestCommitmentIndex get(
      ExternalManifestCommitments.CacheKey key, Supplier<ExternalManifestCommitmentIndex> loader) {
    return enabled ? entries.get(key, ignored -> loader.get()) : loader.get();
  }

  public CacheFamily family() {
    return entries.family();
  }

  public long entryCount() {
    return entries.entryCount();
  }

  public long bytes() {
    return entries.bytes();
  }

  public boolean enabled() {
    return enabled;
  }

  private static long estimatedKeyBytes(ExternalManifestCommitments.CacheKey key) {
    return 256L
        + 3L * key.reference().getSerializedSize()
        + 2L * (key.accountId().length() + key.tableId().length());
  }
}
