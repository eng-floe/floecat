/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.storage.spi.BlobStore;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link BlobStore} capability for schemas whose URIs are immutable content identities.
 *
 * <p>This adapter exists for lower-level readers that consume {@code BlobStore} directly (notably
 * the reusable-artifact index). It must never wrap mutable objects: URI-only cache identity is
 * deliberately stronger than the general blob-store contract.
 */
public final class CachedImmutableBlobStore implements BlobStore {
  private final BlobStore delegate;
  private final BlobCacheAccess cache;
  private final BlobCache.Fill fill;

  public CachedImmutableBlobStore(BlobStore delegate, BlobCacheAccess cache) {
    this(delegate, cache, BlobCache.Fill.FILL);
  }

  public CachedImmutableBlobStore(BlobStore delegate, BlobCacheAccess cache, BlobCache.Fill fill) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.cache = Objects.requireNonNull(cache, "cache");
    this.fill = Objects.requireNonNull(fill, "fill");
  }

  @Override
  public byte[] get(String uri) {
    return cache.immutableBytes(uri, fill, () -> delegate.get(uri)).orElse(null);
  }

  @Override
  public byte[] getRange(String uri, long offset, int length) {
    if (offset < 0L || length < 0) {
      throw new IllegalArgumentException("blob range is invalid");
    }
    Optional<BlobCache.Content> content =
        cache.immutable(uri, BlobCache.Fill.BYPASS_FILL, () -> null);
    if (content.isEmpty()) {
      return delegate.getRange(uri, offset, length);
    }
    try (BlobCache.Content body = content.orElseThrow()) {
      if (offset > body.size() || (long) length > body.size() - offset) {
        throw new IllegalArgumentException("blob range exceeds the object");
      }
      var bytes = body.buffer();
      bytes.position(Math.toIntExact(offset));
      bytes.limit(Math.toIntExact(offset + length));
      byte[] range = new byte[length];
      bytes.get(range);
      return range;
    }
  }

  @Override
  public Map<String, byte[]> getBatch(List<String> uris) {
    return cache.immutableBytes(
        new LinkedHashSet<>(uris).stream().toList(), fill, delegate::getBatch);
  }

  @Override
  public BlobStore.ScopedObjects getBatchScoped(List<String> uris) {
    BlobCacheAccess.Contents contents =
        cache.immutableContents(
            new LinkedHashSet<>(uris).stream().toList(), fill, delegate::getBatch);
    return new BlobStore.ScopedObjects() {
      @Override
      public ByteBuffer get(String uri) {
        return contents.get(uri).orElse(null);
      }

      @Override
      public void close() {
        contents.close();
      }
    };
  }

  @Override
  public void put(String uri, byte[] bytes, String contentType) {
    delegate.put(uri, bytes, contentType);
    cache.putImmutable(uri, bytes);
  }

  @Override
  public void putImmutable(String uri, byte[] bytes, String contentType) {
    delegate.putImmutable(uri, bytes, contentType);
    cache.putImmutable(uri, bytes);
  }

  @Override
  public Optional<BlobHeader> head(String uri) {
    return delegate.head(uri);
  }

  @Override
  public boolean delete(String uri) {
    return delegate.delete(uri);
  }

  @Override
  public boolean supportsVersionedDeletes() {
    return delegate.supportsVersionedDeletes();
  }

  @Override
  public boolean delete(String uri, String versionId) {
    return delegate.delete(uri, versionId);
  }

  @Override
  public int deletePrefix(String prefix) {
    return delegate.deletePrefix(prefix);
  }

  @Override
  public Page list(String prefix, int limit, String pageToken) {
    return delegate.list(prefix, limit, pageToken);
  }

  @Override
  public Page listPrefixes(String prefix, int limit, String pageToken) {
    return delegate.listPrefixes(prefix, limit, pageToken);
  }
}
