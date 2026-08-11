/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexObjectReference;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;

/** Process-wide authenticated LRU for immutable reusable-index objects. */
final class ReusableArtifactIndexObjectCache {
  static final int MAX_SINGLE_OBJECT_BYTES = 16 * 1024 * 1024;
  private static final long DEFAULT_MAX_BYTES = 64L * 1024L * 1024L;

  private final long maxBytes;
  private final LinkedHashMap<String, CachedObject> objects = new LinkedHashMap<>(256, 0.75f, true);
  private long bytes;

  ReusableArtifactIndexObjectCache() {
    this.maxBytes = configuredMaxBytes();
  }

  long maxBytes() {
    return maxBytes;
  }

  synchronized void clear() {
    objects.clear();
    bytes = 0L;
  }

  synchronized void put(String uri, byte[] value) {
    if (value == null || value.length > maxBytes || value.length > MAX_SINGLE_OBJECT_BYTES) {
      return;
    }
    CachedObject replaced = objects.remove(uri);
    if (replaced != null) {
      bytes -= replaced.bytes().length;
    }
    var iterator = objects.entrySet().iterator();
    while (bytes > maxBytes - value.length && iterator.hasNext()) {
      bytes -= iterator.next().getValue().bytes().length;
      iterator.remove();
    }
    objects.put(uri, new CachedObject(value.clone(), sha256(value)));
    bytes += value.length;
  }

  synchronized byte[] get(ReusableArtifactIndexObjectReference reference) {
    CachedObject cached = objects.get(reference.getUri());
    if (cached == null) {
      return null;
    }
    if (cached.bytes().length != reference.getPayloadBytes()
        || !MessageDigest.isEqual(cached.sha256(), reference.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException("reusable artifact index object metadata mismatch");
    }
    return cached.bytes();
  }

  synchronized boolean contains(String uri) {
    return objects.containsKey(uri);
  }

  private static long configuredMaxBytes() {
    String configured =
        System.getProperty("floecat.reusable-artifact-index.cache.max-weight-bytes");
    if (configured == null || configured.isBlank()) {
      configured = System.getenv("FLOECAT_REUSABLE_ARTIFACT_INDEX_CACHE_MAX_WEIGHT_BYTES");
    }
    if (configured == null || configured.isBlank()) {
      return DEFAULT_MAX_BYTES;
    }
    try {
      long parsed = Long.parseLong(configured);
      return parsed > 0L ? parsed : DEFAULT_MAX_BYTES;
    } catch (NumberFormatException ignored) {
      return DEFAULT_MAX_BYTES;
    }
  }

  private static byte[] sha256(byte[] value) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(value);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }

  private record CachedObject(byte[] bytes, byte[] sha256) {}
}
