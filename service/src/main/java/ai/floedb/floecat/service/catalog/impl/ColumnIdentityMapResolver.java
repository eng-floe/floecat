/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityEntry;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityMapRef;
import ai.floedb.floecat.catalog.rpc.ColumnIdentityProjection;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.service.cache.ObjectCache;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/** Resolves immutable identity state through Floecat's established blob and object caches. */
@ApplicationScoped
public class ColumnIdentityMapResolver {
  private final BlobStore blobs;
  private final BlobCacheAccess blobCache;
  private final ObjectCache objects;

  public ColumnIdentityMapResolver(
      BlobStore blobs, BlobCacheAccess blobCache, ObjectCache objects) {
    this.blobs = Objects.requireNonNull(blobs, "blobs");
    this.blobCache = Objects.requireNonNull(blobCache, "blobCache");
    this.objects = Objects.requireNonNull(objects, "objects");
  }

  public void validateReference(String accountId, ColumnIdentityMapRef ref) {
    ColumnIdentityMaps.validateReference(ref);
    String prefix = "/accounts/" + Objects.requireNonNull(accountId, "accountId") + "/";
    if (!ref.getUri().startsWith(prefix)) {
      throw new IllegalArgumentException("Column identity map reference is outside its account");
    }
  }

  public ColumnIdentityMap resolve(String accountId, ColumnIdentityMapRef ref) {
    validateReference(accountId, ref);
    String identity = java.util.HexFormat.of().formatHex(ref.getPayloadSha256().toByteArray());
    ColumnIdentityMap map = objects.columnIdentityMap(accountId, identity, () -> load(ref));
    ColumnIdentityMaps.restore(map, ref);
    return map;
  }

  public ColumnIdentityProjection project(
      String accountId, ColumnIdentityMapRef ref, List<String> requiredColumns) {
    ColumnIdentityMap map = resolve(accountId, ref);
    Set<String> roots = new HashSet<>();
    for (String requested : requiredColumns) {
      if (requested != null && !requested.isBlank()) {
        roots.add(requested.trim().toLowerCase(Locale.ROOT));
      }
    }
    ColumnIdentityProjection.Builder projection =
        ColumnIdentityProjection.newBuilder()
            .setFingerprint(ref.getFingerprint())
            .setMode(ref.getMode());
    for (ColumnIdentityEntry entry : map.getEntriesList()) {
      if (roots.isEmpty() || belongsToRequestedRoot(entry, roots)) {
        projection.addEntries(entry);
      }
    }
    return projection.build();
  }

  private ColumnIdentityMap load(ColumnIdentityMapRef ref) {
    byte[] bytes =
        blobCache
            .immutableBytes(
                ref.getUri(),
                BlobCache.Fill.FILL,
                () ->
                    blobs.getRangeAtMost(
                        ref.getUri(), 0L, Math.toIntExact(ref.getPayloadBytes() + 1L)))
            .orElseThrow(() -> new IllegalArgumentException("Column identity map is unreadable"));
    if (bytes.length != ref.getPayloadBytes()
        || !MessageDigest.isEqual(sha256(bytes), ref.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException("Column identity map digest mismatch");
    }
    try {
      ColumnIdentityMap map = ColumnIdentityMap.parseFrom(bytes);
      ColumnIdentityMaps.restore(map, ref);
      return map;
    } catch (InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("Invalid column identity map payload", invalid);
    }
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException impossible) {
      throw new IllegalStateException("SHA-256 is unavailable", impossible);
    }
  }

  private static boolean belongsToRequestedRoot(
      ColumnIdentityEntry entry, Set<String> requiredRoots) {
    if (entry.getPathCount() == 0) {
      return false;
    }
    String root = entry.getPath(0).getName().toLowerCase(Locale.ROOT);
    return requiredRoots.contains(root);
  }
}
