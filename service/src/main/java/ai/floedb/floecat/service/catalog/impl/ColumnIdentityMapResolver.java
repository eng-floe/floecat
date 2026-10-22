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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.schema.identity.ColumnIdentityMaps;
import ai.floedb.floecat.service.cache.ObjectCache;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.HashSet;
import java.util.HexFormat;
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

  public void validateReference(ResourceId tableId, ColumnIdentityMapRef ref) {
    validateReferenceForTable(tableId, ref);
  }

  public static void validateReferenceForTable(ResourceId tableId, ColumnIdentityMapRef ref) {
    Objects.requireNonNull(tableId, "tableId");
    ColumnIdentityMaps.validateReference(ref);
    String digest = HexFormat.of().formatHex(ref.getPayloadSha256().toByteArray());
    String expectedUri =
        Keys.columnIdentityBlobUri(tableId.getAccountId(), tableId.getId(), digest);
    if (!ref.getUri().equals(expectedUri)) {
      throw new IllegalArgumentException(
          "Column identity map reference URI does not match its table and digest");
    }
  }

  public ColumnIdentityMap resolve(ResourceId tableId, ColumnIdentityMapRef ref) {
    validateReference(tableId, ref);
    String identity = HexFormat.of().formatHex(ref.getPayloadSha256().toByteArray());
    ColumnIdentityMap map =
        objects.columnIdentityMap(
            tableId.getAccountId(), identity, () -> parse(loadBytes(ref), ref));
    ColumnIdentityMaps.restore(map, ref);
    return map;
  }

  /** Returns the exact integrity-checked bytes named by the immutable reference. */
  public byte[] bytes(ResourceId tableId, ColumnIdentityMapRef ref) {
    validateReference(tableId, ref);
    byte[] bytes = loadBytes(ref);
    String identity = HexFormat.of().formatHex(ref.getPayloadSha256().toByteArray());
    ColumnIdentityMap map =
        objects.columnIdentityMap(tableId.getAccountId(), identity, () -> parse(bytes, ref));
    ColumnIdentityMaps.restore(map, ref);
    return bytes;
  }

  public ColumnIdentityProjection project(
      ResourceId tableId, ColumnIdentityMapRef ref, List<String> requiredColumns) {
    ColumnIdentityMap map = resolve(tableId, ref);
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

  private byte[] loadBytes(ColumnIdentityMapRef ref) {
    byte[] bytes =
        blobCache
            .immutableBytes(
                ref.getUri(),
                BlobCache.Fill.FILL,
                () ->
                    blobs.getRangeAtMost(
                        ref.getUri(), 0L, Math.toIntExact(ref.getPayloadBytes() + 1L)))
            .orElseThrow(() -> new IllegalArgumentException("Column identity map is unreadable"));
    ColumnIdentityMaps.validatePayload(bytes, ref);
    return bytes;
  }

  private static ColumnIdentityMap parse(byte[] bytes, ColumnIdentityMapRef ref) {
    try {
      ColumnIdentityMap map = ColumnIdentityMap.parseFrom(bytes);
      ColumnIdentityMaps.restore(map, ref);
      return map;
    } catch (InvalidProtocolBufferException invalid) {
      throw new IllegalArgumentException("Invalid column identity map payload", invalid);
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
