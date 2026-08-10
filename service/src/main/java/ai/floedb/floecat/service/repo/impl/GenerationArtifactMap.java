/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.ImmutableBlobCache;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Structurally shared file-artifact map for a published stats/index generation.
 *
 * <p>The only mutable record is a pointer-store binding from generation to its immutable capture
 * manifest. The manifest embeds the complete reusable-artifact run reference. Runs and their
 * size-bounded blocks are shared with predecessor snapshots; no inherited per-target pointers or
 * additional small S3 nodes are written.
 */
final class GenerationArtifactMap {
  private final PointerStore pointerStore;
  private final BlobStore blobStore;
  private final ImmutableBlobCache blobCache;
  private final ReusableArtifactIndexStore indexStore;

  GenerationArtifactMap(
      PointerStore pointerStore, BlobStore blobStore, ImmutableBlobCache blobCache) {
    this.pointerStore = pointerStore;
    this.blobStore = blobStore;
    this.blobCache = blobCache;
    this.indexStore = new ReusableArtifactIndexStore(blobStore);
  }

  void register(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      String captureManifestUri,
      long captureManifestBytes) {
    if (generationId == null
        || generationId.isBlank()
        || captureManifestUri == null
        || captureManifestUri.isBlank()
        || captureManifestBytes <= 0L) {
      throw new IllegalArgumentException("generation artifact map identity is invalid");
    }
    captureManifestDigest(captureManifestUri);
    String key = key(tableId, snapshotId, generationId);
    Pointer next = PointerReferences.blobPointer(key, captureManifestUri, 1L, captureManifestBytes);
    Pointer existing = pointerStore.get(key).orElse(null);
    if (existing != null) {
      if (existing.getBlobUri().equals(captureManifestUri)
          && existing.getReferencedObjectSizeBytes() == captureManifestBytes) {
        return;
      }
      throw new IllegalStateException("generation artifact map already differs: " + key);
    }
    if (!pointerStore.compareAndSet(key, 0L, next)) {
      existing = pointerStore.get(key).orElse(null);
      if (existing == null
          || !existing.getBlobUri().equals(captureManifestUri)
          || existing.getReferencedObjectSizeBytes() != captureManifestBytes) {
        throw new BaseResourceRepository.AbortRetryableException(
            "generation artifact map publication conflicted: " + key);
      }
    }
  }

  Optional<ReusableArtifactIndexEntry> lookupStats(
      ResourceId tableId, long snapshotId, String generationId, String filePath) {
    return lookup(tableId, snapshotId, generationId, List.of(filePath), List.of()).values().stream()
        .findFirst();
  }

  Map<String, ReusableArtifactIndexEntry> lookupStats(
      ResourceId tableId, long snapshotId, String generationId, Collection<String> filePaths) {
    if (filePaths == null || filePaths.isEmpty()) {
      return Map.of();
    }
    Map<String, ReusableArtifactIndexEntry> found =
        lookup(tableId, snapshotId, generationId, filePaths, List.of());
    Map<String, ReusableArtifactIndexEntry> byPath = new java.util.LinkedHashMap<>();
    found.values().forEach(entry -> byPath.put(entry.getFileStats().getFilePath(), entry));
    return Map.copyOf(byPath);
  }

  Optional<ReusableArtifactIndexEntry> lookupIndex(
      ResourceId tableId, long snapshotId, String generationId, String filePath) {
    return lookup(tableId, snapshotId, generationId, List.of(), List.of(filePath)).values().stream()
        .findFirst();
  }

  Map<String, ReusableArtifactIndexEntry> lookup(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      Collection<String> statsPaths,
      Collection<String> indexPaths) {
    return manifest(tableId, snapshotId, generationId)
        .map(value -> indexStore.lookup(value.getReusableArtifactIndex(), statsPaths, indexPaths))
        .orElse(Map.of());
  }

  ReusableArtifactIndexStore.EntryPage page(
      ResourceId tableId,
      long snapshotId,
      String generationId,
      ReusableArtifactIndexStore.EntryKind kind,
      int limit,
      String pageToken) {
    return manifest(tableId, snapshotId, generationId)
        .map(
            value ->
                indexStore.page(
                    value.getReusableArtifactIndex(), kind, Math.max(1, limit), pageToken))
        .orElseGet(() -> new ReusableArtifactIndexStore.EntryPage(List.of(), ""));
  }

  int countStats(ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId)
        .map(value -> value.getReusableArtifactIndex().getFileStatsRecordCount())
        .orElse(0);
  }

  int countIndexes(ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId)
        .map(value -> value.getReusableArtifactIndex().getIndexArtifactCount())
        .orElse(0);
  }

  Optional<SnapshotCaptureManifest> manifest(
      ResourceId tableId, long snapshotId, String generationId) {
    Pointer pointer = pointerStore.get(key(tableId, snapshotId, generationId)).orElse(null);
    if (pointer == null) {
      return Optional.empty();
    }
    if (pointer.getBlobUri().isBlank()) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map pointer has no manifest URI");
    }
    if (!pointer.hasReferencedObjectSizeBytes() || pointer.getReferencedObjectSizeBytes() <= 0L) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map pointer has no manifest size");
    }
    byte[] expectedDigest;
    try {
      expectedDigest = captureManifestDigest(pointer.getBlobUri());
    } catch (IllegalArgumentException error) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map pointer has no manifest digest", error);
    }
    long expectedBytes = pointer.getReferencedObjectSizeBytes();
    Optional<SnapshotCaptureManifest> loaded =
        blobCache != null && blobCache.enabled()
            ? blobCache.get(
                pointer.getBlobUri(), uri -> loadManifest(uri, expectedBytes, expectedDigest))
            : loadManifest(pointer.getBlobUri(), expectedBytes, expectedDigest);
    if (loaded.isEmpty()) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map manifest is missing: " + pointer.getBlobUri());
    }
    loaded.ifPresent(value -> validateManifest(value, tableId, snapshotId));
    return loaded;
  }

  private static void validateManifest(
      SnapshotCaptureManifest manifest, ResourceId tableId, long snapshotId) {
    if (!tableId.getAccountId().equals(manifest.getAccountId())
        || !tableId.getId().equals(manifest.getTableId())
        || manifest.getSnapshotId() != snapshotId
        || !manifest.hasReusableArtifactIndex()) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map manifest identity is invalid");
    }
    ReusableArtifactIndexStore.validateReference(manifest.getReusableArtifactIndex());
  }

  private Optional<SnapshotCaptureManifest> loadManifest(
      String uri, long expectedBytes, byte[] expectedDigest) {
    try {
      byte[] bytes = blobStore.get(uri);
      if (bytes == null) {
        return Optional.empty();
      }
      if (bytes.length != expectedBytes || !MessageDigest.isEqual(expectedDigest, sha256(bytes))) {
        throw new BaseResourceRepository.CorruptionException(
            "generation artifact map manifest metadata mismatch: " + uri);
      }
      return Optional.of(SnapshotCaptureManifest.parseFrom(bytes));
    } catch (StorageNotFoundException e) {
      return Optional.empty();
    } catch (InvalidProtocolBufferException e) {
      throw new BaseResourceRepository.CorruptionException(
          "invalid generation artifact map manifest: " + uri, e);
    }
  }

  private static byte[] captureManifestDigest(String uri) {
    String value = uri == null ? "" : uri.trim();
    int slash = value.lastIndexOf('/');
    String name = value.substring(slash + 1);
    if (!name.endsWith(".pb") || name.length() != 67) {
      throw new IllegalArgumentException("capture manifest URI is not content-addressed");
    }
    try {
      byte[] digest = HexFormat.of().parseHex(name.substring(0, 64));
      if (digest.length != 32) {
        throw new IllegalArgumentException("capture manifest URI digest is invalid");
      }
      return digest;
    } catch (IllegalArgumentException error) {
      throw new IllegalArgumentException("capture manifest URI digest is invalid", error);
    }
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }

  private static String key(ResourceId tableId, long snapshotId, String generationId) {
    return Keys.snapshotGenerationArtifactMapPointer(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
  }
}
