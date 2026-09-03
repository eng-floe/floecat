/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.cache.BlobCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.impl.ReusableArtifactIndexStore;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.service.repo.cache.BlobCacheAccess;
import ai.floedb.floecat.service.repo.cache.CachedImmutableBlobStore;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * Structurally shared file-artifact map for a published stats/index generation.
 *
 * <p>The only mutable record is a pointer-store binding from generation to its immutable capture
 * manifest. The manifest embeds the complete reusable-artifact run reference. Runs and their
 * size-bounded blocks are shared with predecessor snapshots; no inherited per-target pointers or
 * additional small S3 nodes are written.
 */
final class GenerationArtifactMap {
  private static final Logger LOG = Logger.getLogger(GenerationArtifactMap.class);
  private final PointerStore pointerStore;
  private final PointerStore pointerReads;
  private final BlobStore blobStore;
  private final BlobCacheAccess blobCache;
  private final ReusableArtifactIndexStore indexStore;
  private final ReusableArtifactIndexStore listingIndexStore;

  GenerationArtifactMap(PointerStore pointerStore, BlobStore blobStore, BlobCacheAccess blobCache) {
    this(pointerStore, pointerStore, blobStore, blobCache);
  }

  GenerationArtifactMap(
      PointerStore pointerStore,
      PointerStore pointerReads,
      BlobStore blobStore,
      BlobCacheAccess blobCache) {
    this.pointerStore = pointerStore;
    this.pointerReads = pointerReads;
    this.blobStore = blobStore;
    this.blobCache = java.util.Objects.requireNonNull(blobCache, "blobCache");
    this.indexStore =
        new ReusableArtifactIndexStore(new CachedImmutableBlobStore(blobStore, blobCache));
    this.listingIndexStore =
        new ReusableArtifactIndexStore(
            new CachedImmutableBlobStore(blobStore, blobCache, BlobCache.Fill.BYPASS_FILL));
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
    if (!AccountDeletionFence.compareAndSet(pointerStore, tableId.getAccountId(), key, 0L, next)) {
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
    return manifest(tableId, snapshotId, generationId, BlobCache.Fill.FILL)
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
    return listingManifest(tableId, snapshotId, generationId)
        .map(value -> page(value, kind, limit, pageToken))
        .orElseGet(() -> new ReusableArtifactIndexStore.EntryPage(List.of(), ""));
  }

  Optional<SnapshotCaptureManifest> listingManifest(
      ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId, BlobCache.Fill.BYPASS_FILL);
  }

  ReusableArtifactIndexStore.EntryPage page(
      SnapshotCaptureManifest manifest,
      ReusableArtifactIndexStore.EntryKind kind,
      int limit,
      String pageToken) {
    return listingIndexStore.page(
        manifest.getReusableArtifactIndex(), kind, Math.max(1, limit), pageToken);
  }

  int countStats(ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId, BlobCache.Fill.FILL)
        .map(value -> value.getReusableArtifactIndex().getFileStatsRecordCount())
        .orElse(0);
  }

  int countIndexes(ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId, BlobCache.Fill.FILL)
        .map(value -> value.getReusableArtifactIndex().getIndexArtifactCount())
        .orElse(0);
  }

  Optional<SnapshotCaptureManifest> manifest(
      ResourceId tableId, long snapshotId, String generationId) {
    return manifest(tableId, snapshotId, generationId, BlobCache.Fill.FILL);
  }

  private Optional<SnapshotCaptureManifest> manifest(
      ResourceId tableId, long snapshotId, String generationId, BlobCache.Fill fill) {
    Pointer pointer = pointerReads.get(key(tableId, snapshotId, generationId)).orElse(null);
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
        parseManifest(
            pointer.getBlobUri(),
            blobCache.immutable(
                pointer.getBlobUri(),
                fill,
                () -> loadManifestBytes(pointer.getBlobUri(), expectedBytes, expectedDigest)),
            expectedBytes,
            expectedDigest);
    if (loaded.isEmpty()) {
      throw new BaseResourceRepository.CorruptionException(
          "generation artifact map manifest is missing: " + pointer.getBlobUri());
    }
    SnapshotCaptureManifest manifest = loaded.orElseThrow();
    if (manifest.getReusableArtifactIndex().getFormatVersion()
        != ReusableArtifactIndexStore.FORMAT_VERSION) {
      // An index written against an older contract is not readable, but it is not corruption:
      // report it as absent so reads fall back to the per-target pointers and the next capture
      // re-captures in full. Failing here would break reads on generations already committed.
      LOG.infof(
          "Generation artifact map index is not current; ignoring it tableId=%s snapshotId=%d"
              + " generationId=%s formatVersion=%d",
          tableId.getId(),
          snapshotId,
          generationId,
          manifest.getReusableArtifactIndex().getFormatVersion());
      return Optional.empty();
    }
    validateManifest(manifest, tableId, snapshotId);
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

  private byte[] loadManifestBytes(String uri, long expectedBytes, byte[] expectedDigest) {
    try {
      byte[] bytes = blobStore.get(uri);
      if (bytes == null) {
        return null;
      }
      if (bytes.length != expectedBytes || !MessageDigest.isEqual(expectedDigest, sha256(bytes))) {
        throw new BaseResourceRepository.CorruptionException(
            "generation artifact map manifest metadata mismatch: " + uri);
      }
      return bytes;
    } catch (StorageNotFoundException e) {
      return null;
    }
  }

  private static Optional<SnapshotCaptureManifest> parseManifest(
      String uri, Optional<BlobCache.Content> content, long expectedBytes, byte[] expectedDigest) {
    if (content.isEmpty()) {
      return Optional.empty();
    }
    try (BlobCache.Content body = content.orElseThrow()) {
      ByteBuffer bytes = body.buffer();
      if (body.size() != expectedBytes
          || !MessageDigest.isEqual(expectedDigest, sha256(bytes.duplicate()))) {
        throw new BaseResourceRepository.CorruptionException(
            "generation artifact map manifest metadata mismatch: " + uri);
      }
      return parseManifest(uri, bytes);
    }
  }

  private static Optional<SnapshotCaptureManifest> parseManifest(String uri, ByteBuffer bytes) {
    try {
      return Optional.of(SnapshotCaptureManifest.parseFrom(bytes));
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

  private static byte[] sha256(ByteBuffer bytes) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      digest.update(bytes);
      return digest.digest();
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }

  private static String key(ResourceId tableId, long snapshotId, String generationId) {
    return Keys.snapshotGenerationArtifactMapPointer(
        tableId.getAccountId(), tableId.getId(), snapshotId, generationId);
  }
}
