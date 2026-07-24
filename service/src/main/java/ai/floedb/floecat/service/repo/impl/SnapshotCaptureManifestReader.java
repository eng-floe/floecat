/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.BlobRef;
import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.catalog.rpc.IndexTarget;
import ai.floedb.floecat.catalog.rpc.SnapshotManifestEntry;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.reconciler.rpc.FileGroupResultDescriptor;
import ai.floedb.floecat.reconciler.rpc.FileGroupStatsPayload;
import ai.floedb.floecat.reconciler.rpc.SnapshotCaptureManifest;
import ai.floedb.floecat.reconciler.rpc.SnapshotFinalizeStatsDescriptor;
import ai.floedb.floecat.reconciler.rpc.SnapshotFinalizeStatsPayload;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Lazy, hash-verifying reader for immutable snapshot capture manifests and their stats packs. */
@ApplicationScoped
public class SnapshotCaptureManifestReader {
  private final TableRootRepository roots;
  private final BlobStore blobs;
  private final Map<String, CachedManifest> manifests = new ConcurrentHashMap<>();
  private final Map<String, List<TargetStatsRecord>> statsPacks = new ConcurrentHashMap<>();
  private final Map<String, TargetStatsRecord> statsRecords = new ConcurrentHashMap<>();

  @Inject
  public SnapshotCaptureManifestReader(TableRootRepository roots, BlobStore blobs) {
    this.roots = roots;
    this.blobs = blobs;
  }

  public Optional<String> activeManifestUri(ResourceId tableId, long snapshotId) {
    return manifestRef(tableId, snapshotId).map(BlobRef::getUri);
  }

  public Optional<TargetStatsRecord> getStats(
      ResourceId tableId, long snapshotId, StatsTarget target) {
    return manifestRef(tableId, snapshotId)
        .flatMap(
            ref ->
                getStats(
                    loadManifest(ref.getUri(), ref.getVersion()), tableId, snapshotId, target));
  }

  public Optional<TargetStatsRecord> getStats(
      String manifestUri, ResourceId tableId, long snapshotId, StatsTarget target) {
    return getStats(loadManifest(manifestUri, null), tableId, snapshotId, target);
  }

  private Optional<TargetStatsRecord> getStats(
      SnapshotCaptureManifest manifest, ResourceId tableId, long snapshotId, StatsTarget target) {
    requireManifestIdentity(manifest, tableId, snapshotId);
    String storageId = StatsTargetIdentity.storageId(target);
    SnapshotCaptureManifest.StatsRecordLocation location = null;
    for (var candidate : manifest.getStatsIndexList()) {
      if (storageId.equals(candidate.getTargetStorageId())) {
        location = candidate;
      }
    }
    if (location == null) {
      return Optional.empty();
    }
    TargetStatsRecord record = loadStatsRecord(manifest, location);
    if (!tableId.equals(record.getTableId())
        || snapshotId != record.getSnapshotId()
        || !storageId.equals(StatsTargetIdentity.storageId(record.getTarget()))) {
      throw corruption("capture manifest stats index points at a different target");
    }
    return Optional.of(record);
  }

  private TargetStatsRecord loadStatsRecord(
      SnapshotCaptureManifest manifest, SnapshotCaptureManifest.StatsRecordLocation location) {
    boolean hasRange = location.getByteLength() > 0 || !location.getRecordSha256().isEmpty();
    if (!hasRange) {
      List<TargetStatsRecord> pack = loadStatsPack(manifest, location);
      if (location.getRecordIndex() >= pack.size()) {
        throw corruption("capture manifest stats record index is out of bounds");
      }
      return pack.get(location.getRecordIndex());
    }
    if (location.getByteLength() <= 0 || location.getRecordSha256().size() != 32) {
      throw corruption("capture manifest stats range metadata is incomplete");
    }
    long declaredPayloadBytes = declaredPayloadBytes(manifest, location);
    long rangeEnd = location.getByteOffset() + location.getByteLength();
    if (rangeEnd < location.getByteOffset() || rangeEnd > declaredPayloadBytes) {
      throw corruption("capture manifest stats range is outside its declared payload");
    }
    String cacheKey =
        location.getPayloadUri() + ":" + location.getByteOffset() + ":" + location.getByteLength();
    TargetStatsRecord cached = statsRecords.get(cacheKey);
    if (cached != null) {
      return cached;
    }
    byte[] recordBytes =
        blobs.getRange(
            location.getPayloadUri(), location.getByteOffset(), location.getByteLength());
    if (recordBytes == null
        || recordBytes.length != location.getByteLength()
        || !MessageDigest.isEqual(sha256(recordBytes), location.getRecordSha256().toByteArray())) {
      throw corruption("capture manifest stats range does not match its checksum");
    }
    try {
      TargetStatsRecord decoded = TargetStatsRecord.parseFrom(recordBytes);
      TargetStatsRecord winner = statsRecords.putIfAbsent(cacheKey, decoded);
      return winner == null ? decoded : winner;
    } catch (InvalidProtocolBufferException e) {
      throw corruption("capture manifest stats range is not a valid record", e);
    }
  }

  private static long declaredPayloadBytes(
      SnapshotCaptureManifest manifest, SnapshotCaptureManifest.StatsRecordLocation location) {
    if (location.getFinalAggregate()) {
      SnapshotFinalizeStatsDescriptor descriptor = manifest.getFinalStats();
      if (!descriptor.getPayloadUri().equals(location.getPayloadUri())) {
        throw corruption("final stats range URI is not declared by the capture manifest");
      }
      return descriptor.getPayloadBytes();
    }
    return manifest.getFileGroupsList().stream()
        .filter(descriptor -> descriptor.getStatsPayloadUri().equals(location.getPayloadUri()))
        .findFirst()
        .map(FileGroupResultDescriptor::getStatsPayloadBytes)
        .orElseThrow(() -> corruption("stats range URI is not declared by the capture manifest"));
  }

  public List<TargetStatsRecord> listStats(
      String manifestUri, ResourceId tableId, long snapshotId) {
    return listStats(loadManifest(manifestUri, null), tableId, snapshotId);
  }

  public List<TargetStatsRecord> listStats(ResourceId tableId, long snapshotId) {
    return manifest(tableId, snapshotId)
        .map(manifest -> listStats(manifest, tableId, snapshotId))
        .orElseGet(List::of);
  }

  private List<TargetStatsRecord> listStats(
      SnapshotCaptureManifest manifest, ResourceId tableId, long snapshotId) {
    requireManifestIdentity(manifest, tableId, snapshotId);
    Map<String, TargetStatsRecord> records = new LinkedHashMap<>();
    for (var location : manifest.getStatsIndexList()) {
      List<TargetStatsRecord> pack = loadStatsPack(manifest, location);
      if (location.getRecordIndex() >= pack.size()) {
        throw corruption("capture manifest stats record index is out of bounds");
      }
      TargetStatsRecord record = pack.get(location.getRecordIndex());
      if (!tableId.equals(record.getTableId())
          || snapshotId != record.getSnapshotId()
          || !location
              .getTargetStorageId()
              .equals(StatsTargetIdentity.storageId(record.getTarget()))) {
        throw corruption("capture manifest stats index points at a different target");
      }
      records.put(location.getTargetStorageId(), record);
    }
    return records.values().stream()
        .sorted(Comparator.comparing(r -> StatsTargetIdentity.storageId(r.getTarget())))
        .toList();
  }

  public Optional<IndexArtifactRecord> getIndexArtifact(
      ResourceId tableId, long snapshotId, IndexTarget target) {
    return manifest(tableId, snapshotId)
        .flatMap(
            manifest ->
                manifest.getIndexArtifactsList().stream()
                    .filter(record -> record.getTarget().equals(target))
                    .findFirst());
  }

  public List<IndexArtifactRecord> listIndexArtifacts(ResourceId tableId, long snapshotId) {
    return manifest(tableId, snapshotId)
        .map(SnapshotCaptureManifest::getIndexArtifactsList)
        .map(List::copyOf)
        .orElseGet(List::of);
  }

  private Optional<SnapshotCaptureManifest> manifest(ResourceId tableId, long snapshotId) {
    return manifestRef(tableId, snapshotId)
        .map(ref -> loadManifest(ref.getUri(), ref.getVersion()))
        .map(
            manifest -> {
              requireManifestIdentity(manifest, tableId, snapshotId);
              return manifest;
            });
  }

  private Optional<BlobRef> manifestRef(ResourceId tableId, long snapshotId) {
    return roots
        .get(tableId)
        .flatMap(
            root ->
                SnapshotManifests.findEntry(
                    roots,
                    root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null,
                    snapshotId))
        .filter(SnapshotManifestEntry::hasStatsGenerationRef)
        .map(SnapshotManifestEntry::getStatsGenerationRef)
        .filter(ref -> ref.getUri().endsWith(".capture-manifest.pb"));
  }

  private SnapshotCaptureManifest loadManifest(String uri, String expectedHexSha256) {
    CachedManifest cached = manifests.get(uri);
    if (cached != null) {
      requireExpectedManifestHash(expectedHexSha256, cached.hexSha256());
      return cached.manifest();
    }
    byte[] bytes = blobs.get(uri);
    String actualHexSha256 = HexFormat.of().formatHex(sha256(bytes));
    requireExpectedManifestHash(expectedHexSha256, actualHexSha256);
    try {
      SnapshotCaptureManifest decoded = SnapshotCaptureManifest.parseFrom(bytes);
      if (decoded.getFormatVersion() != 1) {
        throw corruption("unsupported snapshot capture manifest format");
      }
      CachedManifest cachedManifest = new CachedManifest(decoded, actualHexSha256);
      CachedManifest winner = manifests.putIfAbsent(uri, cachedManifest);
      return winner == null ? decoded : winner.manifest();
    } catch (InvalidProtocolBufferException e) {
      throw corruption("snapshot capture manifest is not valid protobuf", e);
    }
  }

  private static void requireExpectedManifestHash(
      String expectedHexSha256, String actualHexSha256) {
    if (expectedHexSha256 != null
        && !expectedHexSha256.isBlank()
        && !MessageDigest.isEqual(
            expectedHexSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII),
            actualHexSha256.getBytes(java.nio.charset.StandardCharsets.US_ASCII))) {
      throw corruption("snapshot capture manifest hash mismatch");
    }
  }

  private List<TargetStatsRecord> loadStatsPack(
      SnapshotCaptureManifest manifest, SnapshotCaptureManifest.StatsRecordLocation location) {
    List<TargetStatsRecord> cached = statsPacks.get(location.getPayloadUri());
    if (cached != null) {
      return cached;
    }
    List<TargetStatsRecord> decoded;
    if (location.getFinalAggregate()) {
      SnapshotFinalizeStatsDescriptor descriptor = manifest.getFinalStats();
      byte[] bytes =
          verifiedBytes(
              descriptor.getPayloadUri(),
              descriptor.getPayloadBytes(),
              descriptor.getPayloadSha256());
      try {
        SnapshotFinalizeStatsPayload payload = SnapshotFinalizeStatsPayload.parseFrom(bytes);
        if (payload.getStatsRecordsCount() != descriptor.getStatsRecordCount()) {
          throw corruption("final stats pack record count mismatch");
        }
        decoded = List.copyOf(payload.getStatsRecordsList());
      } catch (InvalidProtocolBufferException e) {
        throw corruption("final stats pack is not valid protobuf", e);
      }
    } else {
      FileGroupResultDescriptor descriptor =
          manifest.getFileGroupsList().stream()
              .filter(d -> d.getStatsPayloadUri().equals(location.getPayloadUri()))
              .findFirst()
              .orElseThrow(() -> corruption("stats pack is not declared by the capture manifest"));
      byte[] bytes =
          verifiedBytes(
              descriptor.getStatsPayloadUri(),
              descriptor.getStatsPayloadBytes(),
              descriptor.getStatsPayloadSha256());
      try {
        FileGroupStatsPayload payload = FileGroupStatsPayload.parseFrom(bytes);
        if (payload.getFileStatsCount() != descriptor.getFileStatsRecordCount()) {
          throw corruption("file-group stats pack record count mismatch");
        }
        decoded = List.copyOf(payload.getFileStatsList());
      } catch (InvalidProtocolBufferException e) {
        throw corruption("file-group stats pack is not valid protobuf", e);
      }
    }
    List<TargetStatsRecord> winner = statsPacks.putIfAbsent(location.getPayloadUri(), decoded);
    return winner == null ? decoded : winner;
  }

  private byte[] verifiedBytes(String uri, long expectedSize, ByteString expectedSha256) {
    byte[] bytes = blobs.get(uri);
    if (bytes.length != expectedSize
        || expectedSha256.size() != 32
        || !MessageDigest.isEqual(sha256(bytes), expectedSha256.toByteArray())) {
      throw corruption("capture artifact does not match its descriptor: " + uri);
    }
    return bytes;
  }

  private static void requireManifestIdentity(
      SnapshotCaptureManifest manifest, ResourceId tableId, long snapshotId) {
    if (manifest.getFormatVersion() != 1
        || !tableId.getAccountId().equals(manifest.getAccountId())
        || !tableId.getId().equals(manifest.getTableId())
        || snapshotId != manifest.getSnapshotId()) {
      throw corruption("snapshot capture manifest identity mismatch");
    }
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  private static BaseResourceRepository.CorruptionException corruption(String message) {
    return new BaseResourceRepository.CorruptionException(message);
  }

  private static BaseResourceRepository.CorruptionException corruption(
      String message, Throwable cause) {
    return new BaseResourceRepository.CorruptionException(message, cause);
  }

  private record CachedManifest(SnapshotCaptureManifest manifest, String hexSha256) {}
}
