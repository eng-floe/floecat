/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.common.rpc.BlobHeader;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference;
import ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReusableArtifactIndexStoreTest {
  private CountingBlobStore blobs;
  private ReusableArtifactIndexStore store;

  @BeforeEach
  void setUp() {
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs = new CountingBlobStore();
    store = new ReusableArtifactIndexStore(blobs);
  }

  @Test
  void appendPublishesOneImmutableDeltaRunAndRetainsBaseRuns() {
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    assertEquals(1, base.getRunsCount());
    assertEquals(3, blobs.putCount());

    blobs.resetPutCount();
    ReusableArtifactIndexReference appended =
        store.append(
            "/runs/", base, List.of(statsBundle("delta", List.of("s3://bucket/delta.parquet"))));

    assertEquals(3, blobs.putCount());
    assertEquals(2, appended.getRunsCount());
    assertTrue(appended.getRunsList().contains(base.getRuns(0)));
    assertEquals(1, store.loadEntries(base).size());
    assertEquals(2, store.loadEntries(appended).size());
  }

  @Test
  void emptyAppendPerformsNoIo() {
    ReusableArtifactIndexReference empty = ReusableArtifactIndexStore.emptyReference();
    assertEquals(empty, store.append("/runs/", empty, List.of()));
    assertEquals(0, blobs.putCount());
    assertEquals(0, blobs.getCount());
  }

  @Test
  void appendRejectsRunsOutsideOwningPrefix() {
    ReusableArtifactIndexReference base =
        store.append(
            "/other/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    assertThrows(IllegalArgumentException.class, () -> store.append("/runs/", base, List.of()));
  }

  @Test
  void batchedLookupUsesFiltersAndReturnsOnlyRequestedTypedPaths() {
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run < 4; run++) {
      index =
          store.append(
              "/runs/",
              index,
              List.of(statsBundle("run-" + run, List.of("s3://bucket/file-" + run + ".parquet"))));
    }
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs.resetGetCount();

    var found =
        store.lookup(
            index, List.of("s3://bucket/file-2.parquet", "s3://bucket/missing.parquet"), List.of());

    assertEquals(1, found.size());
    assertTrue(found.containsKey("stats\u0000s3://bucket/file-2.parquet"));
    assertEquals(3, blobs.batchCount());
    assertEquals(
        9, blobs.getCount(), "four filters, four authenticated manifests, and one data block");
  }

  @Test
  void duplicateInheritedTypedPathFailsClosed() {
    String path = "s3://bucket/file.parquet";
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of(path))));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.append("/runs/", base, List.of(statsBundle("delta", List.of(path)))));
  }

  @Test
  void statsAndIndexForSamePathRemainDistinctAndBundlesRoundTrip() {
    String path = "s3://bucket/file.parquet";
    ReusableArtifactBundleReference bundle = combinedBundle("bundle", path);
    ReusableArtifactIndexReference index =
        store.append("/runs/", ReusableArtifactIndexStore.emptyReference(), List.of(bundle));

    assertTrue(store.containsFileStats(index, path));
    assertTrue(store.containsIndexArtifact(index, path));
    assertEquals(List.of(bundle), store.loadBundlesForPaths(index, List.of(path), List.of(path)));
  }

  @Test
  void lowLevelRunCountIsBoundedByCompaction() {
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run <= ReusableArtifactIndexStore.MAX_L0_RUNS; run++) {
      index =
          store.append(
              "/runs/",
              index,
              List.of(statsBundle("run-" + run, List.of("s3://bucket/file-" + run + ".parquet"))));
    }

    assertEquals(0, index.getRunsList().stream().filter(run -> run.getLevel() == 0).count());
    assertEquals(1, index.getRunsList().stream().filter(run -> run.getLevel() == 1).count());
    assertEquals(ReusableArtifactIndexStore.MAX_L0_RUNS + 1, store.loadEntries(index).size());
  }

  @Test
  void reachabilityWalkDeduplicatesSharedRunObjects() {
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    ReusableArtifactIndexReference appended =
        store.append(
            "/runs/", base, List.of(statsBundle("delta", List.of("s3://bucket/delta.parquet"))));
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    Set<String> visited = new HashSet<>();
    List<String> entries = new ArrayList<>();

    store.walkReachable(
        base, visited, ignored -> {}, entry -> entries.add(entry.getArtifact().getPayloadUri()));
    store.walkReachable(
        appended,
        visited,
        ignored -> {},
        entry -> entries.add(entry.getArtifact().getPayloadUri()));

    assertEquals(6, visited.size());
    assertEquals(Set.of("/bundles/base.pb", "/bundles/delta.pb"), new HashSet<>(entries));
  }

  @Test
  void corruptAndMissingRunObjectsFailClosed() {
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/file.parquet"))));
    String manifestUri = index.getRuns(0).getManifest().getUri();
    blobs.put(manifestUri, new byte[] {1}, "application/x-protobuf");
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    assertThrows(IllegalArgumentException.class, () -> store.loadEntries(index));

    blobs.delete(manifestUri);
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    assertThrows(StorageNotFoundException.class, () -> store.loadEntries(index));
  }

  private static ReusableArtifactBundleReference statsBundle(String id, List<String> paths) {
    ReusableArtifactBundleReference.Builder bundle =
        ReusableArtifactBundleReference.newBuilder().setArtifact(artifact(id));
    for (String path : paths) {
      bundle.addFileStats(
          ReusableStatsArtifactMetadata.newBuilder()
              .setFilePath(path)
              .setSourceFingerprint("source:" + path)
              .setStatsCaptureSignature("stats-v1"));
    }
    return bundle.build();
  }

  private static ReusableArtifactBundleReference combinedBundle(String id, String path) {
    return ReusableArtifactBundleReference.newBuilder()
        .setArtifact(artifact(id))
        .addFileStats(
            ReusableStatsArtifactMetadata.newBuilder()
                .setFilePath(path)
                .setSourceFingerprint("stats-source")
                .setStatsCaptureSignature("stats-v1"))
        .addIndexArtifacts(
            ReusableIndexArtifactMetadata.newBuilder()
                .setFilePath(path)
                .setSourceFingerprint("index-source")
                .setIndexCaptureSignature("index-v1"))
        .build();
  }

  private static StatsObjectDescriptor artifact(String id) {
    return StatsObjectDescriptor.newBuilder()
        .setTargetStorageId("reuse-bundle:" + id)
        .setPayloadUri("/bundles/" + id + ".pb")
        .setPayloadBytes(10)
        .setPayloadSha256(ByteString.copyFrom(new byte[32]))
        .build();
  }

  private static final class CountingBlobStore implements BlobStore {
    private final Map<String, byte[]> blobs = new HashMap<>();
    private int getCount;
    private int putCount;
    private int batchCount;

    int getCount() {
      return getCount;
    }

    int putCount() {
      return putCount;
    }

    int batchCount() {
      return batchCount;
    }

    void resetPutCount() {
      putCount = 0;
    }

    void resetGetCount() {
      getCount = 0;
      batchCount = 0;
    }

    @Override
    public byte[] get(String uri) {
      getCount++;
      byte[] bytes = blobs.get(uri);
      return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public Map<String, byte[]> getBatch(List<String> uris) {
      batchCount++;
      Map<String, byte[]> out = new LinkedHashMap<>();
      uris.forEach(uri -> out.put(uri, get(uri)));
      return out;
    }

    @Override
    public void put(String uri, byte[] bytes, String contentType) {
      putCount++;
      blobs.put(uri, Arrays.copyOf(bytes, bytes.length));
    }

    @Override
    public Optional<BlobHeader> head(String uri) {
      return Optional.empty();
    }

    @Override
    public boolean delete(String uri) {
      return blobs.remove(uri) != null;
    }

    @Override
    public int deletePrefix(String prefix) {
      int before = blobs.size();
      blobs.keySet().removeIf(uri -> uri.startsWith(prefix));
      return before - blobs.size();
    }

    @Override
    public Page list(String prefix, int limit, String pageToken) {
      List<String> keys =
          blobs.keySet().stream().filter(uri -> uri.startsWith(prefix)).sorted().toList();
      return new Page() {
        @Override
        public List<String> keys() {
          return keys;
        }

        @Override
        public String nextToken() {
          return "";
        }
      };
    }
  }
}
