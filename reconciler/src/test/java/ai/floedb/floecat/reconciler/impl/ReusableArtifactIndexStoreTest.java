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
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunManifest;
import ai.floedb.floecat.reconciler.rpc.ReusableIndexArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.ReusableStatsArtifactMetadata;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
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
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    assertEquals(1, base.getRunsCount());
    assertEquals(0, blobs.putCount());
    assertTrue(base.getRuns(0).getManifest().getUri().isEmpty());
    assertTrue(!base.getRuns(0).getManifest().getInlinePayload().isEmpty());

    blobs.resetPutCount();
    ReusableArtifactIndexReference appended =
        store.append(
            "/runs/",
            "/runs/",
            base,
            List.of(statsBundle("delta", List.of("s3://bucket/delta.parquet"))));

    assertEquals(0, blobs.putCount());
    assertEquals(2, appended.getRunsCount());
    assertTrue(appended.getRunsList().contains(base.getRuns(0)));
    assertEquals(1, entries(store, base).size());
    assertEquals(2, entries(store, appended).size());
  }

  @Test
  void emptyAppendPerformsNoIo() {
    ReusableArtifactIndexReference empty = ReusableArtifactIndexStore.emptyReference();
    assertEquals(empty, store.append("/runs/", "/runs/", empty, List.of()));
    assertEquals(0, blobs.putCount());
    assertEquals(0, blobs.getCount());
  }

  @Test
  void largeDeltaIsSplitIntoBoundedRuns() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 3_000; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }

    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("large", paths)));

    assertTrue(index.getRunsCount() > 1);
    assertEquals(paths.size(), index.getFileStatsRecordCount());
  }

  @Test
  void inlineRunsCanBeStructurallySharedWithoutBlobOwnershipChecks() {
    ReusableArtifactIndexReference base =
        store.append(
            "/other/",
            "/other/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    assertEquals(base, store.append("/runs/", "/other/", base, List.of()));
  }

  @Test
  void prePackBlockLayoutIsInvalid() throws Exception {
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    var run = index.getRuns(0);
    var manifest = ReusableArtifactIndexRunManifest.parseFrom(run.getManifest().getInlinePayload());
    byte[] legacyBytes =
        manifest.toBuilder()
            .setBlocks(0, manifest.getBlocks(0).toBuilder().clearLength().clearBlockSha256())
            .build()
            .toByteArray();
    var legacyManifest =
        run.getManifest().toBuilder()
            .setPayloadBytes(legacyBytes.length)
            .setPayloadSha256(
                ByteString.copyFrom(
                    java.security.MessageDigest.getInstance("SHA-256").digest(legacyBytes)))
            .setInlinePayload(ByteString.copyFrom(legacyBytes));
    ReusableArtifactIndexReference legacy =
        index.toBuilder().setRuns(0, run.toBuilder().setManifest(legacyManifest)).build();

    assertThrows(IllegalArgumentException.class, () -> store.validateReadableReference(legacy));
  }

  @Test
  void appendRejectsUnsupportedRunLevels() {
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    ReusableArtifactIndexReference invalid =
        base.toBuilder().setRuns(0, base.getRuns(0).toBuilder().setLevel(33)).build();

    assertThrows(
        IllegalArgumentException.class, () -> store.append("/runs/", "/runs/", invalid, List.of()));
  }

  @Test
  void legacyReferenceFormatIsRejected() {
    ReusableArtifactIndexReference legacy =
        ReusableArtifactIndexReference.newBuilder().setFormatVersion(1).build();

    assertThrows(IllegalArgumentException.class, () -> store.validateReadableReference(legacy));
  }

  @Test
  void referenceRejectsUncompactedRunLevel() {
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    var run = base.getRuns(0);
    var builder = base.toBuilder().clearRuns().setFileStatsRecordCount(33);
    for (int index = 0; index < 33; index++) {
      var manifest =
          run.getManifest().toBuilder()
              .clearInlinePayload()
              .setUri("/runs/run-manifests/" + index + ".pb");
      builder.addRuns(run.toBuilder().setManifest(manifest));
    }

    assertThrows(
        IllegalArgumentException.class,
        () -> ReusableArtifactIndexStore.validateReference(builder.build()));
  }

  @Test
  void batchedLookupUsesFiltersAndReturnsOnlyRequestedTypedPaths() {
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run < 4; run++) {
      index =
          store.append(
              "/runs/",
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
    assertEquals(0, blobs.batchCount());
    assertEquals(0, blobs.getCount());
  }

  @Test
  void logicalBlocksShareAPackAndPointLookupsUseRanges() throws Exception {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));
    var run = index.getRuns(0);
    ReusableArtifactIndexRunManifest manifest =
        ReusableArtifactIndexRunManifest.parseFrom(run.getManifest().getInlinePayload());

    assertTrue(manifest.getBlocksCount() > 1);
    assertEquals(
        1,
        manifest.getBlocksList().stream()
            .map(block -> block.getObject().getUri())
            .distinct()
            .count());
    assertTrue(manifest.getBlocks(0).getObject().getUri().contains("/packs/"));
    assertEquals(0L, manifest.getBlocks(0).getOffset());
    assertTrue(manifest.getBlocks(1).getOffset() > 0L);

    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs.resetGetCount();
    assertEquals(1, store.lookup(index, List.of(paths.get(200)), List.of()).size());
    assertEquals(1, blobs.rangeCount());
    assertEquals(0, blobs.getCount());

    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs.resetGetCount();
    assertEquals(paths.size(), entries(store, index).size());
    assertEquals(0, blobs.getCount());
    assertTrue(blobs.rangeCount() < manifest.getBlocksCount());
    assertTrue(blobs.maxRangeLength() <= 8 * 1024 * 1024);
  }

  @Test
  void readableReferenceAuthenticatesPackDigest() throws Exception {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));
    ReusableArtifactIndexRunManifest manifest =
        ReusableArtifactIndexRunManifest.parseFrom(
            index.getRuns(0).getManifest().getInlinePayload());
    String packUri = manifest.getBlocks(0).getObject().getUri();
    byte[] corrupt = blobs.bytes(packUri);
    corrupt[corrupt.length / 2] ^= 1;
    blobs.overwrite(packUri, corrupt);

    assertThrows(IllegalArgumentException.class, () -> store.validateReadableReference(index));
  }

  @Test
  void lookupValidationDoesNotScanUnselectedPacks() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs.resetGetCount();

    store.validateLookupReference(index);

    assertEquals(0, blobs.rangeCount());
  }

  @Test
  void ownedReadableReferenceRejectsForeignPacks() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/foreign/",
            "/foreign/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));

    assertThrows(
        IllegalArgumentException.class, () -> store.validateReadableReference("/owned/", index));
  }

  @Test
  void appendRejectsBaseWhoseInlineManifestReferencesForeignPacks() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference foreign =
        store.append(
            "/foreign/",
            "/foreign/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));

    assertThrows(
        IllegalArgumentException.class,
        () -> store.append("/owned/", "/owned/", foreign, List.of()));
  }

  @Test
  void appendAcceptsInheritedRunsFromAnOlderOwnedGeneration() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference base =
        store.append(
            "/owned/old-generation/",
            "/owned/old-generation/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", paths)));

    ReusableArtifactIndexReference appended =
        store.append(
            "/owned/new-generation/",
            "/owned/",
            base,
            List.of(statsBundle("delta", List.of("s3://bucket/delta.parquet"))));

    assertEquals(paths.size() + 1, appended.getFileStatsRecordCount());
    assertTrue(appended.getRunsList().containsAll(base.getRunsList()));
  }

  @Test
  void readableReferenceRejectsOversizedPackBeforeReadingIt() throws Exception {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));
    var run = index.getRuns(0);
    var manifest = ReusableArtifactIndexRunManifest.parseFrom(run.getManifest().getInlinePayload());
    var oversizedObject =
        manifest.getBlocks(0).getObject().toBuilder()
            .setPayloadBytes((long) ReusableArtifactIndexStore.TARGET_PACK_BYTES + 1L);
    byte[] manifestBytes =
        manifest.toBuilder()
            .setBlocks(0, manifest.getBlocks(0).toBuilder().setObject(oversizedObject))
            .build()
            .toByteArray();
    var manifestRef =
        run.getManifest().toBuilder()
            .setPayloadBytes(manifestBytes.length)
            .setPayloadSha256(
                ByteString.copyFrom(
                    java.security.MessageDigest.getInstance("SHA-256").digest(manifestBytes)))
            .setInlinePayload(ByteString.copyFrom(manifestBytes));
    ReusableArtifactIndexReference oversized =
        index.toBuilder().setRuns(0, run.toBuilder().setManifest(manifestRef)).build();
    int rangeReads = blobs.rangeCount();

    assertThrows(IllegalArgumentException.class, () -> store.validateReadableReference(oversized));
    assertEquals(rangeReads, blobs.rangeCount());
  }

  @Test
  void oversizedBloomReferenceIsRejectedBeforeObjectRead() {
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    var oversizedFilter =
        index.getRuns(0).getFilter().toBuilder()
            .clearInlinePayload()
            .setUri("/runs/filters/oversized.bf")
            .setPayloadBytes(9L + ReusableArtifactIndexStore.MAX_BLOOM_BYTES + 1L)
            .build();
    ReusableArtifactIndexReference oversized =
        index.toBuilder()
            .setRuns(0, index.getRuns(0).toBuilder().setFilter(oversizedFilter))
            .build();
    int reads = blobs.getCount();

    assertThrows(IllegalArgumentException.class, () -> store.validateReadableReference(oversized));
    assertEquals(reads, blobs.getCount());
  }

  @Test
  void reachabilityRetriesAnObjectWhoseConsumerWasInterrupted() {
    List<String> paths = new ArrayList<>();
    String padding = "x".repeat(3_000);
    for (int index = 0; index < 400; index++) {
      paths.add("s3://bucket/" + padding + index + ".parquet");
    }
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("packed", paths)));
    Set<String> visited = new HashSet<>();
    Map<String, Integer> progress = new HashMap<>();

    assertThrows(
        IllegalStateException.class,
        () ->
            store.walkReachable(
                index,
                visited,
                progress,
                ignored -> {
                  throw new IllegalStateException("deadline");
                },
                ignored -> {}));
    List<String> rooted = new ArrayList<>();
    store.walkReachable(
        index, visited, progress, object -> rooted.add(object.getUri()), ignored -> {});

    assertTrue(rooted.stream().anyMatch(uri -> uri.contains("/packs/")));
    assertTrue(progress.isEmpty());
  }

  @Test
  void duplicateInheritedTypedPathFailsClosed() {
    String path = "s3://bucket/file.parquet";
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of(path))));
    assertThrows(
        IllegalArgumentException.class,
        () -> store.append("/runs/", "/runs/", base, List.of(statsBundle("delta", List.of(path)))));
  }

  @Test
  void statsAndIndexForSamePathRemainDistinctAndBundlesRoundTrip() {
    String path = "s3://bucket/file.parquet";
    ReusableArtifactBundleReference bundle = combinedBundle("bundle", path);
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/", "/runs/", ReusableArtifactIndexStore.emptyReference(), List.of(bundle));

    assertEquals(2, store.lookup(index, List.of(path), List.of(path)).size());
    assertEquals(List.of(bundle), store.loadBundlesForPaths(index, List.of(path), List.of(path)));
  }

  @Test
  void lowLevelRunCountIsBoundedByCompaction() {
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run <= ReusableArtifactIndexStore.MAX_L0_RUNS; run++) {
      index =
          store.append(
              "/runs/",
              "/runs/",
              index,
              List.of(statsBundle("run-" + run, List.of("s3://bucket/file-" + run + ".parquet"))));
    }

    assertEquals(0, index.getRunsList().stream().filter(run -> run.getLevel() == 0).count());
    assertEquals(1, index.getRunsList().stream().filter(run -> run.getLevel() == 1).count());
    assertEquals(ReusableArtifactIndexStore.MAX_L0_RUNS + 1, entries(store, index).size());
  }

  @Test
  void compactionBoundsRunsAtEveryLevel() {
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    int entries =
        (ReusableArtifactIndexStore.MAX_L0_RUNS + 1) * (ReusableArtifactIndexStore.MAX_L1_RUNS + 1);
    for (int run = 0; run < entries; run++) {
      index =
          store.append(
              "/runs/",
              "/runs/",
              index,
              List.of(statsBundle("run-" + run, List.of("s3://bucket/file-" + run + ".parquet"))));
    }

    Map<Integer, Long> runsByLevel =
        index.getRunsList().stream()
            .collect(
                java.util.stream.Collectors.groupingBy(
                    ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunReference::getLevel,
                    java.util.stream.Collectors.counting()));
    assertTrue(
        runsByLevel.entrySet().stream()
            .allMatch(
                entry ->
                    entry.getValue()
                        <= (entry.getKey() == 0
                            ? ReusableArtifactIndexStore.MAX_L0_RUNS
                            : ReusableArtifactIndexStore.MAX_L1_RUNS)));
    assertEquals(entries, entries(store, index).size());
  }

  @Test
  void appendCompactsWhenLevelZeroExceedsItsRunBound() {
    ReusableArtifactIndexStore budgeted = new ReusableArtifactIndexStore(blobs);
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run < ReusableArtifactIndexStore.MAX_L0_RUNS; run++) {
      index =
          budgeted.append(
              "/runs/",
              "/runs/",
              index,
              List.of(
                  statsBundle("budget-" + run, List.of("s3://bucket/budget-" + run + ".parquet"))));
    }
    blobs.resetPutCount();
    index =
        budgeted.append(
            "/runs/",
            "/runs/",
            index,
            List.of(statsBundle("budget-last", List.of("s3://bucket/budget-last.parquet"))));

    assertEquals(0, index.getRunsList().stream().filter(run -> run.getLevel() == 0).count());
    assertEquals(1, index.getRunsList().stream().filter(run -> run.getLevel() == 1).count());
    assertEquals(0, blobs.putCount(), "small compacted runs remain inline in the manifest");
    assertEquals(ReusableArtifactIndexStore.MAX_L0_RUNS + 1, entries(budgeted, index).size());
  }

  @Test
  void sustainedAppendsKeepEveryRunLevelBounded() {
    ReusableArtifactIndexStore budgeted = new ReusableArtifactIndexStore(blobs);
    ReusableArtifactIndexReference index = ReusableArtifactIndexStore.emptyReference();
    for (int run = 0; run < 200; run++) {
      index =
          budgeted.append(
              "/runs/",
              "/runs/",
              index,
              List.of(
                  statsBundle(
                      "sustained-" + run, List.of("s3://bucket/sustained-" + run + ".parquet"))));
    }

    assertTrue(
        index.getRunsList().stream()
            .collect(
                java.util.stream.Collectors.groupingBy(
                    ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunReference::getLevel,
                    java.util.stream.Collectors.counting()))
            .values()
            .stream()
            .allMatch(count -> count <= ReusableArtifactIndexStore.MAX_L1_RUNS));
    assertEquals(200, entries(budgeted, index).size());
    assertEquals(0, blobs.putCount());
  }

  @Test
  void streamingTraversalRejectsDuplicateTargetsAcrossRuns() {
    ReusableArtifactIndexReference first =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("first", List.of("s3://bucket/duplicate.parquet"))));
    ReusableArtifactIndexReference second =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("second", List.of("s3://bucket/duplicate.parquet"))));
    ReusableArtifactIndexReference duplicate =
        ReusableArtifactIndexReference.newBuilder()
            .setFormatVersion(ReusableArtifactIndexStore.FORMAT_VERSION)
            .setFileStatsRecordCount(2)
            .addRuns(first.getRuns(0))
            .addRuns(second.getRuns(0))
            .build();
    List<ReusableArtifactIndexEntry> visited = new ArrayList<>();

    assertThrows(IllegalArgumentException.class, () -> store.forEachEntry(duplicate, visited::add));
    assertTrue(visited.isEmpty());
  }

  @Test
  void streamingTraversalDoesNotPublishEntriesBeforeCountValidation() {
    ReusableArtifactIndexReference valid =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("first", List.of("s3://bucket/file.parquet"))));
    ReusableArtifactIndexReference invalid = valid.toBuilder().setFileStatsRecordCount(2).build();
    List<ReusableArtifactIndexEntry> visited = new ArrayList<>();

    assertThrows(IllegalArgumentException.class, () -> store.forEachEntry(invalid, visited::add));
    assertTrue(visited.isEmpty());
  }

  @Test
  void bloomSizingCapsLargeRunsWithoutIntegerOverflow() {
    assertEquals(
        ReusableArtifactIndexStore.MAX_BLOOM_BYTES * Byte.SIZE,
        ReusableArtifactIndexStore.bloomBitCount(Integer.MAX_VALUE));
  }

  @Test
  void reachabilityWalkDeduplicatesSharedRunObjects() {
    ReusableArtifactIndexReference base =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/base.parquet"))));
    ReusableArtifactIndexReference appended =
        store.append(
            "/runs/",
            "/runs/",
            base,
            List.of(statsBundle("delta", List.of("s3://bucket/delta.parquet"))));
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    Set<String> visited = new HashSet<>();
    Map<String, Integer> progress = new HashMap<>();
    List<String> entries = new ArrayList<>();

    store.walkReachable(
        base,
        visited,
        progress,
        ignored -> {},
        entry -> entries.add(entry.getArtifact().getPayloadUri()));
    store.walkReachable(
        appended,
        visited,
        progress,
        ignored -> {},
        entry -> entries.add(entry.getArtifact().getPayloadUri()));

    assertTrue(!visited.isEmpty());
    assertEquals(2, entries.size());
    assertEquals(Set.of("/bundles/base.pb", "/bundles/delta.pb"), new HashSet<>(entries));
  }

  @Test
  void corruptInlineRunObjectsFailClosed() {
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/file.parquet"))));
    ReusableArtifactIndexReference corrupt =
        index.toBuilder()
            .setRuns(
                0,
                index.getRuns(0).toBuilder()
                    .setManifest(
                        index.getRuns(0).getManifest().toBuilder()
                            .setInlinePayload(ByteString.copyFrom(new byte[] {1}))))
            .build();
    assertThrows(IllegalArgumentException.class, () -> entries(store, corrupt));
  }

  @Test
  void negativeBlockOffsetsAreRejectedWithoutReadingBlockBytes() throws Exception {
    ReusableArtifactIndexReference index =
        store.append(
            "/runs/",
            "/runs/",
            ReusableArtifactIndexStore.emptyReference(),
            List.of(statsBundle("base", List.of("s3://bucket/file.parquet"))));
    var run = index.getRuns(0);
    ReusableArtifactIndexRunManifest manifest =
        ReusableArtifactIndexRunManifest.parseFrom(run.getManifest().getInlinePayload());
    // uint64 offset: an all-ones wire value reads back as a negative long, which would otherwise
    // inflate the payloadBytes - offset headroom and pass the upper-bound check.
    byte[] mutated =
        manifest.toBuilder()
            .setBlocks(0, manifest.getBlocks(0).toBuilder().setOffset(-1L))
            .build()
            .toByteArray();
    ReusableArtifactIndexReference corrupt =
        index.toBuilder()
            .setRuns(
                0,
                run.toBuilder()
                    .setManifest(
                        run.getManifest().toBuilder()
                            .setInlinePayload(ByteString.copyFrom(mutated))
                            .setPayloadBytes(mutated.length)
                            .setPayloadSha256(
                                ByteString.copyFrom(
                                    java.security.MessageDigest.getInstance("SHA-256")
                                        .digest(mutated)))))
            .build();
    ReusableArtifactIndexStore.clearSharedCacheForTests();
    blobs.resetGetCount();

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> entries(store, corrupt));

    assertTrue(error.getMessage().contains("block range is invalid"));
    assertEquals(0, blobs.rangeCount());
  }

  private static List<ReusableArtifactIndexEntry> entries(
      ReusableArtifactIndexStore store, ReusableArtifactIndexReference index) {
    List<ReusableArtifactIndexEntry> entries = new ArrayList<>();
    store.forEachEntry(index, entries::add);
    return List.copyOf(entries);
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
    private int rangeCount;
    private int maxRangeLength;

    int getCount() {
      return getCount;
    }

    int putCount() {
      return putCount;
    }

    int batchCount() {
      return batchCount;
    }

    int rangeCount() {
      return rangeCount;
    }

    int maxRangeLength() {
      return maxRangeLength;
    }

    byte[] bytes(String uri) {
      return Arrays.copyOf(blobs.get(uri), blobs.get(uri).length);
    }

    void overwrite(String uri, byte[] bytes) {
      blobs.put(uri, Arrays.copyOf(bytes, bytes.length));
    }

    void resetPutCount() {
      putCount = 0;
    }

    void resetGetCount() {
      getCount = 0;
      batchCount = 0;
      rangeCount = 0;
      maxRangeLength = 0;
    }

    @Override
    public byte[] get(String uri) {
      getCount++;
      byte[] bytes = blobs.get(uri);
      return bytes == null ? null : Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public byte[] getRange(String uri, long offset, int length) {
      rangeCount++;
      maxRangeLength = Math.max(maxRangeLength, length);
      byte[] bytes = blobs.get(uri);
      if (bytes == null) {
        return null;
      }
      return Arrays.copyOfRange(bytes, Math.toIntExact(offset), Math.toIntExact(offset + length));
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
      byte[] bytes = blobs.get(uri);
      return bytes == null
          ? Optional.empty()
          : Optional.of(BlobHeader.newBuilder().setContentLength(bytes.length).build());
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
