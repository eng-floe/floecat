/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundleReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexBlock;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexBlockReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexEntry;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexObjectReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexReference;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunManifest;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactIndexRunReference;
import ai.floedb.floecat.reconciler.rpc.StatsObjectDescriptor;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import ai.floedb.floecat.storage.spi.BlobStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Persistent immutable sorted-run index for reusable file artifacts. */
public final class ReusableArtifactIndexStore {
  public static final int FORMAT_VERSION = 2;
  static final int TARGET_BLOCK_BYTES = 512 * 1024;
  static final int TARGET_PACK_BYTES = 64 * 1024 * 1024;
  static final int MAX_L0_RUNS = 32;
  static final int MAX_L1_RUNS = 32;
  static final int MAX_BLOOM_BYTES = 16 * 1024 * 1024;
  private static final int MAX_SINGLE_CACHED_OBJECT_BYTES =
      ReusableArtifactIndexObjectCache.MAX_SINGLE_OBJECT_BYTES;
  private static final long MAX_BLOOM_OBJECT_BYTES = 9L + MAX_BLOOM_BYTES;
  private static final long MAX_RUN_MANIFEST_BYTES = MAX_SINGLE_CACHED_OBJECT_BYTES;
  private static final long MAX_DELTA_BUFFER_BYTES = 8L * 1024L * 1024L;
  private static final int MAX_SEQUENTIAL_READ_WINDOW_BYTES = 8 * 1024 * 1024;
  private static final int MAX_SEQUENTIAL_READ_BUDGET_BYTES = 64 * 1024 * 1024;
  private static final int OBJECT_AUTHENTICATION_WINDOW_BYTES = 8 * 1024 * 1024;
  static final int INLINE_OBJECT_BYTES = 64 * 1024;
  private static final int MAX_RUN_LEVEL = 32;
  private static final ReusableArtifactIndexObjectCache SHARED_OBJECT_CACHE =
      new ReusableArtifactIndexObjectCache();
  private static final long MAX_CACHED_BYTES = SHARED_OBJECT_CACHE.maxBytes();
  private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
  private static final String FILTER_CONTENT_TYPE = "application/x-floecat-bloom-filter";
  private static final String PACK_CONTENT_TYPE = "application/x-floecat-reusable-index-pack";

  private static final Comparator<IndexedEntry> INDEXED_ENTRY_ORDER =
      (left, right) -> {
        int hashOrder = compareUnsigned(left.keyHash(), right.keyHash());
        return hashOrder != 0 ? hashOrder : left.key().compareTo(right.key());
      };

  private final BlobStore blobStore;

  public ReusableArtifactIndexStore(BlobStore blobStore) {
    if (blobStore == null) {
      throw new IllegalArgumentException("blobStore is required");
    }
    this.blobStore = blobStore;
  }

  /** Authenticates run manifests and verifies that their block directory is readable. */
  public void validateReadableReference(ReusableArtifactIndexReference index) {
    validateReferenceObjects(index, null, true);
  }

  /** Authenticates every external object and rejects objects outside the owning table prefix. */
  public void validateReadableReference(String objectPrefix, ReusableArtifactIndexReference index) {
    validateReferenceObjects(index, normalizePrefix(objectPrefix), true);
  }

  /** Authenticates lookup metadata without reading packs that have no candidate block. */
  public void validateLookupReference(ReusableArtifactIndexReference index) {
    validateReferenceObjects(index, null, false);
  }

  private void validateReferenceObjects(
      ReusableArtifactIndexReference index, String ownedPrefix, boolean authenticatePacks) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    Map<String, ReusableArtifactIndexObjectReference> packs = new HashMap<>();
    for (List<ReusableArtifactIndexRunReference> batch : metadataBatches(effective.getRunsList())) {
      primeRunMetadata(batch);
      for (ReusableArtifactIndexRunReference run : batch) {
        validateOwnedObject(ownedPrefix, run.getFilter());
        validateOwnedObject(ownedPrefix, run.getManifest());
        loadFilter(run);
        ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
        for (ReusableArtifactIndexBlockReference block : manifest.getBlocksList()) {
          ReusableArtifactIndexObjectReference object = block.getObject();
          validateOwnedObject(ownedPrefix, object);
          if (!object.getInlinePayload().isEmpty()) {
            validateObjectBytes(object, object.getInlinePayload().toByteArray());
          } else if (authenticatePacks) {
            ReusableArtifactIndexObjectReference existing =
                packs.putIfAbsent(object.getUri(), object);
            if (existing == null) {
              authenticateExternalObject(object);
            } else if (!existing.equals(object)) {
              throw new IllegalArgumentException(
                  "reusable artifact index pack has conflicting references");
            }
          }
        }
      }
    }
  }

  /** Adds one immutable delta run and restores the bounded run-count invariant at every level. */
  public ReusableArtifactIndexReference append(
      String objectPrefix,
      ReusableArtifactIndexReference base,
      List<ReusableArtifactBundleReference> bundles) {
    String prefix = normalizePrefix(objectPrefix);
    ReusableArtifactIndexReference effectiveBase = effectiveReference(base);
    validateReferenceObjects(effectiveBase, prefix, false);

    DeltaRunAccumulator accumulator = new DeltaRunAccumulator(prefix, effectiveBase);
    for (ReusableArtifactBundleReference bundle :
        bundles == null ? List.<ReusableArtifactBundleReference>of() : bundles) {
      if (bundle == null || !bundle.hasArtifact()) {
        throw new IllegalArgumentException("reusable artifact bundle descriptor is required");
      }
      StatsObjectDescriptor artifact = bundle.getArtifact();
      validateArtifact(artifact);
      bundle
          .getFileStatsList()
          .forEach(
              metadata ->
                  accumulator.add(
                      ReusableArtifactIndexEntry.newBuilder()
                          .setArtifact(artifact)
                          .setFileStats(metadata)
                          .build()));
      bundle
          .getIndexArtifactsList()
          .forEach(
              metadata ->
                  accumulator.add(
                      ReusableArtifactIndexEntry.newBuilder()
                          .setArtifact(artifact)
                          .setIndexArtifact(metadata)
                          .build()));
    }
    return accumulator.finish();
  }

  /** Performs one batched lookup for all requested typed paths. */
  public Map<String, ReusableArtifactIndexEntry> lookup(
      ReusableArtifactIndexReference index,
      Collection<String> statsPaths,
      Collection<String> indexPaths) {
    Set<String> keys = new LinkedHashSet<>();
    for (String path : statsPaths == null ? List.<String>of() : statsPaths) {
      keys.add(statsKey(path));
    }
    for (String path : indexPaths == null ? List.<String>of() : indexPaths) {
      keys.add(indexKey(path));
    }
    return lookupKeys(index, keys);
  }

  public List<ReusableArtifactBundleReference> loadBundlesForPaths(
      ReusableArtifactIndexReference index,
      Collection<String> statsPaths,
      Collection<String> indexPaths) {
    return bundlesFromEntries(lookup(index, statsPaths, indexPaths).values());
  }

  public enum EntryKind {
    FILE_STATS,
    INDEX_ARTIFACT
  }

  public record EntryPage(
      List<ReusableArtifactIndexEntry> entries,
      String nextPageToken,
      List<String> continuationTokens) {
    public EntryPage {
      entries = entries == null ? List.of() : List.copyOf(entries);
      nextPageToken = nextPageToken == null ? "" : nextPageToken;
      continuationTokens = continuationTokens == null ? List.of() : List.copyOf(continuationTokens);
      if (!continuationTokens.isEmpty() && continuationTokens.size() != entries.size()) {
        throw new IllegalArgumentException("artifact page continuation count does not match");
      }
    }

    public EntryPage(List<ReusableArtifactIndexEntry> entries, String nextPageToken) {
      this(entries, nextPageToken, List.of());
    }
  }

  /**
   * Pages one artifact kind directly from the immutable sorted runs. The opaque cursor is the last
   * typed key returned, so a page reads at most one starting block per run plus blocks crossed
   * while filling the page; it never materializes the complete index or writes pagination state.
   */
  public EntryPage page(
      ReusableArtifactIndexReference index, EntryKind kind, int limit, String pageToken) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    if (kind == null || limit <= 0) {
      throw new IllegalArgumentException("reusable artifact page arguments are invalid");
    }
    IndexedEntry after = decodePageToken(pageToken);
    List<ReusableArtifactIndexRunReference> runs = orderedRuns(effective.getRunsList());
    PriorityQueue<RunCursor> pending =
        new PriorityQueue<>(
            (left, right) -> INDEXED_ENTRY_ORDER.compare(left.current(), right.current()));
    int readWindowBytes = sequentialReadWindowBytes(runs.size());
    for (List<ReusableArtifactIndexRunReference> batch : metadataBatches(runs)) {
      primeObjects(batch.stream().map(ReusableArtifactIndexRunReference::getManifest).toList());
      for (ReusableArtifactIndexRunReference run : batch) {
        RunCursor cursor = new RunCursor(loadRunManifest(run), after, readWindowBytes);
        if (cursor.current() != null) {
          pending.add(cursor);
        }
      }
    }
    List<ReusableArtifactIndexEntry> entries = new ArrayList<>(limit);
    List<String> continuationTokens = new ArrayList<>(limit);
    String lastKey = null;
    String previousKey = null;
    while (!pending.isEmpty() && entries.size() < limit) {
      RunCursor cursor = pending.remove();
      IndexedEntry next = cursor.current();
      if (next.key().equals(previousKey)) {
        throw new IllegalArgumentException("reusable artifact index contains a duplicate");
      }
      previousKey = next.key();
      boolean selected =
          kind == EntryKind.FILE_STATS
              ? next.entry().hasFileStats()
              : next.entry().hasIndexArtifact();
      if (selected) {
        entries.add(next.entry());
        lastKey = next.key();
        continuationTokens.add(encodePageToken(next.key()));
      }
      cursor.advance();
      if (cursor.current() != null) {
        pending.add(cursor);
      }
    }
    if (entries.size() == limit
        && !pending.isEmpty()
        && pending.peek().current().key().equals(lastKey)) {
      throw new IllegalArgumentException("reusable artifact index contains a duplicate");
    }
    return new EntryPage(
        entries,
        entries.size() == limit && lastKey != null ? encodePageToken(lastKey) : "",
        continuationTokens);
  }

  /**
   * Full traversal retained for exceptional rebuild paths; callers should consume it as a stream.
   */
  public void forEachEntry(
      ReusableArtifactIndexReference index, Consumer<ReusableArtifactIndexEntry> consumer) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    if (consumer == null) {
      throw new IllegalArgumentException("reusable artifact index consumer is required");
    }
    long[] counts = new long[2];
    try (SpillableDuplicateDetector duplicates = new SpillableDuplicateDetector()) {
      for (ReusableArtifactIndexRunReference run : orderedRuns(effective.getRunsList())) {
        ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
        RunCursor cursor = new RunCursor(manifest, MAX_SEQUENTIAL_READ_WINDOW_BYTES);
        while (cursor.current() != null) {
          ReusableArtifactIndexEntry entry = cursor.current().entry();
          duplicates.add(entryKey(entry), entry);
          counts[entry.hasFileStats() ? 0 : 1]++;
          cursor.advance();
        }
      }
      duplicates.verifyNoDuplicates();
      if (counts[0] != effective.getFileStatsRecordCount()
          || counts[1] != effective.getIndexArtifactCount()) {
        throw new IllegalArgumentException("reusable artifact run index kind count mismatch");
      }
      duplicates.forEachEntry(consumer);
    }
  }

  /** Roots immutable run manifests, filters, blocks, and referenced artifact bundles. */
  public void walkReachable(
      ReusableArtifactIndexReference index,
      Set<String> visitedObjectUris,
      Consumer<ReusableArtifactIndexObjectReference> objectConsumer,
      Consumer<ReusableArtifactIndexEntry> entryConsumer) {
    walkReachable(index, visitedObjectUris, new HashMap<>(), objectConsumer, entryConsumer);
  }

  public void walkReachable(
      ReusableArtifactIndexReference index,
      Set<String> visitedObjectUris,
      Map<String, Integer> nextBlockByRun,
      Consumer<ReusableArtifactIndexObjectReference> objectConsumer,
      Consumer<ReusableArtifactIndexEntry> entryConsumer) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    if (visitedObjectUris == null
        || nextBlockByRun == null
        || objectConsumer == null
        || entryConsumer == null) {
      throw new IllegalArgumentException("reusable artifact index walk arguments are required");
    }
    for (ReusableArtifactIndexRunReference run : effective.getRunsList()) {
      acceptObject(run.getFilter(), visitedObjectUris, objectConsumer);
      String runIdentity = objectIdentity(run.getManifest());
      boolean firstVisit = acceptObject(run.getManifest(), visitedObjectUris, objectConsumer);
      if (!firstVisit && !nextBlockByRun.containsKey(runIdentity)) {
        continue;
      }
      nextBlockByRun.putIfAbsent(runIdentity, 0);
      ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
      int nextBlock = nextBlockByRun.get(runIdentity);
      if (nextBlock < 0 || nextBlock > manifest.getBlocksCount()) {
        throw new IllegalArgumentException("reusable artifact index walk cursor is invalid");
      }
      for (int blockIndex = nextBlock; blockIndex < manifest.getBlocksCount(); blockIndex++) {
        ReusableArtifactIndexBlockReference block = manifest.getBlocks(blockIndex);
        acceptObject(block.getObject(), visitedObjectUris, objectConsumer);
        loadBlock(block).getEntriesList().forEach(entryConsumer);
        nextBlockByRun.put(runIdentity, blockIndex + 1);
      }
      nextBlockByRun.remove(runIdentity);
    }
  }

  public static List<ReusableArtifactIndexEntry> entriesFromBundles(
      Collection<ReusableArtifactBundleReference> bundles) {
    List<ReusableArtifactIndexEntry> entries = new ArrayList<>();
    for (ReusableArtifactBundleReference bundle :
        bundles == null ? List.<ReusableArtifactBundleReference>of() : bundles) {
      if (bundle == null || !bundle.hasArtifact()) {
        throw new IllegalArgumentException("reusable artifact bundle descriptor is required");
      }
      StatsObjectDescriptor artifact = bundle.getArtifact();
      validateArtifact(artifact);
      bundle
          .getFileStatsList()
          .forEach(
              metadata ->
                  entries.add(
                      ReusableArtifactIndexEntry.newBuilder()
                          .setArtifact(artifact)
                          .setFileStats(metadata)
                          .build()));
      bundle
          .getIndexArtifactsList()
          .forEach(
              metadata ->
                  entries.add(
                      ReusableArtifactIndexEntry.newBuilder()
                          .setArtifact(artifact)
                          .setIndexArtifact(metadata)
                          .build()));
    }
    entries.forEach(ReusableArtifactIndexStore::validateEntry);
    entries.sort(Comparator.comparing(ReusableArtifactIndexStore::entryKey));
    return List.copyOf(entries);
  }

  public static void validateReference(ReusableArtifactIndexReference reference) {
    ReusableArtifactIndexReference effective = effectiveReference(reference);
    long expected = (long) effective.getFileStatsRecordCount() + effective.getIndexArtifactCount();
    if (effective.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException("reusable artifact run index format is invalid");
    }
    if ((expected == 0L) != effective.getRunsList().isEmpty()) {
      throw new IllegalArgumentException("reusable artifact run index shape is invalid");
    }
    long stats = 0L;
    long indexes = 0L;
    Set<String> manifests = new HashSet<>();
    int[] runsByLevel = new int[MAX_RUN_LEVEL + 1];
    for (ReusableArtifactIndexRunReference run : effective.getRunsList()) {
      validateRunReference(run);
      int maximum = run.getLevel() == 0 ? MAX_L0_RUNS : MAX_L1_RUNS;
      if (++runsByLevel[run.getLevel()] > maximum) {
        throw new IllegalArgumentException("reusable artifact run level is not compacted");
      }
      if (!manifests.add(objectIdentity(run.getManifest()))) {
        throw new IllegalArgumentException("reusable artifact run is duplicated");
      }
      stats = Math.addExact(stats, Integer.toUnsignedLong(run.getFileStatsRecordCount()));
      indexes = Math.addExact(indexes, Integer.toUnsignedLong(run.getIndexArtifactCount()));
    }
    if (stats != effective.getFileStatsRecordCount()
        || indexes != effective.getIndexArtifactCount()) {
      throw new IllegalArgumentException("reusable artifact run index count mismatch");
    }
  }

  public static ReusableArtifactIndexReference emptyReference() {
    return ReusableArtifactIndexReference.newBuilder().setFormatVersion(FORMAT_VERSION).build();
  }

  private Map<String, ReusableArtifactIndexEntry> lookupKeys(
      ReusableArtifactIndexReference index, Collection<String> requestedKeys) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    if (requestedKeys == null || requestedKeys.isEmpty() || effective.getRunsList().isEmpty()) {
      return Map.of();
    }
    Map<String, byte[]> keyHashes = new LinkedHashMap<>();
    for (String key : requestedKeys) {
      keyHashes.put(key, hash(key));
    }
    Map<String, ReusableArtifactIndexEntry> found = new LinkedHashMap<>();
    List<ReusableArtifactIndexRunReference> runs = orderedRuns(effective.getRunsList());
    for (List<ReusableArtifactIndexRunReference> batch : metadataBatches(runs)) {
      primeRunMetadata(batch);
      for (ReusableArtifactIndexRunReference run : batch) {
        ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
        ReusableArtifactIndexBloomFilter filter = loadFilter(run);
        Map<BlockIdentity, List<String>> blockKeys = new LinkedHashMap<>();
        Map<BlockIdentity, ReusableArtifactIndexBlockReference> blocks = new LinkedHashMap<>();
        keyHashes.forEach(
            (key, digest) -> {
              if (!filter.mightContain(digest)) {
                return;
              }
              ReusableArtifactIndexBlockReference block = findBlock(manifest, digest);
              if (block != null) {
                BlockIdentity identity = blockIdentity(block);
                blocks.put(identity, block);
                blockKeys.computeIfAbsent(identity, ignored -> new ArrayList<>()).add(key);
              }
            });
        for (Map.Entry<BlockIdentity, List<String>> selected : blockKeys.entrySet()) {
          ReusableArtifactIndexBlock block = loadBlock(blocks.get(selected.getKey()));
          Map<String, ReusableArtifactIndexEntry> byKey = new HashMap<>();
          for (ReusableArtifactIndexEntry entry : block.getEntriesList()) {
            byKey.put(entryKey(entry), entry);
          }
          for (String key : selected.getValue()) {
            ReusableArtifactIndexEntry entry = byKey.get(key);
            if (entry != null && found.putIfAbsent(key, entry) != null) {
              throw new IllegalArgumentException("reusable artifact index contains a duplicate");
            }
          }
        }
      }
    }
    return Map.copyOf(found);
  }

  private void primeRunMetadata(List<ReusableArtifactIndexRunReference> runs) {
    List<ReusableArtifactIndexObjectReference> references = new ArrayList<>(runs.size() * 2);
    runs.forEach(
        run -> {
          references.add(run.getFilter());
          references.add(run.getManifest());
        });
    primeObjects(references);
  }

  private static List<List<ReusableArtifactIndexRunReference>> metadataBatches(
      Collection<ReusableArtifactIndexRunReference> runs) {
    List<List<ReusableArtifactIndexRunReference>> batches = new ArrayList<>();
    List<ReusableArtifactIndexRunReference> batch = new ArrayList<>();
    long bytes = 0L;
    for (ReusableArtifactIndexRunReference run : runs) {
      long runBytes =
          Math.addExact(run.getFilter().getPayloadBytes(), run.getManifest().getPayloadBytes());
      if (!batch.isEmpty() && runBytes > MAX_CACHED_BYTES - bytes) {
        batches.add(List.copyOf(batch));
        batch.clear();
        bytes = 0L;
      }
      batch.add(run);
      bytes = runBytes > MAX_CACHED_BYTES ? MAX_CACHED_BYTES : bytes + runBytes;
    }
    if (!batch.isEmpty()) {
      batches.add(List.copyOf(batch));
    }
    return List.copyOf(batches);
  }

  private List<ReusableArtifactIndexRunReference> compactBoundedLevels(
      String prefix, List<ReusableArtifactIndexRunReference> source) {
    List<ReusableArtifactIndexRunReference> runs = new ArrayList<>(source);
    for (int level = 0; level <= MAX_RUN_LEVEL; level++) {
      int maximum = level == 0 ? MAX_L0_RUNS : MAX_L1_RUNS;
      while (compactLevel(prefix, runs, level, maximum)) {
        // A level may require more than one merge; the promoted run is handled by the next level.
      }
    }
    return List.copyOf(runs);
  }

  private boolean compactLevel(
      String prefix, List<ReusableArtifactIndexRunReference> runs, int level, int maximum) {
    List<ReusableArtifactIndexRunReference> selected =
        orderedRuns(runs).stream().filter(run -> run.getLevel() == level).toList();
    if (selected.size() <= maximum) {
      return false;
    }
    if (level >= MAX_RUN_LEVEL) {
      throw new IllegalArgumentException("reusable artifact run level exceeds supported range");
    }
    int maximumFanIn = Math.min(maximum + 1, selected.size());
    List<ReusableArtifactIndexRunReference> preferred =
        new ArrayList<>(selected.subList(0, maximumFanIn));
    List<ReusableArtifactIndexRunReference> merge = preferred;
    runs.removeAll(merge);
    runs.add(writeMergedRun(prefix, level + 1, merge));
    return true;
  }

  private ReusableArtifactIndexRunReference writeMergedRun(
      String prefix, int level, List<ReusableArtifactIndexRunReference> runs) {
    long expectedEntries =
        runs.stream()
            .mapToLong(ReusableArtifactIndexRunReference::getEntryCount)
            .reduce(0L, Math::addExact);
    if (expectedEntries <= 0L || expectedEntries > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("reusable artifact compaction size is invalid");
    }
    PriorityQueue<RunCursor> pendingRuns =
        new PriorityQueue<>(
            (left, right) -> INDEXED_ENTRY_ORDER.compare(left.current(), right.current()));
    int readWindowBytes = sequentialReadWindowBytes(runs.size());
    for (ReusableArtifactIndexRunReference run : runs) {
      RunCursor cursor = new RunCursor(loadRunManifest(run), readWindowBytes);
      if (cursor.current() != null) {
        pendingRuns.add(cursor);
      }
    }

    PackWriter packs = new PackWriter(prefix);
    List<ReusableArtifactIndexEntry> pendingBlock = new ArrayList<>();
    int pendingBytes = 8;
    byte[] previousHash = null;
    String previousKey = null;
    long stats = 0L;
    long emitted = 0L;
    ReusableArtifactIndexBloomFilter filter =
        ReusableArtifactIndexBloomFilter.create(Math.toIntExact(expectedEntries));
    while (!pendingRuns.isEmpty()) {
      RunCursor cursor = pendingRuns.remove();
      IndexedEntry next = cursor.current();
      if (next.key().equals(previousKey)) {
        throw new IllegalArgumentException("reusable artifact compaction found a duplicate");
      }
      int entryBytes = next.entry().getSerializedSize() + 8;
      if (!pendingBlock.isEmpty()
          && pendingBytes + entryBytes > TARGET_BLOCK_BYTES
          && compareUnsigned(previousHash, next.keyHash()) != 0) {
        packs.add(buildBlock(pendingBlock));
        pendingBlock = new ArrayList<>();
        pendingBytes = 8;
      }
      pendingBlock.add(next.entry());
      pendingBytes = Math.addExact(pendingBytes, entryBytes);
      previousHash = next.keyHash();
      previousKey = next.key();
      filter.add(next.keyHash());
      if (next.entry().hasFileStats()) {
        stats++;
      }
      emitted++;
      cursor.advance();
      if (cursor.current() != null) {
        pendingRuns.add(cursor);
      }
    }
    if (emitted != expectedEntries) {
      throw new IllegalArgumentException("reusable artifact compaction count mismatch");
    }
    if (!pendingBlock.isEmpty()) {
      packs.add(buildBlock(pendingBlock));
    }
    List<ReusableArtifactIndexBlockReference> blocks = packs.finish();
    return finishRun(prefix, level, blocks, filter, emitted, stats);
  }

  private ReusableArtifactIndexRunReference writeRun(
      String prefix, int level, List<IndexedEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("cannot write an empty reusable artifact run");
    }
    PackWriter packs = new PackWriter(prefix);
    List<ReusableArtifactIndexEntry> pending = new ArrayList<>();
    int pendingBytes = 8;
    byte[] previousHash = null;
    for (IndexedEntry indexed : entries) {
      int entryBytes = indexed.entry().getSerializedSize() + 8;
      if (!pending.isEmpty()
          && pendingBytes + entryBytes > TARGET_BLOCK_BYTES
          && compareUnsigned(previousHash, indexed.keyHash()) != 0) {
        packs.add(buildBlock(pending));
        pending = new ArrayList<>();
        pendingBytes = 8;
      }
      pending.add(indexed.entry());
      pendingBytes = Math.addExact(pendingBytes, entryBytes);
      previousHash = indexed.keyHash();
    }
    if (!pending.isEmpty()) {
      packs.add(buildBlock(pending));
    }
    List<ReusableArtifactIndexBlockReference> blocks = packs.finish();
    ReusableArtifactIndexBloomFilter filter =
        ReusableArtifactIndexBloomFilter.create(
            entries.stream().map(IndexedEntry::keyHash).toList());
    long stats = entries.stream().filter(entry -> entry.entry().hasFileStats()).count();
    return finishRun(prefix, level, blocks, filter, entries.size(), stats);
  }

  private ReusableArtifactIndexRunReference finishRun(
      String prefix,
      int level,
      List<ReusableArtifactIndexBlockReference> blocks,
      ReusableArtifactIndexBloomFilter filter,
      long entryCount,
      long fileStatsRecordCount) {
    ReusableArtifactIndexRunManifest manifest =
        ReusableArtifactIndexRunManifest.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .addAllBlocks(blocks)
            .build();
    ReusableArtifactIndexObjectReference manifestRef =
        writeObject(
            prefix + "run-manifests/", ".pb", manifest.toByteArray(), PROTOBUF_CONTENT_TYPE);
    ReusableArtifactIndexObjectReference filterRef =
        writeObject(prefix + "filters/", ".bf", filter.bytes(), FILTER_CONTENT_TYPE);
    return ReusableArtifactIndexRunReference.newBuilder()
        .setLevel(level)
        .setManifest(manifestRef)
        .setFilter(filterRef)
        .setEntryCount(entryCount)
        .setFileStatsRecordCount(Math.toIntExact(fileStatsRecordCount))
        .setIndexArtifactCount(Math.toIntExact(entryCount - fileStatsRecordCount))
        .build();
  }

  private final class DeltaRunAccumulator {
    private final String prefix;
    private final List<ReusableArtifactIndexRunReference> runs;
    private final List<ReusableArtifactIndexEntry> pending = new ArrayList<>();
    private long pendingBytes;
    private int stats;
    private int indexes;

    private DeltaRunAccumulator(String prefix, ReusableArtifactIndexReference base) {
      this.prefix = prefix;
      this.runs = new ArrayList<>(base.getRunsList());
      this.stats = base.getFileStatsRecordCount();
      this.indexes = base.getIndexArtifactCount();
    }

    private void add(ReusableArtifactIndexEntry entry) {
      validateEntry(entry);
      long weight = Math.addExact(64L, entry.getSerializedSize());
      if (weight > MAX_DELTA_BUFFER_BYTES) {
        throw new IllegalArgumentException(
            "reusable artifact index entry exceeds the delta buffer");
      }
      if (!pending.isEmpty() && weight > MAX_DELTA_BUFFER_BYTES - pendingBytes) {
        flush();
      }
      pending.add(entry);
      pendingBytes += weight;
    }

    private ReusableArtifactIndexReference finish() {
      flush();
      return buildReference(runs, stats, indexes);
    }

    private void flush() {
      if (pending.isEmpty()) {
        return;
      }
      List<IndexedEntry> indexed = indexEntries(pending);
      ReusableArtifactIndexReference current = buildReference(runs, stats, indexes);
      Set<String> existing =
          lookupKeys(current, indexed.stream().map(IndexedEntry::key).toList()).keySet();
      if (!existing.isEmpty()) {
        throw new IllegalArgumentException(
            "reusable artifact index already contains " + existing.iterator().next());
      }
      ReusableArtifactIndexRunReference run = writeRun(prefix, 0, indexed);
      long nextStats = Math.addExact(Integer.toUnsignedLong(stats), run.getFileStatsRecordCount());
      long nextIndexes =
          Math.addExact(Integer.toUnsignedLong(indexes), run.getIndexArtifactCount());
      if (nextStats > Integer.MAX_VALUE || nextIndexes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("reusable artifact index count exceeds supported range");
      }
      runs.add(run);
      List<ReusableArtifactIndexRunReference> compacted = compactBoundedLevels(prefix, runs);
      runs.clear();
      runs.addAll(compacted);
      stats = (int) nextStats;
      indexes = (int) nextIndexes;
      pending.clear();
      pendingBytes = 0L;
    }
  }

  private PendingBlock buildBlock(List<ReusableArtifactIndexEntry> entries) {
    List<IndexedEntry> indexed = indexEntries(entries);
    ReusableArtifactIndexBlock block =
        ReusableArtifactIndexBlock.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .addAllEntries(indexed.stream().map(IndexedEntry::entry).toList())
            .build();
    byte[] bytes = block.toByteArray();
    return new PendingBlock(
        bytes,
        indexed.getFirst().keyHash(),
        indexed.getLast().keyHash(),
        indexed.size(),
        sha256(bytes));
  }

  private final class PackWriter {
    private final String prefix;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final List<PendingBlockMetadata> pending = new ArrayList<>();
    private final List<ReusableArtifactIndexBlockReference> references = new ArrayList<>();

    private PackWriter(String prefix) {
      this.prefix = prefix;
    }

    private void add(PendingBlock block) {
      if (block.bytes().length > TARGET_PACK_BYTES) {
        throw new IllegalArgumentException("reusable artifact index block exceeds pack size");
      }
      if (!pending.isEmpty() && bytes.size() + block.bytes().length > TARGET_PACK_BYTES) {
        flush();
      }
      bytes.writeBytes(block.bytes());
      pending.add(
          new PendingBlockMetadata(
              block.bytes().length,
              block.firstKeyHash(),
              block.lastKeyHash(),
              block.entryCount(),
              block.digest()));
    }

    private List<ReusableArtifactIndexBlockReference> finish() {
      flush();
      return List.copyOf(references);
    }

    private void flush() {
      if (pending.isEmpty()) {
        return;
      }
      byte[] packed = bytes.toByteArray();
      ReusableArtifactIndexObjectReference object =
          writeObject(prefix + "packs/", ".pack", packed, PACK_CONTENT_TYPE);
      long offset = 0L;
      for (PendingBlockMetadata block : pending) {
        references.add(
            ReusableArtifactIndexBlockReference.newBuilder()
                .setObject(object)
                .setFirstKeySha256(ByteString.copyFrom(block.firstKeyHash()))
                .setLastKeySha256(ByteString.copyFrom(block.lastKeyHash()))
                .setEntryCount(block.entryCount())
                .setOffset(offset)
                .setLength(block.length())
                .setBlockSha256(ByteString.copyFrom(block.digest()))
                .build());
        offset = Math.addExact(offset, block.length());
      }
      bytes.reset();
      pending.clear();
    }
  }

  private ReusableArtifactIndexObjectReference writeObject(
      String prefix, String suffix, byte[] bytes, String contentType) {
    byte[] digest = sha256(bytes);
    if (bytes.length <= INLINE_OBJECT_BYTES) {
      return ReusableArtifactIndexObjectReference.newBuilder()
          .setPayloadBytes(bytes.length)
          .setPayloadSha256(ByteString.copyFrom(digest))
          .setInlinePayload(ByteString.copyFrom(bytes))
          .build();
    }
    String uri = prefix + HexFormat.of().formatHex(digest) + suffix;
    blobStore.putImmutable(uri, bytes, contentType);
    cache(uri, bytes);
    return ReusableArtifactIndexObjectReference.newBuilder()
        .setUri(uri)
        .setPayloadBytes(bytes.length)
        .setPayloadSha256(ByteString.copyFrom(digest))
        .build();
  }

  private ReusableArtifactIndexRunManifest loadRunManifest(ReusableArtifactIndexRunReference run) {
    byte[] bytes = loadObject(run.getManifest());
    try {
      ReusableArtifactIndexRunManifest manifest = ReusableArtifactIndexRunManifest.parseFrom(bytes);
      validateRunManifest(manifest, run.getEntryCount());
      return manifest;
    } catch (InvalidProtocolBufferException error) {
      throw new IllegalArgumentException("reusable artifact run manifest is invalid", error);
    }
  }

  private ReusableArtifactIndexBlock loadBlock(ReusableArtifactIndexBlockReference reference) {
    validateBlockReference(reference);
    byte[] bytes = loadBlockBytes(reference);
    return decodeBlock(reference, bytes);
  }

  private ReusableArtifactIndexBlock loadBlockFromValidatedPack(
      ReusableArtifactIndexBlockReference reference, byte[] packBytes) {
    validateBlockReference(reference);
    return decodeBlock(reference, sliceBlock(reference, packBytes));
  }

  private ReusableArtifactIndexBlock loadBlockFromWindow(
      ReusableArtifactIndexBlockReference reference, long windowOffset, byte[] windowBytes) {
    validateBlockReference(reference);
    long relativeOffset = Math.subtractExact(reference.getOffset(), windowOffset);
    int start = Math.toIntExact(relativeOffset);
    int end = Math.addExact(start, reference.getLength());
    if (start < 0 || end > windowBytes.length) {
      throw new IllegalArgumentException("reusable artifact block exceeds its scan window");
    }
    return decodeBlock(reference, Arrays.copyOfRange(windowBytes, start, end));
  }

  private ReusableArtifactIndexBlock decodeBlock(
      ReusableArtifactIndexBlockReference reference, byte[] bytes) {
    if (bytes.length != reference.getLength()
        || !MessageDigest.isEqual(sha256(bytes), reference.getBlockSha256().toByteArray())) {
      throw new IllegalArgumentException("reusable artifact index block metadata mismatch");
    }
    try {
      ReusableArtifactIndexBlock block = ReusableArtifactIndexBlock.parseFrom(bytes);
      if (block.getFormatVersion() != FORMAT_VERSION
          || block.getEntriesCount() != reference.getEntryCount()) {
        throw new IllegalArgumentException("reusable artifact index block shape is invalid");
      }
      List<IndexedEntry> indexed = indexEntries(block.getEntriesList());
      for (int index = 0; index < indexed.size(); index++) {
        if (!indexed.get(index).entry().equals(block.getEntries(index))) {
          throw new IllegalArgumentException("reusable artifact index block is not sorted");
        }
      }
      if (!Arrays.equals(indexed.getFirst().keyHash(), reference.getFirstKeySha256().toByteArray())
          || !Arrays.equals(
              indexed.getLast().keyHash(), reference.getLastKeySha256().toByteArray())) {
        throw new IllegalArgumentException("reusable artifact index block bounds mismatch");
      }
      return block;
    } catch (InvalidProtocolBufferException error) {
      throw new IllegalArgumentException("reusable artifact index block is invalid", error);
    }
  }

  private byte[] loadBlockBytes(ReusableArtifactIndexBlockReference reference) {
    ReusableArtifactIndexObjectReference object = reference.getObject();
    if (!object.getInlinePayload().isEmpty()) {
      byte[] pack = object.getInlinePayload().toByteArray();
      validateObjectBytes(object, pack);
      return sliceBlock(reference, pack);
    }
    byte[] cached = cached(object);
    if (cached != null) {
      return sliceBlock(reference, cached);
    }
    byte[] bytes =
        blobStore.getRange(object.getUri(), reference.getOffset(), reference.getLength());
    if (bytes == null) {
      throw new StorageNotFoundException(
          "reusable artifact index pack is missing: " + object.getUri());
    }
    return bytes;
  }

  private static byte[] sliceBlock(
      ReusableArtifactIndexBlockReference reference, byte[] packBytes) {
    int start = Math.toIntExact(reference.getOffset());
    int end = Math.addExact(start, reference.getLength());
    if (end > packBytes.length) {
      throw new IllegalArgumentException("reusable artifact block range exceeds its pack");
    }
    return Arrays.copyOfRange(packBytes, start, end);
  }

  private ReusableArtifactIndexBloomFilter loadFilter(ReusableArtifactIndexRunReference run) {
    validateFilterReference(run.getFilter());
    return ReusableArtifactIndexBloomFilter.parse(loadObject(run.getFilter()), run.getEntryCount());
  }

  private byte[] loadObject(ReusableArtifactIndexObjectReference reference) {
    validateObjectReference(reference);
    if (!reference.getInlinePayload().isEmpty()) {
      byte[] bytes = reference.getInlinePayload().toByteArray();
      validateObjectBytes(reference, bytes);
      return bytes;
    }
    byte[] cached = cached(reference);
    if (cached != null) {
      return cached;
    }
    byte[] bytes = blobStore.get(reference.getUri());
    validateObjectBytes(reference, bytes);
    cache(reference.getUri(), bytes);
    return bytes;
  }

  private void primeObjects(List<ReusableArtifactIndexObjectReference> references) {
    Map<String, ReusableArtifactIndexObjectReference> missingReferences = new LinkedHashMap<>();
    long missingBytes = 0L;
    for (ReusableArtifactIndexObjectReference reference : references) {
      validateObjectReference(reference);
      if (!reference.getInlinePayload().isEmpty()) {
        validateObjectBytes(reference, reference.getInlinePayload().toByteArray());
        continue;
      }
      if (isCached(reference.getUri()) || missingReferences.containsKey(reference.getUri())) {
        continue;
      }
      if (reference.getPayloadBytes() > MAX_CACHED_BYTES
          || reference.getPayloadBytes() > MAX_SINGLE_CACHED_OBJECT_BYTES) {
        continue;
      }
      if (reference.getPayloadBytes() > MAX_CACHED_BYTES - missingBytes) {
        loadObjectBatch(missingReferences);
        missingReferences.clear();
        missingBytes = 0L;
      }
      missingReferences.put(reference.getUri(), reference);
      missingBytes += reference.getPayloadBytes();
    }
    loadObjectBatch(missingReferences);
  }

  private void loadObjectBatch(
      Map<String, ReusableArtifactIndexObjectReference> missingReferences) {
    List<String> missing = List.copyOf(missingReferences.keySet());
    if (missing.isEmpty()) {
      return;
    }
    Map<String, byte[]> loaded = blobStore.getBatch(missing);
    for (String uri : missing) {
      byte[] bytes = loaded.get(uri);
      if (bytes == null) {
        throw new StorageNotFoundException("reusable artifact index object is missing: " + uri);
      }
      validateObjectBytes(missingReferences.get(uri), bytes);
      cache(uri, bytes);
    }
  }

  private void authenticateExternalObject(ReusableArtifactIndexObjectReference reference) {
    validateObjectReference(reference);
    if (!reference.getInlinePayload().isEmpty()) {
      validateObjectBytes(reference, reference.getInlinePayload().toByteArray());
      return;
    }
    var header =
        blobStore
            .head(reference.getUri())
            .orElseThrow(
                () ->
                    new StorageNotFoundException(
                        "reusable artifact index pack is missing: " + reference.getUri()));
    if (header.getContentLength() != reference.getPayloadBytes()) {
      throw new IllegalArgumentException(
          "reusable artifact index pack size does not match its reference");
    }
    MessageDigest digest = sha256Digest();
    long offset = 0L;
    while (offset < reference.getPayloadBytes()) {
      int length =
          (int) Math.min(OBJECT_AUTHENTICATION_WINDOW_BYTES, reference.getPayloadBytes() - offset);
      byte[] bytes = blobStore.getRange(reference.getUri(), offset, length);
      if (bytes == null) {
        throw new StorageNotFoundException(
            "reusable artifact index pack is missing: " + reference.getUri());
      }
      if (bytes.length != length) {
        throw new IllegalArgumentException(
            "reusable artifact index pack range size does not match its reference");
      }
      digest.update(bytes);
      offset = Math.addExact(offset, length);
    }
    if (!MessageDigest.isEqual(digest.digest(), reference.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException(
          "reusable artifact index pack sha256 does not match its reference");
    }
  }

  private static ReusableArtifactIndexBlockReference findBlock(
      ReusableArtifactIndexRunManifest manifest, byte[] keyHash) {
    int low = 0;
    int high = manifest.getBlocksCount() - 1;
    while (low <= high) {
      int middle = (low + high) >>> 1;
      ReusableArtifactIndexBlockReference block = manifest.getBlocks(middle);
      if (compareUnsigned(keyHash, block.getFirstKeySha256().toByteArray()) < 0) {
        high = middle - 1;
      } else if (compareUnsigned(keyHash, block.getLastKeySha256().toByteArray()) > 0) {
        low = middle + 1;
      } else {
        return block;
      }
    }
    return null;
  }

  private static List<ReusableArtifactBundleReference> bundlesFromEntries(
      Collection<ReusableArtifactIndexEntry> entries) {
    Map<String, ReusableArtifactBundleReference.Builder> bundles = new TreeMap<>();
    for (ReusableArtifactIndexEntry entry : entries) {
      StatsObjectDescriptor artifact = entry.getArtifact();
      ReusableArtifactBundleReference.Builder bundle =
          bundles.computeIfAbsent(
              artifact.getPayloadUri(), ignored -> ReusableArtifactBundleReference.newBuilder());
      if (bundle.hasArtifact() && !bundle.getArtifact().equals(artifact)) {
        throw new IllegalArgumentException("reusable artifact index contains conflicting bundles");
      }
      bundle.setArtifact(artifact);
      if (entry.hasFileStats()) {
        bundle.addFileStats(entry.getFileStats());
      } else {
        bundle.addIndexArtifacts(entry.getIndexArtifact());
      }
    }
    return bundles.values().stream().map(ReusableArtifactBundleReference.Builder::build).toList();
  }

  private static ReusableArtifactIndexReference buildReference(
      List<ReusableArtifactIndexRunReference> runs, int stats, int indexes) {
    return ReusableArtifactIndexReference.newBuilder()
        .setFormatVersion(FORMAT_VERSION)
        .setFileStatsRecordCount(stats)
        .setIndexArtifactCount(indexes)
        .addAllRuns(orderedRuns(runs))
        .build();
  }

  private static List<ReusableArtifactIndexRunReference> orderedRuns(
      Collection<ReusableArtifactIndexRunReference> runs) {
    return runs.stream()
        .sorted(
            Comparator.comparingInt(ReusableArtifactIndexRunReference::getLevel)
                .thenComparing(run -> objectIdentity(run.getManifest())))
        .toList();
  }

  private static List<IndexedEntry> indexEntries(Collection<ReusableArtifactIndexEntry> entries) {
    List<IndexedEntry> indexed =
        entries.stream()
            .map(ReusableArtifactIndexStore::indexed)
            .sorted(INDEXED_ENTRY_ORDER)
            .toList();
    for (int index = 1; index < indexed.size(); index++) {
      if (indexed.get(index - 1).key().equals(indexed.get(index).key())) {
        throw new IllegalArgumentException("reusable artifact index block contains a duplicate");
      }
    }
    return indexed;
  }

  private static IndexedEntry indexed(ReusableArtifactIndexEntry entry) {
    validateEntry(entry);
    String key = entryKey(entry);
    return new IndexedEntry(key, hash(key), entry);
  }

  private static String encodePageToken(String key) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(key.getBytes(StandardCharsets.UTF_8));
  }

  private static IndexedEntry decodePageToken(String token) {
    if (token == null || token.isBlank()) {
      return null;
    }
    try {
      String key = new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
      if (!key.startsWith("stats\u0000") && !key.startsWith("index\u0000")) {
        throw new IllegalArgumentException("invalid reusable artifact page token");
      }
      return new IndexedEntry(key, hash(key), null);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("invalid reusable artifact page token", e);
    }
  }

  private static boolean acceptObject(
      ReusableArtifactIndexObjectReference reference,
      Set<String> visited,
      Consumer<ReusableArtifactIndexObjectReference> consumer) {
    String identity = objectIdentity(reference);
    if (visited.contains(identity)) {
      return false;
    }
    consumer.accept(reference);
    visited.add(identity);
    return true;
  }

  private static String objectIdentity(ReusableArtifactIndexObjectReference reference) {
    return reference.getUri().isEmpty()
        ? "inline:" + HexFormat.of().formatHex(reference.getPayloadSha256().toByteArray())
        : "uri:" + reference.getUri();
  }

  private static BlockIdentity blockIdentity(ReusableArtifactIndexBlockReference reference) {
    return new BlockIdentity(
        objectIdentity(reference.getObject()), reference.getOffset(), reference.getLength());
  }

  private static void validateOwnedObject(
      String prefix, ReusableArtifactIndexObjectReference reference) {
    if (prefix != null
        && reference.getInlinePayload().isEmpty()
        && !reference.getUri().startsWith(prefix)) {
      throw new IllegalArgumentException("reusable artifact index object belongs to another table");
    }
  }

  private static void validateRunReference(ReusableArtifactIndexRunReference run) {
    if (run == null
        || !run.hasManifest()
        || !run.hasFilter()
        || run.getEntryCount() <= 0L
        || run.getLevel() < 0
        || run.getLevel() > MAX_RUN_LEVEL) {
      throw new IllegalArgumentException("reusable artifact run reference is invalid");
    }
    validateObjectReference(run.getManifest());
    validateObjectReference(run.getFilter());
    if (run.getManifest().getPayloadBytes() > MAX_RUN_MANIFEST_BYTES) {
      throw new IllegalArgumentException("reusable artifact run manifest is too large");
    }
    validateFilterReference(run.getFilter());
    if ((long) run.getFileStatsRecordCount() + run.getIndexArtifactCount() != run.getEntryCount()) {
      throw new IllegalArgumentException("reusable artifact run count is invalid");
    }
  }

  private static void validateRunManifest(
      ReusableArtifactIndexRunManifest manifest, long expectedEntries) {
    if (manifest.getFormatVersion() != FORMAT_VERSION) {
      throw new IllegalArgumentException("reusable artifact run manifest format is invalid");
    }
    if (manifest.getBlocksList().isEmpty()) {
      throw new IllegalArgumentException("reusable artifact run manifest shape is invalid");
    }
    long entries = 0L;
    byte[] previous = null;
    for (ReusableArtifactIndexBlockReference block : manifest.getBlocksList()) {
      validateBlockReference(block);
      if (previous != null
          && compareUnsigned(previous, block.getFirstKeySha256().toByteArray()) >= 0) {
        throw new IllegalArgumentException("reusable artifact run blocks are not sorted");
      }
      previous = block.getLastKeySha256().toByteArray();
      entries = Math.addExact(entries, block.getEntryCount());
    }
    if (entries != expectedEntries) {
      throw new IllegalArgumentException("reusable artifact run manifest count mismatch");
    }
  }

  private static void validateBlockReference(ReusableArtifactIndexBlockReference reference) {
    if (reference == null
        || !reference.hasObject()
        || reference.getEntryCount() <= 0L
        || reference.getLength() <= 0
        || reference.getBlockSha256().size() != 32
        || reference.getFirstKeySha256().size() != 32
        || reference.getLastKeySha256().size() != 32
        || compareUnsigned(
                reference.getFirstKeySha256().toByteArray(),
                reference.getLastKeySha256().toByteArray())
            > 0) {
      throw new IllegalArgumentException("reusable artifact block reference is invalid");
    }
    validateObjectReference(reference.getObject());
    if (reference.getObject().getPayloadBytes() > TARGET_PACK_BYTES) {
      throw new IllegalArgumentException("reusable artifact index pack is too large");
    }
    long length = Integer.toUnsignedLong(reference.getLength());
    if (reference.getOffset() < 0
        || reference.getOffset() > reference.getObject().getPayloadBytes()
        || length > reference.getObject().getPayloadBytes() - reference.getOffset()) {
      throw new IllegalArgumentException("reusable artifact block range is invalid");
    }
  }

  private static void validateObjectReference(ReusableArtifactIndexObjectReference reference) {
    boolean inline = reference != null && !reference.getInlinePayload().isEmpty();
    if (reference == null
        || inline == !reference.getUri().isBlank()
        || reference.getPayloadBytes() <= 0L
        || reference.getPayloadSha256().size() != 32
        || (inline && reference.getInlinePayload().size() != reference.getPayloadBytes())) {
      throw new IllegalArgumentException("reusable artifact index object reference is invalid");
    }
  }

  private static void validateFilterReference(ReusableArtifactIndexObjectReference reference) {
    validateObjectReference(reference);
    if (reference.getPayloadBytes() > MAX_BLOOM_OBJECT_BYTES) {
      throw new IllegalArgumentException("reusable artifact Bloom filter is too large");
    }
  }

  private static void validateObjectBytes(
      ReusableArtifactIndexObjectReference reference, byte[] bytes) {
    if (bytes == null) {
      throw new StorageNotFoundException(
          "reusable artifact index object is missing: " + reference.getUri());
    }
    if (bytes.length != reference.getPayloadBytes()
        || !MessageDigest.isEqual(sha256(bytes), reference.getPayloadSha256().toByteArray())) {
      throw new IllegalArgumentException("reusable artifact index object metadata mismatch");
    }
  }

  private static void validateEntry(ReusableArtifactIndexEntry entry) {
    if (entry == null || !entry.hasArtifact()) {
      throw new IllegalArgumentException("reusable artifact index entry is invalid");
    }
    validateArtifact(entry.getArtifact());
    if (entry.hasFileStats()) {
      var metadata = entry.getFileStats();
      if (metadata.getFilePath().isBlank()
          || metadata.getSourceFingerprint().isBlank()
          || metadata.getStatsCaptureSignature().isBlank()) {
        throw new IllegalArgumentException("reusable stats index metadata is invalid");
      }
    } else if (entry.hasIndexArtifact()) {
      var metadata = entry.getIndexArtifact();
      if (metadata.getFilePath().isBlank()
          || metadata.getSourceFingerprint().isBlank()
          || metadata.getIndexCaptureSignature().isBlank()) {
        throw new IllegalArgumentException("reusable page-index metadata is invalid");
      }
    } else {
      throw new IllegalArgumentException("reusable artifact index entry metadata is required");
    }
  }

  private static void validateArtifact(StatsObjectDescriptor artifact) {
    if (artifact == null
        || artifact.getTargetStorageId().isBlank()
        || artifact.getPayloadUri().isBlank()
        || artifact.getPayloadBytes() <= 0L
        || artifact.getPayloadSha256().size() != 32) {
      throw new IllegalArgumentException("reusable artifact bundle identity is invalid");
    }
  }

  private static String entryKey(ReusableArtifactIndexEntry entry) {
    if (entry.hasFileStats()) {
      return statsKey(entry.getFileStats().getFilePath());
    }
    if (entry.hasIndexArtifact()) {
      return indexKey(entry.getIndexArtifact().getFilePath());
    }
    throw new IllegalArgumentException("reusable artifact index entry metadata is required");
  }

  private static String statsKey(String path) {
    return "stats\u0000" + requirePath(path);
  }

  private static String indexKey(String path) {
    return "index\u0000" + requirePath(path);
  }

  private static String requirePath(String path) {
    String effective = path == null ? "" : path;
    if (effective.isBlank() || !effective.equals(effective.trim())) {
      throw new IllegalArgumentException("artifact index file path is required");
    }
    return effective;
  }

  private static String normalizePrefix(String prefix) {
    String effective = prefix == null ? "" : prefix.trim();
    if (effective.isBlank()) {
      throw new IllegalArgumentException("artifact index object prefix is required");
    }
    return effective.endsWith("/") ? effective : effective + "/";
  }

  private static ReusableArtifactIndexReference effectiveReference(
      ReusableArtifactIndexReference reference) {
    return reference == null
            || reference.equals(ReusableArtifactIndexReference.getDefaultInstance())
        ? emptyReference()
        : reference;
  }

  static void clearSharedCacheForTests() {
    SHARED_OBJECT_CACHE.clear();
  }

  private static void cache(String uri, byte[] bytes) {
    SHARED_OBJECT_CACHE.put(uri, bytes);
  }

  private static byte[] cached(ReusableArtifactIndexObjectReference reference) {
    return SHARED_OBJECT_CACHE.get(reference);
  }

  private static boolean isCached(String uri) {
    return SHARED_OBJECT_CACHE.contains(uri);
  }

  private static byte[] hash(String value) {
    return sha256(value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] sha256(byte[] bytes) {
    return sha256Digest().digest(bytes);
  }

  private static MessageDigest sha256Digest() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    return Arrays.compareUnsigned(left, right);
  }

  private record IndexedEntry(String key, byte[] keyHash, ReusableArtifactIndexEntry entry) {}

  private record PendingBlock(
      byte[] bytes, byte[] firstKeyHash, byte[] lastKeyHash, int entryCount, byte[] digest) {}

  private record PendingBlockMetadata(
      int length, byte[] firstKeyHash, byte[] lastKeyHash, int entryCount, byte[] digest) {}

  private record BlockIdentity(String objectIdentity, long offset, int length) {}

  private final class RunCursor {
    private final ReusableArtifactIndexRunManifest manifest;
    private final int readWindowBytes;
    private int blockIndex;
    private int entryIndex;
    private ReusableArtifactIndexBlock block;
    private IndexedEntry current;
    private String loadedPackIdentity;
    private long loadedWindowOffset;
    private byte[] loadedWindow;

    private RunCursor(ReusableArtifactIndexRunManifest manifest, int readWindowBytes) {
      this.manifest = manifest;
      this.readWindowBytes = readWindowBytes;
      advance();
    }

    private RunCursor(
        ReusableArtifactIndexRunManifest manifest, IndexedEntry after, int readWindowBytes) {
      this.manifest = manifest;
      this.readWindowBytes = readWindowBytes;
      if (after != null) {
        while (blockIndex < manifest.getBlocksCount()
            && compareUnsigned(
                    manifest.getBlocks(blockIndex).getLastKeySha256().toByteArray(),
                    after.keyHash())
                < 0) {
          blockIndex++;
        }
      }
      advance();
      while (current != null && after != null && INDEXED_ENTRY_ORDER.compare(current, after) <= 0) {
        advance();
      }
    }

    private IndexedEntry current() {
      return current;
    }

    private void advance() {
      while (block == null || entryIndex >= block.getEntriesCount()) {
        if (blockIndex >= manifest.getBlocksCount()) {
          current = null;
          block = null;
          return;
        }
        ReusableArtifactIndexBlockReference reference = manifest.getBlocks(blockIndex++);
        String packIdentity = objectIdentity(reference.getObject());
        long blockEnd = Math.addExact(reference.getOffset(), reference.getLength());
        long windowEnd =
            loadedWindow == null ? 0L : Math.addExact(loadedWindowOffset, loadedWindow.length);
        if (!packIdentity.equals(loadedPackIdentity)
            || reference.getOffset() < loadedWindowOffset
            || blockEnd > windowEnd) {
          ReusableArtifactIndexObjectReference object = reference.getObject();
          loadedWindowOffset = object.getInlinePayload().isEmpty() ? reference.getOffset() : 0L;
          if (object.getInlinePayload().isEmpty()) {
            long remaining = object.getPayloadBytes() - loadedWindowOffset;
            int length =
                Math.toIntExact(
                    Math.min(remaining, Math.max(reference.getLength(), readWindowBytes)));
            loadedWindow = blobStore.getRange(object.getUri(), loadedWindowOffset, length);
            if (loadedWindow == null) {
              throw new StorageNotFoundException(
                  "reusable artifact index pack is missing: " + object.getUri());
            }
          } else {
            loadedWindow = object.getInlinePayload().toByteArray();
            validateObjectBytes(object, loadedWindow);
          }
          loadedPackIdentity = packIdentity;
        }
        block = loadBlockFromWindow(reference, loadedWindowOffset, loadedWindow);
        entryIndex = 0;
      }
      current = indexed(block.getEntries(entryIndex++));
    }
  }

  private static int sequentialReadWindowBytes(int runCount) {
    if (runCount <= 0) {
      return TARGET_BLOCK_BYTES;
    }
    return Math.max(
        TARGET_BLOCK_BYTES,
        Math.min(MAX_SEQUENTIAL_READ_WINDOW_BYTES, MAX_SEQUENTIAL_READ_BUDGET_BYTES / runCount));
  }

  static int bloomBitCount(int entryCount) {
    return ReusableArtifactIndexBloomFilter.bitCount(entryCount);
  }
}
