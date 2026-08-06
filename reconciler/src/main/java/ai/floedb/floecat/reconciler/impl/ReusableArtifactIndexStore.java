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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

/** Persistent immutable sorted-run index for reusable file artifacts. */
public final class ReusableArtifactIndexStore {
  static final int FORMAT_VERSION = 1;
  static final int TARGET_BLOCK_BYTES = 512 * 1024;
  static final int MAX_L0_RUNS = 32;
  static final int MAX_L1_RUNS = 32;
  private static final int BLOOM_BITS_PER_ENTRY = 20;
  private static final int BLOOM_HASHES = 14;
  private static final int MIN_BLOOM_BITS = 1024;
  private static final int MAX_CACHED_OBJECTS = 8_192;
  private static final String PROTOBUF_CONTENT_TYPE = "application/x-protobuf";
  private static final String FILTER_CONTENT_TYPE = "application/x-floecat-bloom-filter";

  private static final Map<String, byte[]> SHARED_OBJECT_CACHE =
      Collections.synchronizedMap(
          new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, byte[]> eldest) {
              return size() > MAX_CACHED_OBJECTS;
            }
          });

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

  /** Adds one immutable delta run and performs bounded low-level compaction. */
  public ReusableArtifactIndexReference append(
      String objectPrefix,
      ReusableArtifactIndexReference base,
      List<ReusableArtifactBundleReference> bundles) {
    String prefix = normalizePrefix(objectPrefix);
    ReusableArtifactIndexReference effectiveBase = effectiveReference(base);
    validateReference(effectiveBase);
    validateOwnedRuns(prefix, effectiveBase);

    List<ReusableArtifactIndexEntry> additions = entriesFromBundles(bundles);
    if (additions.isEmpty()) {
      return effectiveBase;
    }
    List<IndexedEntry> indexed = indexEntries(additions);
    Set<String> additionKeys = new HashSet<>();
    for (IndexedEntry entry : indexed) {
      if (!additionKeys.add(entry.key())) {
        throw new IllegalArgumentException(
            "duplicate reusable artifact index entry " + entry.key());
      }
    }
    Set<String> existing = lookupKeys(effectiveBase, additionKeys).keySet();
    if (!existing.isEmpty()) {
      throw new IllegalArgumentException(
          "reusable artifact index already contains " + existing.iterator().next());
    }

    List<ReusableArtifactIndexRunReference> runs = new ArrayList<>(effectiveBase.getRunsList());
    runs.add(writeRun(prefix, 0, indexed));
    runs = compactBoundedLevels(prefix, runs);

    long fileStats =
        Math.addExact(
            Integer.toUnsignedLong(effectiveBase.getFileStatsRecordCount()),
            additions.stream().filter(ReusableArtifactIndexEntry::hasFileStats).count());
    long indexes =
        Math.addExact(
            Integer.toUnsignedLong(effectiveBase.getIndexArtifactCount()),
            additions.stream().filter(ReusableArtifactIndexEntry::hasIndexArtifact).count());
    if (fileStats > Integer.MAX_VALUE || indexes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("reusable artifact index count exceeds supported range");
    }
    return buildReference(runs, (int) fileStats, (int) indexes);
  }

  public boolean containsFileStats(ReusableArtifactIndexReference index, String filePath) {
    return !lookupKeys(index, Set.of(statsKey(filePath))).isEmpty();
  }

  public boolean containsIndexArtifact(ReusableArtifactIndexReference index, String filePath) {
    return !lookupKeys(index, Set.of(indexKey(filePath))).isEmpty();
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
    for (ReusableArtifactIndexRunReference run : orderedRuns(effective.getRunsList())) {
      ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
      for (ReusableArtifactIndexBlockReference block : manifest.getBlocksList()) {
        ReusableArtifactIndexBlock loaded = loadBlock(block);
        for (ReusableArtifactIndexEntry entry : loaded.getEntriesList()) {
          counts[entry.hasFileStats() ? 0 : 1]++;
          consumer.accept(entry);
        }
      }
    }
    if (counts[0] != effective.getFileStatsRecordCount()
        || counts[1] != effective.getIndexArtifactCount()) {
      throw new IllegalArgumentException("reusable artifact run index kind count mismatch");
    }
  }

  public List<ReusableArtifactIndexEntry> loadEntries(ReusableArtifactIndexReference index) {
    List<ReusableArtifactIndexEntry> entries = new ArrayList<>();
    forEachEntry(index, entries::add);
    entries.sort(Comparator.comparing(ReusableArtifactIndexStore::entryKey));
    return List.copyOf(entries);
  }

  public List<ReusableArtifactBundleReference> loadBundles(ReusableArtifactIndexReference index) {
    List<ReusableArtifactIndexEntry> entries = new ArrayList<>();
    forEachEntry(index, entries::add);
    return bundlesFromEntries(entries);
  }

  /** Roots immutable run manifests, filters, blocks, and referenced artifact bundles. */
  public void walkReachable(
      ReusableArtifactIndexReference index,
      Set<String> visitedObjectUris,
      Consumer<ReusableArtifactIndexObjectReference> objectConsumer,
      Consumer<ReusableArtifactIndexEntry> entryConsumer) {
    ReusableArtifactIndexReference effective = effectiveReference(index);
    validateReference(effective);
    if (visitedObjectUris == null || objectConsumer == null || entryConsumer == null) {
      throw new IllegalArgumentException("reusable artifact index walk arguments are required");
    }
    for (ReusableArtifactIndexRunReference run : effective.getRunsList()) {
      acceptObject(run.getFilter(), visitedObjectUris, objectConsumer);
      boolean newManifest = acceptObject(run.getManifest(), visitedObjectUris, objectConsumer);
      if (!newManifest) {
        continue;
      }
      ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
      for (ReusableArtifactIndexBlockReference block : manifest.getBlocksList()) {
        boolean newBlock = acceptObject(block.getObject(), visitedObjectUris, objectConsumer);
        if (newBlock) {
          loadBlock(block).getEntriesList().forEach(entryConsumer);
        }
      }
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
    for (ReusableArtifactIndexRunReference run : effective.getRunsList()) {
      validateRunReference(run);
      if (!manifests.add(run.getManifest().getUri())) {
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
    primeObjects(runs.stream().map(ReusableArtifactIndexRunReference::getFilter).toList());
    primeObjects(runs.stream().map(ReusableArtifactIndexRunReference::getManifest).toList());
    Map<String, ReusableArtifactIndexRunManifest> manifests = new HashMap<>();
    for (ReusableArtifactIndexRunReference run : runs) {
      manifests.put(run.getManifest().getUri(), loadRunManifest(run));
    }
    List<CandidateRun> candidateRuns = new ArrayList<>();
    for (ReusableArtifactIndexRunReference run : runs) {
      BloomFilter filter = loadFilter(run);
      Map<String, byte[]> candidates = new LinkedHashMap<>();
      keyHashes.forEach(
          (key, digest) -> {
            if (filter.mightContain(digest)) {
              candidates.put(key, digest);
            }
          });
      if (!candidates.isEmpty()) {
        candidateRuns.add(new CandidateRun(run, candidates));
      }
    }
    Map<String, List<String>> blockKeys = new LinkedHashMap<>();
    Map<String, ReusableArtifactIndexBlockReference> blocks = new LinkedHashMap<>();
    for (CandidateRun candidateRun : candidateRuns) {
      ReusableArtifactIndexRunManifest manifest =
          manifests.get(candidateRun.run().getManifest().getUri());
      candidateRun
          .keys()
          .forEach(
              (key, digest) -> {
                ReusableArtifactIndexBlockReference block = findBlock(manifest, digest);
                if (block != null) {
                  blocks.put(block.getObject().getUri(), block);
                  blockKeys
                      .computeIfAbsent(block.getObject().getUri(), ignored -> new ArrayList<>())
                      .add(key);
                }
              });
    }
    primeObjects(
        blocks.values().stream().map(ReusableArtifactIndexBlockReference::getObject).toList());
    for (Map.Entry<String, List<String>> selected : blockKeys.entrySet()) {
      ReusableArtifactIndexBlock block = loadBlock(blocks.get(selected.getKey()));
      Map<String, ReusableArtifactIndexEntry> byKey = new HashMap<>();
      for (ReusableArtifactIndexEntry entry : block.getEntriesList()) {
        byKey.put(entryKey(entry), entry);
      }
      for (String key : selected.getValue()) {
        ReusableArtifactIndexEntry entry = byKey.get(key);
        if (entry != null) {
          found.put(key, entry);
        }
      }
    }
    return Map.copyOf(found);
  }

  private List<ReusableArtifactIndexRunReference> compactBoundedLevels(
      String prefix, List<ReusableArtifactIndexRunReference> source) {
    List<ReusableArtifactIndexRunReference> runs = new ArrayList<>(source);
    compactLevel(prefix, runs, 0, MAX_L0_RUNS);
    compactLevel(prefix, runs, 1, MAX_L1_RUNS);
    return List.copyOf(runs);
  }

  private void compactLevel(
      String prefix, List<ReusableArtifactIndexRunReference> runs, int level, int maximum) {
    List<ReusableArtifactIndexRunReference> selected =
        runs.stream().filter(run -> run.getLevel() == level).toList();
    if (selected.size() <= maximum) {
      return;
    }
    List<IndexedEntry> entries = new ArrayList<>();
    Set<String> keys = new HashSet<>();
    for (ReusableArtifactIndexRunReference run : selected) {
      ReusableArtifactIndexRunManifest manifest = loadRunManifest(run);
      for (ReusableArtifactIndexBlockReference block : manifest.getBlocksList()) {
        for (ReusableArtifactIndexEntry entry : loadBlock(block).getEntriesList()) {
          IndexedEntry indexed = indexed(entry);
          if (!keys.add(indexed.key())) {
            throw new IllegalArgumentException("reusable artifact compaction found a duplicate");
          }
          entries.add(indexed);
        }
      }
    }
    entries.sort(INDEXED_ENTRY_ORDER);
    runs.removeAll(selected);
    runs.add(writeRun(prefix, level + 1, entries));
  }

  private ReusableArtifactIndexRunReference writeRun(
      String prefix, int level, List<IndexedEntry> entries) {
    if (entries.isEmpty()) {
      throw new IllegalArgumentException("cannot write an empty reusable artifact run");
    }
    List<ReusableArtifactIndexBlockReference> blocks = new ArrayList<>();
    List<ReusableArtifactIndexEntry> pending = new ArrayList<>();
    int pendingBytes = 8;
    byte[] previousHash = null;
    for (IndexedEntry indexed : entries) {
      int entryBytes = indexed.entry().getSerializedSize() + 8;
      if (!pending.isEmpty()
          && pendingBytes + entryBytes > TARGET_BLOCK_BYTES
          && compareUnsigned(previousHash, indexed.keyHash()) != 0) {
        blocks.add(writeBlock(prefix, pending));
        pending = new ArrayList<>();
        pendingBytes = 8;
      }
      pending.add(indexed.entry());
      pendingBytes = Math.addExact(pendingBytes, entryBytes);
      previousHash = indexed.keyHash();
    }
    if (!pending.isEmpty()) {
      blocks.add(writeBlock(prefix, pending));
    }
    ReusableArtifactIndexRunManifest manifest =
        ReusableArtifactIndexRunManifest.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .addAllBlocks(blocks)
            .build();
    ReusableArtifactIndexObjectReference manifestRef =
        writeObject(
            prefix + "run-manifests/", ".pb", manifest.toByteArray(), PROTOBUF_CONTENT_TYPE);
    BloomFilter filter = BloomFilter.create(entries.stream().map(IndexedEntry::keyHash).toList());
    ReusableArtifactIndexObjectReference filterRef =
        writeObject(prefix + "filters/", ".bf", filter.bytes(), FILTER_CONTENT_TYPE);
    long stats = entries.stream().filter(entry -> entry.entry().hasFileStats()).count();
    long indexes = entries.size() - stats;
    return ReusableArtifactIndexRunReference.newBuilder()
        .setLevel(level)
        .setManifest(manifestRef)
        .setFilter(filterRef)
        .setEntryCount(entries.size())
        .setFileStatsRecordCount(Math.toIntExact(stats))
        .setIndexArtifactCount(Math.toIntExact(indexes))
        .build();
  }

  private ReusableArtifactIndexBlockReference writeBlock(
      String prefix, List<ReusableArtifactIndexEntry> entries) {
    List<IndexedEntry> indexed = indexEntries(entries);
    ReusableArtifactIndexBlock block =
        ReusableArtifactIndexBlock.newBuilder()
            .setFormatVersion(FORMAT_VERSION)
            .addAllEntries(indexed.stream().map(IndexedEntry::entry).toList())
            .build();
    ReusableArtifactIndexObjectReference object =
        writeObject(prefix + "blocks/", ".pb", block.toByteArray(), PROTOBUF_CONTENT_TYPE);
    return ReusableArtifactIndexBlockReference.newBuilder()
        .setObject(object)
        .setFirstKeySha256(ByteString.copyFrom(indexed.getFirst().keyHash()))
        .setLastKeySha256(ByteString.copyFrom(indexed.getLast().keyHash()))
        .setEntryCount(indexed.size())
        .build();
  }

  private ReusableArtifactIndexObjectReference writeObject(
      String prefix, String suffix, byte[] bytes, String contentType) {
    byte[] digest = sha256(bytes);
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
    byte[] bytes = loadObject(reference.getObject());
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

  private BloomFilter loadFilter(ReusableArtifactIndexRunReference run) {
    return BloomFilter.parse(loadObject(run.getFilter()), run.getEntryCount());
  }

  private byte[] loadObject(ReusableArtifactIndexObjectReference reference) {
    validateObjectReference(reference);
    byte[] cached = SHARED_OBJECT_CACHE.get(reference.getUri());
    if (cached != null) {
      validateObjectBytes(reference, cached);
      return cached.clone();
    }
    byte[] bytes = blobStore.get(reference.getUri());
    validateObjectBytes(reference, bytes);
    cache(reference.getUri(), bytes);
    return bytes;
  }

  private void primeObjects(List<ReusableArtifactIndexObjectReference> references) {
    List<String> missing =
        references.stream()
            .map(ReusableArtifactIndexObjectReference::getUri)
            .distinct()
            .filter(uri -> !SHARED_OBJECT_CACHE.containsKey(uri))
            .toList();
    if (missing.isEmpty()) {
      return;
    }
    Map<String, byte[]> loaded = blobStore.getBatch(missing);
    Map<String, ReusableArtifactIndexObjectReference> byUri = new HashMap<>();
    references.forEach(reference -> byUri.put(reference.getUri(), reference));
    for (String uri : missing) {
      byte[] bytes = loaded.get(uri);
      if (bytes == null) {
        throw new StorageNotFoundException("reusable artifact index object is missing: " + uri);
      }
      validateObjectBytes(byUri.get(uri), bytes);
      cache(uri, bytes);
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
                .thenComparing(run -> run.getManifest().getUri()))
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

  private static boolean acceptObject(
      ReusableArtifactIndexObjectReference reference,
      Set<String> visited,
      Consumer<ReusableArtifactIndexObjectReference> consumer) {
    if (!visited.add(reference.getUri())) {
      return false;
    }
    consumer.accept(reference);
    return true;
  }

  private static void validateOwnedRuns(String prefix, ReusableArtifactIndexReference reference) {
    for (ReusableArtifactIndexRunReference run : reference.getRunsList()) {
      if (!run.getManifest().getUri().startsWith(prefix)
          || !run.getFilter().getUri().startsWith(prefix)) {
        throw new IllegalArgumentException("reusable artifact run belongs to another table");
      }
    }
  }

  private static void validateRunReference(ReusableArtifactIndexRunReference run) {
    if (run == null || !run.hasManifest() || !run.hasFilter() || run.getEntryCount() <= 0L) {
      throw new IllegalArgumentException("reusable artifact run reference is invalid");
    }
    validateObjectReference(run.getManifest());
    validateObjectReference(run.getFilter());
    if ((long) run.getFileStatsRecordCount() + run.getIndexArtifactCount() != run.getEntryCount()) {
      throw new IllegalArgumentException("reusable artifact run count is invalid");
    }
  }

  private static void validateRunManifest(
      ReusableArtifactIndexRunManifest manifest, long expectedEntries) {
    if (manifest.getFormatVersion() != FORMAT_VERSION || manifest.getBlocksList().isEmpty()) {
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
        || reference.getFirstKeySha256().size() != 32
        || reference.getLastKeySha256().size() != 32
        || compareUnsigned(
                reference.getFirstKeySha256().toByteArray(),
                reference.getLastKeySha256().toByteArray())
            > 0) {
      throw new IllegalArgumentException("reusable artifact block reference is invalid");
    }
    validateObjectReference(reference.getObject());
  }

  private static void validateObjectReference(ReusableArtifactIndexObjectReference reference) {
    if (reference == null
        || reference.getUri().isBlank()
        || reference.getPayloadBytes() <= 0L
        || reference.getPayloadSha256().size() != 32) {
      throw new IllegalArgumentException("reusable artifact index object reference is invalid");
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
    SHARED_OBJECT_CACHE.put(uri, bytes.clone());
  }

  private static byte[] hash(String value) {
    return sha256(value.getBytes(StandardCharsets.UTF_8));
  }

  private static byte[] sha256(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException error) {
      throw new IllegalStateException("SHA-256 unavailable", error);
    }
  }

  private static int compareUnsigned(byte[] left, byte[] right) {
    return Arrays.compareUnsigned(left, right);
  }

  private record IndexedEntry(String key, byte[] keyHash, ReusableArtifactIndexEntry entry) {}

  private record CandidateRun(ReusableArtifactIndexRunReference run, Map<String, byte[]> keys) {}

  private record BloomFilter(int bitCount, int hashes, int entryCount, byte[] bits) {
    static BloomFilter create(List<byte[]> digests) {
      int bitCount =
          Math.max(MIN_BLOOM_BITS, Math.multiplyExact(digests.size(), BLOOM_BITS_PER_ENTRY));
      bitCount = Math.ceilDiv(bitCount, 64) * 64;
      byte[] bits = new byte[Math.ceilDiv(bitCount, 8)];
      BloomFilter filter = new BloomFilter(bitCount, BLOOM_HASHES, digests.size(), bits);
      digests.forEach(filter::add);
      return filter;
    }

    static BloomFilter parse(byte[] bytes, long expectedEntries) {
      if (bytes == null || bytes.length < 9) {
        throw new IllegalArgumentException("reusable artifact Bloom filter is invalid");
      }
      ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
      int bitCount = input.getInt();
      int hashes = Byte.toUnsignedInt(input.get());
      int entryCount = input.getInt();
      byte[] bits = new byte[input.remaining()];
      input.get(bits);
      if (bitCount <= 0
          || bitCount % 64 != 0
          || hashes <= 0
          || bits.length != Math.ceilDiv(bitCount, 8)
          || Integer.toUnsignedLong(entryCount) != expectedEntries) {
        throw new IllegalArgumentException("reusable artifact Bloom filter shape is invalid");
      }
      return new BloomFilter(bitCount, hashes, entryCount, bits);
    }

    byte[] bytes() {
      return ByteBuffer.allocate(9 + bits.length)
          .order(ByteOrder.BIG_ENDIAN)
          .putInt(bitCount)
          .put((byte) hashes)
          .putInt(entryCount)
          .put(bits)
          .array();
    }

    private void add(byte[] digest) {
      long first = longAt(digest, 0);
      long second = longAt(digest, 8) | 1L;
      for (int index = 0; index < hashes; index++) {
        int bit = (int) Long.remainderUnsigned(first + index * second, bitCount);
        bits[bit >>> 3] |= (byte) (1 << (bit & 7));
      }
    }

    boolean mightContain(byte[] digest) {
      long first = longAt(digest, 0);
      long second = longAt(digest, 8) | 1L;
      for (int index = 0; index < hashes; index++) {
        int bit = (int) Long.remainderUnsigned(first + index * second, bitCount);
        if ((bits[bit >>> 3] & (1 << (bit & 7))) == 0) {
          return false;
        }
      }
      return true;
    }

    private static long longAt(byte[] digest, int offset) {
      return ByteBuffer.wrap(digest, offset, Long.BYTES).order(ByteOrder.BIG_ENDIAN).getLong();
    }
  }
}
