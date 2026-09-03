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

package ai.floedb.floecat.service.repo.cache;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.cache.CacheFamily;
import ai.floedb.floecat.cache.CacheWeights;
import ai.floedb.floecat.cache.CaffeineMemoryCache;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.concurrent.MetadataFanout;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * Storage owned by the pointer-store cache layer.
 *
 * <p>Complete SQL-addressing families live in one sorted map per account. The first read blocks
 * while that account's four durable subtrees are read consistently; only then can a missing key be
 * returned as authoritative absence. Writes maintain a complete map in place. Everything outside
 * those structurally recognised families uses the ordinary admission-controlled memory cache.
 *
 * <p>The complete maps and the resident remainder share one byte ceiling. Growing a complete map
 * shrinks the Caffeine remainder; if the map cannot fit, the account becomes {@link
 * Readiness#DEGRADED} and reads fall back to the store. Capacity can therefore cost latency, never
 * correctness. A degraded account retries after a cooldown; one caller reloads it while concurrent
 * callers keep using the store.
 */
public final class PointerCache {

  private static final int LOAD_PAGE_SIZE = 1_000;
  private static final long SORTED_MAP_NODE_BYTES = 48L;
  private static final String TOKEN_PREFIX = "pc1.";
  private static final Duration DEFAULT_DEGRADED_RETRY_DELAY = Duration.ofSeconds(30);

  enum Readiness {
    LOADING,
    RECOVERING,
    COMPLETE,
    DEGRADED
  }

  private final PointerStore source;
  private final long maxBytes;
  private final CacheEvents events;
  private final MetadataFanout loadFanout;
  private final long degradedRetryNanos;
  private final LongSupplier nanoTime;
  private final CaffeineMemoryCache<String, Pointer> resident;
  private final ConcurrentHashMap<String, Partition> partitions = new ConcurrentHashMap<>();
  private final AtomicLong completeBytes = new AtomicLong();
  private final AtomicLong completeEntries = new AtomicLong();
  private final Object budgetLock = new Object();

  public PointerCache(PointerStore authoritativeSource, long maxBytes, CacheEvents events) {
    this(
        authoritativeSource,
        maxBytes,
        events,
        MetadataFanout.serial(),
        DEFAULT_DEGRADED_RETRY_DELAY);
  }

  public PointerCache(
      PointerStore authoritativeSource,
      long maxBytes,
      CacheEvents events,
      MetadataFanout loadFanout) {
    this(authoritativeSource, maxBytes, events, loadFanout, DEFAULT_DEGRADED_RETRY_DELAY);
  }

  public PointerCache(
      PointerStore authoritativeSource,
      long maxBytes,
      CacheEvents events,
      MetadataFanout loadFanout,
      Duration degradedRetryDelay) {
    this(authoritativeSource, maxBytes, events, loadFanout, degradedRetryDelay, System::nanoTime);
  }

  PointerCache(
      PointerStore authoritativeSource,
      long maxBytes,
      CacheEvents events,
      MetadataFanout loadFanout,
      Duration degradedRetryDelay,
      LongSupplier nanoTime) {
    if (maxBytes <= 0L) {
      throw new IllegalArgumentException("pointer cache needs a positive byte budget");
    }
    if (degradedRetryDelay == null
        || degradedRetryDelay.isZero()
        || degradedRetryDelay.isNegative()) {
      throw new IllegalArgumentException("pointer cache degraded retry delay must be positive");
    }
    this.source = java.util.Objects.requireNonNull(authoritativeSource, "authoritativeSource");
    this.maxBytes = maxBytes;
    this.events = java.util.Objects.requireNonNull(events, "events");
    this.loadFanout = java.util.Objects.requireNonNull(loadFanout, "loadFanout");
    this.degradedRetryNanos = degradedRetryDelay.toNanos();
    this.nanoTime = java.util.Objects.requireNonNull(nanoTime, "nanoTime");
    this.resident =
        new CaffeineMemoryCache<>(CacheFamily.POINTER, maxBytes, key -> 2L * key.length(), events);
  }

  Optional<Pointer> get(String key) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match != null) {
      Partition partition = complete(match.partition());
      if (partition != null) {
        partition.lock.readLock().lock();
        try {
          return Optional.ofNullable(partition.entries.get(key));
        } finally {
          partition.lock.readLock().unlock();
        }
      }
    }
    return Optional.ofNullable(resident.get(key, k -> source.get(k).orElse(null)));
  }

  Map<String, Pointer> getBatch(List<String> keys) {
    if (keys == null || keys.isEmpty()) {
      return Map.of();
    }
    Map<String, Pointer> answer = new LinkedHashMap<>();
    List<String> fallback = new ArrayList<>();
    Map<String, Partition> ready = new LinkedHashMap<>();
    Set<String> attempted = new HashSet<>();
    for (String key : new java.util.LinkedHashSet<>(keys)) {
      IndexLayout.Match match = IndexLayout.match(key).orElse(null);
      if (match == null) {
        fallback.add(key);
        continue;
      }
      String partitionKey = match.partition();
      if (attempted.add(partitionKey)) {
        Partition partition = complete(partitionKey);
        if (partition != null) {
          ready.put(partitionKey, partition);
        }
      }
      Partition partition = ready.get(partitionKey);
      if (partition == null) {
        fallback.add(key);
        continue;
      }
      partition.lock.readLock().lock();
      try {
        Pointer pointer = partition.entries.get(key);
        if (pointer != null) {
          answer.put(key, pointer);
        }
      } finally {
        partition.lock.readLock().unlock();
      }
    }
    if (!fallback.isEmpty()) {
      answer.putAll(resident.getAll(fallback, misses -> source.getBatch(List.copyOf(misses))));
    }
    return Map.copyOf(answer);
  }

  List<Pointer> list(String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
    IndexLayout.Match match = IndexLayout.match(prefix).orElse(null);
    String cachedAfter = decodeToken(pageToken).orElse(null);
    if (match == null || (pageToken != null && !pageToken.isBlank() && cachedAfter == null)) {
      return source.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }
    Partition partition = complete(match.partition());
    if (partition == null) {
      return source.listPointersByPrefix(prefix, limit, tokenForSource(pageToken), nextTokenOut);
    }
    if (cachedAfter != null && !cachedAfter.startsWith(prefix)) {
      throw new IllegalArgumentException("bad page token");
    }

    partition.lock.readLock().lock();
    try {
      int pageSize = Math.max(1, limit);
      NavigableMap<String, Pointer> tail =
          cachedAfter == null
              ? partition.entries.tailMap(prefix, true)
              : partition.entries.tailMap(cachedAfter, false);
      List<Pointer> page = new ArrayList<>();
      boolean hasMore = false;
      for (Map.Entry<String, Pointer> entry : tail.entrySet()) {
        if (!entry.getKey().startsWith(prefix)) {
          break;
        }
        if (page.size() == pageSize) {
          hasMore = true;
          break;
        }
        page.add(entry.getValue());
      }
      setNextToken(nextTokenOut, hasMore ? encodeToken(page.getLast().getKey()) : "");
      return List.copyOf(page);
    } finally {
      partition.lock.readLock().unlock();
    }
  }

  int count(String prefix) {
    IndexLayout.Match match = IndexLayout.match(prefix).orElse(null);
    if (match == null) {
      return source.countByPrefix(prefix);
    }
    Partition partition = complete(match.partition());
    if (partition == null) {
      return source.countByPrefix(prefix);
    }
    partition.lock.readLock().lock();
    try {
      return countHeld(partition, prefix);
    } finally {
      partition.lock.readLock().unlock();
    }
  }

  String pageTokenAfterKey(String key) {
    return IndexLayout.match(key).isPresent() ? encodeToken(key) : source.pageTokenAfterKey(key);
  }

  String tokenForSource(String token) {
    return decodeToken(token).map(source::pageTokenAfterKey).orElse(token);
  }

  void repair(String key, Optional<Pointer> fresh) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match == null) {
      Optional<Pointer> cached = resident.peek(key);
      if (cached.isPresent() && fresh.isPresent()) {
        if (!cached.orElseThrow().equals(fresh.orElseThrow())) {
          resident.put(key, fresh.orElseThrow());
        }
      } else {
        // Also moves the read-through fence when the value is not visible yet, preventing an older
        // in-flight load from installing after this authoritative read.
        resident.evict(key);
      }
      return;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      resident.evict(key);
      return;
    }
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness == Readiness.COMPLETE) {
        replace(partition, key, fresh.orElse(null));
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  void repairPrefix(String prefix, String pageToken, List<Pointer> fresh, boolean exhausted) {
    IndexLayout.Match match = IndexLayout.match(prefix).orElse(null);
    if (match == null) {
      evictResidentPrefix(prefix, null);
      return;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      return;
    }
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness != Readiness.COMPLETE) {
        return;
      }
      String cachedAfter = decodeToken(pageToken).orElse(null);
      boolean startsAtPrefix = pageToken == null || pageToken.isBlank();
      if (!startsAtPrefix && cachedAfter == null) {
        // Native store tokens are opaque: without the preceding pointer key we cannot prove which
        // cached entries lie inside this page's authoritative interval. Future tokens emitted by
        // CachingPointerStore carry that boundary; an old or externally supplied native token
        // safely falls back to the store instead of leaving a falsely complete index.
        degrade(partition);
        return;
      }
      if (!exhausted && fresh.isEmpty()) {
        // An opaque continuation without a returned key gives us no upper repair boundary.
        degrade(partition);
        return;
      }
      String lastFresh = fresh.isEmpty() ? null : fresh.getLast().getKey();
      String lower = startsAtPrefix ? prefix : cachedAfter;
      boolean lowerInclusive = startsAtPrefix;

      // A strongly consistent page proves absence inside the ordered interval it returned. It says
      // nothing after its last key unless the continuation is exhausted.
      List<String> old =
          lower == null
              ? List.of()
              : partition.entries.tailMap(lower, lowerInclusive).keySet().stream()
                  .takeWhile(key -> key.startsWith(prefix))
                  .takeWhile(key -> exhausted || lastFresh == null || key.compareTo(lastFresh) <= 0)
                  .toList();
      old.forEach(key -> replace(partition, key, null));
      for (Pointer pointer : fresh) {
        replace(partition, pointer.getKey(), pointer);
        if (partition.readiness == Readiness.DEGRADED) {
          return;
        }
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  void verifyCount(String prefix, int freshCount) {
    IndexLayout.Match match = IndexLayout.match(prefix).orElse(null);
    if (match == null) {
      evictResidentPrefix(prefix, null);
      return;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      return;
    }
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness == Readiness.COMPLETE && countHeld(partition, prefix) != freshCount) {
        degrade(partition);
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  /** Returns true when the complete index owned this publication. */
  boolean publish(String key, Pointer pointer) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match == null) {
      return false;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      return false;
    }
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness != Readiness.COMPLETE) {
        return false;
      }
      Pointer current = partition.entries.get(key);
      if (current == null || current.getVersion() < pointer.getVersion()) {
        replace(partition, key, pointer);
      } else if (!current.equals(pointer)) {
        degrade(partition);
      }
      return true;
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  /** Returns true when the complete index owned this deletion. */
  boolean remove(String key) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match == null) {
      return false;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      return false;
    }
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness != Readiness.COMPLETE) {
        return false;
      }
      replace(partition, key, null);
      return true;
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  void removePrefix(String prefix, String excludedKey) {
    for (Partition partition : partitions.values()) {
      if (!isLoadingOrComplete(partition)) {
        continue;
      }
      partition.lock.writeLock().lock();
      try {
        if (partition.readiness != Readiness.COMPLETE) {
          continue;
        }
        List<String> covered =
            partition.entries.entrySet().stream()
                .map(Map.Entry::getKey)
                .filter(key -> (prefix == null || prefix.isEmpty() || key.startsWith(prefix)))
                .filter(key -> !key.equals(excludedKey))
                .toList();
        covered.forEach(key -> replace(partition, key, null));
      } finally {
        partition.lock.writeLock().unlock();
      }
    }
    evictResidentPrefix(prefix, excludedKey);
    IndexLayout.accountPartitionForRootPrefix(prefix).ifPresent(this::forgetPartition);
  }

  private void forgetPartition(String partitionKey) {
    Partition partition = partitions.get(partitionKey);
    if (partition == null) {
      return;
    }
    partition.lock.writeLock().lock();
    try {
      if (partitions.remove(partitionKey, partition)) {
        degrade(partition);
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  Optional<Pointer> peekResident(String key) {
    return resident.peek(key);
  }

  Optional<Pointer> peek(String key) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match != null) {
      Partition partition = partitions.get(match.partition());
      if (partition != null && partition.readiness == Readiness.COMPLETE) {
        partition.lock.readLock().lock();
        try {
          return Optional.ofNullable(partition.entries.get(key));
        } finally {
          partition.lock.readLock().unlock();
        }
      }
    }
    return resident.peek(key);
  }

  void putResident(String key, Pointer pointer) {
    resident.put(key, pointer);
  }

  void evictResident(String key) {
    resident.evict(key);
  }

  /** Keeps a successful check only when the complete index already holds the proved version. */
  void checkedPresent(String key, long version) {
    IndexLayout.Match match = IndexLayout.match(key).orElse(null);
    if (match == null) {
      resident.evict(key);
      return;
    }
    Partition partition = partitions.get(match.partition());
    if (!isLoadingOrComplete(partition)) {
      resident.evict(key);
      return;
    }
    partition.lock.writeLock().lock();
    try {
      Pointer current = partition.entries.get(key);
      if (partition.readiness == Readiness.COMPLETE
          && (current == null || current.getVersion() != version)) {
        degrade(partition);
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  void evictResidentPrefix(String prefix, String excludedKey) {
    resident.evictPartition(
        key ->
            (prefix == null || prefix.isEmpty() || key.startsWith(prefix))
                && !key.equals(excludedKey));
  }

  public long bytes() {
    return completeBytes.get() + resident.bytes();
  }

  public long entryCount() {
    return completeEntries.get() + resident.entryCount();
  }

  public CacheFamily family() {
    return CacheFamily.POINTER;
  }

  public long loadingAccountCount() {
    return accountCount(Readiness.LOADING) + accountCount(Readiness.RECOVERING);
  }

  public long completeAccountCount() {
    return accountCount(Readiness.COMPLETE);
  }

  public long degradedAccountCount() {
    return accountCount(Readiness.DEGRADED);
  }

  private long accountCount(Readiness readiness) {
    return partitions.entrySet().stream()
        .filter(entry -> !IndexLayout.GLOBAL_PARTITION.equals(entry.getKey()))
        .filter(entry -> entry.getValue().readiness == readiness)
        .count();
  }

  private Partition complete(String partitionKey) {
    long start = System.nanoTime();
    Partition partition =
        partitions.computeIfAbsent(
            partitionKey,
            key ->
                new Partition(
                    IndexLayout.GLOBAL_PARTITION.equals(key)
                        ? events
                        : events.forAccount(Keys.decodeSegment(key))));
    if (partition.readiness == Readiness.COMPLETE) {
      partition.events.hit(Duration.ofNanos(System.nanoTime() - start));
      return partition;
    }
    if (partition.readiness == Readiness.DEGRADED && !retryDue(partition)) {
      partition.events.miss();
      return null;
    }
    if (partition.readiness == Readiness.RECOVERING) {
      partition.events.miss();
      return null;
    }
    if (partition.readiness == Readiness.DEGRADED) {
      if (!partition.lock.writeLock().tryLock()) {
        partition.events.miss();
        return null;
      }
    } else {
      partition.lock.writeLock().lock();
    }
    try {
      if (partition.readiness == Readiness.COMPLETE) {
        partition.events.hit(Duration.ofNanos(System.nanoTime() - start));
        return partition;
      }
      if (partition.readiness == Readiness.DEGRADED) {
        if (!retryDue(partition)) {
          partition.events.miss();
          return null;
        }
        partition.readiness = Readiness.RECOVERING;
      }
      boolean recovering = partition.readiness == Readiness.RECOVERING;
      partition.events.miss();
      long loadStart = System.nanoTime();
      try {
        NavigableMap<String, Pointer> loaded = load(partitionKey);
        long loadedBytes = weight(loaded);
        synchronized (budgetLock) {
          long available = maxBytes - completeBytes.get();
          if (loadedBytes > available) {
            degradeLocked(partition);
            partition.events.admissionRejected();
            return null;
          }
          completeBytes.addAndGet(loadedBytes);
          completeEntries.addAndGet(loaded.size());
          resident.maximumBytes(maxBytes - completeBytes.get());
          partition.entries.putAll(loaded);
          partition.weightBytes = loadedBytes;
          partition.readiness = Readiness.COMPLETE;
        }
        if (recovering) {
          // Reads stay available through the ordinary resident cache while a retry runs. Once the
          // complete map is published, fence and remove those temporary copies so a later
          // degradation cannot expose one that predates this authoritative load.
          evictResidentPartition(partitionKey);
        }
        partition.events.loadTime(Duration.ofNanos(System.nanoTime() - loadStart));
        return partition;
      } catch (RuntimeException failure) {
        degrade(partition);
        partition.events.loadFailed(Duration.ofNanos(System.nanoTime() - loadStart), failure);
        return null;
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  private boolean retryDue(Partition partition) {
    return nanoTime.getAsLong() - partition.retryAtNanos >= 0L;
  }

  private void evictResidentPartition(String partitionKey) {
    resident.evictPartition(
        key ->
            IndexLayout.match(key)
                .map(match -> match.partition().equals(partitionKey))
                .orElse(false));
  }

  private NavigableMap<String, Pointer> load(String partitionKey) {
    TreeMap<String, Pointer> loaded = new TreeMap<>();
    for (NavigableMap<String, Pointer> subtree :
        loadFanout.mapOrdered(
            IndexLayout.loadPrefixes(partitionKey), prefix -> loadPrefix(partitionKey, prefix))) {
      loaded.putAll(subtree);
    }
    return loaded;
  }

  private NavigableMap<String, Pointer> loadPrefix(String partitionKey, String prefix) {
    TreeMap<String, Pointer> loaded = new TreeMap<>();
    String token = "";
    Set<String> seen = new HashSet<>();
    do {
      StringBuilder next = new StringBuilder();
      for (Pointer pointer : source.listPointersByPrefix(prefix, LOAD_PAGE_SIZE, token, next)) {
        if (IndexLayout.match(pointer.getKey())
            .filter(match -> match.partition().equals(partitionKey))
            .isPresent()) {
          loaded.put(pointer.getKey(), pointer);
        }
      }
      token = next.toString();
      if (!token.isBlank() && !seen.add(token)) {
        throw new IllegalStateException("stagnant pointer index token");
      }
    } while (!token.isBlank());
    return loaded;
  }

  private void replace(Partition partition, String key, Pointer next) {
    Pointer previous = partition.entries.get(key);
    long oldWeight = previous == null ? 0L : weight(key, previous);
    long newWeight = next == null ? 0L : weight(key, next);
    long delta = newWeight - oldWeight;
    synchronized (budgetLock) {
      if (delta > 0L && delta > maxBytes - completeBytes.get()) {
        degradeLocked(partition);
        return;
      }
      if (next == null) {
        partition.entries.remove(key);
      } else {
        partition.entries.put(key, next);
      }
      partition.weightBytes += delta;
      completeBytes.addAndGet(delta);
      if (previous == null && next != null) {
        completeEntries.incrementAndGet();
      } else if (previous != null && next == null) {
        completeEntries.decrementAndGet();
      }
      resident.maximumBytes(maxBytes - completeBytes.get());
    }
  }

  private void degrade(Partition partition) {
    synchronized (budgetLock) {
      degradeLocked(partition);
    }
  }

  private void degradeLocked(Partition partition) {
    completeBytes.addAndGet(-partition.weightBytes);
    completeEntries.addAndGet(-partition.entries.size());
    partition.entries.clear();
    partition.weightBytes = 0L;
    partition.retryAtNanos = nanoTime.getAsLong() + degradedRetryNanos;
    partition.readiness = Readiness.DEGRADED;
    resident.maximumBytes(maxBytes - completeBytes.get());
  }

  private static int countHeld(Partition partition, String prefix) {
    int count = 0;
    for (String key : partition.entries.tailMap(prefix, true).keySet()) {
      if (!key.startsWith(prefix)) {
        break;
      }
      count++;
    }
    return count;
  }

  private static long weight(NavigableMap<String, Pointer> pointers) {
    long total = 0L;
    for (Map.Entry<String, Pointer> entry : pointers.entrySet()) {
      total = Math.addExact(total, weight(entry.getKey(), entry.getValue()));
    }
    return total;
  }

  private static long weight(String key, Pointer pointer) {
    return Math.addExact(CacheWeights.entry(pointer, 2L * key.length()), SORTED_MAP_NODE_BYTES);
  }

  private static void setNextToken(StringBuilder out, String token) {
    if (out == null) {
      return;
    }
    out.setLength(0);
    out.append(token);
  }

  private static String encodeToken(String key) {
    return TOKEN_PREFIX
        + Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString(key.getBytes(StandardCharsets.UTF_8));
  }

  private static Optional<String> decodeToken(String token) {
    if (token == null || token.isBlank() || !token.startsWith(TOKEN_PREFIX)) {
      return Optional.empty();
    }
    try {
      return Optional.of(
          new String(
              Base64.getUrlDecoder().decode(token.substring(TOKEN_PREFIX.length())),
              StandardCharsets.UTF_8));
    } catch (IllegalArgumentException malformed) {
      throw new IllegalArgumentException("bad page token", malformed);
    }
  }

  private static boolean isLoadingOrComplete(Partition partition) {
    return partition != null
        && (partition.readiness == Readiness.LOADING
            || partition.readiness == Readiness.RECOVERING
            || partition.readiness == Readiness.COMPLETE);
  }

  /** The durable key layout mirrored by the complete pointer index. */
  private static final class IndexLayout {
    private static final String GLOBAL_PARTITION = "<accounts>";

    private IndexLayout() {}

    /** Structurally identifies the eleven complete addressing families. */
    private static Optional<Match> match(String keyOrPrefix) {
      if (keyOrPrefix == null || !keyOrPrefix.startsWith(Keys.accountRootPrefix())) {
        return Optional.empty();
      }
      String[] segment = keyOrPrefix.substring(1).split("/", -1);
      if (segment.length < 3 || !"accounts".equals(segment[0])) {
        return Optional.empty();
      }
      if (segment.length == 3 && Keys.isReservedAccountDirectorySegment(segment[1])) {
        return Optional.of(new Match(GLOBAL_PARTITION));
      }
      String account = segment[1];
      if (account.isEmpty()) {
        return Optional.empty();
      }
      if (segment.length == 5
          && (("catalogs".equals(segment[2])
                  && ("by-id".equals(segment[3]) || "by-name".equals(segment[3])))
              || ("namespaces".equals(segment[2]) && "by-id".equals(segment[3]))
              || ("tables".equals(segment[2]) && "by-id".equals(segment[3]))
              || ("views".equals(segment[2]) && "by-id".equals(segment[3])))) {
        return Optional.of(new Match(account));
      }
      if (segment.length >= 7
          && "catalogs".equals(segment[2])
          && !segment[3].isEmpty()
          && "namespaces".equals(segment[4])
          && "by-path".equals(segment[5])) {
        return Optional.of(new Match(account));
      }
      if (segment.length == 9
          && "catalogs".equals(segment[2])
          && !segment[3].isEmpty()
          && "namespaces".equals(segment[4])
          && !segment[5].isEmpty()
          && ("tables".equals(segment[6])
              || "views".equals(segment[6])
              || "relations".equals(segment[6]))
          && "by-name".equals(segment[7])) {
        return Optional.of(new Match(account));
      }
      return Optional.empty();
    }

    private static List<String> loadPrefixes(String partition) {
      if (GLOBAL_PARTITION.equals(partition)) {
        return List.of(Keys.accountPointerByIdPrefix(), Keys.accountPointerByNamePrefix());
      }
      // The partition came from an existing pointer key, so it is already encoded. Passing it back
      // through a Keys method that accepts a logical account id would encode '%' a second time.
      String root = Keys.accountRootPrefix() + partition + "/";
      return List.of(
          root + "catalogs/",
          root + "namespaces/by-id/",
          root + "tables/by-id/",
          root + "views/by-id/");
    }

    private static Optional<String> accountPartitionForRootPrefix(String prefix) {
      String root = Keys.accountRootPrefix();
      if (prefix == null
          || prefix.length() <= root.length()
          || !prefix.startsWith(root)
          || !prefix.endsWith("/")) {
        return Optional.empty();
      }
      String account = prefix.substring(root.length(), prefix.length() - 1);
      if (account.isEmpty()
          || account.indexOf('/') >= 0
          || Keys.isReservedAccountDirectorySegment(account)) {
        return Optional.empty();
      }
      return Optional.of(account);
    }

    private record Match(String partition) {}
  }

  private static final class Partition {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final NavigableMap<String, Pointer> entries = new TreeMap<>();
    private final CacheEvents events;
    private volatile Readiness readiness = Readiness.LOADING;
    private volatile long retryAtNanos;
    private long weightBytes;

    private Partition(CacheEvents events) {
      this.events = events;
    }
  }
}
