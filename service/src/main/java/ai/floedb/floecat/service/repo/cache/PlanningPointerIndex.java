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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.errors.StorageAbortRetryableException;
import ai.floedb.floecat.storage.spi.PointerStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** The authoritative in-memory index for planner-visible pointer state. */
public final class PlanningPointerIndex {
  private static final int PAGE_SIZE = 1_000;
  private static final String GLOBAL = "<account-directory>";

  enum Readiness {
    LOADING,
    COMPLETE
  }

  /**
   * The ownership seam is deliberately smaller than the managed ownership implementation.
   * Standalone Floecat supplies {@link #ALWAYS_OWNED}; managed deployments supply the local account
   * authority.
   */
  @FunctionalInterface
  public interface Ownership {
    Ownership ALWAYS_OWNED = (accountId, access) -> Optional.of(Permit.NOOP);

    enum Access {
      READ,
      WRITE
    }

    @FunctionalInterface
    interface Permit extends AutoCloseable {
      Permit NOOP = () -> {};

      @Override
      void close();
    }

    Optional<Permit> acquire(String accountId, Access access);
  }

  private final PointerStore durable;
  private final Ownership ownership;
  private final ConcurrentHashMap<String, Partition> partitions = new ConcurrentHashMap<>();

  public PlanningPointerIndex(PointerStore durable) {
    this(durable, Ownership.ALWAYS_OWNED);
  }

  public PlanningPointerIndex(PointerStore durable, Ownership ownership) {
    this.durable = java.util.Objects.requireNonNull(durable, "durable");
    this.ownership = java.util.Objects.requireNonNull(ownership, "ownership");
  }

  Optional<Pointer> get(String key) {
    String partitionKey = partitionFor(key);
    if (partitionKey == null || !isPlanningKey(key)) return durable.get(key);
    Optional<Ownership.Permit> permit = acquireOne(partitionKey, Ownership.Access.READ, false);
    if (permit.isEmpty()) return durable.get(key);
    try {
      Partition partition = complete(partitionKey);
      if (partition == null) return durable.get(key);
      partition.lock.readLock().lock();
      try {
        return Optional.ofNullable(partition.entries.get(key));
      } finally {
        partition.lock.readLock().unlock();
      }
    } finally {
      permit.orElseThrow().close();
    }
  }

  Map<String, Pointer> getBatch(List<String> keys) {
    if (keys == null || keys.isEmpty()) return Map.of();
    // The batch is one logical read. Operational/global keys are not part of an account planner
    // partition, so a batch containing one must use the durable path for every key.
    if (keys.stream().anyMatch(key -> !isPlanningKey(key))) return durable.getBatch(keys);
    List<String> partitionsToRead =
        keys.stream().map(this::partitionFor).distinct().sorted().toList();
    // A batch is one logical read. If any planner partition is not ready or not owned, use the
    // durable store for the whole operation instead of mixing an index snapshot with KV results.
    List<Ownership.Permit> permits = acquire(partitionsToRead, Ownership.Access.READ, false);
    if (permits == null) {
      return durable.getBatch(keys);
    }
    try {
      for (String partitionKey : partitionsToRead) {
        if (complete(partitionKey) == null) return durable.getBatch(keys);
      }
      List<Partition> locked = new ArrayList<>();
      for (String partitionKey : partitionsToRead) {
        Partition partition = partitions.get(partitionKey);
        partition.lock.readLock().lock();
        locked.add(partition);
      }
      try {
        Map<String, Pointer> result = new LinkedHashMap<>();
        for (String key : new java.util.LinkedHashSet<>(keys)) {
          Partition partition = partitions.get(partitionFor(key));
          Pointer value = partition.entries.get(key);
          if (value != null) result.put(key, value);
        }
        return Map.copyOf(result);
      } finally {
        for (int i = locked.size() - 1; i >= 0; i--) {
          locked.get(i).lock.readLock().unlock();
        }
      }
    } finally {
      closeReverse(permits);
    }
  }

  List<Pointer> list(String prefix, int limit, String token, StringBuilder nextToken) {
    String partitionKey = partitionFor(prefix);
    if (partitionKey == null || !isPlanningPrefix(prefix))
      return durable.listPointersByPrefix(prefix, limit, token, nextToken);
    Optional<Ownership.Permit> permit = acquireOne(partitionKey, Ownership.Access.READ, false);
    if (permit.isEmpty()) return durableList(prefix, limit, token, nextToken);
    try {
      Partition partition = complete(partitionKey);
      if (partition == null) return durableList(prefix, limit, token, nextToken);
      partition.lock.readLock().lock();
      try {
        String after = token == null || token.isBlank() ? null : token;
        if (after != null && !after.startsWith("index:"))
          return durable.listPointersByPrefix(prefix, limit, token, nextToken);
        after = after == null ? null : after.substring("index:".length());
        NavigableMap<String, Pointer> tail =
            after == null
                ? partition.entries.tailMap(prefix, true)
                : partition.entries.tailMap(after, false);
        List<Pointer> result = new ArrayList<>();
        boolean more = false;
        for (Map.Entry<String, Pointer> entry : tail.entrySet()) {
          if (!entry.getKey().startsWith(prefix)) break;
          if (result.size() >= Math.max(1, limit)) {
            more = true;
            break;
          }
          result.add(entry.getValue());
        }
        if (nextToken != null) {
          nextToken.setLength(0);
          if (more && !result.isEmpty())
            nextToken.append("index:").append(result.get(result.size() - 1).getKey());
        }
        return List.copyOf(result);
      } finally {
        partition.lock.readLock().unlock();
      }
    } finally {
      permit.orElseThrow().close();
    }
  }

  int count(String prefix) {
    String partitionKey = partitionFor(prefix);
    if (partitionKey == null || !isPlanningPrefix(prefix)) return durable.countByPrefix(prefix);
    Optional<Ownership.Permit> permit = acquireOne(partitionKey, Ownership.Access.READ, false);
    if (permit.isEmpty()) return durable.countByPrefix(prefix);
    try {
      Partition partition = complete(partitionKey);
      if (partition == null) return durable.countByPrefix(prefix);
      partition.lock.readLock().lock();
      try {
        int count = 0;
        for (String key : partition.entries.tailMap(prefix, true).keySet()) {
          if (!key.startsWith(prefix)) break;
          count++;
        }
        return count;
      } finally {
        partition.lock.readLock().unlock();
      }
    } finally {
      permit.orElseThrow().close();
    }
  }

  String pageTokenAfterKey(String key) {
    String partitionKey = partitionFor(key);
    if (isPlanningKey(key) && partitionKey != null && indexReady(partitionKey)) {
      return "index:" + key;
    }
    return durable.pageTokenAfterKey(key);
  }

  <T> T mutate(Collection<String> keys, Supplier<T> durableMutation, Consumer<T> publish) {
    List<Ownership.Permit> permits = acquire(accountPartitions(keys), Ownership.Access.WRITE, true);
    try {
      if (keys != null && keys.contains(Keys.accountRootPrefix())) {
        synchronized (partitions) {
          return mutateLocked(keys, durableMutation, publish);
        }
      }
      return mutateLocked(keys, durableMutation, publish);
    } finally {
      closeReverse(permits);
    }
  }

  private <T> T mutateLocked(
      Collection<String> keys, Supplier<T> durableMutation, Consumer<T> publish) {
    List<Partition> locked = lockPartitions(keys);
    try {
      T result = durableMutation.get();
      publish.accept(result);
      return result;
    } finally {
      for (int i = locked.size() - 1; i >= 0; i--) locked.get(i).lock.writeLock().unlock();
    }
  }

  private List<String> accountPartitions(Collection<String> keys) {
    if (keys == null) return List.of();
    if (keys.contains(Keys.accountRootPrefix())) return List.of(GLOBAL);
    return keys.stream()
        .map(this::partitionFor)
        .filter(partition -> partition != null && !GLOBAL.equals(partition))
        .distinct()
        .sorted()
        .toList();
  }

  private List<Ownership.Permit> acquire(
      Collection<String> partitionKeys, Ownership.Access access, boolean required) {
    List<Ownership.Permit> permits = new ArrayList<>();
    for (String partitionKey : partitionKeys) {
      Optional<Ownership.Permit> permit = ownership.acquire(partitionKey, access);
      if (permit.isEmpty()) {
        closeReverse(permits);
        if (required) {
          throw new StorageAbortRetryableException(
              "account is not owned by this Floecat instance: " + partitionKey);
        }
        return null;
      }
      permits.add(permit.orElseThrow());
    }
    return permits;
  }

  private Optional<Ownership.Permit> acquireOne(
      String partitionKey, Ownership.Access access, boolean required) {
    List<Ownership.Permit> permits = acquire(List.of(partitionKey), access, required);
    return permits == null || permits.isEmpty() ? Optional.empty() : Optional.of(permits.get(0));
  }

  private boolean indexReady(String partitionKey) {
    Optional<Ownership.Permit> permit = acquireOne(partitionKey, Ownership.Access.READ, false);
    if (permit.isEmpty()) return false;
    try {
      return complete(partitionKey) != null;
    } finally {
      permit.orElseThrow().close();
    }
  }

  private static void closeReverse(List<Ownership.Permit> permits) {
    for (int i = permits.size() - 1; i >= 0; i--) {
      permits.get(i).close();
    }
  }

  private List<Pointer> durableList(
      String prefix, int limit, String token, StringBuilder nextToken) {
    // An index continuation is local to this process. If ownership or readiness changes between
    // pages, translate it to the durable store's token instead of leaking the index format into
    // the KV adapter.
    String durableToken = token;
    if (token != null && token.startsWith("index:")) {
      String lastKey = token.substring("index:".length());
      durableToken = lastKey.isBlank() ? null : durable.pageTokenAfterKey(lastKey);
    }
    return durable.listPointersByPrefix(prefix, limit, durableToken, nextToken);
  }

  void publish(String key, Pointer value) {
    if (!isPlanningKey(key)) return;
    Partition partition = partitions.get(partitionFor(key));
    if (partition != null && partition.readiness == Readiness.COMPLETE)
      partition.entries.put(key, value);
  }

  void remove(String key) {
    if (!isPlanningKey(key)) return;
    Partition partition = partitions.get(partitionFor(key));
    if (partition != null && partition.readiness == Readiness.COMPLETE)
      partition.entries.remove(key);
  }

  void refresh(String key, Optional<Pointer> value) {
    if (value.isPresent()) publish(key, value.orElseThrow());
    else remove(key);
  }

  void removePrefix(String prefix, String excludedKey) {
    if (Keys.accountRootPrefix().equals(prefix)) {
      for (Partition partition : partitions.values()) {
        if (partition.readiness == Readiness.COMPLETE) {
          partition
              .entries
              .keySet()
              .removeIf(
                  key -> key.startsWith(prefix) && !java.util.Objects.equals(key, excludedKey));
        }
      }
      partitions.clear();
      return;
    }
    String partitionKey = partitionFor(prefix);
    Partition partition = partitionKey == null ? null : partitions.get(partitionKey);
    if (partition != null && partition.readiness == Readiness.COMPLETE) {
      partition
          .entries
          .keySet()
          .removeIf(key -> key.startsWith(prefix) && !java.util.Objects.equals(key, excludedKey));
    }
    if (isAccountRoot(prefix)) partitions.remove(accountPartition(prefix));
  }

  Readiness readiness(String accountId) {
    Partition partition = partitions.get(accountId);
    return partition == null ? Readiness.LOADING : partition.readiness;
  }

  long entryCount() {
    return partitions.values().stream().mapToLong(partition -> partition.entries.size()).sum();
  }

  public long completePartitionCount() {
    return partitions.values().stream()
        .filter(partition -> partition.readiness == Readiness.COMPLETE)
        .count();
  }

  public long loadingPartitionCount() {
    return partitions.values().stream()
        .filter(partition -> partition.readiness == Readiness.LOADING)
        .count();
  }

  private Partition complete(String partitionKey) {
    Partition partition;
    synchronized (partitions) {
      partition = partitions.computeIfAbsent(partitionKey, ignored -> new Partition());
    }
    if (partition.readiness == Readiness.COMPLETE) return partition;
    partition.lock.writeLock().lock();
    try {
      if (partition.readiness == Readiness.COMPLETE) return partition;
      try {
        loadLocked(partitionKey, partition);
        return partition;
      } catch (RuntimeException failure) {
        // A failed load is not a partial index. Keep the partition LOADING and let this read use
        // durable KV; a later read can retry the complete load.
        return null;
      }
    } finally {
      partition.lock.writeLock().unlock();
    }
  }

  private void loadLocked(String partitionKey, Partition partition) {
    TreeMap<String, Pointer> loaded = new TreeMap<>();
    if (!GLOBAL.equals(partitionKey)) {
      String root = Keys.accountRootPrefix() + partitionKey;
      durable.getConsistent(root).ifPresent(pointer -> loaded.put(root, pointer));
    }
    for (String prefix : loadPrefixes(partitionKey)) {
      String token = "";
      do {
        StringBuilder next = new StringBuilder();
        for (Pointer pointer :
            durable.listPointersByPrefixConsistent(prefix, PAGE_SIZE, token, next)) {
          if (partitionKey.equals(partitionFor(pointer.getKey()))
              && isPlanningKey(pointer.getKey())) loaded.put(pointer.getKey(), pointer);
        }
        String newToken = next.toString();
        if (!newToken.isBlank() && newToken.equals(token))
          throw new IllegalStateException("stagnant pointer index token");
        token = newToken;
      } while (!token.isBlank());
    }
    partition.entries.clear();
    partition.entries.putAll(loaded);
    partition.readiness = Readiness.COMPLETE;
  }

  private List<Partition> lockPartitions(Collection<String> keys) {
    synchronized (partitions) {
      java.util.Set<String> names = new java.util.TreeSet<>();
      if (keys != null) {
        for (String key : keys) {
          if (Keys.accountRootPrefix().equals(key)) {
            names.addAll(partitions.keySet());
          } else if (isPlanningKey(key)) {
            names.add(partitionFor(key));
          }
        }
      }
      List<Partition> locked = new ArrayList<>();
      for (String name : names) {
        Partition partition = partitions.computeIfAbsent(name, ignored -> new Partition());
        partition.lock.writeLock().lock();
        locked.add(partition);
        if (partition.readiness != Readiness.COMPLETE) {
          try {
            loadLocked(name, partition);
          } catch (RuntimeException ignored) {
            // The durable mutation remains valid. A later read retries the complete load.
          }
        }
      }
      return locked;
    }
  }

  private boolean isPlanningKey(String key) {
    String partition = partitionFor(key);
    return key != null && partition != null && !GLOBAL.equals(partition) && !isOperational(key);
  }

  private boolean isPlanningPrefix(String prefix) {
    String partition = partitionFor(prefix);
    return prefix != null
        && partition != null
        && !GLOBAL.equals(partition)
        && !isOperational(prefix);
  }

  private String partitionFor(String key) {
    if (key == null || !key.startsWith(Keys.accountRootPrefix())) return null;
    String remainder = key.substring(Keys.accountRootPrefix().length());
    int slash = remainder.indexOf('/');
    String account = slash < 0 ? remainder : remainder.substring(0, slash);
    if (account.isBlank()) return null;
    return Keys.isReservedAccountDirectorySegment(account) ? GLOBAL : account;
  }

  private static boolean isOperational(String key) {
    // Default account-scoped keys are planner state. This is an exclusion list for durable work
    // queues and fences, not a planner-family allowlist: new planning pointers are indexed by
    // construction.
    return key.contains(Keys.SEG_TRANSACTIONS)
        || key.contains(Keys.SEG_IDEMPOTENCY)
        || key.contains(Keys.SEG_MARKERS)
        || key.contains(Keys.SEG_CATALOG_INTEGRATION_CREDENTIAL_CLEANUP)
        || key.endsWith("/deleting")
        || key.contains("/reconcile/")
        || key.contains("/gc/");
  }

  private static List<String> loadPrefixes(String partition) {
    if (GLOBAL.equals(partition))
      return List.of(Keys.accountPointerByIdPrefix(), Keys.accountPointerByNamePrefix());
    return List.of(Keys.accountRootPrefix() + partition + "/");
  }

  private static boolean isAccountRoot(String prefix) {
    return prefix != null
        && prefix.endsWith("/")
        && prefix.substring(0, prefix.length() - 1).lastIndexOf('/')
            == Keys.accountRootPrefix().length() - 1;
  }

  private static String accountPartition(String prefix) {
    return prefix.substring(Keys.accountRootPrefix().length(), prefix.length() - 1);
  }

  private static final class Partition {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final NavigableMap<String, Pointer> entries = new TreeMap<>();
    private volatile Readiness readiness = Readiness.LOADING;
  }
}
