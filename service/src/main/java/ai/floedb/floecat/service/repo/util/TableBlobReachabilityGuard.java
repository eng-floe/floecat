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

package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.ResourceId;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Process-local publication epochs for pointer-less, table-scoped blobs.
 *
 * <p>A publisher holds its table entry while making a new manifest-page or shared-sidecar reference
 * visible, then advances the entry epoch. CAS GC may build a re-mark incrementally, but it can act
 * on that proof only while holding the same entry and only when the epoch is unchanged. This closes
 * the re-reference-between-remark-and-delete race without retaining a lock across GC deadline
 * continuations. Entries are reference counted and removed as soon as no publication or retained GC
 * proof uses them, so memory is bounded by concurrent work rather than table cardinality. A
 * retained proof keeps its exact table entry alive across deadline continuations; unrelated tables
 * never cause false invalidations.
 *
 * <p>The guard is intentionally process-local, matching CAS GC's existing single-node safety
 * contract: every service process that can publish table references must disable CAS GC unless all
 * such publication and query-pin traffic is routed through the GC process.
 */
@ApplicationScoped
public class TableBlobReachabilityGuard {
  private final ConcurrentHashMap<TableKey, Entry> entries = new ConcurrentHashMap<>();

  /** Begins one exact table proof and retains its epoch across deadline continuations. */
  public Proof beginProof(String accountId, String tableId) {
    TableKey key = new TableKey(accountId, tableId);
    Entry entry = acquire(key);
    entry.lock.lock();
    try {
      return new Proof(this, key, entry, entry.epoch);
    } finally {
      entry.lock.unlock();
    }
  }

  /** Runs one publication attempt and invalidates any re-mark that overlapped it. */
  public <T> T publishing(ResourceId tableId, Supplier<T> publication) {
    return publishing(tableId.getAccountId(), tableId.getId(), publication);
  }

  public <T> T publishing(String accountId, String tableId, Supplier<T> publication) {
    TableKey key = new TableKey(accountId, tableId);
    Entry entry = acquire(key);
    entry.lock.lock();
    try {
      return publication.get();
    } finally {
      entry.epoch++;
      entry.lock.unlock();
      release(key, entry);
    }
  }

  /** Runs table-scoped reclamation atomically with respect to publishers without advancing it. */
  public <T> T exclusive(ResourceId tableId, Supplier<T> action) {
    return exclusive(tableId.getAccountId(), tableId.getId(), action);
  }

  public <T> T exclusive(String accountId, String tableId, Supplier<T> action) {
    TableKey key = new TableKey(accountId, tableId);
    Entry entry = acquire(key);
    entry.lock.lock();
    try {
      return action.get();
    } finally {
      entry.lock.unlock();
      release(key, entry);
    }
  }

  /**
   * Runs {@code deletion} only when no publication overlapped the proof identified by {@code
   * expectedEpoch}. The epoch check and deletion are atomic with respect to publishers.
   */
  public <T> GuardedResult<T> deleteIfUnchanged(Proof proof, Supplier<T> deletion) {
    if (proof == null || proof.owner != this) {
      throw new IllegalArgumentException("an active proof from this guard is required");
    }
    synchronized (proof) {
      if (proof.closed) {
        throw new IllegalArgumentException("an active proof from this guard is required");
      }
      proof.entry.lock.lock();
      try {
        if (proof.entry.epoch != proof.expectedEpoch) {
          return GuardedResult.changedResult();
        }
        return GuardedResult.unchangedResult(deletion.get());
      } finally {
        proof.entry.lock.unlock();
      }
    }
  }

  int retainedEntryCount() {
    return entries.size();
  }

  private Entry acquire(TableKey key) {
    return entries.compute(
        key,
        (ignored, current) -> {
          Entry entry = current == null ? new Entry() : current;
          entry.references++;
          return entry;
        });
  }

  private void release(TableKey key, Entry expected) {
    entries.computeIfPresent(
        key,
        (ignored, current) -> {
          if (current != expected) {
            return current;
          }
          current.references--;
          return current.references == 0 ? null : current;
        });
  }

  private record TableKey(String accountId, String tableId) {}

  private static final class Entry {
    private final ReentrantLock lock = new ReentrantLock();
    private long epoch;
    private int references;
  }

  public static final class Proof implements AutoCloseable {
    private final TableBlobReachabilityGuard owner;
    private final TableKey key;
    private final Entry entry;
    private final long expectedEpoch;
    private boolean closed;

    private Proof(TableBlobReachabilityGuard owner, TableKey key, Entry entry, long expectedEpoch) {
      this.owner = owner;
      this.key = key;
      this.entry = entry;
      this.expectedEpoch = expectedEpoch;
    }

    @Override
    public synchronized void close() {
      if (!closed) {
        closed = true;
        owner.release(key, entry);
      }
    }
  }

  public record GuardedResult<T>(boolean changed, T value) {
    private static <T> GuardedResult<T> changedResult() {
      return new GuardedResult<>(true, null);
    }

    private static <T> GuardedResult<T> unchangedResult(T value) {
      return new GuardedResult<>(false, value);
    }
  }
}
