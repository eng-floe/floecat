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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.LongConsumer;

/**
 * The durable queue of tables whose post-transaction root resync failed and awaits re-drive by the
 * periodic transaction GC. A table only ever written through REST transactions has no other writer
 * to converge its root, so an absorbed resync failure must leave a durable trace.
 *
 * <p>Enqueueing when a marker already exists TOUCHES it (bumps the pointer version): the GC pass
 * clears markers with a versioned delete taken against the version it listed, so a failure recorded
 * after the pass resynced — but before it deleted — must change the version, making the stale
 * delete lose and the new failure stay recorded.
 */
@ApplicationScoped
public class RootResyncQueue {

  private static final int ENQUEUE_ATTEMPTS = 4;
  private static final long[] RETRY_BACKOFF_MS = {10L, 25L, 50L};

  private final PointerStore pointerStore;
  private final LongConsumer retrySleeper;

  @Inject
  public RootResyncQueue(PointerStore pointerStore) {
    this(pointerStore, RootResyncQueue::sleepUnchecked);
  }

  RootResyncQueue(PointerStore pointerStore, LongConsumer retrySleeper) {
    this.pointerStore = pointerStore;
    this.retrySleeper = retrySleeper;
  }

  /**
   * Records that the table's root needs a re-driven resync. Loops until the marker durably reflects
   * THIS failure — a fresh create or a version-bumping touch. The loop matters: a GC pass's
   * versioned delete can land between a failed create and the touch (the marker vanishes under us),
   * and silently skipping there would lose this failure exactly like the old no-op enqueue did.
   */
  public void enqueue(ResourceId tableId) {
    String key = Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId());
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < ENQUEUE_ATTEMPTS; attempt++) {
      try {
        if (pointerStore.compareAndSet(key, 0L, PointerReferences.blobPointer(key, "", 1L))) {
          return;
        }
        var existing = pointerStore.get(key).orElse(null);
        if (existing == null) {
          continue; // deleted between the failed create and the read: re-create
        }
        if (pointerStore.compareAndSet(
            key,
            existing.getVersion(),
            PointerReferences.blobPointer(key, "", existing.getVersion() + 1))) {
          return;
        }
        // Lost the touch to another enqueue or a delete: retry from the top.
      } catch (RuntimeException e) {
        // A transient store fault mid-attempt must not lose the marker any more than a lost CAS:
        // retry within the bound before surfacing the failure.
        lastFailure = e;
        if (attempt + 1 < ENQUEUE_ATTEMPTS) {
          retrySleeper.accept(RETRY_BACKOFF_MS[Math.min(attempt, RETRY_BACKOFF_MS.length - 1)]);
        }
      }
    }
    throw new IllegalStateException(
        "root-resync marker for table " + tableId.getId() + " could not be recorded", lastFailure);
  }

  private static void sleepUnchecked(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while retrying root-resync marker enqueue", e);
    }
  }
}
