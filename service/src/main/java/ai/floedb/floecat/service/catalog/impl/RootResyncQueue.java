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
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.LongConsumer;

/**
 * The durable queue of tables whose root needs a re-driven resync by the periodic transaction GC.
 * Two kinds of writer enqueue: the post-transaction resync path when its attempt was absorbed (a
 * table only ever written through REST transactions has no other writer to converge its root), and
 * query-path reads that observed a broken root ({@link RootRepairRequests}).
 *
 * <p>Enqueueing when a marker already exists TOUCHES it (bumps the pointer version): the GC pass
 * clears markers with a versioned delete taken against the version it listed, so a failure recorded
 * after the pass resynced — but before it deleted — must change the version, making the stale
 * delete lose and the new failure stay recorded.
 */
@ApplicationScoped
public class RootResyncQueue {

  static final int ENQUEUE_ATTEMPTS = 4;
  private static final long[] RETRY_BACKOFF_MS = {10L, 25L, 50L};

  private final PointerStore pointerStore;
  private final LongConsumer retrySleeper;
  private final MarkerStore markerStore;

  @Inject
  public RootResyncQueue(PointerStore pointerStore, MarkerStore markerStore) {
    this(pointerStore, RootResyncQueue::sleepUnchecked, markerStore);
  }

  public RootResyncQueue(PointerStore pointerStore) {
    this(pointerStore, RootResyncQueue::sleepUnchecked, null);
  }

  RootResyncQueue(PointerStore pointerStore, LongConsumer retrySleeper) {
    this(pointerStore, retrySleeper, null);
  }

  private RootResyncQueue(
      PointerStore pointerStore, LongConsumer retrySleeper, MarkerStore markerStore) {
    this.pointerStore = pointerStore;
    this.retrySleeper = retrySleeper;
    this.markerStore = markerStore;
  }

  /**
   * Records that the table's root needs a re-driven resync. Loops until the marker durably reflects
   * THIS failure — a fresh create or a version-bumping touch. The loop matters: a GC pass's
   * versioned delete can land between a failed create and the touch (the marker vanishes under us),
   * and silently skipping there would lose this failure exactly like the old no-op enqueue did.
   */
  public void enqueue(ResourceId tableId) {
    String key = Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId());
    BatchGuard tableGuard = liveTableGuardOrNull(tableId);
    if (tableGuard == null) {
      return;
    }
    RuntimeException lastFailure = null;
    for (int attempt = 0; attempt < ENQUEUE_ATTEMPTS; attempt++) {
      try {
        if (compareAndSetGuarded(key, 0L, PointerReferences.blobPointer(key, "", 1L), tableGuard)) {
          return;
        }
        if (tableGuard.reevaluate() == BatchGuard.Outcome.BROKEN) {
          tableGuard = liveTableGuardOrNull(tableId);
          if (tableGuard == null) {
            return;
          }
          continue;
        }
        var existing = pointerStore.get(key).orElse(null);
        if (existing == null) {
          continue; // deleted between the failed create and the read: re-create
        }
        if (compareAndSetGuarded(
            key,
            existing.getVersion(),
            PointerReferences.blobPointer(key, "", existing.getVersion() + 1),
            tableGuard)) {
          return;
        }
        if (tableGuard.reevaluate() == BatchGuard.Outcome.BROKEN) {
          tableGuard = liveTableGuardOrNull(tableId);
          if (tableGuard == null) {
            return;
          }
          continue;
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

  private BatchGuard liveTableGuardOrNull(ResourceId tableId) {
    try {
      return markerStore == null ? BatchGuard.NONE : markerStore.tableLiveGuard(tableId);
    } catch (BaseResourceRepository.BatchGuardFailedException tableGone) {
      return null;
    }
  }

  private boolean compareAndSetGuarded(
      String key,
      long expectedVersion,
      ai.floedb.floecat.common.rpc.Pointer next,
      BatchGuard guard) {
    if (guard == BatchGuard.NONE) {
      return pointerStore.compareAndSet(key, expectedVersion, next);
    }
    var ops = new java.util.ArrayList<PointerStore.CasOp>();
    ops.add(new PointerStore.CasUpsert(key, expectedVersion, next));
    ops.addAll(guard.ops());
    return pointerStore.compareAndSetBatch(ops);
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
