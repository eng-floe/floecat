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

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.common.ResourceIds;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Removes a relation's state outside its resource row. */
@ApplicationScoped
public class RelationPurge {
  // One slot of DynamoDB's 100-item transaction goes to the fence check.
  static final int DELETE_BATCH = 99;
  // Enough for a table without per-file stats to finish inline; larger subtrees go to GC.
  static final int INLINE_BATCH_BUDGET = 8;
  private static final int MAX_STALLED_BATCHES = 3;

  @Inject PointerStore pointerStore;
  @Inject HintCache hints;

  /**
   * Purges a table inline after its row delete, within {@link #INLINE_BATCH_BUDGET} batches.
   *
   * @see #purgeTableState(ResourceId, int)
   */
  public boolean purgeTableState(ResourceId tableId) {
    return purgeTableState(tableId, INLINE_BATCH_BUDGET);
  }

  /**
   * Advances deletion of a table's scoped pointers by at most {@code maxBatches} fenced deletes,
   * each either one page of scoped pointers or the pending root resync. Once no state remains,
   * evicts the table's hints and acknowledges its purge marker, one further write outside the
   * budget. Blobs are left to {@code CasBlobGc}, which reclaims them once nothing references them.
   *
   * <p>Inline and recovery purges of one table may overlap, so every delete is fenced on the marker
   * this purge started from. While that marker survives, the table id cannot have been recreated,
   * so everything under it belongs to the deleted table. Once another purge has acknowledged it,
   * this one stops: the id may since have been recreated, and an unfenced delete would remove the
   * new table's state. Without a marker nothing is owed: every row delete leaves one.
   *
   * <p>The bounded work keeps DROP and overlay requests from draining arbitrarily large statistics
   * subtrees inline. {@code TablePurgeQueue} repeatedly invokes this method within the GC deadline
   * until the durable marker is acknowledged.
   *
   * @return whether this purge finished and acknowledged its marker
   */
  public boolean purgeTableState(ResourceId tableId, int maxBatches) {
    if (maxBatches < 1) {
      throw new IllegalArgumentException("maxBatches must be positive");
    }
    requireCanonicalTableId(tableId);
    String accountId = tableId.getAccountId();
    String marker = Keys.tablePurgePointer(accountId, tableId.getId());
    Optional<Pointer> started = pointerStore.get(marker);
    if (started.isEmpty()) {
      return false;
    }
    PointerStore.CasCheck fence = new PointerStore.CasCheck(marker, started.get().getVersion());
    int batches = 0;
    for (String prefix :
        List.of(
            Keys.tablePrefix(accountId, tableId.getId()),
            Keys.reconcileFinalizedSnapshotIdentityPointerPrefix(accountId, tableId.getId()),
            Keys.relationHintsPointerPrefix(accountId, tableId.getId()))) {
      while (true) {
        if (batches >= maxBatches) {
          // The budget is spent: carry on only to confirm nothing is left to delete.
          if (!isEmpty(prefix)) {
            return false;
          }
          break;
        }
        DeleteStep step = deleteOneBatchWhileFenced(prefix, fence);
        if (step == DeleteStep.EMPTY) {
          break;
        }
        if (step == DeleteStep.LOST_FENCE) {
          return false;
        }
        batches++;
      }
    }
    // A pending root resync would otherwise re-drive against a table recreated under this id.
    String rootResync = Keys.rootResyncPendingPointer(accountId, tableId.getId());
    if (batches >= maxBatches && pointerStore.get(rootResync).isPresent()) {
      return false;
    }
    if (deleteKeyWhileFenced(rootResync, fence) == DeleteStep.LOST_FENCE) {
      return false;
    }
    hints.evictRelation(tableId);
    return pointerStore.compareAndDelete(marker, started.get().getVersion());
  }

  private enum DeleteStep {
    EMPTY,
    DELETED,
    LOST_FENCE
  }

  /** Deletes the single pointer {@code key}, if present, atomically with {@code fence}. */
  private DeleteStep deleteKeyWhileFenced(String key, PointerStore.CasCheck fence) {
    for (int stalled = 0; ; stalled++) {
      Optional<Pointer> current = pointerStore.get(key);
      if (current.isEmpty()) {
        return DeleteStep.EMPTY;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(fence, new PointerStore.CasDelete(key, current.get().getVersion())))) {
        return DeleteStep.DELETED;
      }
      if (!fenceHolds(fence)) {
        return DeleteStep.LOST_FENCE;
      }
      if (stalled + 1 >= MAX_STALLED_BATCHES) {
        throw new IllegalStateException("table purge made no progress under " + key);
      }
    }
  }

  /** Deletes at most one page below {@code prefix}, atomically with {@code fence}. */
  private DeleteStep deleteOneBatchWhileFenced(String prefix, PointerStore.CasCheck fence) {
    int stalled = 0;
    while (true) {
      List<Pointer> page =
          pointerStore.listPointersByPrefix(prefix, DELETE_BATCH, "", new StringBuilder());
      if (page.isEmpty()) {
        return DeleteStep.EMPTY;
      }
      List<PointerStore.CasOp> batch = new ArrayList<>(page.size() + 1);
      batch.add(fence);
      for (Pointer pointer : page) {
        batch.add(new PointerStore.CasDelete(pointer.getKey(), pointer.getVersion()));
      }
      if (pointerStore.compareAndSetBatch(batch)) {
        return DeleteStep.DELETED;
      }
      if (!fenceHolds(fence)) {
        return DeleteStep.LOST_FENCE;
      }
      // A listed pointer changed between the listing and the delete: list again.
      if (++stalled >= MAX_STALLED_BATCHES) {
        throw new IllegalStateException("table purge made no progress under " + prefix);
      }
    }
  }

  private boolean isEmpty(String prefix) {
    return pointerStore.listPointersByPrefix(prefix, 1, "", new StringBuilder()).isEmpty();
  }

  private boolean fenceHolds(PointerStore.CasCheck fence) {
    return pointerStore
        .get(fence.key())
        .map(pointer -> pointer.getVersion() == fence.expectedVersion())
        .orElse(false);
  }

  private static void requireCanonicalTableId(ResourceId tableId) {
    if (tableId == null || !ResourceIds.isCanonicalUuid(tableId.getId())) {
      throw new IllegalArgumentException("table id must be a canonical UUID");
    }
  }
}
