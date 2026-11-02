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
  private static final int MAX_STALLED_BATCHES = 3;

  @Inject PointerStore pointerStore;
  @Inject HintCache hints;

  /**
   * Deletes every table-scoped pointer and the table's hints, then acknowledges the table's purge
   * marker. Blobs are left to {@code CasBlobGc}, which reclaims them once nothing references them.
   *
   * <p>Inline and recovery purges of one table may overlap, so every delete is fenced on the marker
   * this purge started from. While that marker survives, the table id cannot have been recreated,
   * so everything under it belongs to the deleted table. Once another purge has acknowledged it,
   * this one stops: the id may since have been recreated, and an unfenced delete would remove the
   * new table's state. Without a marker nothing is owed: every row delete leaves one.
   *
   * @return whether this purge finished and acknowledged its marker
   */
  public boolean purgeTableState(ResourceId tableId) {
    requireCanonicalTableId(tableId);
    String accountId = tableId.getAccountId();
    String marker = Keys.tablePurgePointer(accountId, tableId.getId());
    Optional<Pointer> started = pointerStore.get(marker);
    if (started.isEmpty()) {
      return false;
    }
    PointerStore.CasCheck fence = new PointerStore.CasCheck(marker, started.get().getVersion());
    for (String prefix :
        List.of(
            Keys.tablePrefix(accountId, tableId.getId()),
            Keys.reconcileFinalizedSnapshotIdentityPointerPrefix(accountId, tableId.getId()),
            Keys.relationHintsPointerPrefix(accountId, tableId.getId()))) {
      if (!deleteWhileFenced(prefix, fence)) {
        return false;
      }
    }
    // A pending root resync would otherwise re-drive against a table recreated under this id.
    if (!deleteKeyWhileFenced(Keys.rootResyncPendingPointer(accountId, tableId.getId()), fence)) {
      return false;
    }
    hints.evictRelation(tableId);
    return pointerStore.compareAndDelete(marker, started.get().getVersion());
  }

  /** Deletes the single pointer {@code key}, if present, atomically with {@code fence}. */
  private boolean deleteKeyWhileFenced(String key, PointerStore.CasCheck fence) {
    for (int stalled = 0; ; stalled++) {
      Optional<Pointer> current = pointerStore.get(key);
      if (current.isEmpty()) {
        return true;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(fence, new PointerStore.CasDelete(key, current.get().getVersion())))) {
        return true;
      }
      if (!fenceHolds(fence)) {
        return false;
      }
      if (stalled + 1 >= MAX_STALLED_BATCHES) {
        throw new IllegalStateException("table purge made no progress under " + key);
      }
    }
  }

  /** Deletes {@code prefix} page by page, each page atomically with {@code fence}. */
  private boolean deleteWhileFenced(String prefix, PointerStore.CasCheck fence) {
    int stalled = 0;
    while (true) {
      List<Pointer> page =
          pointerStore.listPointersByPrefix(prefix, DELETE_BATCH, "", new StringBuilder());
      if (page.isEmpty()) {
        return true;
      }
      List<PointerStore.CasOp> batch = new ArrayList<>(page.size() + 1);
      batch.add(fence);
      for (Pointer pointer : page) {
        batch.add(new PointerStore.CasDelete(pointer.getKey(), pointer.getVersion()));
      }
      if (pointerStore.compareAndSetBatch(batch)) {
        stalled = 0;
        continue;
      }
      if (!fenceHolds(fence)) {
        return false;
      }
      // A listed pointer changed between the listing and the delete: list again.
      if (++stalled >= MAX_STALLED_BATCHES) {
        throw new IllegalStateException("table purge made no progress under " + prefix);
      }
    }
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
