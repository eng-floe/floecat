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
import ai.floedb.floecat.service.cache.HintCache;
import ai.floedb.floecat.service.common.ResourceIds;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Removes a relation's state outside its resource row. */
@ApplicationScoped
public class RelationPurge {
  @Inject PointerStore pointerStore;
  @Inject TableRootRepository tableRoots;
  @Inject HintCache hints;

  /**
   * Deletes every table-scoped pointer and the table's hints. Blobs are left to {@code CasBlobGc},
   * which reclaims them once nothing references them.
   */
  public void purgeTableState(ResourceId tableId) {
    requireCanonicalTableId(tableId);
    // Route root removal through the repository first so its pointer cache observes the delete.
    tableRoots.purgeRoot(tableId);
    pointerStore.deleteByPrefix(Keys.tablePrefix(tableId.getAccountId(), tableId.getId()));
    pointerStore.deleteByPrefix(
        Keys.reconcileFinalizedSnapshotIdentityPointerPrefix(
            tableId.getAccountId(), tableId.getId()));
    hints.deleteRelation(tableId);
  }

  private static void requireCanonicalTableId(ResourceId tableId) {
    if (tableId == null || !ResourceIds.isCanonicalUuid(tableId.getId())) {
      throw new IllegalArgumentException("table id must be a canonical UUID");
    }
  }
}
