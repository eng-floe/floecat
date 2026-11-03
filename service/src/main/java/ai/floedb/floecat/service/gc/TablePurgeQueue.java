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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.catalog.impl.RelationPurge;
import ai.floedb.floecat.service.common.ResourceIds;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.function.LongSupplier;
import org.jboss.logging.Logger;

/**
 * Finishes table purges that did not complete after their table row was deleted.
 *
 * <p>Every table-row delete leaves a {@link Keys#tablePurgePointer} marker, and the purge removes
 * it once the table's state is gone. A marker this pass finds therefore names a purge that failed,
 * was interrupted, outgrew its inline batch budget, or is still running inline. This pass advances
 * each one a batch at a time so it stops at its deadline. Overlapping with an inline purge is safe:
 * both fence their deletes on the marker, so whichever finishes second stops instead of deleting
 * state a recreated table may own by then (see {@link RelationPurge#purgeTableState}).
 */
@ApplicationScoped
public class TablePurgeQueue {
  private static final Logger LOG = Logger.getLogger(TablePurgeQueue.class);

  @Inject PointerStore pointerStore;
  @Inject RelationPurge relations;

  LongSupplier nowMillis = System::currentTimeMillis;

  public record Result(int scanned, int purged) {}

  public Result drainAccount(String accountId, long deadlineMs, int pageSize) {
    String prefix = Keys.tablePurgePrefix(accountId);
    int scanned = 0;
    int purged = 0;
    String token = "";
    while (nowMillis.getAsLong() < deadlineMs) {
      StringBuilder next = new StringBuilder();
      List<Pointer> markers =
          pointerStore.listPointersByPrefix(prefix, Math.max(1, pageSize), token, next);
      for (Pointer marker : markers) {
        if (nowMillis.getAsLong() >= deadlineMs) break;
        scanned++;
        String tableId = Keys.idAfterPrefix(prefix, marker.getKey());
        if (tableId == null || !ResourceIds.isCanonicalUuid(tableId)) {
          LOG.errorf("invalid table purge key=%s", marker.getKey());
          pointerStore.compareAndDelete(marker.getKey(), marker.getVersion());
          continue;
        }
        ResourceId resourceId =
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(tableId)
                .setKind(ResourceKind.RK_TABLE)
                .build();
        try {
          while (nowMillis.getAsLong() < deadlineMs
              && pointerStore.get(marker.getKey()).isPresent()) {
            if (relations.purgeTableState(resourceId, 1)) {
              purged++;
              break;
            }
          }
        } catch (RuntimeException failure) {
          LOG.errorf(failure, "table_purge_failed account_id=%s table_id=%s", accountId, tableId);
        }
      }
      token = next.toString();
      if (token.isEmpty()) break;
    }
    return new Result(scanned, purged);
  }
}
