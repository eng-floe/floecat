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
import ai.floedb.floecat.service.repo.cache.DurablePointerReads;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

/** Drains durable relation cleanup work published atomically with namespace deletion. */
@ApplicationScoped
public class RelationPurgeQueue {
  private static final Logger LOG = Logger.getLogger(RelationPurgeQueue.class);

  @Inject PointerStore pointerStore;
  @Inject DurablePointerReads durablePointers;
  @Inject RelationPurge relations;
  @Inject OwnedAccounts accounts;

  public record Result(int scanned, int completed, int relationsDeleted) {}

  public Result drain(long deadlineMs, int pageSize) {
    int scanned = 0;
    int completed = 0;
    int relationsDeleted = 0;
    String token = "";
    while (System.currentTimeMillis() < deadlineMs) {
      StringBuilder next = new StringBuilder();
      List<Pointer> entries =
          pointerStore.listPointersByPrefix(
              Keys.relationPurgePrefix(), Math.max(1, pageSize), token, next);
      for (Pointer entry : entries) {
        if (System.currentTimeMillis() >= deadlineMs) break;
        scanned++;
        Work work = parse(entry.getKey());
        if (work == null) {
          LOG.errorf("invalid relation purge key=%s", entry.getKey());
          if (pointerStore.compareAndDelete(entry.getKey(), entry.getVersion())) completed++;
          continue;
        }
        if (!accounts.owns(work.accountId())) continue;
        try {
          relationsDeleted += purge(work, deadlineMs, pageSize);
          if (System.currentTimeMillis() < deadlineMs
              && empty(work)
              && pointerStore.compareAndDelete(entry.getKey(), entry.getVersion())) {
            completed++;
          }
        } catch (RuntimeException failure) {
          LOG.errorf(
              failure,
              "relation purge failed account_id=%s catalog_id=%s namespace_id=%s",
              work.accountId(),
              work.catalogId(),
              work.namespaceId());
        }
      }
      token = next.toString();
      if (token.isEmpty()) break;
    }
    return new Result(scanned, completed, relationsDeleted);
  }

  private int purge(Work work, long deadlineMs, int pageSize) {
    int deleted =
        purgePrefix(
            work,
            ResourceKind.RK_TABLE,
            Keys.tablePointerByNamePrefix(work.accountId(), work.catalogId(), work.namespaceId()),
            deadlineMs,
            pageSize);
    if (System.currentTimeMillis() >= deadlineMs) return deleted;
    return deleted
        + purgePrefix(
            work,
            ResourceKind.RK_VIEW,
            Keys.viewPointerByNamePrefix(work.accountId(), work.catalogId(), work.namespaceId()),
            deadlineMs,
            pageSize);
  }

  private int purgePrefix(
      Work work, ResourceKind kind, String prefix, long deadlineMs, int pageSize) {
    int deleted = 0;
    String token = "";
    while (System.currentTimeMillis() < deadlineMs) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, Math.max(1, pageSize), token, next);
      for (Pointer pointer : pointers) {
        if (System.currentTimeMillis() >= deadlineMs) break;
        var relation = PointerReferences.relationRef(work.accountId(), kind, pointer);
        if (relation.isPresent()) {
          if (relations.deleteStranded(relation.get().id(), work.namespaceId())) {
            deleted++;
          } else if (durablePointers.get(relationRow(relation.get().id())).isEmpty()) {
            // The row is already gone, so this is only an orphaned secondary. The namespace is
            // gone too, which makes deleting the exact observed name pointer safe.
            pointerStore.compareAndDelete(pointer.getKey(), pointer.getVersion());
          }
        }
      }
      token = next.toString();
      if (token.isEmpty()) break;
    }
    return deleted;
  }

  private static String relationRow(ResourceId relationId) {
    return relationId.getKind() == ResourceKind.RK_VIEW
        ? Keys.viewPointerById(relationId.getAccountId(), relationId.getId())
        : Keys.tablePointerById(relationId.getAccountId(), relationId.getId());
  }

  private boolean empty(Work work) {
    return pointerStore.countByPrefix(
                Keys.tablePointerByNamePrefix(
                    work.accountId(), work.catalogId(), work.namespaceId()))
            == 0
        && pointerStore.countByPrefix(
                Keys.viewPointerByNamePrefix(
                    work.accountId(), work.catalogId(), work.namespaceId()))
            == 0;
  }

  static Work parse(String key) {
    String prefix = Keys.relationPurgePrefix();
    if (key == null || !key.startsWith(prefix)) return null;
    String[] parts = key.substring(prefix.length()).split("/", -1);
    if (parts.length != 3) return null;
    try {
      String accountId = Keys.decodeSegment(parts[0]);
      String catalogId = Keys.decodeSegment(parts[1]);
      String namespaceId = Keys.decodeSegment(parts[2]);
      if (accountId.isBlank() || catalogId.isBlank() || namespaceId.isBlank()) return null;
      return new Work(accountId, catalogId, namespaceId);
    } catch (IllegalArgumentException failure) {
      return null;
    }
  }

  record Work(String accountId, String catalogId, String namespaceId) {}
}
