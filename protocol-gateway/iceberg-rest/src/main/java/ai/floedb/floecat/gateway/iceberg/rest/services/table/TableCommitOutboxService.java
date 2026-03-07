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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitOutboxService {
  private static final Logger LOG = Logger.getLogger(TableCommitOutboxService.class);
  private static final String OUTBOX_PENDING_MARKER = "/tx-outbox/pending/";

  @Inject TableCommitSideEffectService sideEffectService;
  @Inject TableCommitJournalService commitJournalService;
  @Inject Instance<PointerStore> pointerStores;
  @Inject Instance<BlobStore> blobStores;

  PointerStore pointerStore;
  BlobStore blobStore;

  public record WorkItem(
      String pendingKey,
      List<String> namespacePath,
      String tableName,
      ResourceId tableId,
      ResourceId connectorId,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds) {}

  public void processPendingNow(TableGatewaySupport tableSupport, List<WorkItem> items) {
    if (items == null || items.isEmpty()) {
      return;
    }
    for (WorkItem item : items) {
      if (item == null || item.pendingKey() == null || item.pendingKey().isBlank()) {
        continue;
      }
      boolean pruned =
          sideEffectService.pruneRemovedSnapshots(item.tableId(), item.removedSnapshotIds());
      boolean statsCaptured =
          sideEffectService.runPostCommitStatsSyncAttempt(
              tableSupport,
              item.connectorId(),
              item.namespacePath(),
              item.tableName(),
              item.addedSnapshotIds());
      if (pruned && statsCaptured) {
        clearPending(item.pendingKey());
      } else {
        LOG.warnf(
            "Leaving table-commit outbox pending for retry key=%s table=%s",
            item.pendingKey(), item.tableId() == null ? "<missing>" : item.tableId().getId());
      }
    }
  }

  public void drainPending(TableGatewaySupport tableSupport, int batchSize) {
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pointerStore == null || blobStore == null || batchSize <= 0) {
      return;
    }
    StringBuilder next = new StringBuilder();
    List<Pointer> pointers =
        pointerStore.listPointersByPrefix(
            ai.floedb.floecat.storage.kv.Keys.tableCommitOutboxPendingScanPrefix(),
            batchSize,
            "",
            next);
    if (pointers == null || pointers.isEmpty()) {
      return;
    }
    List<WorkItem> items = new ArrayList<>();
    for (Pointer pointer : pointers) {
      if (pointer == null
          || pointer.getKey().isBlank()
          || pointer.getBlobUri().isBlank()
          || !pointer.getKey().contains(OUTBOX_PENDING_MARKER)) {
        continue;
      }
      try {
        byte[] payload = blobStore.get(pointer.getBlobUri());
        if (payload == null) {
          throw new IllegalStateException("missing outbox blob: " + pointer.getBlobUri());
        }
        IcebergCommitOutboxEntry entry = IcebergCommitOutboxEntry.parseFrom(payload);
        Optional<IcebergCommitJournalEntry> journal =
            commitJournalService.get(entry.getAccountId(), entry.getTableId(), entry.getTxId());
        if (journal.isEmpty()) {
          LOG.warnf(
              "Skipping table-commit outbox key=%s; journal missing for tx=%s table=%s",
              pointer.getKey(), entry.getTxId(), entry.getTableId());
          continue;
        }
        items.add(toWorkItem(pointer.getKey(), journal.get()));
      } catch (InvalidProtocolBufferException e) {
        LOG.warnf(e, "Skipping unreadable table-commit outbox payload key=%s", pointer.getKey());
      } catch (RuntimeException e) {
        LOG.warnf(e, "Skipping table-commit outbox key=%s", pointer.getKey());
      }
    }
    processPendingNow(tableSupport, items);
  }

  public boolean isPending(String pendingKey) {
    if (pendingKey == null || pendingKey.isBlank()) {
      return false;
    }
    PointerStore pointerStore = resolvePointerStore();
    return pointerStore != null && pointerStore.get(pendingKey).isPresent();
  }

  public WorkItem toWorkItem(String pendingKey, IcebergCommitJournalEntry journal) {
    return new WorkItem(
        pendingKey,
        List.copyOf(journal.getNamespacePathList()),
        journal.getTableName(),
        journal.getTableId(),
        journal.hasConnectorId() ? journal.getConnectorId() : null,
        List.copyOf(journal.getAddedSnapshotIdsList()),
        List.copyOf(journal.getRemovedSnapshotIdsList()));
  }

  private void clearPending(String pendingKey) {
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pointerStore == null || blobStore == null) {
      return;
    }
    var current = pointerStore.get(pendingKey).orElse(null);
    if (current == null) {
      return;
    }
    if (!pointerStore.delete(pendingKey)) {
      LOG.warnf("Failed to clear table-commit outbox pending key=%s", pendingKey);
      return;
    }
    if (current.getBlobUri() != null && !current.getBlobUri().isBlank()) {
      try {
        blobStore.delete(current.getBlobUri());
      } catch (RuntimeException e) {
        LOG.debugf(e, "Failed to delete table-commit outbox blob uri=%s", current.getBlobUri());
      }
    }
  }

  private PointerStore resolvePointerStore() {
    if (pointerStore != null) {
      return pointerStore;
    }
    return pointerStores != null && pointerStores.isResolvable() ? pointerStores.get() : null;
  }

  private BlobStore resolveBlobStore() {
    if (blobStore != null) {
      return blobStore;
    }
    return blobStores != null && blobStores.isResolvable() ? blobStores.get() : null;
  }
}
