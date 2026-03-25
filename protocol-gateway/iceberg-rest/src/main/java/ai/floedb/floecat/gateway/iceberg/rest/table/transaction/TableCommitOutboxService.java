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

package ai.floedb.floecat.gateway.iceberg.rest.table.transaction;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.table.SnapshotUpdateService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import com.google.protobuf.InvalidProtocolBufferException;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitOutboxService {
  private static final Logger LOG = Logger.getLogger(TableCommitOutboxService.class);
  private static final String OUTBOX_PENDING_MARKER = "/tx-outbox/pending/";
  private static final int DEFAULT_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_INITIAL_BACKOFF_MS = 1_000L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 60_000L;
  private static final String OUTBOX_PROTO_CONTENT_TYPE = "application/x-protobuf";

  @Inject TableCommitJournalService commitJournalService;
  @Inject SnapshotUpdateService snapshotUpdateService;
  @Inject TableGatewaySupport tableSupport;
  @Inject Instance<PointerStore> pointerStores;
  @Inject Instance<BlobStore> blobStores;

  PointerStore pointerStore;
  BlobStore blobStore;

  @Scheduled(
      every = "${floecat.gateway.table-commit-outbox.tick-every:30s}",
      concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
  void tick() {
    boolean enabled =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.enabled", Boolean.class)
            .orElse(true);
    if (!enabled) {
      return;
    }
    int batchSize =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.batch-size", Integer.class)
            .orElse(100);
    drainPending(tableSupport, batchSize);
  }

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
      boolean pruned = pruneRemovedSnapshots(item.tableId(), item.removedSnapshotIds());
      boolean statsCaptured =
          runPostCommitStatsSyncAttempt(
              tableSupport,
              item.connectorId(),
              item.namespacePath(),
              item.tableName(),
              item.addedSnapshotIds());
      if (pruned && statsCaptured) {
        clearPending(item.pendingKey());
      } else {
        markPendingForRetryOrDeadLetter(
            item.pendingKey(), "post-commit side effects returned false");
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
          dropCorruptPending(
              pointer.getKey(), pointer, "missing outbox blob uri=" + pointer.getBlobUri(), null);
          continue;
        }
        IcebergCommitOutboxEntry entry = IcebergCommitOutboxEntry.parseFrom(payload);
        if (entry.getNextAttemptAtMs() > 0
            && entry.getNextAttemptAtMs() > System.currentTimeMillis()) {
          continue;
        }
        Optional<IcebergCommitJournalEntry> journal =
            commitJournalService.get(entry.getAccountId(), entry.getTableId(), entry.getTxId());
        if (journal.isEmpty()) {
          LOG.warnf(
              "Skipping table-commit outbox key=%s; journal missing for tx=%s table=%s",
              pointer.getKey(), entry.getTxId(), entry.getTableId());
          markPendingForRetryOrDeadLetter(
              pointer.getKey(),
              "journal missing for tx=" + entry.getTxId() + " table=" + entry.getTableId());
          continue;
        }
        items.add(toWorkItem(pointer.getKey(), journal.get()));
      } catch (InvalidProtocolBufferException e) {
        dropCorruptPending(pointer.getKey(), pointer, "unreadable table-commit outbox payload", e);
      } catch (RuntimeException e) {
        LOG.warnf(e, "Skipping table-commit outbox key=%s", pointer.getKey());
      }
    }
    processPendingNow(tableSupport, items);
  }

  public List<WorkItem> loadPendingWorkItemsForTx(
      String accountId, String txId, long createdAtMs, String requestHash) {
    if (accountId == null
        || accountId.isBlank()
        || txId == null
        || txId.isBlank()
        || requestHash == null
        || requestHash.isBlank()) {
      return List.of();
    }
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pointerStore == null || blobStore == null) {
      return List.of();
    }

    String prefix =
        Keys.tableCommitOutboxPendingPrefix(accountId)
            + String.format("%019d/", Math.max(0L, createdAtMs));
    List<WorkItem> items = new ArrayList<>();
    String pageToken = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, 100, pageToken, next);
      if (pointers == null || pointers.isEmpty()) {
        break;
      }
      for (Pointer pointer : pointers) {
        WorkItem item = loadWorkItemForReplay(pointer, accountId, txId, requestHash, blobStore);
        if (item != null) {
          items.add(item);
        }
      }
      if (next.isEmpty() || next.toString().equals(pageToken)) {
        break;
      }
      pageToken = next.toString();
    }
    return List.copyOf(items);
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

  boolean runPostCommitStatsSyncAttempt(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName,
      List<Long> snapshotIds) {
    if (!tableSupport.connectorIntegrationEnabled()) {
      return true;
    }
    if (connectorId == null || connectorId.getId().isBlank()) {
      return true;
    }
    if (snapshotIds == null || snapshotIds.isEmpty()) {
      return true;
    }
    try {
      tableSupport.runSyncStatisticsCapture(
          connectorId, namespacePath, tableName, snapshotIds, true);
      return true;
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Connector stats-only sync failed connectorId=%s namespace=%s table=%s",
          connectorId.getId(),
          namespacePath == null ? "<null>" : String.join(".", namespacePath),
          tableName);
      return false;
    }
  }

  boolean pruneRemovedSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    if (tableId == null || snapshotIds == null || snapshotIds.isEmpty()) {
      return true;
    }
    try {
      snapshotUpdateService.deleteSnapshots(tableId, snapshotIds);
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Snapshot prune failed tableId=%s snapshotIds=%s", tableId.getId(), snapshotIds);
      return false;
    }
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
    if (!pointerStore.compareAndDelete(pendingKey, current.getVersion())) {
      LOG.warnf(
          "Failed to clear table-commit outbox pending key=%s version=%d",
          pendingKey, current.getVersion());
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

  private void dropCorruptPending(
      String pendingKey, Pointer current, String reason, Throwable error) {
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pendingKey == null || pendingKey.isBlank() || pointerStore == null || blobStore == null) {
      return;
    }
    if (error != null) {
      LOG.warnf(
          error,
          "Dropping corrupt table-commit outbox pending key=%s reason=%s",
          pendingKey,
          reason);
    } else {
      LOG.warnf(
          "Dropping corrupt table-commit outbox pending key=%s reason=%s", pendingKey, reason);
    }
    boolean deleted;
    if (current != null) {
      deleted = pointerStore.compareAndDelete(pendingKey, current.getVersion());
      if (!deleted) {
        LOG.warnf(
            "Failed to drop corrupt table-commit outbox pending key=%s version=%d",
            pendingKey, current.getVersion());
        return;
      }
    } else {
      deleted = pointerStore.delete(pendingKey);
      if (!deleted) {
        LOG.warnf("Failed to drop corrupt table-commit outbox pending key=%s", pendingKey);
        return;
      }
    }
    if (current == null || current.getBlobUri().isBlank()) {
      return;
    }
    try {
      blobStore.delete(current.getBlobUri());
    } catch (RuntimeException e) {
      LOG.debugf(
          e, "Failed to delete corrupt table-commit outbox blob uri=%s", current.getBlobUri());
    }
  }

  private void markPendingForRetryOrDeadLetter(String pendingKey, String errorMessage) {
    PointerStore pointerStore = resolvePointerStore();
    BlobStore blobStore = resolveBlobStore();
    if (pointerStore == null || blobStore == null) {
      return;
    }

    var current = pointerStore.get(pendingKey).orElse(null);
    if (current == null || current.getBlobUri().isBlank()) {
      return;
    }

    IcebergCommitOutboxEntry entry;
    try {
      byte[] payload = blobStore.get(current.getBlobUri());
      if (payload == null) {
        LOG.warnf("Cannot update outbox retry; missing blob uri=%s", current.getBlobUri());
        return;
      }
      entry = IcebergCommitOutboxEntry.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      LOG.warnf(e, "Cannot parse outbox retry payload key=%s", pendingKey);
      return;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Cannot update outbox retry state for key=%s", pendingKey);
      return;
    }

    long nowMs = System.currentTimeMillis();
    int nextAttemptCount = Math.max(0, entry.getAttemptCount()) + 1;
    IcebergCommitOutboxEntry.Builder updated =
        entry.toBuilder()
            .setAttemptCount(nextAttemptCount)
            .setLastAttemptAtMs(nowMs)
            .setLastError(errorMessage == null ? "" : errorMessage);

    if (nextAttemptCount >= configuredMaxAttempts()) {
      updated.setDeadLetteredAtMs(nowMs).setNextAttemptAtMs(0L);
      IcebergCommitOutboxEntry deadLetterEntry = updated.build();
      if (!persistOutboxBlob(blobStore, current.getBlobUri(), deadLetterEntry)) {
        return;
      }
      movePendingToDeadLetter(pointerStore, current, deadLetterEntry, pendingKey);
      return;
    }

    long backoffMs = computeBackoffMs(nextAttemptCount);
    long nextAttemptAtMs = nowMs + backoffMs;
    IcebergCommitOutboxEntry nextEntry = updated.setNextAttemptAtMs(nextAttemptAtMs).build();
    if (!persistOutboxBlob(blobStore, current.getBlobUri(), nextEntry)) {
      return;
    }

    Pointer nextPointer =
        current.toBuilder().setKey(pendingKey).setBlobUri(current.getBlobUri()).build();
    if (!pointerStore.compareAndSet(pendingKey, current.getVersion(), nextPointer)) {
      LOG.debugf(
          "Outbox retry pointer CAS failed key=%s version=%d", pendingKey, current.getVersion());
    }
  }

  private void movePendingToDeadLetter(
      PointerStore pointerStore,
      Pointer current,
      IcebergCommitOutboxEntry entry,
      String pendingKey) {
    if (entry.getAccountId().isBlank()
        || entry.getTableId().isBlank()
        || entry.getTxId().isBlank()) {
      LOG.warnf(
          "Cannot dead-letter outbox key=%s due to missing identity fields account=%s table=%s tx=%s",
          pendingKey, entry.getAccountId(), entry.getTableId(), entry.getTxId());
      return;
    }
    String deadLetterKey =
        Keys.tableCommitOutboxDeadLetterPointer(
            Math.max(0L, entry.getCreatedAtMs()),
            entry.getAccountId(),
            entry.getTableId(),
            entry.getTxId());
    Pointer deadLetterPointer =
        current.toBuilder().setKey(deadLetterKey).setBlobUri(current.getBlobUri()).build();
    boolean moved =
        pointerStore.compareAndSetBatch(
            List.of(
                new PointerStore.CasUpsert(deadLetterKey, 0L, deadLetterPointer),
                new PointerStore.CasDelete(pendingKey, current.getVersion())));
    if (!moved) {
      LOG.warnf(
          "Failed to move table-commit outbox to dead-letter key=%s deadLetterKey=%s",
          pendingKey, deadLetterKey);
    } else {
      LOG.warnf(
          "Moved table-commit outbox to dead-letter key=%s deadLetterKey=%s attempts=%d",
          pendingKey, deadLetterKey, entry.getAttemptCount());
    }
  }

  private boolean persistOutboxBlob(
      BlobStore blobStore, String blobUri, IcebergCommitOutboxEntry entry) {
    try {
      blobStore.put(blobUri, entry.toByteArray(), OUTBOX_PROTO_CONTENT_TYPE);
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Failed to persist outbox retry state blob uri=%s", blobUri);
      return false;
    }
  }

  private int configuredMaxAttempts() {
    int value =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.max-attempts", Integer.class)
            .orElse(DEFAULT_MAX_ATTEMPTS);
    return Math.max(1, value);
  }

  private long configuredInitialBackoffMs() {
    long value =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.initial-backoff-ms", Long.class)
            .orElse(DEFAULT_INITIAL_BACKOFF_MS);
    return Math.max(100L, value);
  }

  private long configuredMaxBackoffMs() {
    long value =
        ConfigProvider.getConfig()
            .getOptionalValue("floecat.gateway.table-commit-outbox.max-backoff-ms", Long.class)
            .orElse(DEFAULT_MAX_BACKOFF_MS);
    return Math.max(configuredInitialBackoffMs(), value);
  }

  private long computeBackoffMs(int attemptCount) {
    long initial = configuredInitialBackoffMs();
    long max = configuredMaxBackoffMs();
    int shifts = Math.max(0, attemptCount - 1);
    long backoff = initial;
    for (int i = 0; i < shifts; i++) {
      if (backoff >= max / 2) {
        backoff = max;
        break;
      }
      backoff *= 2L;
    }
    return Math.min(backoff, max);
  }

  private WorkItem loadWorkItemForReplay(
      Pointer pointer, String accountId, String txId, String requestHash, BlobStore blobStore) {
    if (pointer == null
        || pointer.getKey().isBlank()
        || pointer.getBlobUri().isBlank()
        || !pointer.getKey().contains(OUTBOX_PENDING_MARKER)) {
      return null;
    }
    try {
      byte[] payload = blobStore.get(pointer.getBlobUri());
      if (payload == null) {
        throw new IllegalStateException("missing outbox blob: " + pointer.getBlobUri());
      }
      IcebergCommitOutboxEntry entry = IcebergCommitOutboxEntry.parseFrom(payload);
      if (!accountId.equals(entry.getAccountId())
          || !txId.equals(entry.getTxId())
          || !requestHash.equals(entry.getRequestHash())) {
        return null;
      }
      Optional<IcebergCommitJournalEntry> journal =
          commitJournalService.get(entry.getAccountId(), entry.getTableId(), entry.getTxId());
      if (journal.isEmpty()) {
        LOG.warnf(
            "Skipping replay work item key=%s; journal missing for tx=%s table=%s",
            pointer.getKey(), entry.getTxId(), entry.getTableId());
        return null;
      }
      if (!requestHash.equals(journal.get().getRequestHash())) {
        LOG.warnf(
            "Skipping replay work item key=%s; journal hash mismatch for tx=%s table=%s",
            pointer.getKey(), entry.getTxId(), entry.getTableId());
        return null;
      }
      return toWorkItem(pointer.getKey(), journal.get());
    } catch (InvalidProtocolBufferException e) {
      LOG.warnf(e, "Skipping unreadable replay outbox payload key=%s", pointer.getKey());
      return null;
    } catch (RuntimeException e) {
      LOG.warnf(e, "Skipping replay outbox key=%s", pointer.getKey());
      return null;
    }
  }

  private PointerStore resolvePointerStore() {
    if (pointerStore == null && pointerStores != null && pointerStores.isResolvable()) {
      pointerStore = pointerStores.get();
    }
    return pointerStore;
  }

  private BlobStore resolveBlobStore() {
    if (blobStore == null && blobStores != null && blobStores.isResolvable()) {
      blobStore = blobStores.get();
    }
    return blobStore;
  }
}
