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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/** Durable enumeration path for table-owned state after the table pointer has been removed. */
@ApplicationScoped
public class TableCleanupRepository {

  private static final int PAGE_SIZE = 200;
  private static final int MAX_TRANSACTION_ITEMS = 100;

  private final PointerStore pointerStore;

  @Inject
  public TableCleanupRepository(PointerStore pointerStore) {
    this.pointerStore = pointerStore;
  }

  public record Cleanup(
      ResourceId namespaceId,
      ResourceId tableId,
      String pointerKey,
      long pointerVersion,
      String indexKey,
      long indexVersion) {
    public Cleanup(
        ResourceId namespaceId, ResourceId tableId, String pointerKey, long pointerVersion) {
      this(
          namespaceId,
          tableId,
          pointerKey,
          pointerVersion,
          Keys.tableCleanupPointerByTable(tableId.getAccountId(), tableId.getId()),
          0L);
    }
  }

  /** A durable cleanup task and the guard that stages it in the table-delete batch. */
  public record DeletePlan(Cleanup cleanup, BatchGuard guard) {}

  /** Stages a handle before pointer deletion, so process death cannot erase the last table id. */
  public Cleanup prepare(ResourceId namespaceId, ResourceId tableId) {
    return prepare(namespaceId, tableId, BatchGuard.NONE);
  }

  /** Stages a handle atomically with an independent guard. */
  public Cleanup prepare(ResourceId namespaceId, ResourceId tableId, BatchGuard guard) {
    String key =
        Keys.namespaceTableCleanupGenerationPointer(
            namespaceId.getAccountId(),
            namespaceId.getId(),
            tableId.getId(),
            UUID.randomUUID().toString());
    String indexKey = Keys.tableCleanupPointerByTable(tableId.getAccountId(), tableId.getId());
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var existingIndex = pointerStore.get(indexKey).orElse(null);
      long expectedIndex = existingIndex == null ? 0L : existingIndex.getVersion();
      var marker = cleanupTask(key, tableId, 1L);
      var index = cleanupIndex(indexKey, namespaceId, key, expectedIndex + 1L);
      var ops = new ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(key, 0L, marker));
      ops.add(new PointerStore.CasUpsert(indexKey, expectedIndex, index));
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return new Cleanup(
            namespaceId, tableId, key, marker.getVersion(), indexKey, index.getVersion());
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "table cleanup staging lost the race against " + guard.describe());
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "table cleanup staging contended for: " + key);
  }

  /**
   * Plans cleanup-task publication as part of the table-pointer delete itself.
   *
   * <p>The task is not visible unless the delete commits, so a lost table CAS cannot strand a task
   * under the table's former namespace. The namespace guard is pin-only: deletion removes a child
   * and must not advance the child-publication marker.
   */
  public DeletePlan planDelete(
      ResourceId namespaceId, ResourceId tableId, BatchGuard namespaceGuard) {
    String key =
        Keys.namespaceTableCleanupGenerationPointer(
            namespaceId.getAccountId(),
            namespaceId.getId(),
            tableId.getId(),
            UUID.randomUUID().toString());
    String indexKey = Keys.tableCleanupPointerByTable(tableId.getAccountId(), tableId.getId());
    var existingIndex = pointerStore.get(indexKey).orElse(null);
    long[] expectedIndexVersion = {existingIndex == null ? 0L : existingIndex.getVersion()};
    var marker = cleanupTask(key, tableId, 1L);
    var index =
        new Pointer[] {cleanupIndex(indexKey, namespaceId, key, expectedIndexVersion[0] + 1L)};
    BatchGuard taskGuard =
        new BatchGuard() {
          @Override
          public List<PointerStore.CasOp> ops() {
            return List.of(
                new PointerStore.CasUpsert(key, 0L, marker),
                new PointerStore.CasUpsert(indexKey, expectedIndexVersion[0], index[0]));
          }

          @Override
          public Outcome reevaluate() {
            var currentIndex = pointerStore.get(indexKey).orElse(null);
            long currentIndexVersion = currentIndex == null ? 0L : currentIndex.getVersion();
            if (pointerStore.get(key).isEmpty() && currentIndexVersion == expectedIndexVersion[0]) {
              return Outcome.HOLDS;
            }
            expectedIndexVersion[0] = currentIndexVersion;
            index[0] = cleanupIndex(indexKey, namespaceId, key, currentIndexVersion + 1L);
            return Outcome.RETRY;
          }

          @Override
          public String describe() {
            return "cleanup task for table " + tableId.getId();
          }
        };
    return new DeletePlan(
        new Cleanup(
            namespaceId, tableId, key, marker.getVersion(), indexKey, index[0].getVersion()),
        BatchGuard.all(namespaceGuard, taskGuard));
  }

  /** Refreshes an exact task handle after the batch that was expected to stage it. */
  public Optional<Cleanup> pending(Cleanup cleanup) {
    var pointer = pointerStore.get(cleanup.pointerKey()).orElse(null);
    if (pointer == null) {
      return Optional.empty();
    }
    var index = pointerStore.get(cleanup.indexKey()).orElse(null);
    boolean ownsIndex = index != null && cleanup.pointerKey().equals(index.getBlobUri());
    return Optional.of(
        new Cleanup(
            cleanup.namespaceId(),
            cleanup.tableId(),
            cleanup.pointerKey(),
            pointer.getVersion(),
            cleanup.indexKey(),
            ownsIndex ? index.getVersion() : 0L));
  }

  /** Claims a task only while the table pointer is absent and the enclosing drop guard holds. */
  public Optional<Cleanup> claim(Cleanup cleanup, BatchGuard guard) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return Optional.empty();
      }
      var currentIndex = pointerStore.get(cleanup.indexKey()).orElse(null);
      boolean ownsIndex =
          currentIndex != null && cleanup.pointerKey().equals(currentIndex.getBlobUri());
      String tablePointer =
          Keys.tablePointerById(cleanup.tableId().getAccountId(), cleanup.tableId().getId());
      var claimed = cleanupTask(cleanup.pointerKey(), cleanup.tableId(), current.getVersion() + 1L);
      long indexVersion = ownsIndex ? currentIndex.getVersion() : 0L;
      var claimedIndex =
          ownsIndex
              ? cleanupIndex(
                  cleanup.indexKey(),
                  cleanup.namespaceId(),
                  cleanup.pointerKey(),
                  indexVersion + 1L)
              : null;
      var ops = new ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(cleanup.pointerKey(), current.getVersion(), claimed));
      if (ownsIndex) {
        ops.add(new PointerStore.CasUpsert(cleanup.indexKey(), indexVersion, claimedIndex));
      }
      ops.add(new PointerStore.CasCheckAbsent(tablePointer));
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return Optional.of(
            new Cleanup(
                cleanup.namespaceId(),
                cleanup.tableId(),
                cleanup.pointerKey(),
                claimed.getVersion(),
                cleanup.indexKey(),
                ownsIndex ? claimedIndex.getVersion() : 0L));
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "table cleanup lost the race against " + guard.describe());
      }
      if (pointerStore.get(tablePointer).isPresent()) {
        discardWhileTablePresent(cleanup);
        return Optional.empty();
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "table cleanup claim contended for: " + cleanup.pointerKey());
  }

  /** Removes a stale task only while an exact live table pointer proves cleanup is inapplicable. */
  private void discardWhileTablePresent(Cleanup cleanup) {
    String tablePointer =
        Keys.tablePointerById(cleanup.tableId().getAccountId(), cleanup.tableId().getId());
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var task = pointerStore.get(cleanup.pointerKey()).orElse(null);
      var index = pointerStore.get(cleanup.indexKey()).orElse(null);
      var table = pointerStore.get(tablePointer).orElse(null);
      if (task == null || table == null) {
        return;
      }
      var ops = new ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasDelete(cleanup.pointerKey(), task.getVersion()));
      if (index != null && cleanup.pointerKey().equals(index.getBlobUri())) {
        ops.add(new PointerStore.CasDelete(cleanup.indexKey(), index.getVersion()));
      }
      ops.add(new PointerStore.CasCheck(tablePointer, table.getVersion()));
      if (pointerStore.compareAndSetBatch(ops)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "stale table cleanup removal contended for: " + cleanup.pointerKey());
  }

  /** Pins every destructive cleanup write to the claimed task and an absent table pointer. */
  public BatchGuard claimedGuard(Cleanup cleanup) {
    String tablePointer =
        Keys.tablePointerById(cleanup.tableId().getAccountId(), cleanup.tableId().getId());
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        var ops = new ArrayList<PointerStore.CasOp>();
        ops.add(new PointerStore.CasCheck(cleanup.pointerKey(), cleanup.pointerVersion()));
        if (cleanup.indexVersion() > 0L) {
          ops.add(new PointerStore.CasCheck(cleanup.indexKey(), cleanup.indexVersion()));
        }
        ops.add(new PointerStore.CasCheckAbsent(tablePointer));
        return ops;
      }

      @Override
      public Outcome reevaluate() {
        var task = pointerStore.get(cleanup.pointerKey()).orElse(null);
        var index = pointerStore.get(cleanup.indexKey()).orElse(null);
        return task != null
                && task.getVersion() == cleanup.pointerVersion()
                && (cleanup.indexVersion() == 0L
                    || (index != null
                        && index.getVersion() == cleanup.indexVersion()
                        && cleanup.pointerKey().equals(index.getBlobUri())))
                && pointerStore.get(tablePointer).isEmpty()
            ? Outcome.HOLDS
            : Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return "claimed cleanup for table " + cleanup.tableId().getId();
      }
    };
  }

  /** Pins an idempotent repair that has no durable task to an absent table pointer. */
  public BatchGuard tableAbsentGuard(ResourceId tableId) {
    String tablePointer = Keys.tablePointerById(tableId.getAccountId(), tableId.getId());
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return List.of(new PointerStore.CasCheckAbsent(tablePointer));
      }

      @Override
      public Outcome reevaluate() {
        return pointerStore.get(tablePointer).isEmpty() ? Outcome.HOLDS : Outcome.BROKEN;
      }

      @Override
      public String describe() {
        return "absence of table " + tableId.getId();
      }
    };
  }

  /** Removes snapshot pointers in guarded transaction-sized batches. */
  public int deleteSnapshotPointers(ResourceId tableId, BatchGuard guard) {
    return deleteSnapshotPointers(
        tableId, guard, new BaseResourceRepository.GuardedDeleteProgress());
  }

  public int deleteSnapshotPointers(
      ResourceId tableId,
      BatchGuard guard,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    String prefix = Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId());
    int deleted = 0;
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      var rows = pointerStore.listPointersByPrefix(prefix, PAGE_SIZE, token, next, true);
      int guardItems = guard.ops().size();
      int capacity = MAX_TRANSACTION_ITEMS - guardItems;
      if (capacity <= 0) {
        throw new IllegalArgumentException("snapshot cleanup guard exhausts transaction capacity");
      }
      for (int from = 0; from < rows.size(); from += capacity) {
        int to = Math.min(rows.size(), from + capacity);
        deleted += deleteSnapshotBatch(rows.subList(from, to), guard, deleteProgress);
      }
      token = next.toString();
      if (token.isBlank()) {
        return deleted;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  private int deleteSnapshotBatch(
      List<ai.floedb.floecat.common.rpc.Pointer> scanned,
      BatchGuard guard,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    var remaining = new ArrayList<>(scanned);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      if (remaining.isEmpty()) {
        return 0;
      }
      var ops = new ArrayList<PointerStore.CasOp>(remaining.size() + guard.ops().size());
      for (var row : remaining) {
        ops.add(new PointerStore.CasDelete(row.getKey(), row.getVersion()));
      }
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        deleteProgress.recordWrite();
        return remaining.size();
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        String message = "snapshot cleanup lost the race against " + guard.describe();
        if (deleteProgress.hasPriorWrite()) {
          throw new BaseResourceRepository.BatchGuardFailedAfterWriteException(message);
        }
        throw new BaseResourceRepository.BatchGuardFailedException(message);
      }
      var refreshed = new ArrayList<ai.floedb.floecat.common.rpc.Pointer>(remaining.size());
      for (var row : remaining) {
        pointerStore.get(row.getKey()).ifPresent(refreshed::add);
      }
      remaining = refreshed;
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "snapshot cleanup batch contended for: " + scanned.getFirst().getKey());
  }

  /** Whether this namespace still owns a durable table-cleanup task. */
  public boolean hasAny(ResourceId namespaceId) {
    String prefix =
        Keys.namespaceTableCleanupPrefix(namespaceId.getAccountId(), namespaceId.getId());
    var seenTokens = new java.util.HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      if (!pointerStore.listPointersByPrefix(prefix, 1, token, next, true).isEmpty()) {
        return true;
      }
      token = next.toString();
      if (token.isBlank()) {
        return false;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  /**
   * Deletes cleanup rows that survived the account's namespace walk.
   *
   * <p>The namespace-scoped task is removed before its direct by-table index. If teardown stops
   * between the two families, the remaining index is directly reachable on the next pass; doing
   * this in the opposite order could leave a task whose namespace has already gone and no index
   * still names it. Every row delete carries {@code accountGone}, so account-id reuse cannot turn
   * this residual sweep against a replacement account.
   */
  public int deleteResidualRows(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    return deleteNamespaceTaskRows(accountId, accountGone, deleteProgress)
        + deleteRowsByPrefix(
            Keys.tableCleanupPointerByTablePrefix(accountId), accountGone, deleteProgress, false);
  }

  private int deleteNamespaceTaskRows(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    return deleteRowsByPrefix(
        Keys.namespaceRootPrefix(accountId), accountGone, deleteProgress, true);
  }

  private int deleteRowsByPrefix(
      String prefix,
      BatchGuard guard,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress,
      boolean namespaceTasksOnly) {
    int deleted = 0;
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row : pointerStore.listPointersByPrefix(prefix, PAGE_SIZE, token, next, true)) {
        if (namespaceTasksOnly && !isNamespaceCleanupTask(prefix, row.getKey())) {
          continue;
        }
        if (BaseResourceRepository.deletePointerWithGuard(
            pointerStore, row, guard, deleteProgress.hasPriorWrite())) {
          deleted++;
          deleteProgress.recordWrite();
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return deleted;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  private static boolean isNamespaceCleanupTask(String namespaceRoot, String key) {
    if (!key.startsWith(namespaceRoot)) {
      return false;
    }
    String relative = key.substring(namespaceRoot.length());
    int namespaceEnd = relative.indexOf('/');
    String family = "/table-cleanup/by-table/";
    return namespaceEnd > 0
        && relative.startsWith(family, namespaceEnd)
        && relative.length() > namespaceEnd + family.length();
  }

  /**
   * Enumerates every table id still named by either cleanup-handle family in an account.
   *
   * <p>The same table may be handed over twice, once per family. Callers consume cleanup
   * idempotently, keeping this scan streaming instead of retaining an account-sized deduplication
   * set during teardown.
   */
  public void forEachResidualTableId(String accountId, Consumer<ResourceId> action) {
    forEachResidualTableIdUnder(
        Keys.tableCleanupPointerByTablePrefix(accountId), false, accountId, action);
    forEachResidualTableIdUnder(Keys.namespaceRootPrefix(accountId), true, accountId, action);
  }

  private void forEachResidualTableIdUnder(
      String prefix, boolean namespaceTasksOnly, String accountId, Consumer<ResourceId> action) {
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row : pointerStore.listPointersByPrefix(prefix, PAGE_SIZE, token, next, true)) {
        if (namespaceTasksOnly && !isNamespaceCleanupTask(prefix, row.getKey())) {
          continue;
        }
        String tableId =
            namespaceTasksOnly
                ? (row.hasResourceId() ? row.getResourceId().getId() : "")
                : Keys.extractLastSegment(row.getKey());
        if (!tableId.isBlank()) {
          action.accept(
              ResourceId.newBuilder()
                  .setAccountId(accountId)
                  .setId(tableId)
                  .setKind(ResourceKind.RK_TABLE)
                  .build());
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "table cleanup residual scan did not advance; repeated page token: " + token);
      }
    }
  }

  /** Removes one optional owned-state pointer with the table-absence proof in the same batch. */
  public void deletePointer(String key, BatchGuard guard) {
    deletePointer(key, guard, new BaseResourceRepository.GuardedDeleteProgress());
  }

  public void deletePointer(
      String key, BatchGuard guard, BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    var current = pointerStore.get(key).orElse(null);
    if (current != null
        && BaseResourceRepository.deletePointerWithGuard(
            pointerStore, current, guard, deleteProgress.hasPriorWrite())) {
      deleteProgress.recordWrite();
    }
  }

  /** Removes a completed task; another idempotent worker may already have removed it. */
  public void complete(Cleanup cleanup) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return;
      }
      if (current.getVersion() != cleanup.pointerVersion()) {
        return;
      }
      var index = pointerStore.get(cleanup.indexKey()).orElse(null);
      boolean ownsExactIndex =
          cleanup.indexVersion() > 0L
              && index != null
              && index.getVersion() == cleanup.indexVersion()
              && cleanup.pointerKey().equals(index.getBlobUri());
      var ops = new ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasDelete(cleanup.pointerKey(), cleanup.pointerVersion()));
      if (ownsExactIndex) {
        ops.add(new PointerStore.CasDelete(cleanup.indexKey(), cleanup.indexVersion()));
      }
      if (pointerStore.compareAndSetBatch(ops)) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "table cleanup completion contended for: " + cleanup.pointerKey());
  }

  public void forEach(ResourceId namespaceId, Consumer<Cleanup> action) {
    String prefix =
        Keys.namespaceTableCleanupPrefix(namespaceId.getAccountId(), namespaceId.getId());
    var seenTokens = new HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row : pointerStore.listPointersByPrefix(prefix, PAGE_SIZE, token, next, true)) {
        String tableId = row.hasResourceId() ? row.getResourceId().getId() : "";
        if (tableId.isBlank()) {
          continue;
        }
        String indexKey = Keys.tableCleanupPointerByTable(namespaceId.getAccountId(), tableId);
        var index = pointerStore.get(indexKey).orElse(null);
        boolean ownsIndex = index != null && row.getKey().equals(index.getBlobUri());
        action.accept(
            new Cleanup(
                namespaceId,
                ResourceId.newBuilder()
                    .setAccountId(namespaceId.getAccountId())
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE)
                    .build(),
                row.getKey(),
                row.getVersion(),
                indexKey,
                ownsIndex ? index.getVersion() : 0L));
      }
      token = next.toString();
      if (token.isBlank()) {
        return;
      }
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  /** Finds a durable cleanup task for one table through its direct index. */
  public void forTable(ResourceId tableId, Consumer<Cleanup> action) {
    forTable(tableId, BatchGuard.NONE, new BaseResourceRepository.GuardedDeleteProgress(), action);
  }

  /** Finds a task while carrying the lifecycle guard through repair of a dangling direct index. */
  public void forTable(
      ResourceId tableId,
      BatchGuard guard,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress,
      Consumer<Cleanup> action) {
    String indexKey = Keys.tableCleanupPointerByTable(tableId.getAccountId(), tableId.getId());
    var index = pointerStore.get(indexKey).orElse(null);
    if (index == null || index.getBlobUri().isBlank() || !index.hasResourceId()) {
      return;
    }
    var namespaceId =
        index.getResourceId().toBuilder().setAccountId(tableId.getAccountId()).build();
    String taskKey = index.getBlobUri();
    var task = pointerStore.get(taskKey).orElse(null);
    if (task != null) {
      action.accept(
          new Cleanup(
              namespaceId, tableId, taskKey, task.getVersion(), indexKey, index.getVersion()));
      return;
    }
    if (BaseResourceRepository.deletePointerWithGuard(
        pointerStore,
        index,
        BatchGuard.all(guard, tableAbsentGuard(tableId)),
        deleteProgress.hasPriorWrite())) {
      deleteProgress.recordWrite();
    }
  }

  private static Pointer cleanupTask(String taskKey, ResourceId tableId, long version) {
    return PointerReferences.asOpaqueMarkerPointer(
            Pointer.newBuilder().setKey(taskKey).setVersion(version).setResourceId(tableId),
            taskKey)
        .build();
  }

  private static Pointer cleanupIndex(
      String indexKey, ResourceId namespaceId, String taskKey, long version) {
    return PointerReferences.asOpaqueMarkerPointer(
            Pointer.newBuilder().setKey(indexKey).setVersion(version).setResourceId(namespaceId),
            taskKey)
        .build();
  }
}
