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
      ResourceId namespaceId, ResourceId tableId, String pointerKey, long pointerVersion) {}

  /** Stages a handle before pointer deletion, so process death cannot erase the last table id. */
  public Cleanup prepare(ResourceId namespaceId, ResourceId tableId) {
    String key =
        Keys.namespaceTableCleanupPointer(
            namespaceId.getAccountId(), namespaceId.getId(), tableId.getId());
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var existing = pointerStore.get(key).orElse(null);
      if (existing != null) {
        return new Cleanup(namespaceId, tableId, key, existing.getVersion());
      }
      var marker = PointerReferences.opaqueMarkerPointer(key, tableId.getId(), 1L);
      if (pointerStore.compareAndSetBatch(List.of(new PointerStore.CasUpsert(key, 0L, marker)))) {
        return new Cleanup(namespaceId, tableId, key, 1L);
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "table cleanup staging contended for: " + key);
  }

  /** Claims a task only while the table pointer is absent and the enclosing drop guard holds. */
  public Optional<Cleanup> claim(Cleanup cleanup, BatchGuard guard) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return Optional.empty();
      }
      String tablePointer =
          Keys.tablePointerById(cleanup.tableId().getAccountId(), cleanup.tableId().getId());
      var claimed =
          PointerReferences.opaqueMarkerPointer(
              cleanup.pointerKey(), cleanup.tableId().getId(), current.getVersion() + 1L);
      var ops = new ArrayList<PointerStore.CasOp>();
      ops.add(new PointerStore.CasUpsert(cleanup.pointerKey(), current.getVersion(), claimed));
      ops.add(new PointerStore.CasCheckAbsent(tablePointer));
      ops.addAll(guard.ops());
      if (pointerStore.compareAndSetBatch(ops)) {
        return Optional.of(
            new Cleanup(
                cleanup.namespaceId(),
                cleanup.tableId(),
                cleanup.pointerKey(),
                claimed.getVersion()));
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        throw new BaseResourceRepository.BatchGuardFailedException(
            "table cleanup lost the race against " + guard.describe());
      }
      if (pointerStore.get(tablePointer).isPresent()) {
        return Optional.empty();
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "table cleanup claim contended for: " + cleanup.pointerKey());
  }

  /** Pins every destructive cleanup write to the claimed task and an absent table pointer. */
  public BatchGuard claimedGuard(Cleanup cleanup) {
    String tablePointer =
        Keys.tablePointerById(cleanup.tableId().getAccountId(), cleanup.tableId().getId());
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        return List.of(
            new PointerStore.CasCheck(cleanup.pointerKey(), cleanup.pointerVersion()),
            new PointerStore.CasCheckAbsent(tablePointer));
      }

      @Override
      public Outcome reevaluate() {
        var task = pointerStore.get(cleanup.pointerKey()).orElse(null);
        return task != null
                && task.getVersion() == cleanup.pointerVersion()
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
        deleted += deleteSnapshotBatch(rows.subList(from, to), guard, deleted > 0);
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
      List<ai.floedb.floecat.common.rpc.Pointer> scanned, BatchGuard guard, boolean priorWrite) {
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
        return remaining.size();
      }
      if (guard.reevaluate() == BatchGuard.Outcome.BROKEN) {
        String message = "snapshot cleanup lost the race against " + guard.describe();
        if (priorWrite) {
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

  /** Removes one optional owned-state pointer with the table-absence proof in the same batch. */
  public void deletePointer(String key, BatchGuard guard) {
    var current = pointerStore.get(key).orElse(null);
    if (current != null) {
      BaseResourceRepository.deletePointerWithGuard(pointerStore, current, guard, false);
    }
  }

  /** Removes a completed task; another idempotent worker may already have removed it. */
  public void complete(Cleanup cleanup) {
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      var current = pointerStore.get(cleanup.pointerKey()).orElse(null);
      if (current == null) {
        return;
      }
      if (pointerStore.compareAndSetBatch(
          List.of(new PointerStore.CasDelete(cleanup.pointerKey(), current.getVersion())))) {
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
        String tableId = Keys.extractLastSegment(row.getKey());
        action.accept(
            new Cleanup(
                namespaceId,
                ResourceId.newBuilder()
                    .setAccountId(namespaceId.getAccountId())
                    .setId(tableId)
                    .setKind(ResourceKind.RK_TABLE)
                    .build(),
                row.getKey(),
                row.getVersion()));
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
}
