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

package ai.floedb.floecat.service.transaction.impl;

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionIntentApplierSupport {

  private static final Logger LOG = Logger.getLogger(TransactionIntentApplierSupport.class);
  private static final int MAX_POINTER_TXN_OPS = 100;

  public enum ApplyStatus {
    APPLIED,
    RETRYABLE,
    CONFLICT
  }

  public record ApplyOutcome(
      ApplyStatus status,
      String errorCode,
      String errorMessage,
      Long expectedVersion,
      Long actualVersion,
      String conflictOwner,
      List<TableCleanupRepository.Cleanup> tableCleanups) {
    public static ApplyOutcome applied() {
      return applied(List.of());
    }

    public static ApplyOutcome applied(List<TableCleanupRepository.Cleanup> tableCleanups) {
      return new ApplyOutcome(
          ApplyStatus.APPLIED, null, null, null, null, null, List.copyOf(tableCleanups));
    }

    public static ApplyOutcome retryable(String errorCode, String errorMessage) {
      return new ApplyOutcome(
          ApplyStatus.RETRYABLE, errorCode, errorMessage, null, null, null, List.of());
    }

    public static ApplyOutcome conflict(
        String errorCode,
        String errorMessage,
        Long expectedVersion,
        Long actualVersion,
        String conflictOwner) {
      return new ApplyOutcome(
          ApplyStatus.CONFLICT,
          errorCode,
          errorMessage,
          expectedVersion,
          actualVersion,
          conflictOwner,
          List.of());
    }
  }

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject CatalogOverlay overlay;
  @Inject MarkerStore markerStore;

  public boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  public boolean isConnectorByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/connectors/by-id/");
  }

  public Table readTable(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.debugf("table blob missing: %s", blobUri);
        return null;
      }
      return Table.parseFrom(bytes);
    } catch (Exception e) {
      LOG.debugf("table blob parse failed: %s", blobUri, e);
      return null;
    }
  }

  public Connector readConnector(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.debugf("connector blob missing: %s", blobUri);
        return null;
      }
      return Connector.parseFrom(bytes);
    } catch (Exception e) {
      LOG.debugf("connector blob parse failed: %s", blobUri, e);
      return null;
    }
  }

  public ApplyOutcome updateTableNamePointers(
      Pointer currentPtr, Table nextTable, String nextBlobUri) {
    String accountId = nextTable.getResourceId().getAccountId();
    String newKey =
        Keys.tablePointerByName(
            accountId,
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());
    ApplyOutcome upsertResult = ensureNamePointer(newKey, nextTable, nextBlobUri);
    if (upsertResult.status != ApplyStatus.APPLIED) {
      return upsertResult;
    }

    if (currentPtr == null) {
      return ApplyOutcome.applied();
    }
    Table oldTable = readTable(currentPtr.getBlobUri());
    if (oldTable == null) {
      return ApplyOutcome.applied();
    }
    String oldKey =
        Keys.tablePointerByName(
            oldTable.getResourceId().getAccountId(),
            oldTable.getCatalogId().getId(),
            oldTable.getNamespaceId().getId(),
            oldTable.getDisplayName());
    if (!oldKey.equals(newKey)) {
      ApplyOutcome deleteResult = deleteNamePointerIfOwned(oldKey, oldTable);
      if (deleteResult.status != ApplyStatus.APPLIED) {
        return deleteResult;
      }
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome ensureNamePointer(String key, Table nextTable, String nextBlobUri) {
    String nextTableId = nextTable.getResourceId().getId();
    for (int i = 0; i < 3; i++) {
      var ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L);
        if (pointerStore.compareAndSet(key, 0L, created)) {
          return ApplyOutcome.applied();
        }
        continue;
      }
      if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
        return ApplyOutcome.applied();
      }
      Table existing = readTable(ptr.getBlobUri());
      if (existing == null || !existing.hasResourceId()) {
        return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "name pointer table missing");
      }
      String existingId = existing.getResourceId().getId();
      if (!Objects.equals(existingId, nextTableId)) {
        return ApplyOutcome.conflict(
            "NAME_POINTER_CONFLICT",
            "name pointer is owned by a different table",
            null,
            null,
            existingId);
      }
      Pointer next = PointerReferences.blobPointer(key, nextBlobUri, ptr.getVersion() + 1);
      if (pointerStore.compareAndSet(key, ptr.getVersion(), next)) {
        return ApplyOutcome.applied();
      }
    }
    return ApplyOutcome.retryable("NAME_POINTER_UPDATE_FAILED", "name pointer update conflict");
  }

  private ApplyOutcome deleteNamePointerIfOwned(String key, Table expectedOwner) {
    String ownerId = expectedOwner.getResourceId().getId();
    for (int i = 0; i < 3; i++) {
      var ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        return ApplyOutcome.applied();
      }
      Table existing = readTable(ptr.getBlobUri());
      if (existing == null || !existing.hasResourceId()) {
        return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
      }
      String existingId = existing.getResourceId().getId();
      if (!Objects.equals(existingId, ownerId)) {
        return ApplyOutcome.applied();
      }
      if (pointerStore.compareAndDelete(key, ptr.getVersion())) {
        return ApplyOutcome.applied();
      }
    }
    return ApplyOutcome.retryable("NAME_POINTER_DELETE_FAILED", "old name pointer delete conflict");
  }

  public void upsertPointerBestEffort(String key, String blobUri) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, blobUri, 1L);
      if (pointerStore.compareAndSet(key, 0L, created)) {
        return;
      }
      ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        LOG.warnf("pointer missing for %s", key);
        return;
      }
    }
    Pointer next = PointerReferences.blobPointer(key, blobUri, ptr.getVersion() + 1);
    if (!pointerStore.compareAndSet(key, ptr.getVersion(), next)) {
      LOG.warnf("pointer update conflict for %s", key);
    }
  }

  public ApplyOutcome applyIntentBestEffort(
      TransactionIntent intent, TransactionIntentRepository intentRepo) {
    return applyTransactionBestEffort(List.of(intent), intentRepo);
  }

  public ApplyOutcome applyTransactionBestEffort(
      List<TransactionIntent> intents, TransactionIntentRepository intentRepo) {
    if (intents == null || intents.isEmpty()) {
      return ApplyOutcome.retryable("EMPTY_TRANSACTION", "transaction has no intents");
    }

    var tableIntentContext = analyzeTableIntents(intents);
    if (tableIntentContext.conflict() != null) {
      return tableIntentContext.conflict();
    }

    var ops = new ArrayList<PointerStore.CasOp>();
    Set<String> touchedKeys = new HashSet<>();
    Set<String> fencedNamespaces = new HashSet<>();
    Set<String> fencedAccounts = new HashSet<>();
    Set<String> fencedTables = new HashSet<>();
    var tableCleanups = new ArrayList<TableCleanupRepository.Cleanup>();
    for (var intent : intents) {
      ApplyOutcome planOutcome =
          planIntentOps(
              intent,
              ops,
              touchedKeys,
              fencedNamespaces,
              fencedAccounts,
              fencedTables,
              tableIntentContext.mutations(),
              tableCleanups);
      if (planOutcome.status != ApplyStatus.APPLIED) {
        return planOutcome;
      }
      if (ops.size() > MAX_POINTER_TXN_OPS) {
        return ApplyOutcome.conflict(
            "POINTER_TXN_TOO_LARGE",
            "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
            null,
            null,
            null);
      }
    }

    if (!ops.isEmpty() && !pointerStore.compareAndSetBatch(ops)) {
      ApplyOutcome conflictOutcome = findExpectedVersionConflict(intents);
      if (conflictOutcome != null) {
        return conflictOutcome;
      }
      return ApplyOutcome.retryable("POINTER_TXN_CAS_FAILED", "pointer transaction conflict");
    }

    return ApplyOutcome.applied(tableCleanups);
  }

  public ApplyOutcome applyTransactionAtomically(
      Transaction appliedTransaction,
      long expectedTransactionPointerVersion,
      List<TransactionIntent> intents,
      TransactionIntentRepository intentRepo) {
    if (appliedTransaction == null) {
      return ApplyOutcome.retryable("MISSING_TRANSACTION", "applied transaction is required");
    }
    if (intents == null || intents.isEmpty()) {
      return ApplyOutcome.retryable("EMPTY_TRANSACTION", "transaction has no intents");
    }
    if (intentRepo == null) {
      return ApplyOutcome.retryable(
          "MISSING_INTENT_REPOSITORY", "transaction intent repository is required");
    }

    var tableIntentContext = analyzeTableIntents(intents);
    if (tableIntentContext.conflict() != null) {
      return tableIntentContext.conflict();
    }

    var ops = new ArrayList<PointerStore.CasOp>();
    Set<String> touchedKeys = new HashSet<>();
    Set<String> fencedNamespaces = new HashSet<>();
    Set<String> fencedAccounts = new HashSet<>();
    Set<String> fencedTables = new HashSet<>();
    var tableCleanups = new ArrayList<TableCleanupRepository.Cleanup>();
    for (var intent : intents) {
      ApplyOutcome planOutcome =
          planIntentOps(
              intent,
              ops,
              touchedKeys,
              fencedNamespaces,
              fencedAccounts,
              fencedTables,
              tableIntentContext.mutations(),
              tableCleanups);
      if (planOutcome.status != ApplyStatus.APPLIED) {
        return planOutcome;
      }
      ApplyOutcome cleanupOutcome = appendIntentCleanupOps(intent, intentRepo, touchedKeys, ops);
      if (cleanupOutcome.status != ApplyStatus.APPLIED) {
        return cleanupOutcome;
      }
      if (ops.size() > MAX_POINTER_TXN_OPS - 1) {
        return ApplyOutcome.conflict(
            "POINTER_TXN_TOO_LARGE",
            "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
            null,
            null,
            null);
      }
    }

    // The transaction-state publication is itself account-owned state. Table-only and
    // delete-only batches may not otherwise need an account fence, so carry one unconditionally
    // before making TS_APPLIED visible.
    ApplyOutcome accountOutcome =
        appendAccountFence(appliedTransaction.getAccountId(), fencedAccounts, touchedKeys, ops);
    if (accountOutcome.status != ApplyStatus.APPLIED) {
      return accountOutcome;
    }

    ApplyOutcome txOutcome =
        appendTransactionAppliedOp(
            appliedTransaction, expectedTransactionPointerVersion, touchedKeys, ops);
    if (txOutcome.status != ApplyStatus.APPLIED) {
      return txOutcome;
    }
    if (ops.size() > MAX_POINTER_TXN_OPS) {
      return ApplyOutcome.conflict(
          "POINTER_TXN_TOO_LARGE",
          "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
          null,
          null,
          null);
    }

    if (pointerStore.compareAndSetBatch(ops)) {
      return ApplyOutcome.applied(tableCleanups);
    }
    // None of this invocation's planned cleanup handles exist when its batch loses. In
    // particular, a concurrent winner may have committed different generation keys. Leave the
    // outcome empty so an observed TS_APPLIED state reconstructs the winner's durable handles
    // from the retained by-transaction delete intents.
    return classifyAtomicApplyFailure(
        appliedTransaction, expectedTransactionPointerVersion, intents, intentRepo);
  }

  private ApplyOutcome planIntentOps(
      TransactionIntent intent,
      List<PointerStore.CasOp> ops,
      Set<String> touchedKeys,
      Set<String> fencedNamespaces,
      Set<String> fencedAccounts,
      Set<String> fencedTables,
      Map<String, Boolean> tableMutations,
      List<TableCleanupRepository.Cleanup> tableCleanups) {
    String pointerKey = intent.getTargetPointerKey();
    if (isTableByIdPointer(pointerKey)) {
      return planTableIntentOps(intent, ops, touchedKeys, fencedNamespaces, tableCleanups);
    }
    if (isConnectorByIdPointer(pointerKey)) {
      return planConnectorIntentOps(intent, ops, touchedKeys, fencedAccounts);
    }

    var current = pointerStore.get(pointerKey).orElse(null);
    long actualVersion = current == null ? 0L : current.getVersion();
    if (intent.hasExpectedVersion() && actualVersion != intent.getExpectedVersion()) {
      return ApplyOutcome.conflict(
          "EXPECTED_VERSION_MISMATCH",
          "pointer version does not match intent expected_version",
          intent.getExpectedVersion(),
          actualVersion,
          null);
    }

    final ApplyOutcome mutationOutcome;
    if (isDeleteSentinel(intent)) {
      if (current == null) {
        mutationOutcome = addAbsentCheck(pointerKey, touchedKeys, ops);
      } else {
        long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
        mutationOutcome =
            addOp(new PointerStore.CasDelete(pointerKey, expected), pointerKey, touchedKeys, ops);
      }
    } else if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      mutationOutcome = addCheck(pointerKey, expected, touchedKeys, ops);
    } else {
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
      mutationOutcome =
          addOp(
              new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
    }
    if (mutationOutcome.status != ApplyStatus.APPLIED) {
      return mutationOutcome;
    }
    ApplyOutcome tableOutcome =
        appendTableFence(intent, fencedTables, tableMutations, touchedKeys, ops);
    if (tableOutcome.status != ApplyStatus.APPLIED) {
      return tableOutcome;
    }
    return appendAccountFence(intent.getAccountId(), fencedAccounts, touchedKeys, ops);
  }

  /** Pins generic snapshot-pointer intents to their live owning table in the atomic apply batch. */
  private ApplyOutcome appendTableFence(
      TransactionIntent intent,
      Set<String> fencedTables,
      Map<String, Boolean> tableMutations,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    String tableIdValue = Keys.tableIdFromSnapshotPointerKey(intent.getTargetPointerKey());
    if (tableIdValue == null || isDeleteSentinel(intent)) {
      return ApplyOutcome.applied();
    }
    String tablePointerKey = Keys.tablePointerById(intent.getAccountId(), tableIdValue);
    Boolean remainsLive = tableMutations.get(tablePointerKey);
    if (Boolean.TRUE.equals(remainsLive)) {
      // The table upsert/check is already in this same all-or-nothing batch. It is stronger than a
      // separate liveness check and avoids claiming the canonical key twice.
      return ApplyOutcome.applied();
    }
    if (Boolean.FALSE.equals(remainsLive)) {
      return tableDeleteSnapshotPublishConflict(tablePointerKey);
    }
    if (!fencedTables.add(tablePointerKey)) {
      return ApplyOutcome.applied();
    }
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId(intent.getAccountId())
            .setId(tableIdValue)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    final List<PointerStore.CasOp> fenceOps;
    try {
      fenceOps = markerStore.tableLiveGuard(tableId).ops();
    } catch (BaseResourceRepository.BatchGuardFailedException tableGone) {
      return ApplyOutcome.conflict("TABLE_MISSING", tableGone.getMessage(), null, null, null);
    }
    for (var op : fenceOps) {
      ApplyOutcome added = addOp(op, op.key(), touchedKeys, ops);
      if (added.status != ApplyStatus.APPLIED) {
        return added;
      }
    }
    return ApplyOutcome.applied();
  }

  private record TableIntentContext(Map<String, Boolean> mutations, ApplyOutcome conflict) {}

  /**
   * Classifies canonical table mutations before planning so a snapshot intent can share the table
   * mutation's liveness proof regardless of intent order.
   */
  private TableIntentContext analyzeTableIntents(List<TransactionIntent> intents) {
    Map<String, Boolean> mutations = new HashMap<>();
    for (var intent : intents) {
      String pointerKey = intent.getTargetPointerKey();
      if (!isTableByIdPointer(pointerKey)) {
        continue;
      }
      if (mutations.putIfAbsent(pointerKey, !isDeleteSentinel(intent)) != null) {
        return new TableIntentContext(
            Map.of(),
            ApplyOutcome.conflict(
                "POINTER_TXN_DUPLICATE_KEY",
                "transaction attempts multiple updates to pointer key " + pointerKey,
                null,
                null,
                null));
      }
    }
    for (var intent : intents) {
      String tableId = Keys.tableIdFromSnapshotPointerKey(intent.getTargetPointerKey());
      if (tableId == null || isDeleteSentinel(intent)) {
        continue;
      }
      String tablePointerKey = Keys.tablePointerById(intent.getAccountId(), tableId);
      if (Boolean.FALSE.equals(mutations.get(tablePointerKey))) {
        return new TableIntentContext(
            Map.of(), tableDeleteSnapshotPublishConflict(tablePointerKey));
      }
    }
    return new TableIntentContext(Map.copyOf(mutations), null);
  }

  private static ApplyOutcome tableDeleteSnapshotPublishConflict(String tablePointerKey) {
    return ApplyOutcome.conflict(
        "TABLE_DELETE_WITH_SNAPSHOT_PUBLISH",
        "transaction cannot publish a snapshot while deleting table pointer " + tablePointerKey,
        null,
        null,
        null);
  }

  private boolean isDeleteSentinel(TransactionIntent intent) {
    if (intent == null
        || intent.getBlobUri().isBlank()
        || intent.getAccountId().isBlank()
        || intent.getTxId().isBlank()
        || intent.getTargetPointerKey().isBlank()) {
      return false;
    }
    try {
      return Keys.transactionDeleteSentinelUri(
              intent.getAccountId(), intent.getTxId(), intent.getTargetPointerKey())
          .equals(intent.getBlobUri());
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private ApplyOutcome planTableIntentOps(
      TransactionIntent intent,
      List<PointerStore.CasOp> ops,
      Set<String> touchedKeys,
      Set<String> fencedNamespaces,
      List<TableCleanupRepository.Cleanup> tableCleanups) {
    String pointerKey = intent.getTargetPointerKey();
    var current = pointerStore.get(pointerKey).orElse(null);
    long actualVersion = current == null ? 0L : current.getVersion();
    if (intent.hasExpectedVersion() && actualVersion != intent.getExpectedVersion()) {
      return ApplyOutcome.conflict(
          "EXPECTED_VERSION_MISMATCH",
          "pointer version does not match intent expected_version",
          intent.getExpectedVersion(),
          actualVersion,
          null);
    }

    if (isDeleteSentinel(intent)) {
      return planTableDeleteIntentOps(
          intent, current, actualVersion, ops, touchedKeys, tableCleanups);
    }

    Table nextTable = readTable(intent.getBlobUri());
    if (nextTable == null) {
      return ApplyOutcome.retryable("TABLE_BLOB_MISSING", "table blob missing");
    }
    ApplyOutcome targetValidation = validateTableIntentTarget(pointerKey, nextTable);
    if (targetValidation.status != ApplyStatus.APPLIED) {
      return targetValidation;
    }
    ApplyOutcome writeEligibility =
        validateTableWriteEligibility(
            nextTable, /* checkExistingTable= */ current != null, /* checkTargetScope= */ true);
    if (writeEligibility.status != ApplyStatus.APPLIED) {
      return writeEligibility;
    }

    String nextTableId = nextTable.getResourceId().getId();
    String newNameKey =
        Keys.tablePointerByName(
            nextTable.getResourceId().getAccountId(),
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;

    // A table with no pointer yet becomes visible in its namespace when this batch commits, so the
    // batch is a child publish and needs the namespace fence. A reparent is one too, and is
    // detected below once the old table has been read.
    boolean publishesChild = current == null;

    if (current == null || !Objects.equals(current.getBlobUri(), intent.getBlobUri())) {
      Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
      ApplyOutcome outcome =
          addOp(
              new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    } else {
      ApplyOutcome outcome = addCheck(pointerKey, expected, touchedKeys, ops);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    }

    ApplyOutcome newNameOutcome =
        buildNameUpsertOp(newNameKey, nextTableId, intent.getBlobUri(), touchedKeys, ops);
    if (newNameOutcome.status != ApplyStatus.APPLIED) {
      return newNameOutcome;
    }

    String newRelationKey =
        Keys.relationPointerByName(
            nextTable.getResourceId().getAccountId(),
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());
    ApplyOutcome newClaimOutcome =
        buildRelationClaimUpsertOp(
            newRelationKey, nextTable.getResourceId(), intent.getBlobUri(), touchedKeys, ops);
    if (newClaimOutcome.status != ApplyStatus.APPLIED) {
      return newClaimOutcome;
    }

    if (current != null) {
      Table oldTable = readTable(current.getBlobUri());
      if (oldTable == null) {
        return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
      }
      publishesChild =
          !oldTable.getNamespaceId().getId().equals(nextTable.getNamespaceId().getId())
              || !oldTable.getCatalogId().getId().equals(nextTable.getCatalogId().getId());
      String oldNameKey =
          Keys.tablePointerByName(
              oldTable.getResourceId().getAccountId(),
              oldTable.getCatalogId().getId(),
              oldTable.getNamespaceId().getId(),
              oldTable.getDisplayName());
      if (!oldNameKey.equals(newNameKey)) {
        ApplyOutcome oldNameOutcome =
            buildOwnedNameDeleteOp(oldNameKey, oldTable.getResourceId().getId(), touchedKeys, ops);
        if (oldNameOutcome.status != ApplyStatus.APPLIED) {
          return oldNameOutcome;
        }
        String oldRelationKey =
            Keys.relationPointerByName(
                oldTable.getResourceId().getAccountId(),
                oldTable.getCatalogId().getId(),
                oldTable.getNamespaceId().getId(),
                oldTable.getDisplayName());
        ApplyOutcome oldClaimOutcome =
            buildOwnedRelationClaimDeleteOp(
                oldRelationKey, oldTable.getResourceId().getId(), touchedKeys, ops);
        if (oldClaimOutcome.status != ApplyStatus.APPLIED) {
          return oldClaimOutcome;
        }
      }
    }

    // Last, so a name collision is still reported as such rather than as namespace contention.
    if (publishesChild) {
      return appendNamespaceChildFence(nextTable, fencedNamespaces, touchedKeys, ops);
    }
    return ApplyOutcome.applied();
  }

  /**
   * Folds the namespace child fence into this batch when it publishes a newly visible table or
   * reparents a table into the destination namespace.
   *
   * <p>The applier assembles its own {@link PointerStore#compareAndSetBatch} instead of going
   * through the repository, so it does not inherit the guard the service-side create and reparent
   * paths pass to {@code TableRepository} and {@code TableCleanupRepository} (see {@link
   * MarkerStore#namespaceChildGuard}). Without the fence here, a committing transaction can publish
   * a table or cleanup task into a namespace a concurrent {@code DeleteNamespace} is tearing down:
   * the deleter's batch checks only the children marker, which an unfenced apply never moves, so
   * both batches commit and state survives under a namespace that is gone (see {@link BatchGuard}).
   *
   * <p>One fence per namespace per batch. The marker advance is a single CAS, so two tables created
   * in the same namespace by one transaction share it — and issuing it twice would collide on that
   * key.
   */
  private ApplyOutcome appendNamespaceChildFence(
      Table table,
      Set<String> fencedNamespaces,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    ResourceId namespaceId =
        table.getNamespaceId().toBuilder()
            .setAccountId(table.getResourceId().getAccountId())
            .build();
    if (!fencedNamespaces.add(namespaceId.getId())) {
      return ApplyOutcome.applied();
    }
    List<PointerStore.CasOp> fenceOps;
    try {
      var writePolicy = new CatalogSurfaceWritePolicy(overlay);
      var namespace =
          writePolicy.requireWritableNamespace(
              namespaceId, "table.namespace_id", "transaction-apply");
      writePolicy.requireNamespaceInCatalog(
          namespace, namespaceId, table.getCatalogId(), "transaction-apply");
      fenceOps = markerStore.namespaceChildGuard(namespaceId, namespace.blobUri()).ops();
    } catch (BaseResourceRepository.BatchGuardFailedException namespaceGone) {
      // No live namespace pointer to pin to, so there is nothing legitimate to publish into.
      //
      // Terminal, not retryable. Commit does not re-plan: it re-reads the intents frozen at
      // prepare (TransactionsServiceImpl#commitTransaction) and applies them unchanged, so the
      // namespace this intent names stays gone and every attempt fails here identically. Calling
      // it retryable spent the transaction's whole attempt budget on a verdict that could not
      // change, and the by-target and by-tx intent pointers — released only by a successful apply
      // batch — stayed held against those tables for the duration, blocking any other transaction
      // that wanted them.
      return ApplyOutcome.conflict(
          "NAMESPACE_MISSING", namespaceGone.getMessage(), null, null, null);
    } catch (StatusRuntimeException namespaceChanged) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_NOT_WRITABLE", "table intent target namespace changed", null, null, null);
    }
    for (var op : fenceOps) {
      ApplyOutcome added = addOp(op, op.key(), touchedKeys, ops);
      if (added.status != ApplyStatus.APPLIED) {
        return added;
      }
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome planTableDeleteIntentOps(
      TransactionIntent intent,
      Pointer current,
      long actualVersion,
      List<PointerStore.CasOp> ops,
      Set<String> touchedKeys,
      List<TableCleanupRepository.Cleanup> tableCleanups) {
    if (current == null) {
      return addAbsentCheck(intent.getTargetPointerKey(), touchedKeys, ops);
    }

    Table currentTable = readTable(current.getBlobUri());
    if (currentTable == null || !currentTable.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "current table pointer missing");
    }
    ApplyOutcome targetValidation =
        validateTableIntentTarget(intent.getTargetPointerKey(), currentTable);
    if (targetValidation.status != ApplyStatus.APPLIED) {
      return targetValidation;
    }
    ApplyOutcome writeEligibility =
        validateTableWriteEligibility(
            currentTable, /* checkExistingTable= */ true, /* checkTargetScope= */ false);
    if (writeEligibility.status != ApplyStatus.APPLIED) {
      return writeEligibility;
    }

    String accountId = currentTable.getResourceId().getAccountId();
    var namespaceId = currentTable.getNamespaceId().toBuilder().setAccountId(accountId).build();
    var tableId = currentTable.getResourceId().toBuilder().setAccountId(accountId).build();
    String cleanupKey =
        Keys.namespaceTableCleanupGenerationPointer(
            accountId, namespaceId.getId(), tableId.getId(), UUID.randomUUID().toString());
    long cleanupVersion = 1L;
    Pointer cleanupTask =
        PointerReferences.asOpaqueMarkerPointer(
                Pointer.newBuilder()
                    .setKey(cleanupKey)
                    .setVersion(cleanupVersion)
                    .setResourceId(tableId),
                cleanupKey)
            .build();
    PointerStore.CasOp cleanupOp = new PointerStore.CasUpsert(cleanupKey, 0L, cleanupTask);
    ApplyOutcome stagedCleanup = addOp(cleanupOp, cleanupKey, touchedKeys, ops);
    if (stagedCleanup.status != ApplyStatus.APPLIED) {
      return stagedCleanup;
    }
    String cleanupIndexKey = Keys.tableCleanupPointerByTable(accountId, tableId.getId());
    Pointer existingCleanupIndex = pointerStore.get(cleanupIndexKey).orElse(null);
    long cleanupIndexExpected =
        existingCleanupIndex == null ? 0L : existingCleanupIndex.getVersion();
    long cleanupIndexVersion = cleanupIndexExpected + 1L;
    Pointer cleanupIndex =
        PointerReferences.asOpaqueMarkerPointer(
                Pointer.newBuilder()
                    .setKey(cleanupIndexKey)
                    .setVersion(cleanupIndexVersion)
                    .setResourceId(namespaceId),
                cleanupKey)
            .build();
    ApplyOutcome stagedCleanupIndex =
        addOp(
            new PointerStore.CasUpsert(cleanupIndexKey, cleanupIndexExpected, cleanupIndex),
            cleanupIndexKey,
            touchedKeys,
            ops);
    if (stagedCleanupIndex.status != ApplyStatus.APPLIED) {
      return stagedCleanupIndex;
    }
    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    ApplyOutcome deletePrimary =
        addOp(
            new PointerStore.CasDelete(intent.getTargetPointerKey(), expected),
            intent.getTargetPointerKey(),
            touchedKeys,
            ops);
    if (deletePrimary.status != ApplyStatus.APPLIED) {
      return deletePrimary;
    }

    String nameKey =
        Keys.tablePointerByName(
            currentTable.getResourceId().getAccountId(),
            currentTable.getCatalogId().getId(),
            currentTable.getNamespaceId().getId(),
            currentTable.getDisplayName());
    ApplyOutcome nameDelete =
        buildOwnedNameDeleteOp(nameKey, currentTable.getResourceId().getId(), touchedKeys, ops);
    if (nameDelete.status != ApplyStatus.APPLIED) {
      return nameDelete;
    }
    String relationKey =
        Keys.relationPointerByName(
            currentTable.getResourceId().getAccountId(),
            currentTable.getCatalogId().getId(),
            currentTable.getNamespaceId().getId(),
            currentTable.getDisplayName());
    ApplyOutcome relationDelete =
        buildOwnedRelationClaimDeleteOp(
            relationKey, currentTable.getResourceId().getId(), touchedKeys, ops);
    if (relationDelete.status == ApplyStatus.APPLIED) {
      tableCleanups.add(
          new TableCleanupRepository.Cleanup(
              namespaceId,
              tableId,
              cleanupKey,
              cleanupVersion,
              cleanupIndexKey,
              cleanupIndexVersion));
    }
    return relationDelete;
  }

  private ApplyOutcome planConnectorIntentOps(
      TransactionIntent intent,
      List<PointerStore.CasOp> ops,
      Set<String> touchedKeys,
      Set<String> fencedAccounts) {
    String pointerKey = intent.getTargetPointerKey();
    var current = pointerStore.get(pointerKey).orElse(null);
    long actualVersion = current == null ? 0L : current.getVersion();
    if (intent.hasExpectedVersion() && actualVersion != intent.getExpectedVersion()) {
      return ApplyOutcome.conflict(
          "EXPECTED_VERSION_MISMATCH",
          "pointer version does not match intent expected_version",
          intent.getExpectedVersion(),
          actualVersion,
          null);
    }

    if (isDeleteSentinel(intent)) {
      if (current == null) {
        return addAbsentCheck(intent.getTargetPointerKey(), touchedKeys, ops);
      }
      Connector currentConnector = readConnector(current.getBlobUri());
      if (currentConnector == null || !currentConnector.hasResourceId()) {
        return ApplyOutcome.retryable(
            "NAME_POINTER_READ_FAILED", "current connector pointer missing");
      }
      ApplyOutcome targetValidation =
          validateConnectorIntentTarget(intent.getTargetPointerKey(), currentConnector);
      if (targetValidation.status != ApplyStatus.APPLIED) {
        return targetValidation;
      }
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      ApplyOutcome deletePrimary =
          addOp(
              new PointerStore.CasDelete(intent.getTargetPointerKey(), expected),
              intent.getTargetPointerKey(),
              touchedKeys,
              ops);
      if (deletePrimary.status != ApplyStatus.APPLIED) {
        return deletePrimary;
      }
      String nameKey =
          Keys.connectorPointerByName(
              currentConnector.getResourceId().getAccountId(), currentConnector.getDisplayName());
      return buildOwnedConnectorNameDeleteOp(
          nameKey, currentConnector.getResourceId().getId(), touchedKeys, ops);
    }

    Connector nextConnector = readConnector(intent.getBlobUri());
    if (nextConnector == null) {
      return ApplyOutcome.retryable("CONNECTOR_BLOB_MISSING", "connector blob missing");
    }
    ApplyOutcome targetValidation = validateConnectorIntentTarget(pointerKey, nextConnector);
    if (targetValidation.status != ApplyStatus.APPLIED) {
      return targetValidation;
    }

    String nextConnectorId = nextConnector.getResourceId().getId();
    String newNameKey =
        Keys.connectorPointerByName(
            nextConnector.getResourceId().getAccountId(), nextConnector.getDisplayName());

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    if (current == null || !Objects.equals(current.getBlobUri(), intent.getBlobUri())) {
      Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
      ApplyOutcome outcome =
          addOp(
              new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    } else {
      ApplyOutcome outcome = addCheck(pointerKey, expected, touchedKeys, ops);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    }

    ApplyOutcome newNameOutcome =
        buildConnectorNameUpsertOp(
            newNameKey, nextConnectorId, intent.getBlobUri(), touchedKeys, ops);
    if (newNameOutcome.status != ApplyStatus.APPLIED) {
      return newNameOutcome;
    }

    if (current != null) {
      Connector oldConnector = readConnector(current.getBlobUri());
      if (oldConnector == null || !oldConnector.hasResourceId()) {
        return ApplyOutcome.retryable(
            "NAME_POINTER_READ_FAILED", "old name pointer connector missing");
      }
      String oldNameKey =
          Keys.connectorPointerByName(
              oldConnector.getResourceId().getAccountId(), oldConnector.getDisplayName());
      if (!oldNameKey.equals(newNameKey)) {
        ApplyOutcome oldNameOutcome =
            buildOwnedConnectorNameDeleteOp(
                oldNameKey, oldConnector.getResourceId().getId(), touchedKeys, ops);
        if (oldNameOutcome.status != ApplyStatus.APPLIED) {
          return oldNameOutcome;
        }
      }
    }
    return appendAccountFence(
        nextConnector.getResourceId().getAccountId(), fencedAccounts, touchedKeys, ops);
  }

  private ApplyOutcome appendAccountFence(
      String accountId,
      Set<String> fencedAccounts,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    if (!fencedAccounts.add(accountId)) {
      return ApplyOutcome.applied();
    }
    var accountLive = markerStore.accountLiveGuard(accountId);
    if (accountLive.isEmpty()) {
      return ApplyOutcome.conflict(
          "ACCOUNT_MISSING", "transaction intent account is missing", null, null, null);
    }
    for (var op : accountLive.get().ops()) {
      ApplyOutcome added = addOp(op, op.key(), touchedKeys, ops);
      if (added.status != ApplyStatus.APPLIED) {
        return added;
      }
    }
    return ApplyOutcome.applied();
  }

  /**
   * Evaluates whether a table intent may be applied. The two checks are orthogonal: {@code
   * checkExistingTable} verifies the table row itself is user-owned and writable (used when the
   * intent mutates or deletes a row that already exists), while {@code checkTargetScope} verifies
   * the destination catalog and namespace are writable and mutually consistent (used when the
   * intent writes a row into a scope). Create passes scope-only, update passes both, delete passes
   * existing-only.
   */
  private ApplyOutcome validateTableWriteEligibility(
      Table table, boolean checkExistingTable, boolean checkTargetScope) {
    if (table == null || !table.hasResourceId()) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_INVALID_PAYLOAD", "table payload is missing resource_id", null, null, null);
    }
    if (SystemResourceIdGenerator.isSystemId(table.getResourceId())) {
      return tableImmutableConflict(table.getResourceId().getId());
    }
    if (overlay == null) {
      // The overlay is @Inject-ed on an @ApplicationScoped bean, so this is unreachable in
      // production. A guard whose purpose is closing write-eligibility gaps must not default to
      // "allow" on its own null case — treat an absent overlay as retryable rather than applied.
      return ApplyOutcome.retryable(
          "TABLE_WRITE_ELIGIBILITY_UNAVAILABLE",
          "catalog overlay unavailable for write-eligibility check");
    }

    try {
      var writePolicy = new CatalogSurfaceWritePolicy(overlay);
      if (checkExistingTable) {
        writePolicy.requireWritableTable(table.getResourceId(), "transaction-apply");
      }
      if (checkTargetScope) {
        writePolicy.requireWritableCatalog(
            table.getCatalogId(), "table.catalog_id", "transaction-apply");
        var namespace =
            writePolicy.requireWritableNamespace(
                table.getNamespaceId(), "table.namespace_id", "transaction-apply");
        writePolicy.requireNamespaceInCatalog(
            namespace, table.getNamespaceId(), table.getCatalogId(), "transaction-apply");
      }
      return ApplyOutcome.applied();
    } catch (StatusRuntimeException policyViolation) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_NOT_WRITABLE", "table intent target is not writable", null, null, null);
    } catch (RuntimeException unexpected) {
      // Overlay resolution can fail transiently (storage/cache errors). Do not turn that into a
      // terminal conflict that abandons an otherwise-valid transaction; let it be retried.
      return ApplyOutcome.retryable(
          "TABLE_WRITE_ELIGIBILITY_ERROR",
          "unable to evaluate table write eligibility: " + unexpected.getMessage());
    }
  }

  private ApplyOutcome tableImmutableConflict(String tableId) {
    return ApplyOutcome.conflict(
        "SYSTEM_OBJECT_IMMUTABLE",
        "system table is immutable",
        null,
        null,
        tableId == null ? "" : tableId);
  }

  private ApplyOutcome validateTableIntentTarget(String pointerKey, Table nextTable) {
    if (nextTable == null || !nextTable.hasResourceId()) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_INVALID_PAYLOAD", "table payload is missing resource_id", null, null, null);
    }
    String expectedKey;
    try {
      expectedKey =
          Keys.tablePointerById(
              nextTable.getResourceId().getAccountId(), nextTable.getResourceId().getId());
    } catch (IllegalArgumentException e) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_INVALID_PAYLOAD",
          "table payload has invalid resource_id fields",
          null,
          null,
          null);
    }
    if (!Objects.equals(expectedKey, pointerKey)) {
      return ApplyOutcome.conflict(
          "TABLE_INTENT_TARGET_MISMATCH",
          "table payload resource_id does not match target pointer",
          null,
          null,
          null);
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome validateConnectorIntentTarget(String pointerKey, Connector nextConnector) {
    if (nextConnector == null || !nextConnector.hasResourceId()) {
      return ApplyOutcome.conflict(
          "CONNECTOR_INTENT_INVALID_PAYLOAD",
          "connector payload is missing resource_id",
          null,
          null,
          null);
    }
    String expectedKey;
    try {
      expectedKey =
          Keys.connectorPointerById(
              nextConnector.getResourceId().getAccountId(), nextConnector.getResourceId().getId());
    } catch (IllegalArgumentException e) {
      return ApplyOutcome.conflict(
          "CONNECTOR_INTENT_INVALID_PAYLOAD",
          "connector payload has invalid resource_id fields",
          null,
          null,
          null);
    }
    if (!Objects.equals(expectedKey, pointerKey)) {
      return ApplyOutcome.conflict(
          "CONNECTOR_INTENT_TARGET_MISMATCH",
          "connector payload resource_id does not match target pointer",
          null,
          null,
          null);
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome buildNameUpsertOp(
      String key,
      String nextTableId,
      String nextBlobUri,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L);
      return addOp(new PointerStore.CasUpsert(key, 0L, created), key, touchedKeys, ops);
    }
    if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
      return addCheck(key, ptr.getVersion(), touchedKeys, ops);
    }
    Table existing = readTable(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "name pointer table missing");
    }
    String existingId = existing.getResourceId().getId();
    if (!Objects.equals(existingId, nextTableId)) {
      return ApplyOutcome.conflict(
          "NAME_POINTER_CONFLICT",
          "name pointer is owned by a different table",
          null,
          null,
          existingId);
    }
    Pointer next = PointerReferences.blobPointer(key, nextBlobUri, ptr.getVersion() + 1L);
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), key, touchedKeys, ops);
  }

  /**
   * Reserves the shared, kind-agnostic relation-name claim ({@link Keys#relationPointerByName}) so
   * a table and a view can never hold the same (namespace, name). Ownership and kind are read from
   * the claim's stored {@link ResourceId} (stable across renames) rather than from the name or
   * blob, so a claim held by any other relation — a different table or a view of the same name — is
   * a hard conflict.
   */
  private ApplyOutcome buildRelationClaimUpsertOp(
      String key,
      ResourceId owner,
      String nextBlobUri,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L, owner, "");
      return addOp(new PointerStore.CasUpsert(key, 0L, created), key, touchedKeys, ops);
    }
    ResourceId held = ptr.getResourceId();
    if (!Objects.equals(held.getId(), owner.getId())) {
      // Same cross-kind invariant that ViewServiceImpl#relationNameConflict enforces on the direct
      // RPC path. These are two independent claim implementations (this one hand-rolls the CAS ops
      // rather than routing through the generic repository), so the surfaced codes differ by
      // channel — RELATION_NAME_CONFLICT here (transaction apply outcome) vs
      // RELATION_NAME_ALREADY_CLAIMED there (gRPC ALREADY_EXISTS) — but the condition is identical:
      // the name is already claimed by a relation of any kind.
      return ApplyOutcome.conflict(
          "RELATION_NAME_CONFLICT",
          "relation name is already claimed by another relation",
          null,
          null,
          held.getId());
    }
    if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
      return ApplyOutcome.applied();
    }
    Pointer next =
        PointerReferences.blobPointer(key, nextBlobUri, ptr.getVersion() + 1L, owner, "");
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), key, touchedKeys, ops);
  }

  private ApplyOutcome buildOwnedRelationClaimDeleteOp(
      String key, String ownerId, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return ApplyOutcome.applied();
    }
    if (Objects.equals(ptr.getResourceId().getId(), ownerId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), key, touchedKeys, ops);
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome buildOwnedNameDeleteOp(
      String key, String ownerTableId, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return addAbsentCheck(key, touchedKeys, ops);
    }
    Table existing = readTable(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
    }
    if (Objects.equals(existing.getResourceId().getId(), ownerTableId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), key, touchedKeys, ops);
    }
    return addCheck(key, ptr.getVersion(), touchedKeys, ops);
  }

  private ApplyOutcome buildConnectorNameUpsertOp(
      String key,
      String nextConnectorId,
      String nextBlobUri,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L);
      return addOp(new PointerStore.CasUpsert(key, 0L, created), key, touchedKeys, ops);
    }
    if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
      return addCheck(key, ptr.getVersion(), touchedKeys, ops);
    }
    Connector existing = readConnector(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "name pointer connector missing");
    }
    String existingId = existing.getResourceId().getId();
    if (!Objects.equals(existingId, nextConnectorId)) {
      return ApplyOutcome.conflict(
          "NAME_POINTER_CONFLICT",
          "name pointer is owned by a different connector",
          null,
          null,
          existingId);
    }
    Pointer next = PointerReferences.blobPointer(key, nextBlobUri, ptr.getVersion() + 1L);
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), key, touchedKeys, ops);
  }

  private ApplyOutcome buildOwnedConnectorNameDeleteOp(
      String key, String ownerConnectorId, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return addAbsentCheck(key, touchedKeys, ops);
    }
    Connector existing = readConnector(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable(
          "NAME_POINTER_READ_FAILED", "old name pointer connector missing");
    }
    if (Objects.equals(existing.getResourceId().getId(), ownerConnectorId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), key, touchedKeys, ops);
    }
    return addCheck(key, ptr.getVersion(), touchedKeys, ops);
  }

  private ApplyOutcome addOp(
      PointerStore.CasOp op, String key, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    if (!touchedKeys.add(key)) {
      return ApplyOutcome.conflict(
          "POINTER_TXN_DUPLICATE_KEY",
          "transaction attempts multiple updates to pointer key " + key,
          null,
          null,
          null);
    }
    ops.add(op);
    return ApplyOutcome.applied();
  }

  private ApplyOutcome addCheck(
      String key, long expectedVersion, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    return addOp(new PointerStore.CasCheck(key, expectedVersion), key, touchedKeys, ops);
  }

  private ApplyOutcome addAbsentCheck(
      String key, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    return addOp(new PointerStore.CasCheckAbsent(key), key, touchedKeys, ops);
  }

  private ApplyOutcome findExpectedVersionConflict(List<TransactionIntent> intents) {
    for (var intent : intents) {
      if (!intent.hasExpectedVersion()) {
        continue;
      }
      long actual =
          pointerStore.get(intent.getTargetPointerKey()).map(Pointer::getVersion).orElse(0L);
      if (actual != intent.getExpectedVersion()) {
        return ApplyOutcome.conflict(
            "EXPECTED_VERSION_MISMATCH",
            "pointer version changed before apply",
            intent.getExpectedVersion(),
            actual,
            null);
      }
    }
    return null;
  }

  private ApplyOutcome appendIntentCleanupOps(
      TransactionIntent intent,
      TransactionIntentRepository intentRepo,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    if (intent == null) {
      return ApplyOutcome.retryable("MISSING_INTENT", "transaction intent is required");
    }
    TransactionIntent current =
        intentRepo.getByTarget(intent.getAccountId(), intent.getTargetPointerKey()).orElse(null);
    if (current == null) {
      return ApplyOutcome.retryable("LOCK_OWNERSHIP_MISMATCH", "intent lock missing during apply");
    }
    if (!intent.getTxId().equals(current.getTxId())) {
      return ApplyOutcome.retryable(
          "LOCK_OWNERSHIP_MISMATCH", "intent lock is owned by another transaction");
    }

    var byTargetPointer =
        intentRepo
            .getTargetPointer(intent.getAccountId(), intent.getTargetPointerKey())
            .orElse(null);
    if (byTargetPointer == null) {
      return ApplyOutcome.retryable("LOCK_OWNERSHIP_MISMATCH", "target intent pointer missing");
    }
    ApplyOutcome byTargetDelete =
        addOp(
            new PointerStore.CasDelete(
                Keys.transactionIntentPointerByTarget(
                    intent.getAccountId(), intent.getTargetPointerKey()),
                byTargetPointer.getVersion()),
            Keys.transactionIntentPointerByTarget(
                intent.getAccountId(), intent.getTargetPointerKey()),
            touchedKeys,
            ops);
    if (byTargetDelete.status != ApplyStatus.APPLIED) {
      return byTargetDelete;
    }

    String byTxKey =
        Keys.transactionIntentPointerByTx(
            intent.getAccountId(), intent.getTxId(), intent.getTargetPointerKey());
    var byTxPointer = pointerStore.get(byTxKey).orElse(null);
    if (byTxPointer == null) {
      return ApplyOutcome.retryable("LOCK_OWNERSHIP_MISMATCH", "tx intent pointer missing");
    }
    // A transactional table delete can commit TS_APPLIED before its durable cleanup task is
    // consumed. Keep the by-tx intent as the recovery index until post-apply convergence succeeds;
    // the by-target lock above is still released in this batch, so no other transaction is held.
    if (isTableByIdPointer(intent.getTargetPointerKey()) && isDeleteSentinel(intent)) {
      return ApplyOutcome.applied();
    }
    return addOp(
        new PointerStore.CasDelete(byTxKey, byTxPointer.getVersion()), byTxKey, touchedKeys, ops);
  }

  private ApplyOutcome appendTransactionAppliedOp(
      Transaction appliedTransaction,
      long expectedTransactionPointerVersion,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    if (expectedTransactionPointerVersion <= 0L) {
      return ApplyOutcome.retryable(
          "INVALID_TRANSACTION_POINTER_VERSION",
          "expected transaction pointer version must be positive");
    }
    try {
      String blobUri = writeTransactionBlob(appliedTransaction);
      String key =
          Keys.transactionPointerById(
              appliedTransaction.getAccountId(), appliedTransaction.getTxId());
      return addOp(
          new PointerStore.CasUpsert(
              key,
              expectedTransactionPointerVersion,
              PointerReferences.blobPointer(key, blobUri, expectedTransactionPointerVersion + 1L)),
          key,
          touchedKeys,
          ops);
    } catch (RuntimeException e) {
      LOG.debugf(e, "transaction blob write failed for %s", appliedTransaction.getTxId());
      return ApplyOutcome.retryable(
          "TRANSACTION_BLOB_WRITE_FAILED", "failed to stage applied transaction blob");
    }
  }

  private String writeTransactionBlob(Transaction appliedTransaction) {
    byte[] bytes = appliedTransaction.toByteArray();
    String blobUri =
        Keys.transactionBlobUri(
            appliedTransaction.getAccountId(),
            appliedTransaction.getTxId(),
            ai.floedb.floecat.types.Hashing.sha256Hex(bytes));
    blobStore.put(blobUri, bytes, "application/x-protobuf");
    return blobUri;
  }

  private ApplyOutcome classifyAtomicApplyFailure(
      Transaction appliedTransaction,
      long expectedTransactionPointerVersion,
      List<TransactionIntent> intents,
      TransactionIntentRepository intentRepo) {
    ApplyOutcome appliedState =
        findAppliedTransactionState(appliedTransaction, expectedTransactionPointerVersion);
    if (appliedState != null) {
      return appliedState;
    }
    ApplyOutcome pointerConflict = findExpectedVersionConflict(intents);
    if (pointerConflict != null) {
      return pointerConflict;
    }
    ApplyOutcome intentConflict = findIntentCleanupConflict(intents, intentRepo);
    if (intentConflict != null) {
      return intentConflict;
    }
    return ApplyOutcome.retryable("POINTER_TXN_CAS_FAILED", "pointer transaction conflict");
  }

  private ApplyOutcome findAppliedTransactionState(
      Transaction appliedTransaction, long expectedTransactionPointerVersion) {
    String key =
        Keys.transactionPointerById(
            appliedTransaction.getAccountId(), appliedTransaction.getTxId());
    Pointer pointer = pointerStore.get(key).orElse(null);
    if (pointer == null) {
      return null;
    }
    if (pointer.getVersion() == expectedTransactionPointerVersion) {
      return null;
    }
    Transaction existing = readTransaction(pointer.getBlobUri());
    if (existing != null && existing.getState() == TransactionState.TS_APPLIED) {
      return ApplyOutcome.applied();
    }
    return null;
  }

  private ApplyOutcome findIntentCleanupConflict(
      List<TransactionIntent> intents, TransactionIntentRepository intentRepo) {
    for (TransactionIntent intent : intents) {
      if (intent == null) {
        continue;
      }
      TransactionIntent current =
          intentRepo.getByTarget(intent.getAccountId(), intent.getTargetPointerKey()).orElse(null);
      if (current == null) {
        return ApplyOutcome.retryable(
            "LOCK_OWNERSHIP_MISMATCH", "intent lock missing during apply");
      }
      if (!intent.getTxId().equals(current.getTxId())) {
        return ApplyOutcome.retryable(
            "LOCK_OWNERSHIP_MISMATCH", "intent lock is owned by another transaction");
      }
      String byTxKey =
          Keys.transactionIntentPointerByTx(
              intent.getAccountId(), intent.getTxId(), intent.getTargetPointerKey());
      if (pointerStore.get(byTxKey).isEmpty()) {
        return ApplyOutcome.retryable("LOCK_OWNERSHIP_MISMATCH", "tx intent pointer missing");
      }
    }
    return null;
  }

  private Transaction readTransaction(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        LOG.debugf("transaction blob missing: %s", blobUri);
        return null;
      }
      return Transaction.parseFrom(bytes);
    } catch (Exception e) {
      LOG.debugf("transaction blob parse failed: %s", blobUri, e);
      return null;
    }
  }
}
