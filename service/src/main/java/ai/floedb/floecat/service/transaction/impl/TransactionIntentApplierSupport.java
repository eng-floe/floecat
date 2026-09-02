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
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.AccountDeletionFence;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionIntentApplierSupport {

  private static final Logger LOG = Logger.getLogger(TransactionIntentApplierSupport.class);
  private static final int MAX_POINTER_TXN_OPS = 100;

  /** Owns ordered, key-unique assembly of one pointer-store transaction. */
  private static final class PointerBatch {
    private enum Role {
      GUARD,
      MUTATION
    }

    private record Entry(PointerStore.CasOp operation, Role role) {}

    private final LinkedHashMap<String, Entry> entries = new LinkedHashMap<>();

    /** Adds a mutation only when no earlier operation owns its pointer key. */
    boolean addMutation(PointerStore.CasOp operation) {
      return entries.putIfAbsent(operation.key(), new Entry(operation, Role.MUTATION)) == null;
    }

    /**
     * Adds the first guard for a key and reuses it for later guards in the same batch.
     *
     * <p>A pointer can change between two samples. Keeping the first guard makes the final CAS lose
     * normally and the caller retry the whole plan; treating the second sample as a contradictory
     * mutation would turn ordinary contention into a terminal duplicate-key failure.
     */
    boolean addGuard(PointerStore.CasOp operation) {
      Entry existing = entries.putIfAbsent(operation.key(), new Entry(operation, Role.GUARD));
      return existing == null || existing.role() == Role.GUARD;
    }

    int size() {
      return entries.size();
    }

    boolean isEmpty() {
      return entries.isEmpty();
    }

    List<PointerStore.CasOp> operations() {
      return entries.values().stream().map(Entry::operation).toList();
    }
  }

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
      String conflictOwner) {
    public static ApplyOutcome applied() {
      return new ApplyOutcome(ApplyStatus.APPLIED, null, null, null, null, null);
    }

    public static ApplyOutcome retryable(String errorCode, String errorMessage) {
      return new ApplyOutcome(ApplyStatus.RETRYABLE, errorCode, errorMessage, null, null, null);
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
          conflictOwner);
    }
  }

  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject CatalogGraphView graphView;
  @Inject CatalogRepository catalogRepo;
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

    var batch = new PointerBatch();
    ApplyOutcome fenceOutcome = appendAccountDeletionFenceChecks(intents, batch);
    if (fenceOutcome.status != ApplyStatus.APPLIED) {
      return fenceOutcome;
    }
    for (var intent : intents) {
      ApplyOutcome planOutcome = planIntentOps(intent, batch);
      if (planOutcome.status != ApplyStatus.APPLIED) {
        return planOutcome;
      }
      if (batch.size() > MAX_POINTER_TXN_OPS) {
        return ApplyOutcome.conflict(
            "POINTER_TXN_TOO_LARGE",
            "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
            null,
            null,
            null);
      }
    }

    if (!batch.isEmpty() && !pointerStore.compareAndSetBatch(batch.operations())) {
      ApplyOutcome fenceOutcomeAfterFailure = findAccountDeletionConflict(intents);
      if (fenceOutcomeAfterFailure != null) {
        return fenceOutcomeAfterFailure;
      }
      ApplyOutcome conflictOutcome = findExpectedVersionConflict(intents);
      if (conflictOutcome != null) {
        return conflictOutcome;
      }
      return ApplyOutcome.retryable("POINTER_TXN_CAS_FAILED", "pointer transaction conflict");
    }

    return ApplyOutcome.applied();
  }

  public ApplyOutcome applyTransactionAtomically(
      Transaction appliedTransaction,
      long expectedTransactionPointerVersion,
      List<TransactionIntent> intents,
      TransactionIntentRepository intentRepo) {
    return applyTransactionAtomically(
        appliedTransaction, expectedTransactionPointerVersion, intents, intentRepo, List.of());
  }

  public ApplyOutcome applyTransactionAtomically(
      Transaction appliedTransaction,
      long expectedTransactionPointerVersion,
      List<TransactionIntent> intents,
      TransactionIntentRepository intentRepo,
      List<PointerStore.CasOp> completionOps) {
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

    var batch = new PointerBatch();
    ApplyOutcome fenceOutcome = appendAccountDeletionFenceChecks(intents, batch);
    if (fenceOutcome.status != ApplyStatus.APPLIED) {
      return fenceOutcome;
    }
    for (var intent : intents) {
      ApplyOutcome planOutcome = planIntentOps(intent, batch);
      if (planOutcome.status != ApplyStatus.APPLIED) {
        return planOutcome;
      }
      ApplyOutcome cleanupOutcome = appendIntentCleanupOps(intent, intentRepo, batch);
      if (cleanupOutcome.status != ApplyStatus.APPLIED) {
        return cleanupOutcome;
      }
      if (batch.size() > MAX_POINTER_TXN_OPS - 1 - completionOps.size()) {
        return ApplyOutcome.conflict(
            "POINTER_TXN_TOO_LARGE",
            "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
            null,
            null,
            null);
      }
    }

    ApplyOutcome txOutcome =
        appendTransactionAppliedOp(appliedTransaction, expectedTransactionPointerVersion, batch);
    if (txOutcome.status != ApplyStatus.APPLIED) {
      return txOutcome;
    }
    for (PointerStore.CasOp completionOp : completionOps) {
      ApplyOutcome completionOutcome = addOp(completionOp, batch);
      if (completionOutcome.status != ApplyStatus.APPLIED) {
        return completionOutcome;
      }
    }
    if (batch.size() > MAX_POINTER_TXN_OPS) {
      return ApplyOutcome.conflict(
          "POINTER_TXN_TOO_LARGE",
          "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations",
          null,
          null,
          null);
    }

    if (pointerStore.compareAndSetBatch(batch.operations())) {
      return ApplyOutcome.applied();
    }
    ApplyOutcome fenceOutcomeAfterFailure = findAccountDeletionConflict(intents);
    if (fenceOutcomeAfterFailure != null) {
      return fenceOutcomeAfterFailure;
    }
    return classifyAtomicApplyFailure(
        appliedTransaction, expectedTransactionPointerVersion, intents, intentRepo);
  }

  private ApplyOutcome appendAccountDeletionFenceChecks(
      List<TransactionIntent> intents, PointerBatch batch) {
    Set<String> fencedAccounts = new HashSet<>();
    for (TransactionIntent intent : intents) {
      if (intent == null || intent.getAccountId().isBlank()) {
        return ApplyOutcome.conflict(
            "TRANSACTION_INTENT_INVALID_ACCOUNT",
            "transaction intent account_id is required",
            null,
            null,
            null);
      }
      if (fencedAccounts.add(intent.getAccountId())) {
        PointerStore.CasCheckAbsent check =
            AccountDeletionFence.checkForAccountWrite(
                intent.getAccountId(), intent.getTargetPointerKey());
        batch.addGuard(check);
      }
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome findAccountDeletionConflict(List<TransactionIntent> intents) {
    for (TransactionIntent intent : intents) {
      if (intent != null
          && !intent.getAccountId().isBlank()
          && pointerStore.get(Keys.accountDeletionMarker(intent.getAccountId())).isPresent()) {
        return accountDeletionConflict(intent.getAccountId());
      }
    }
    return null;
  }

  private ApplyOutcome accountDeletionConflict(String accountId) {
    return ApplyOutcome.conflict(
        "ACCOUNT_DELETION_IN_PROGRESS", "account deletion is in progress", null, null, accountId);
  }

  private ApplyOutcome planIntentOps(TransactionIntent intent, PointerBatch batch) {
    String pointerKey = intent.getTargetPointerKey();
    if (isTableByIdPointer(pointerKey)) {
      return planTableIntentOps(intent, batch);
    }
    if (isConnectorByIdPointer(pointerKey)) {
      return planConnectorIntentOps(intent, batch);
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

    if (isDeleteSentinel(intent)) {
      if (current == null) {
        return addAbsentCheck(pointerKey, batch);
      }
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      return addOp(new PointerStore.CasDelete(pointerKey, expected), batch);
    }

    if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      return addCheck(pointerKey, expected, batch);
    }

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
    return addOp(new PointerStore.CasUpsert(pointerKey, expected, next), batch);
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

  private ApplyOutcome planTableIntentOps(TransactionIntent intent, PointerBatch batch) {
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
      return planTableDeleteIntentOps(intent, current, actualVersion, batch);
    }

    Table nextTable = readTable(intent.getBlobUri());
    if (nextTable == null) {
      return ApplyOutcome.retryable("TABLE_BLOB_MISSING", "table blob missing");
    }
    ApplyOutcome targetValidation = validateTableIntentTarget(pointerKey, nextTable);
    if (targetValidation.status != ApplyStatus.APPLIED) {
      return targetValidation;
    }
    // The row being replaced, read before the fence because the fence depends on it: a relation
    // that stays in the container it is already counted in changes no namespace's relation set.
    Table currentTable = current == null ? null : readTable(current.getBlobUri());
    if (current != null && currentTable == null) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
    }
    // Sampled before the eligibility check below, not after. That check is what the conditions
    // guard, and a version read after it is the version a concurrent namespace delete already
    // moved -- so the CAS would confirm that delete instead of losing to it.
    NamespaceJoin namespaceJoin = readNamespaceJoin(currentTable, nextTable);

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

    if (current == null || !Objects.equals(current.getBlobUri(), intent.getBlobUri())) {
      Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
      ApplyOutcome outcome = addOp(new PointerStore.CasUpsert(pointerKey, expected, next), batch);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    } else {
      ApplyOutcome outcome = addCheck(pointerKey, expected, batch);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    }

    ApplyOutcome newNameOutcome =
        buildNameUpsertOp(newNameKey, nextTableId, intent.getBlobUri(), batch);
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
            newRelationKey, nextTable.getResourceId(), intent.getBlobUri(), batch);
    if (newClaimOutcome.status != ApplyStatus.APPLIED) {
      return newClaimOutcome;
    }

    // Putting a table in this namespace changes its relation set, so both conditions ride this
    // batch. Exclusion here is only key overlap: a DeleteNamespace asserts the relation marker to
    // prove the namespace empty, and without it the two batches share no key -- neither can lose to
    // the other, and the delete commits while this table lands.
    ApplyOutcome joinOutcome = addNamespaceJoin(namespaceJoin, batch);
    if (joinOutcome.status != ApplyStatus.APPLIED) {
      return joinOutcome;
    }

    if (currentTable != null) {
      Table oldTable = currentTable;
      String oldNameKey =
          Keys.tablePointerByName(
              oldTable.getResourceId().getAccountId(),
              oldTable.getCatalogId().getId(),
              oldTable.getNamespaceId().getId(),
              oldTable.getDisplayName());
      if (!oldNameKey.equals(newNameKey)) {
        ApplyOutcome oldNameOutcome =
            buildOwnedNameDeleteOp(oldNameKey, oldTable.getResourceId().getId(), batch);
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
                oldRelationKey, oldTable.getResourceId().getId(), batch);
        if (oldClaimOutcome.status != ApplyStatus.APPLIED) {
          return oldClaimOutcome;
        }
      }
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome planTableDeleteIntentOps(
      TransactionIntent intent, Pointer current, long actualVersion, PointerBatch batch) {
    if (current == null) {
      return addAbsentCheck(intent.getTargetPointerKey(), batch);
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

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    ApplyOutcome deletePrimary =
        addOp(new PointerStore.CasDelete(intent.getTargetPointerKey(), expected), batch);
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
        buildOwnedNameDeleteOp(nameKey, currentTable.getResourceId().getId(), batch);
    if (nameDelete.status != ApplyStatus.APPLIED) {
      return nameDelete;
    }
    String relationKey =
        Keys.relationPointerByName(
            currentTable.getResourceId().getAccountId(),
            currentTable.getCatalogId().getId(),
            currentTable.getNamespaceId().getId(),
            currentTable.getDisplayName());
    return buildOwnedRelationClaimDeleteOp(
        relationKey, currentTable.getResourceId().getId(), batch);
  }

  private ApplyOutcome planConnectorIntentOps(TransactionIntent intent, PointerBatch batch) {
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
        return addAbsentCheck(intent.getTargetPointerKey(), batch);
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
          addOp(new PointerStore.CasDelete(intent.getTargetPointerKey(), expected), batch);
      if (deletePrimary.status != ApplyStatus.APPLIED) {
        return deletePrimary;
      }
      String nameKey =
          Keys.connectorPointerByName(
              currentConnector.getResourceId().getAccountId(), currentConnector.getDisplayName());
      return buildOwnedConnectorNameDeleteOp(
          nameKey, currentConnector.getResourceId().getId(), batch);
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
      ApplyOutcome outcome = addOp(new PointerStore.CasUpsert(pointerKey, expected, next), batch);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    } else {
      ApplyOutcome outcome = addCheck(pointerKey, expected, batch);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    }

    ApplyOutcome newNameOutcome =
        buildConnectorNameUpsertOp(newNameKey, nextConnectorId, intent.getBlobUri(), batch);
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
                oldNameKey, oldConnector.getResourceId().getId(), batch);
        if (oldNameOutcome.status != ApplyStatus.APPLIED) {
          return oldNameOutcome;
        }
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
    if (graphView == null) {
      // The graph view is @Inject-ed on an @ApplicationScoped bean, so this is unreachable in
      // production. A guard whose purpose is closing write-eligibility gaps must not default to
      // "allow" on its own null case — treat an absent graph view as retryable rather than applied.
      return ApplyOutcome.retryable(
          "TABLE_WRITE_ELIGIBILITY_UNAVAILABLE",
          "catalog graph view unavailable for write-eligibility check");
    }

    try {
      var writePolicy = new CatalogSurfaceWritePolicy(graphView);
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
      // Graph-view resolution can fail transiently (storage/cache errors). Do not turn that into a
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
      String key, String nextTableId, String nextBlobUri, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L);
      return addOp(new PointerStore.CasUpsert(key, 0L, created), batch);
    }
    if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
      return addCheck(key, ptr.getVersion(), batch);
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
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), batch);
  }

  /**
   * Reserves the shared, kind-agnostic relation-name claim ({@link Keys#relationPointerByName}) so
   * a table and a view can never hold the same (namespace, name). Ownership and kind are read from
   * the claim's stored {@link ResourceId} (stable across renames) rather than from the name or
   * blob, so a claim held by any other relation — a different table or a view of the same name — is
   * a hard conflict.
   */
  private ApplyOutcome buildRelationClaimUpsertOp(
      String key, ResourceId owner, String nextBlobUri, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L, owner, "");
      return addOp(new PointerStore.CasUpsert(key, 0L, created), batch);
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
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), batch);
  }

  /** A sampled namespace join, or the fact that the namespace it needed is already gone. */
  private record NamespaceJoin(PointerConditions conditions, boolean namespaceGone) {
    static final NamespaceJoin NOTHING_TO_JOIN = new NamespaceJoin(null, false);
    static final NamespaceJoin GONE = new NamespaceJoin(null, true);
  }

  /**
   * Samples what this table write has to assert about its namespace.
   *
   * <p>The conditions come from {@link MarkerStore}, not from keys derived here. This is the
   * highest-volume relation writer, and a second copy of the marker key, the advance shape and the
   * read dependency would make it a second source of truth for the delete guard -- one that a
   * change to the protocol would leave behind with no compile-time signal.
   *
   * <p>It also applies the same policy as every other writer, rather than asserting on every
   * upsert. A relation that stays in the container it is already counted in -- a schema edit, a
   * property change, an idempotent replay, a rename inside one namespace -- changes no namespace's
   * relation set, so there is nothing for a delete to be excluded from. Asserting anyway made
   * unrelated table commits into one namespace contend on a hot key and burn the retry budget for
   * no guard.
   *
   * <p>An already-deleted namespace is reported when the ops are emitted rather than here, so that
   * the eligibility checks between the two keep the precedence they had.
   */
  private NamespaceJoin readNamespaceJoin(Table currentTable, Table nextTable) {
    ResourceId to = nextTable.getNamespaceId();
    if (to == null || to.getAccountId().isBlank() || to.getId().isBlank()) {
      return NamespaceJoin.NOTHING_TO_JOIN;
    }
    try {
      if (currentTable == null) {
        return new NamespaceJoin(markerStore.relationCreateFence(to), false);
      }
      boolean changesCatalog =
          !currentTable.getCatalogId().getId().equals(nextTable.getCatalogId().getId());
      return new NamespaceJoin(
          markerStore.relationMoveFence(currentTable.getNamespaceId(), to, changesCatalog), false);
    } catch (BaseResourceRepository.NotFoundException gone) {
      return NamespaceJoin.GONE;
    }
  }

  /**
   * Emits the namespace join into this batch.
   *
   * <p>The marker is advanced, which is what a namespace delete racing this batch loses to. The
   * namespace's canonical pointer is only checked, which is what refuses a batch whose namespace
   * was already deleted -- the marker cannot catch that, because a read afterwards returns the
   * post-delete version and matches it. The existence check this path performs resolves through a
   * per-process cached graph that another instance's delete does not invalidate.
   *
   * <p>Once per batch for each key: a second intent joining the same namespace gets the same
   * guarantee from the advance already queued, and two CAS ops on one key are rejected outright.
   */
  private ApplyOutcome addNamespaceJoin(NamespaceJoin join, PointerBatch batch) {
    if (join.namespaceGone()) {
      return ApplyOutcome.conflict(
          "NAMESPACE_NOT_FOUND", "namespace no longer exists", null, null, null);
    }
    if (join.conditions() != null) {
      for (PointerStore.CasOp condition : join.conditions().toCasOps()) {
        if (!batch.addGuard(condition)) {
          return ApplyOutcome.conflict(
              "POINTER_TXN_DUPLICATE_KEY",
              "transaction condition conflicts with another operation on pointer key "
                  + condition.key(),
              null,
              null,
              null);
        }
      }
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome buildOwnedRelationClaimDeleteOp(
      String key, String ownerId, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return ApplyOutcome.applied();
    }
    if (Objects.equals(ptr.getResourceId().getId(), ownerId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), batch);
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome buildOwnedNameDeleteOp(String key, String ownerTableId, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return addAbsentCheck(key, batch);
    }
    Table existing = readTable(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
    }
    if (Objects.equals(existing.getResourceId().getId(), ownerTableId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), batch);
    }
    return addCheck(key, ptr.getVersion(), batch);
  }

  private ApplyOutcome buildConnectorNameUpsertOp(
      String key, String nextConnectorId, String nextBlobUri, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = PointerReferences.blobPointer(key, nextBlobUri, 1L);
      return addOp(new PointerStore.CasUpsert(key, 0L, created), batch);
    }
    if (Objects.equals(ptr.getBlobUri(), nextBlobUri)) {
      return addCheck(key, ptr.getVersion(), batch);
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
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), batch);
  }

  private ApplyOutcome buildOwnedConnectorNameDeleteOp(
      String key, String ownerConnectorId, PointerBatch batch) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return addAbsentCheck(key, batch);
    }
    Connector existing = readConnector(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable(
          "NAME_POINTER_READ_FAILED", "old name pointer connector missing");
    }
    if (Objects.equals(existing.getResourceId().getId(), ownerConnectorId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), batch);
    }
    return addCheck(key, ptr.getVersion(), batch);
  }

  private ApplyOutcome addOp(PointerStore.CasOp op, PointerBatch batch) {
    if (!batch.addMutation(op)) {
      return ApplyOutcome.conflict(
          "POINTER_TXN_DUPLICATE_KEY",
          "transaction attempts multiple updates to pointer key " + op.key(),
          null,
          null,
          null);
    }
    return ApplyOutcome.applied();
  }

  private ApplyOutcome addCheck(String key, long expectedVersion, PointerBatch batch) {
    return addOp(new PointerStore.CasCheck(key, expectedVersion), batch);
  }

  private ApplyOutcome addAbsentCheck(String key, PointerBatch batch) {
    return addOp(new PointerStore.CasCheckAbsent(key), batch);
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
      TransactionIntent intent, TransactionIntentRepository intentRepo, PointerBatch batch) {
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
            batch);
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
    return addOp(new PointerStore.CasDelete(byTxKey, byTxPointer.getVersion()), batch);
  }

  private ApplyOutcome appendTransactionAppliedOp(
      Transaction appliedTransaction, long expectedTransactionPointerVersion, PointerBatch batch) {
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
          batch);
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
