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
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.surface.CatalogSurfaceWritePolicy;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
  @Inject CatalogOverlay overlay;

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

    var ops = new ArrayList<PointerStore.CasOp>();
    Set<String> touchedKeys = new HashSet<>();
    for (var intent : intents) {
      ApplyOutcome planOutcome = planIntentOps(intent, ops, touchedKeys);
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

    return ApplyOutcome.applied();
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

    var ops = new ArrayList<PointerStore.CasOp>();
    Set<String> touchedKeys = new HashSet<>();
    for (var intent : intents) {
      ApplyOutcome planOutcome = planIntentOps(intent, ops, touchedKeys);
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
      return ApplyOutcome.applied();
    }
    return classifyAtomicApplyFailure(
        appliedTransaction, expectedTransactionPointerVersion, intents, intentRepo);
  }

  private ApplyOutcome planIntentOps(
      TransactionIntent intent, List<PointerStore.CasOp> ops, Set<String> touchedKeys) {
    String pointerKey = intent.getTargetPointerKey();
    if (isTableByIdPointer(pointerKey)) {
      return planTableIntentOps(intent, ops, touchedKeys);
    }
    if (isConnectorByIdPointer(pointerKey)) {
      return planConnectorIntentOps(intent, ops, touchedKeys);
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
        return addAbsentCheck(pointerKey, touchedKeys, ops);
      }
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      return addOp(new PointerStore.CasDelete(pointerKey, expected), pointerKey, touchedKeys, ops);
    }

    if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
      return addCheck(pointerKey, expected, touchedKeys, ops);
    }

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    Pointer next = PointerReferences.blobPointer(pointerKey, intent.getBlobUri(), expected + 1L);
    return addOp(
        new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
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
      TransactionIntent intent, List<PointerStore.CasOp> ops, Set<String> touchedKeys) {
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
      return planTableDeleteIntentOps(intent, current, actualVersion, ops, touchedKeys);
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
    return ApplyOutcome.applied();
  }

  private ApplyOutcome planTableDeleteIntentOps(
      TransactionIntent intent,
      Pointer current,
      long actualVersion,
      List<PointerStore.CasOp> ops,
      Set<String> touchedKeys) {
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
    return buildOwnedRelationClaimDeleteOp(
        relationKey, currentTable.getResourceId().getId(), touchedKeys, ops);
  }

  private ApplyOutcome planConnectorIntentOps(
      TransactionIntent intent, List<PointerStore.CasOp> ops, Set<String> touchedKeys) {
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
