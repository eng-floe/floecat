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
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
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

  public boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
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
        Pointer created =
            Pointer.newBuilder().setKey(key).setBlobUri(nextBlobUri).setVersion(1L).build();
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
      Pointer next =
          Pointer.newBuilder()
              .setKey(key)
              .setBlobUri(nextBlobUri)
              .setVersion(ptr.getVersion() + 1)
              .build();
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
      Pointer created = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
      if (pointerStore.compareAndSet(key, 0L, created)) {
        return;
      }
      ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        LOG.warnf("pointer missing for %s", key);
        return;
      }
    }
    Pointer next =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(blobUri)
            .setVersion(ptr.getVersion() + 1)
            .build();
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

  private ApplyOutcome planIntentOps(
      TransactionIntent intent, List<PointerStore.CasOp> ops, Set<String> touchedKeys) {
    String pointerKey = intent.getTargetPointerKey();
    if (isTableByIdPointer(pointerKey)) {
      return planTableIntentOps(intent, ops, touchedKeys);
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

    if (current != null && intent.getBlobUri().equals(current.getBlobUri())) {
      return ApplyOutcome.applied();
    }

    long expected = intent.hasExpectedVersion() ? intent.getExpectedVersion() : actualVersion;
    Pointer next =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(intent.getBlobUri())
            .setVersion(expected + 1L)
            .build();
    return addOp(
        new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
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

    Table nextTable = readTable(intent.getBlobUri());
    if (nextTable == null) {
      return ApplyOutcome.retryable("TABLE_BLOB_MISSING", "table blob missing");
    }
    ApplyOutcome targetValidation = validateTableIntentTarget(pointerKey, nextTable);
    if (targetValidation.status != ApplyStatus.APPLIED) {
      return targetValidation;
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
      Pointer next =
          Pointer.newBuilder()
              .setKey(pointerKey)
              .setBlobUri(intent.getBlobUri())
              .setVersion(expected + 1L)
              .build();
      ApplyOutcome outcome =
          addOp(
              new PointerStore.CasUpsert(pointerKey, expected, next), pointerKey, touchedKeys, ops);
      if (outcome.status != ApplyStatus.APPLIED) {
        return outcome;
      }
    }

    ApplyOutcome newNameOutcome =
        buildNameUpsertOp(newNameKey, nextTableId, intent.getBlobUri(), touchedKeys, ops);
    if (newNameOutcome.status != ApplyStatus.APPLIED) {
      return newNameOutcome;
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
      }
    }
    return ApplyOutcome.applied();
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

  private ApplyOutcome buildNameUpsertOp(
      String key,
      String nextTableId,
      String nextBlobUri,
      Set<String> touchedKeys,
      List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created =
          Pointer.newBuilder().setKey(key).setBlobUri(nextBlobUri).setVersion(1L).build();
      return addOp(new PointerStore.CasUpsert(key, 0L, created), key, touchedKeys, ops);
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
    Pointer next =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(nextBlobUri)
            .setVersion(ptr.getVersion() + 1L)
            .build();
    return addOp(new PointerStore.CasUpsert(key, ptr.getVersion(), next), key, touchedKeys, ops);
  }

  private ApplyOutcome buildOwnedNameDeleteOp(
      String key, String ownerTableId, Set<String> touchedKeys, List<PointerStore.CasOp> ops) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      return ApplyOutcome.applied();
    }
    Table existing = readTable(ptr.getBlobUri());
    if (existing == null || !existing.hasResourceId()) {
      return ApplyOutcome.retryable("NAME_POINTER_READ_FAILED", "old name pointer table missing");
    }
    if (Objects.equals(existing.getResourceId().getId(), ownerTableId)) {
      return addOp(new PointerStore.CasDelete(key, ptr.getVersion()), key, touchedKeys, ops);
    }
    return ApplyOutcome.applied();
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
}
