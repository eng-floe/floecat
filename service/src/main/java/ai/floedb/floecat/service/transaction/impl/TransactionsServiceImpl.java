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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository.PreconditionFailedException;
import ai.floedb.floecat.service.repo.util.ResourceHash;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import ai.floedb.floecat.transaction.rpc.AbortTransactionRequest;
import ai.floedb.floecat.transaction.rpc.AbortTransactionResponse;
import ai.floedb.floecat.transaction.rpc.BeginTransactionRequest;
import ai.floedb.floecat.transaction.rpc.BeginTransactionResponse;
import ai.floedb.floecat.transaction.rpc.CommitTransactionRequest;
import ai.floedb.floecat.transaction.rpc.CommitTransactionResponse;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.GetTransactionResponse;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest;
import ai.floedb.floecat.transaction.rpc.PrepareTransactionResponse;
import ai.floedb.floecat.transaction.rpc.Transaction;
import ai.floedb.floecat.transaction.rpc.TransactionIntent;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import ai.floedb.floecat.transaction.rpc.Transactions;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.jboss.logging.Logger;

@GrpcService
public class TransactionsServiceImpl extends BaseServiceImpl implements Transactions {

  private static final Logger LOG = Logger.getLogger(TransactionsServiceImpl.class);
  private static final int MAX_POINTER_TXN_OPS = 100;

  @Inject TransactionRepository txRepo;
  @Inject TransactionIntentRepository intentRepo;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject NameResolver nameResolver;
  @Inject Authorizer authz;
  @Inject PrincipalProvider principalProvider;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TransactionIntentApplierSupport intentApplierSupport;

  @Override
  public Uni<BeginTransactionResponse> beginTransaction(BeginTransactionRequest request) {
    var L = LogHelper.start(LOG, "BeginTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.write");
                  String accountId = requireAccountId(principalContext.getAccountId());

                  String idempotencyKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  idempotencyKey = idempotencyKey.isBlank() ? null : idempotencyKey;

                  Timestamp now = Timestamps.fromMillis(clock.millis());
                  if (idempotencyKey == null) {
                    Transaction txn = createTransaction(accountId, request, now);
                    return BeginTransactionResponse.newBuilder().setTransaction(txn).build();
                  }

                  var result =
                      IdempotencyGuard.runOnce(
                          accountId,
                          "BeginTransaction",
                          idempotencyKey,
                          request.toByteArray(),
                          () -> {
                            Transaction txn = createTransaction(accountId, request, now);
                            return new IdempotencyGuard.CreateResult<>(
                                txn, transactionResourceId(accountId, txn.getTxId()));
                          },
                          txn -> txRepo.metaFor(accountId, txn.getTxId(), now),
                          Transaction::toByteArray,
                          bytes -> {
                            try {
                              return Transaction.parseFrom(bytes);
                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                          },
                          idempotencyStore,
                          idempotencyTtlSeconds(),
                          now,
                          this::correlationId);
                  return BeginTransactionResponse.newBuilder()
                      .setTransaction(result.resource())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<PrepareTransactionResponse> prepareTransaction(PrepareTransactionRequest request) {
    var L = LogHelper.start(LOG, "PrepareTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.write");
                  String accountId = requireAccountId(principalContext.getAccountId());
                  String idempotencyKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  idempotencyKey = idempotencyKey.isBlank() ? null : idempotencyKey;

                  Timestamp now = Timestamps.fromMillis(clock.millis());
                  if (idempotencyKey == null) {
                    Transaction updated = prepareTransaction(accountId, request, now);
                    return PrepareTransactionResponse.newBuilder().setTransaction(updated).build();
                  }

                  var result =
                      IdempotencyGuard.runOnce(
                          accountId,
                          "PrepareTransaction",
                          idempotencyKey,
                          request.toByteArray(),
                          () -> {
                            Transaction updated = prepareTransaction(accountId, request, now);
                            return new IdempotencyGuard.CreateResult<>(
                                updated, transactionResourceId(accountId, updated.getTxId()));
                          },
                          txn -> txRepo.metaFor(accountId, txn.getTxId(), now),
                          Transaction::toByteArray,
                          bytes -> {
                            try {
                              return Transaction.parseFrom(bytes);
                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                          },
                          idempotencyStore,
                          idempotencyTtlSeconds(),
                          now,
                          this::correlationId);
                  return PrepareTransactionResponse.newBuilder()
                      .setTransaction(result.resource())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<CommitTransactionResponse> commitTransaction(CommitTransactionRequest request) {
    var L = LogHelper.start(LOG, "CommitTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.write");
                  String accountId = requireAccountId(principalContext.getAccountId());
                  String idempotencyKey =
                      request.hasIdempotency() ? request.getIdempotency().getKey().trim() : "";
                  idempotencyKey = idempotencyKey.isBlank() ? null : idempotencyKey;

                  Timestamp now = Timestamps.fromMillis(clock.millis());
                  if (idempotencyKey == null) {
                    Transaction committed = commitTransaction(accountId, request, now);
                    return CommitTransactionResponse.newBuilder().setTransaction(committed).build();
                  }

                  var result =
                      IdempotencyGuard.runOnce(
                          accountId,
                          "CommitTransaction",
                          idempotencyKey,
                          request.toByteArray(),
                          () -> {
                            Transaction committed = commitTransaction(accountId, request, now);
                            return new IdempotencyGuard.CreateResult<>(
                                committed, transactionResourceId(accountId, committed.getTxId()));
                          },
                          txn -> txRepo.metaFor(accountId, txn.getTxId(), now),
                          Transaction::toByteArray,
                          bytes -> {
                            try {
                              return Transaction.parseFrom(bytes);
                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                          },
                          idempotencyStore,
                          idempotencyTtlSeconds(),
                          now,
                          this::correlationId);
                  return CommitTransactionResponse.newBuilder()
                      .setTransaction(result.resource())
                      .build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<AbortTransactionResponse> abortTransaction(AbortTransactionRequest request) {
    var L = LogHelper.start(LOG, "AbortTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.write");
                  String accountId = requireAccountId(principalContext.getAccountId());
                  Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
                  if (txn.getState() == TransactionState.TS_ABORTED) {
                    return AbortTransactionResponse.newBuilder().setTransaction(txn).build();
                  }
                  if (isCommittedFamily(txn.getState())) {
                    throw new IllegalArgumentException("transaction already committed");
                  }
                  Transaction aborted =
                      txn.toBuilder()
                          .setState(TransactionState.TS_ABORTED)
                          .setUpdatedAt(Timestamps.fromMillis(clock.millis()))
                          .build();
                  updateTransaction(aborted);
                  cleanupIntents(accountId, txn.getTxId());
                  return AbortTransactionResponse.newBuilder().setTransaction(aborted).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  @Override
  public Uni<GetTransactionResponse> getTransaction(GetTransactionRequest request) {
    var L = LogHelper.start(LOG, "GetTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.read");
                  String accountId = requireAccountId(principalContext.getAccountId());
                  Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
                  return GetTransactionResponse.newBuilder().setTransaction(txn).build();
                }),
            correlationId())
        .onFailure()
        .invoke(L::fail)
        .onItem()
        .invoke(L::ok);
  }

  private Transaction getTransactionOrThrow(String accountId, String txId) {
    if (txId == null || txId.isBlank()) {
      throw new IllegalArgumentException("missing tx_id");
    }
    return txRepo
        .getById(accountId, txId)
        .orElseThrow(() -> new IllegalArgumentException("transaction not found: " + txId));
  }

  private void updateTransaction(Transaction txn) {
    long version = txRepo.metaFor(txn.getAccountId(), txn.getTxId()).getPointerVersion();
    if (!txRepo.update(txn, version)) {
      throw new PreconditionFailedException("transaction update conflict: " + txn.getTxId());
    }
  }

  private String requireAccountId(String accountId) {
    if (accountId == null || accountId.isBlank()) {
      throw new IllegalArgumentException("missing account_id");
    }
    return accountId;
  }

  private ResourceId transactionResourceId(String accountId, String txId) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(txId)
        .setKind(ResourceKind.RK_TRANSACTION)
        .build();
  }

  private Transaction createTransaction(
      String accountId, BeginTransactionRequest request, Timestamp now) {
    String txId = randomUuid();
    Timestamp expires =
        request.hasTtl()
            ? Timestamps.add(now, request.getTtl())
            : Timestamps.add(now, defaultTtl());

    Transaction txn =
        Transaction.newBuilder()
            .setTxId(txId)
            .setAccountId(accountId)
            .setState(TransactionState.TS_OPEN)
            .setCreatedAt(now)
            .setUpdatedAt(now)
            .setExpiresAt(expires)
            .putAllProperties(request.getPropertiesMap())
            .build();
    txRepo.create(txn);
    return txn;
  }

  private Transaction prepareTransaction(
      String accountId, PrepareTransactionRequest request, Timestamp now) {
    Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
    if (isExpired(txn, now)) {
      abortExpired(txn, now);
      throw new IllegalArgumentException("transaction expired");
    }
    if (txn.getState() == TransactionState.TS_PREPARED) {
      return txn;
    }
    if (txn.getState() != TransactionState.TS_OPEN) {
      throw new IllegalArgumentException("transaction not open: " + txn.getState().name());
    }

    List<TransactionIntent> intents = new ArrayList<>();
    java.util.Set<String> seenTargets = new java.util.HashSet<>();
    List<PendingBlob> pendingBlobs = new ArrayList<>();
    int estimatedOps = 0;

    try {
      for (var change : request.getChangesList()) {
        ResolvedTxTarget target = resolveTarget(accountId, change);
        ResourceId tableId = target.tableId();
        String pointerKey = target.pointerKey();
        if (!seenTargets.add(pointerKey)) {
          throw new IllegalArgumentException("duplicate change for " + pointerKey);
        }
        estimatedOps += isTableByIdPointer(pointerKey) ? 3 : 1;
        if (estimatedOps > MAX_POINTER_TXN_OPS) {
          throw new IllegalArgumentException(
              "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations");
        }
        long currentVersion = pointerStore.get(pointerKey).map(Pointer::getVersion).orElse(0L);
        Precondition pre = change.getPrecondition();
        long expectedVersion = currentVersion;
        if (pre != null && pre.hasExpectedVersion()) {
          if (currentVersion != pre.getExpectedVersion()) {
            throw new PreconditionFailedException("precondition failed for " + pointerKey);
          }
          expectedVersion = pre.getExpectedVersion();
        }

        String blobUri;
        switch (change.getChangePayloadCase()) {
          case INTENDED_BLOB_URI -> {
            blobUri = change.getIntendedBlobUri().trim();
            if (blobUri.isEmpty()) {
              throw new IllegalArgumentException("intended_blob_uri is empty for " + pointerKey);
            }
            if (!blobUri.startsWith(Keys.accountRootPrefix(accountId))) {
              throw new IllegalArgumentException(
                  "intended_blob_uri outside account scope for " + pointerKey);
            }
          }
          case TABLE -> {
            if (tableId == null) {
              throw new IllegalArgumentException(
                  "table payload requires table_id/table_fq target for " + pointerKey);
            }
            var tablePayload = change.getTable();
            if (!tablePayload.hasResourceId()) {
              throw new IllegalArgumentException("table payload missing resource_id");
            }
            if (!tablePayload.getResourceId().getId().equals(tableId.getId())) {
              throw new IllegalArgumentException("table payload resource_id does not match target");
            }
            if (!tablePayload.getResourceId().getAccountId().equals(accountId)) {
              throw new IllegalArgumentException("table payload account mismatch for target");
            }
            String sha = ResourceHash.sha256Hex(tablePayload.toByteArray());
            blobUri = Keys.tableBlobUri(accountId, tableId.getId(), sha);
            pendingBlobs.add(
                new PendingBlob(blobUri, tablePayload.toByteArray(), "application/x-protobuf"));
          }
          case PAYLOAD -> {
            String sha = ResourceHash.sha256Hex(change.getPayload().toByteArray());
            blobUri = Keys.transactionObjectBlobUri(accountId, txn.getTxId(), sha);
            pendingBlobs.add(
                new PendingBlob(
                    blobUri, change.getPayload().toByteArray(), "application/octet-stream"));
          }
          case CHANGEPAYLOAD_NOT_SET -> {
            throw new IllegalArgumentException(
                "missing payload or intended_blob_uri for " + pointerKey);
          }
          default ->
              throw new IllegalArgumentException(
                  "unknown payload type for " + pointerKey + ": " + change.getChangePayloadCase());
        }

        var intentBuilder =
            TransactionIntent.newBuilder()
                .setTxId(txn.getTxId())
                .setAccountId(accountId)
                .setTargetPointerKey(pointerKey)
                .setBlobUri(blobUri)
                .setCreatedAt(now)
                .setExpectedVersion(expectedVersion);
        TransactionIntent intent = intentBuilder.build();
        intents.add(intent);
      }
    } catch (RuntimeException e) {
      throw e;
    }

    List<TransactionIntent> created = new ArrayList<>();
    try {
      for (var intent : intents) {
        ensureIntentTargetAvailable(accountId, txn.getTxId(), intent.getTargetPointerKey(), now);
        intentRepo.create(intent);
        created.add(intent);
      }
    } catch (RuntimeException e) {
      for (var intent : created) {
        intentRepo.deleteBothIndices(intent);
      }
      throw e;
    }

    try {
      for (var blob : pendingBlobs) {
        blobStore.put(blob.uri(), blob.bytes(), blob.contentType());
      }
    } catch (RuntimeException e) {
      for (var intent : created) {
        intentRepo.deleteBothIndices(intent);
      }
      throw e;
    }

    Transaction updated =
        txn.toBuilder().setState(TransactionState.TS_PREPARED).setUpdatedAt(now).build();
    try {
      updateTransaction(updated);
    } catch (RuntimeException e) {
      for (var intent : created) {
        intentRepo.deleteBothIndices(intent);
      }
      throw e;
    }
    return updated;
  }

  private Transaction commitTransaction(
      String accountId, CommitTransactionRequest request, Timestamp now) {
    Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
    if (isExpired(txn, now)) {
      abortExpired(txn, now);
      cleanupIntents(accountId, txn.getTxId());
      throw new IllegalArgumentException("transaction expired");
    }
    if (txn.getState() == TransactionState.TS_APPLIED) {
      return txn;
    }
    if (txn.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return txn;
    }
    if (txn.getState() != TransactionState.TS_PREPARED
        && txn.getState() != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
      throw new IllegalArgumentException("transaction not prepared: " + txn.getState().name());
    }

    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txn.getTxId());
    if (intents.isEmpty()) {
      throw new IllegalArgumentException("transaction has no intents");
    }
    for (var intent : intents) {
      var lockOwner = intentRepo.getByTarget(accountId, intent.getTargetPointerKey()).orElse(null);
      if (lockOwner == null || !txn.getTxId().equals(lockOwner.getTxId())) {
        Transaction failed =
            txn.toBuilder()
                .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE)
                .setUpdatedAt(now)
                .build();
        updateTransaction(failed);
        return failed;
      }
    }

    var outcome = intentApplierSupport.applyTransactionBestEffort(intents, intentRepo);
    if (outcome.status() == TransactionIntentApplierSupport.ApplyStatus.APPLIED) {
      Transaction applied =
          txn.toBuilder().setState(TransactionState.TS_APPLIED).setUpdatedAt(now).build();
      updateTransaction(applied);
      cleanupIntentsBestEffort(intents);
      return applied;
    }

    if (outcome.status() == TransactionIntentApplierSupport.ApplyStatus.CONFLICT) {
      annotateIntentApplyFailure(intents, outcome, now);
      Transaction failed =
          txn.toBuilder()
              .setState(TransactionState.TS_APPLY_FAILED_CONFLICT)
              .setUpdatedAt(now)
              .build();
      updateTransaction(failed);
      return failed;
    }

    annotateIntentApplyFailure(intents, outcome, now);
    Transaction failed =
        txn.toBuilder()
            .setState(TransactionState.TS_APPLY_FAILED_RETRYABLE)
            .setUpdatedAt(now)
            .build();
    updateTransaction(failed);
    return failed;
  }

  private void annotateIntentApplyFailure(
      List<TransactionIntent> intents,
      TransactionIntentApplierSupport.ApplyOutcome outcome,
      Timestamp now) {
    for (var intent : intents) {
      var updated =
          intent.toBuilder()
              .setApplyErrorCode(outcome.errorCode() == null ? "" : outcome.errorCode())
              .setApplyErrorMessage(outcome.errorMessage() == null ? "" : outcome.errorMessage())
              .setApplyErrorAt(now);
      if (outcome.expectedVersion() != null) {
        updated.setApplyErrorExpectedVersion(outcome.expectedVersion());
      }
      if (outcome.actualVersion() != null) {
        updated.setApplyErrorActualVersion(outcome.actualVersion());
      }
      if (outcome.conflictOwner() != null && !outcome.conflictOwner().isBlank()) {
        updated.setApplyErrorConflictOwner(outcome.conflictOwner());
      }
      if (!intentRepo.update(updated.build())) {
        LOG.debugf(
            "Failed to persist apply failure details for tx=%s target=%s",
            intent.getTxId(), intent.getTargetPointerKey());
      }
    }
  }

  private void cleanupIntents(String accountId, String txId) {
    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txId);
    cleanupIntentsBestEffort(intents);
  }

  private void cleanupIntentsBestEffort(List<TransactionIntent> intents) {
    if (intents == null || intents.isEmpty()) {
      return;
    }
    for (var intent : intents) {
      if (!intentRepo.deleteBothIndicesBestEffort(intent)) {
        LOG.warnf(
            "Failed to fully remove transaction intent indices tx=%s target=%s",
            intent.getTxId(), intent.getTargetPointerKey());
      }
    }
  }

  private boolean isExpired(Transaction txn, Timestamp now) {
    if (txn == null || !txn.hasExpiresAt()) {
      return false;
    }
    return Timestamps.compare(now, txn.getExpiresAt()) > 0;
  }

  private Transaction abortExpired(Transaction txn, Timestamp now) {
    if (txn == null) {
      return txn;
    }
    if (txn.getState() == TransactionState.TS_ABORTED || isCommittedFamily(txn.getState())) {
      return txn;
    }
    Transaction aborted =
        txn.toBuilder().setState(TransactionState.TS_ABORTED).setUpdatedAt(now).build();
    updateTransaction(aborted);
    return aborted;
  }

  private ResourceId resolveTableId(String accountId, ResourceId tableId, String tableFq) {
    if (tableId != null && !tableId.getId().isBlank()) {
      if (tableId.getKind() != ResourceKind.RK_TABLE) {
        throw new IllegalArgumentException("resource kind must be table");
      }
      if (!accountId.equals(tableId.getAccountId())) {
        throw new IllegalArgumentException("account mismatch for table_id");
      }
      return tableId;
    }
    if (tableFq == null || tableFq.isBlank()) {
      throw new IllegalArgumentException("missing table reference");
    }
    NameRef ref = parseTableFq(tableFq);
    var resolved = nameResolver.resolveTableRelation(accountId, ref);
    if (resolved.isEmpty()) {
      throw new IllegalArgumentException("table not found: " + tableFq);
    }
    return resolved.get().resourceId();
  }

  private ResourceId resolveTableId(
      String accountId, ai.floedb.floecat.transaction.rpc.TxChange c) {
    switch (c.getResourceRefCase()) {
      case TABLE_ID:
        return resolveTableId(accountId, c.getTableId(), null);
      case TABLE_FQ:
        return resolveTableId(accountId, null, c.getTableFq());
      case RESOURCEREF_NOT_SET:
      default:
        throw new IllegalArgumentException("missing table reference");
    }
  }

  private ResolvedTxTarget resolveTarget(
      String accountId, ai.floedb.floecat.transaction.rpc.TxChange change) {
    String explicitPointer =
        change.hasTargetPointerKey() ? change.getTargetPointerKey().trim() : "";
    ResourceId resolvedTableId = null;
    if (change.getResourceRefCase()
        != ai.floedb.floecat.transaction.rpc.TxChange.ResourceRefCase.RESOURCEREF_NOT_SET) {
      resolvedTableId = resolveTableId(accountId, change);
    }

    if (!explicitPointer.isBlank()) {
      validatePointerInAccountScope(accountId, explicitPointer);
      if (resolvedTableId != null) {
        String expectedTablePointer = Keys.tablePointerById(accountId, resolvedTableId.getId());
        if (!expectedTablePointer.equals(explicitPointer)) {
          throw new IllegalArgumentException(
              "target_pointer_key does not match table reference for " + explicitPointer);
        }
      }
      return new ResolvedTxTarget(explicitPointer, resolvedTableId);
    }

    if (resolvedTableId == null) {
      throw new IllegalArgumentException("missing table reference");
    }
    return new ResolvedTxTarget(
        Keys.tablePointerById(accountId, resolvedTableId.getId()), resolvedTableId);
  }

  private void validatePointerInAccountScope(String accountId, String pointerKey) {
    String key = pointerKey == null ? "" : pointerKey.trim();
    if (key.isBlank()) {
      throw new IllegalArgumentException("target_pointer_key is empty");
    }
    String expectedPrefix = Keys.accountRootPrefix(accountId);
    if (!key.startsWith(expectedPrefix)) {
      throw new IllegalArgumentException("target_pointer_key outside account scope");
    }
  }

  private NameRef parseTableFq(String tableFq) {
    String trimmed = tableFq.trim();
    String[] parts = trimmed.split("\\.");
    if (parts.length < 2) {
      throw new IllegalArgumentException("table_fq must be catalog.ns.table");
    }
    NameRef.Builder b = NameRef.newBuilder().setCatalog(parts[0]).setName(parts[parts.length - 1]);
    for (int i = 1; i < parts.length - 1; i++) {
      b.addPath(parts[i]);
    }
    return b.build();
  }

  private com.google.protobuf.Duration defaultTtl() {
    return com.google.protobuf.Duration.newBuilder().setSeconds(600).build();
  }

  private boolean isCommittedFamily(TransactionState state) {
    return state == TransactionState.TS_APPLIED
        || state == TransactionState.TS_APPLY_FAILED_CONFLICT;
  }

  private void ensureIntentTargetAvailable(
      String accountId, String txId, String targetPointerKey, Timestamp now) {
    if (targetPointerKey == null || targetPointerKey.isBlank()) {
      throw new IllegalArgumentException("missing target pointer key");
    }
    for (int attempt = 0; attempt < 3; attempt++) {
      var existingOpt = intentRepo.getByTarget(accountId, targetPointerKey);
      if (existingOpt.isEmpty()) {
        return;
      }
      TransactionIntent existing = existingOpt.get();
      if (txId.equals(existing.getTxId())) {
        return;
      }
      if (!isIntentOwnerStale(accountId, existing.getTxId(), now)) {
        throw new PreconditionFailedException(
            "target already locked by transaction: " + existing.getTxId());
      }
      throw new PreconditionFailedException(
          "target locked by stale transaction: " + existing.getTxId());
    }
    var remaining = intentRepo.getByTarget(accountId, targetPointerKey);
    if (remaining.isPresent() && !txId.equals(remaining.get().getTxId())) {
      throw new PreconditionFailedException(
          "target already locked by transaction: " + remaining.get().getTxId());
    }
  }

  private boolean isIntentOwnerStale(String accountId, String ownerTxId, Timestamp now) {
    if (ownerTxId == null || ownerTxId.isBlank()) {
      return true;
    }
    Transaction owner = txRepo.getById(accountId, ownerTxId).orElse(null);
    if (owner == null) {
      return true;
    }
    if (owner.getState() == TransactionState.TS_ABORTED
        || owner.getState() == TransactionState.TS_APPLIED
        || owner.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return true;
    }
    if (owner.getState() == TransactionState.TS_APPLY_FAILED_RETRYABLE) {
      return isExpired(owner, now);
    }
    return isExpired(owner, now);
  }

  private boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  private record ResolvedTxTarget(String pointerKey, ResourceId tableId) {}

  private record PendingBlob(String uri, byte[] bytes, String contentType) {}
}
