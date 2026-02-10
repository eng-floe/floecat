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

  private static final Logger LOG = Logger.getLogger(Transactions.class);

  @Inject TransactionRepository txRepo;
  @Inject TransactionIntentRepository intentRepo;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject NameResolver nameResolver;
  @Inject Authorizer authz;
  @Inject PrincipalProvider principalProvider;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TransactionIntentApplierSupport applierSupport;

  @Override
  public Uni<BeginTransactionResponse> beginTransaction(BeginTransactionRequest request) {
    var L = LogHelper.start(LOG, "BeginTransaction");
    return mapFailures(
            run(
                () -> {
                  var principalContext = principalProvider.get();
                  authz.require(principalContext, "table.write");
                  String accountId = principalContext.getAccountId();
                  if (accountId == null || accountId.isBlank()) {
                    throw new IllegalArgumentException("missing account_id");
                  }

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
                  String accountId = principalContext.getAccountId();
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
                  String accountId = principalContext.getAccountId();
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
                  String accountId = principalContext.getAccountId();
                  Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
                  if (txn.getState() == TransactionState.TS_ABORTED) {
                    return AbortTransactionResponse.newBuilder().setTransaction(txn).build();
                  }
                  if (txn.getState() == TransactionState.TS_COMMITTED
                      || txn.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
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
                  String accountId = principalContext.getAccountId();
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
      throw new IllegalArgumentException("transaction update conflict: " + txn.getTxId());
    }
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
    List<String> createdBlobs = new ArrayList<>();

    try {
      for (var change : request.getChangesList()) {
        ResourceId tableId = resolveTableId(accountId, change);
        String pointerKey = Keys.tablePointerById(accountId, tableId.getId());
        if (!seenTargets.add(pointerKey)) {
          throw new IllegalArgumentException("duplicate change for " + pointerKey);
        }
        var existing = intentRepo.getByTarget(accountId, pointerKey).orElse(null);
        if (existing != null && !txn.getTxId().equals(existing.getTxId())) {
          throw new IllegalArgumentException("intent already exists for " + pointerKey);
        }
        long currentVersion = pointerStore.get(pointerKey).map(Pointer::getVersion).orElse(0L);
        Precondition pre = change.getPrecondition();
        Long expectedVersion = currentVersion;
        if (pre != null && pre.hasExpectedVersion()) {
          if (currentVersion != pre.getExpectedVersion()) {
            throw new IllegalArgumentException("precondition failed for " + pointerKey);
          }
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
            boolean existed = blobStore.head(blobUri).isPresent();
            blobStore.put(blobUri, tablePayload.toByteArray(), "application/x-protobuf");
            if (!existed) {
              createdBlobs.add(blobUri);
            }
          }
          case PAYLOAD -> {
            String sha = ResourceHash.sha256Hex(change.getPayload().toByteArray());
            blobUri = Keys.transactionObjectBlobUri(accountId, txn.getTxId(), sha);
            boolean existed = blobStore.head(blobUri).isPresent();
            blobStore.put(blobUri, change.getPayload().toByteArray(), "application/octet-stream");
            if (!existed) {
              createdBlobs.add(blobUri);
            }
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
                .setCreatedAt(now);
        if (expectedVersion != null) {
          intentBuilder.setExpectedVersion(expectedVersion);
        }
        TransactionIntent intent = intentBuilder.build();
        intents.add(intent);
      }
    } catch (RuntimeException e) {
      deletePreparedBlobs(createdBlobs);
      throw e;
    }

    List<TransactionIntent> created = new ArrayList<>();
    try {
      for (var intent : intents) {
        intentRepo.create(intent);
        created.add(intent);
      }
    } catch (RuntimeException e) {
      for (var intent : created) {
        intentRepo.deleteBothIndices(intent);
      }
      deletePreparedBlobs(createdBlobs);
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
      deletePreparedBlobs(createdBlobs);
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
    if (txn.getState() == TransactionState.TS_COMMITTED) {
      return txn;
    }
    if (txn.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return txn;
    }
    if (txn.getState() != TransactionState.TS_PREPARED) {
      throw new IllegalArgumentException("transaction not prepared: " + txn.getState().name());
    }

    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txn.getTxId());
    if (intents.isEmpty()) {
      throw new IllegalArgumentException("transaction has no intents");
    }
    for (var intent : intents) {
      String pointerKey = intent.getTargetPointerKey();
      long currentVersion = pointerStore.get(pointerKey).map(Pointer::getVersion).orElse(0L);
      if (intent.hasExpectedVersion() && currentVersion != intent.getExpectedVersion()) {
        Transaction aborted =
            txn.toBuilder().setState(TransactionState.TS_ABORTED).setUpdatedAt(now).build();
        updateTransaction(aborted);
        cleanupIntents(accountId, txn.getTxId());
        throw new IllegalArgumentException("transaction conflict for " + pointerKey);
      }
    }

    Transaction committed =
        txn.toBuilder().setState(TransactionState.TS_COMMITTED).setUpdatedAt(now).build();
    updateTransaction(committed);

    boolean conflict = false;
    for (var intent : intents) {
      var outcome = applierSupport.applyIntentBestEffort(intent, intentRepo);
      if (outcome.status() == TransactionIntentApplierSupport.ApplyStatus.CONFLICT) {
        recordIntentConflict(intent, outcome, now);
        conflict = true;
        break;
      }
    }

    if (conflict) {
      Transaction failed =
          committed.toBuilder()
              .setState(TransactionState.TS_APPLY_FAILED_CONFLICT)
              .setUpdatedAt(now)
              .build();
      updateTransaction(failed);
      return failed;
    }
    return committed;
  }

  private void cleanupIntents(String accountId, String txId) {
    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txId);
    for (var intent : intents) {
      intentRepo.deleteBothIndices(intent);
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
    if (txn.getState() == TransactionState.TS_ABORTED
        || txn.getState() == TransactionState.TS_COMMITTED
        || txn.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return txn;
    }
    Transaction aborted =
        txn.toBuilder().setState(TransactionState.TS_ABORTED).setUpdatedAt(now).build();
    updateTransaction(aborted);
    return aborted;
  }

  private void deletePreparedBlobs(List<String> uris) {
    if (uris == null || uris.isEmpty()) {
      return;
    }
    for (String uri : uris) {
      if (uri == null || uri.isBlank()) {
        continue;
      }
      try {
        blobStore.delete(uri);
      } catch (RuntimeException e) {
        LOG.debugf(e, "Failed to cleanup prepared blob %s", uri);
      }
    }
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

  private void recordIntentConflict(
      TransactionIntent intent,
      TransactionIntentApplierSupport.ApplyOutcome outcome,
      Timestamp now) {
    TransactionIntent updated =
        intent.toBuilder()
            .setApplyErrorCode(outcome.errorCode())
            .setApplyErrorMessage(outcome.errorMessage())
            .setApplyErrorAt(now)
            .build();
    if (outcome.expectedVersion() != null) {
      updated = updated.toBuilder().setApplyErrorExpectedVersion(outcome.expectedVersion()).build();
    }
    if (outcome.actualVersion() != null) {
      updated = updated.toBuilder().setApplyErrorActualVersion(outcome.actualVersion()).build();
    }
    if (outcome.conflictOwner() != null) {
      updated = updated.toBuilder().setApplyErrorConflictOwner(outcome.conflictOwner()).build();
    }
    if (!intentRepo.update(updated)) {
      LOG.warnf("failed to persist intent conflict for %s", intent.getTargetPointerKey());
    }
  }
}
