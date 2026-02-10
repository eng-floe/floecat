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
        .setKind(ResourceKind.RK_UNSPECIFIED)
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
    if (txn.getState() == TransactionState.TS_PREPARED) {
      return txn;
    }
    if (txn.getState() != TransactionState.TS_OPEN) {
      throw new IllegalArgumentException("transaction not open: " + txn.getState().name());
    }

    List<TransactionIntent> intents = new ArrayList<>();

    for (var change : request.getChangesList()) {
      ResourceId tableId = resolveTableId(accountId, change);
      String pointerKey = Keys.tablePointerById(accountId, tableId.getId());
      long currentVersion = pointerStore.get(pointerKey).map(Pointer::getVersion).orElse(0L);
      Precondition pre = change.getPrecondition();
      if (pre != null && pre.getExpectedVersion() != 0L) {
        if (currentVersion != pre.getExpectedVersion()) {
          throw new IllegalArgumentException("precondition failed for " + pointerKey);
        }
      }

      String blobUri = change.getIntendedBlobUri();
      if (blobUri == null || blobUri.isBlank()) {
        switch (change.getChangePayloadCase()) {
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
            blobStore.put(blobUri, tablePayload.toByteArray(), "application/x-protobuf");
          }
          case PAYLOAD -> {
            String sha = ResourceHash.sha256Hex(change.getPayload().toByteArray());
            blobUri = Keys.transactionObjectBlobUri(accountId, txn.getTxId(), sha);
            blobStore.put(blobUri, change.getPayload().toByteArray(), "application/octet-stream");
          }
          case CHANGEPAYLOAD_NOT_SET -> {
            throw new IllegalArgumentException(
                "missing payload or intended_blob_uri for " + pointerKey);
          }
        }
      }

      TransactionIntent intent =
          TransactionIntent.newBuilder()
              .setTxId(txn.getTxId())
              .setAccountId(accountId)
              .setTargetPointerKey(pointerKey)
              .setBlobUri(blobUri)
              .setExpectedVersion(currentVersion)
              .setCreatedAt(now)
              .build();
      intents.add(intent);
    }

    for (var intent : intents) {
      intentRepo.create(intent);
    }

    Transaction updated =
        txn.toBuilder().setState(TransactionState.TS_PREPARED).setUpdatedAt(now).build();
    updateTransaction(updated);
    return updated;
  }

  private Transaction commitTransaction(
      String accountId, CommitTransactionRequest request, Timestamp now) {
    Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
    if (txn.getState() == TransactionState.TS_COMMITTED) {
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
      if (intent.getExpectedVersion() != 0L && currentVersion != intent.getExpectedVersion()) {
        Transaction aborted =
            txn.toBuilder().setState(TransactionState.TS_ABORTED).setUpdatedAt(now).build();
        updateTransaction(aborted);
        throw new IllegalArgumentException("transaction conflict for " + pointerKey);
      }
    }

    Transaction committed =
        txn.toBuilder().setState(TransactionState.TS_COMMITTED).setUpdatedAt(now).build();
    updateTransaction(committed);

    for (var intent : intents) {
      applyIntentBestEffort(intent);
    }

    return committed;
  }

  private void applyIntentBestEffort(TransactionIntent intent) {
    String pointerKey = intent.getTargetPointerKey();
    var current = pointerStore.get(pointerKey).orElse(null);
    long expected = current == null ? 0L : current.getVersion();
    if (intent.getExpectedVersion() != 0L && expected != intent.getExpectedVersion()) {
      LOG.warnf(
          "transaction intent apply skipped (version mismatch) key=%s expected=%d actual=%d",
          pointerKey, intent.getExpectedVersion(), expected);
      return;
    }
    long nextVersion = expected + 1;
    Pointer next =
        Pointer.newBuilder()
            .setKey(pointerKey)
            .setBlobUri(intent.getBlobUri())
            .setVersion(nextVersion)
            .build();
    if (!pointerStore.compareAndSet(pointerKey, expected, next)) {
      LOG.warnf("transaction intent apply CAS failed key=%s", pointerKey);
      return;
    }

    if (isTableByIdPointer(pointerKey)) {
      Table nextTable = readTable(intent.getBlobUri());
      if (nextTable != null) {
        updateTableNamePointers(current, nextTable, intent.getBlobUri());
      }
    }
    intentRepo.deleteByTarget(intent.getAccountId(), pointerKey);
  }

  private void cleanupIntents(String accountId, String txId) {
    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txId);
    for (var intent : intents) {
      intentRepo.deleteByTarget(accountId, intent.getTargetPointerKey());
    }
  }

  private boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  private Table readTable(String blobUri) {
    try {
      byte[] bytes = blobStore.get(blobUri);
      if (bytes == null) {
        return null;
      }
      return Table.parseFrom(bytes);
    } catch (Exception e) {
      return null;
    }
  }

  private void updateTableNamePointers(Pointer currentPtr, Table nextTable, String nextBlobUri) {
    String accountId = nextTable.getResourceId().getAccountId();
    String newKey =
        Keys.tablePointerByName(
            accountId,
            nextTable.getCatalogId().getId(),
            nextTable.getNamespaceId().getId(),
            nextTable.getDisplayName());
    upsertPointer(newKey, nextBlobUri);

    if (currentPtr == null) {
      return;
    }
    Table oldTable = readTable(currentPtr.getBlobUri());
    if (oldTable == null) {
      return;
    }
    String oldKey =
        Keys.tablePointerByName(
            oldTable.getResourceId().getAccountId(),
            oldTable.getCatalogId().getId(),
            oldTable.getNamespaceId().getId(),
            oldTable.getDisplayName());
    if (!oldKey.equals(newKey)) {
      pointerStore
          .get(oldKey)
          .ifPresent(ptr -> pointerStore.compareAndDelete(oldKey, ptr.getVersion()));
    }
  }

  private void upsertPointer(String key, String blobUri) {
    var ptr = pointerStore.get(key).orElse(null);
    if (ptr == null) {
      Pointer created = Pointer.newBuilder().setKey(key).setBlobUri(blobUri).setVersion(1L).build();
      if (pointerStore.compareAndSet(key, 0L, created)) {
        return;
      }
      ptr = pointerStore.get(key).orElse(null);
      if (ptr == null) {
        throw new IllegalArgumentException("pointer missing for " + key);
      }
    }
    Pointer next =
        Pointer.newBuilder()
            .setKey(key)
            .setBlobUri(blobUri)
            .setVersion(ptr.getVersion() + 1)
            .build();
    if (!pointerStore.compareAndSet(key, ptr.getVersion(), next)) {
      throw new IllegalArgumentException("pointer update conflict for " + key);
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
}
