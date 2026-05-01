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

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.AuthConfig;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.ConnectorKind;
import ai.floedb.floecat.connector.rpc.ConnectorState;
import ai.floedb.floecat.connector.rpc.DestinationTarget;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.connector.rpc.SourceSelector;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.common.BaseServiceImpl;
import ai.floedb.floecat.service.common.IdempotencyGuard;
import ai.floedb.floecat.service.common.LogHelper;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.metagraph.resolver.NameResolver;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.repo.impl.TransactionIntentRepository;
import ai.floedb.floecat.service.repo.impl.TransactionRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository.PreconditionFailedException;
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
import ai.floedb.floecat.transaction.rpc.TxChange;
import ai.floedb.floecat.types.Hashing;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;

@GrpcService
public class TransactionsServiceImpl extends BaseServiceImpl implements Transactions {

  private static final Logger LOG = Logger.getLogger(TransactionsServiceImpl.class);
  private static final int MAX_POINTER_TXN_OPS = 100;
  private static final int TABLE_NAME_REPLAY_SCAN_PAGE_SIZE = 200;
  private static final String CAPTURE_STATISTICS_PROPERTY = "floecat.connector.capture-statistics";
  private static final String CONNECTOR_MODE_PROPERTY = "floecat.connector.mode";
  private static final String CONNECTOR_MODE_CAPTURE_ONLY = "capture-only";
  @Inject TransactionRepository txRepo;
  @Inject TransactionIntentRepository intentRepo;
  @Inject IdempotencyRepository idempotencyStore;
  @Inject ConnectorRepository connectorRepo;
  @Inject ReconcileJobStore reconcileJobs;
  @Inject NameResolver nameResolver;
  @Inject Authorizer authz;
  @Inject PrincipalProvider principalProvider;
  @Inject PointerStore pointerStore;
  @Inject BlobStore blobStore;
  @Inject TransactionIntentApplierSupport intentApplierSupport;
  @Inject UserGraph metadataGraph;
  private volatile java.util.concurrent.Executor postCommitExecutor =
      java.util.concurrent.ForkJoinPool.commonPool();

  @Inject
  void init(Instance<ManagedExecutor> managedExecutors) {
    if (managedExecutors == null) {
      return;
    }
    managedExecutors.stream().findFirst().ifPresent(executor -> postCommitExecutor = executor);
  }

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
                  rejectDirectConnectorPayloads(request);
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

  private void rejectDirectConnectorPayloads(PrepareTransactionRequest request) {
    if (request == null || request.getChangesCount() == 0) {
      return;
    }
    for (TxChange change : request.getChangesList()) {
      if (change == null) {
        continue;
      }
      if (change.getChangePayloadCase()
          == ai.floedb.floecat.transaction.rpc.TxChange.ChangePayloadCase.CONNECTOR) {
        throw new IllegalArgumentException(
            "connector payload is not accepted in prepare_transaction");
      }
    }
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
                  if (txn.getState() == TransactionState.TS_APPLYING) {
                    throw new IllegalArgumentException("transaction apply is in progress");
                  }
                  if (isTerminalNonAbortableState(txn.getState())) {
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

  private Transaction transitionTransactionState(
      String accountId,
      String txId,
      Set<TransactionState> expectedStates,
      TransactionState nextState,
      Timestamp now,
      String conflictReason) {
    for (int attempt = 0; attempt < 3; attempt++) {
      Transaction current = getTransactionOrThrow(accountId, txId);
      TransactionState currentState = current.getState();
      if (currentState == nextState) {
        return current;
      }
      if (!expectedStates.contains(currentState)) {
        throw new PreconditionFailedException(
            conflictReason + ": tx=" + txId + " state=" + currentState.name());
      }
      long version = txRepo.metaFor(accountId, txId).getPointerVersion();
      Transaction updated = current.toBuilder().setState(nextState).setUpdatedAt(now).build();
      if (txRepo.update(updated, version)) {
        return updated;
      }
    }
    throw new PreconditionFailedException("transaction update conflict: " + txId);
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
    // Public prepare_transaction rejects direct CONNECTOR payloads. CONNECTOR changes processed
    // here must come only from service-side materialization of connector_provisioning hints.
    Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
    if (isExpired(txn, now)) {
      abortExpired(txn, now);
      throw new IllegalArgumentException("transaction expired");
    }
    if (txn.getState() == TransactionState.TS_PREPARED) {
      ensurePreparedRequestMatchesExistingIntents(accountId, txn.getTxId(), request);
      return txn;
    }
    if (txn.getState() != TransactionState.TS_OPEN) {
      throw new IllegalArgumentException("transaction not open: " + txn.getState().name());
    }

    List<TxChange> effectiveChanges =
        materializeConnectorProvisioningChanges(
            accountId, txn.getTxId(), txn.getCreatedAt(), request.getChangesList());
    List<TransactionIntent> intents = new ArrayList<>();
    java.util.Set<String> seenTargets = new java.util.HashSet<>();
    List<PendingBlob> pendingBlobs = new ArrayList<>();
    int estimatedOps = 0;

    for (var change : effectiveChanges) {
      PlannedIntent planned = planIntent(accountId, txn.getTxId(), change);
      String pointerKey = planned.targetPointerKey();
      if (!seenTargets.add(pointerKey)) {
        throw new IllegalArgumentException("duplicate change for " + pointerKey);
      }
      estimatedOps += isTableByIdPointer(pointerKey) || isConnectorByIdPointer(pointerKey) ? 3 : 1;
      if (estimatedOps > MAX_POINTER_TXN_OPS) {
        throw new IllegalArgumentException(
            "transaction requires more than " + MAX_POINTER_TXN_OPS + " pointer operations");
      }
      if (planned.inlineBytes() != null) {
        pendingBlobs.add(
            new PendingBlob(
                planned.blobUri(),
                planned.inlineBytes(),
                planned.inlineContentType() == null
                    ? "application/octet-stream"
                    : planned.inlineContentType()));
      }

      var intentBuilder =
          TransactionIntent.newBuilder()
              .setTxId(txn.getTxId())
              .setAccountId(accountId)
              .setTargetPointerKey(pointerKey)
              .setBlobUri(planned.blobUri())
              .setCreatedAt(now)
              .setExpectedVersion(planned.expectedVersion());
      if (planned.expectedOwnedNamePointerKey() != null
          && !planned.expectedOwnedNamePointerKey().isBlank()) {
        intentBuilder.setExpectedOwnedNamePointerKey(planned.expectedOwnedNamePointerKey());
      }
      TransactionIntent intent = intentBuilder.build();
      intents.add(intent);
    }

    List<TransactionIntent> created = new ArrayList<>();
    try {
      for (var intent : intents) {
        ensureIntentTargetAvailable(accountId, txn.getTxId(), intent.getTargetPointerKey(), now);
        // Relies on repository create path to enforce unique target ownership atomically.
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

    try {
      return transitionTransactionState(
          accountId,
          txn.getTxId(),
          Set.of(TransactionState.TS_OPEN),
          TransactionState.TS_PREPARED,
          now,
          "cannot transition to prepared");
    } catch (RuntimeException e) {
      for (var intent : created) {
        intentRepo.deleteBothIndices(intent);
      }
      throw e;
    }
  }

  private Transaction commitTransaction(
      String accountId, CommitTransactionRequest request, Timestamp now) {
    Transaction txn = getTransactionOrThrow(accountId, request.getTxId());
    if (txn.getState() == TransactionState.TS_APPLIED) {
      return txn;
    }
    if (txn.getState() == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return txn;
    }
    if (txn.getState() != TransactionState.TS_APPLYING && isExpired(txn, now)) {
      abortExpired(txn, now);
      cleanupIntents(accountId, txn.getTxId());
      throw new IllegalArgumentException("transaction expired");
    }
    if (txn.getState() != TransactionState.TS_PREPARED
        && txn.getState() != TransactionState.TS_APPLYING
        && txn.getState() != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
      throw new IllegalArgumentException("transaction not prepared: " + txn.getState().name());
    }

    List<TransactionIntent> intents =
        new ArrayList<>(intentRepo.listByTx(accountId, txn.getTxId()));
    if (intents.isEmpty()) {
      throw new IllegalArgumentException("transaction has no intents");
    }
    intents.sort(Comparator.comparing(TransactionIntent::getTargetPointerKey));

    Transaction applyPhaseTxn = txn;
    if (txn.getState() != TransactionState.TS_APPLYING) {
      applyPhaseTxn =
          transitionTransactionState(
              accountId,
              txn.getTxId(),
              Set.of(TransactionState.TS_PREPARED, TransactionState.TS_APPLY_FAILED_RETRYABLE),
              TransactionState.TS_APPLYING,
              now,
              "cannot transition to applying");
    }

    for (var intent : intents) {
      var lockOwner = intentRepo.getByTarget(accountId, intent.getTargetPointerKey()).orElse(null);
      if (lockOwner == null || !applyPhaseTxn.getTxId().equals(lockOwner.getTxId())) {
        Transaction failed =
            transitionTransactionState(
                accountId,
                applyPhaseTxn.getTxId(),
                Set.of(TransactionState.TS_APPLYING),
                TransactionState.TS_APPLY_FAILED_RETRYABLE,
                now,
                "lock ownership mismatch");
        logCommitFailure(
            accountId,
            failed,
            "LOCK_OWNERSHIP_MISMATCH",
            "lock ownership mismatch during apply",
            intents);
        return failed;
      }
    }

    var outcome = intentApplierSupport.applyTransactionBestEffort(intents, intentRepo);
    if (outcome.status() == TransactionIntentApplierSupport.ApplyStatus.APPLIED) {
      Transaction applied =
          transitionTransactionState(
              accountId,
              applyPhaseTxn.getTxId(),
              Set.of(TransactionState.TS_APPLYING),
              TransactionState.TS_APPLIED,
              now,
              "cannot transition to applied");
      schedulePostCommitCaptureBootstrap(accountId, applied.getTxId(), List.copyOf(intents));
      invalidateTouchedGraphEntries(intents);
      cleanupIntentsBestEffort(intents);
      return applied;
    }

    if (outcome.status() == TransactionIntentApplierSupport.ApplyStatus.CONFLICT) {
      if (intentsAlreadyApplied(intents)) {
        Transaction applied =
            transitionTransactionState(
                accountId,
                applyPhaseTxn.getTxId(),
                Set.of(TransactionState.TS_APPLYING),
                TransactionState.TS_APPLIED,
                now,
                "cannot finalize already-applied transaction");
        schedulePostCommitCaptureBootstrap(accountId, applied.getTxId(), List.copyOf(intents));
        invalidateTouchedGraphEntries(intents);
        cleanupIntentsBestEffort(intents);
        return applied;
      }
      annotateIntentApplyFailure(intents, outcome, now);
      Transaction failed =
          transitionTransactionState(
              accountId,
              applyPhaseTxn.getTxId(),
              Set.of(TransactionState.TS_APPLYING),
              TransactionState.TS_APPLY_FAILED_CONFLICT,
              now,
              "cannot transition to apply_failed_conflict");
      logCommitFailure(accountId, failed, outcome, intents);
      return failed;
    }

    annotateIntentApplyFailure(intents, outcome, now);
    Transaction failed =
        transitionTransactionState(
            accountId,
            applyPhaseTxn.getTxId(),
            Set.of(TransactionState.TS_APPLYING),
            TransactionState.TS_APPLY_FAILED_RETRYABLE,
            now,
            "cannot transition to apply_failed_retryable");
    logCommitFailure(accountId, failed, outcome, intents);
    return failed;
  }

  private List<TxChange> materializeConnectorProvisioningChanges(
      String accountId, String txId, Timestamp connectorTimestamp, List<TxChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, ai.floedb.floecat.transaction.rpc.ConnectorProvisioning>
        bootstrapByTable = new LinkedHashMap<>();
    boolean hasProvisioning = false;
    for (TxChange change : changes) {
      if (change == null
          || change.getChangePayloadCase()
              != ai.floedb.floecat.transaction.rpc.TxChange.ChangePayloadCase
                  .CONNECTOR_PROVISIONING) {
        continue;
      }
      hasProvisioning = true;
      ResourceId tableId = resolveTableId(accountId, change);
      String tableKey = tableId.getId();
      if (bootstrapByTable.containsKey(tableKey)) {
        throw new IllegalArgumentException(
            "duplicate connector provisioning intent for table " + tableKey);
      }
      bootstrapByTable.put(tableKey, change.getConnectorProvisioning());
    }
    if (!hasProvisioning) {
      return List.copyOf(changes);
    }

    List<TxChange> effective = new ArrayList<>(changes.size());
    Set<String> tablesWithMaterializedTablePayload = new java.util.HashSet<>();
    for (TxChange change : changes) {
      if (change == null) {
        continue;
      }
      if (change.getChangePayloadCase()
          == ai.floedb.floecat.transaction.rpc.TxChange.ChangePayloadCase.CONNECTOR_PROVISIONING) {
        continue;
      }
      if (change.getChangePayloadCase()
          == ai.floedb.floecat.transaction.rpc.TxChange.ChangePayloadCase.TABLE) {
        ResourceId tableId = resolveTableId(accountId, change);
        var bootstrap = bootstrapByTable.get(tableId.getId());
        if (bootstrap != null) {
          Connector connector =
              buildProvisionedConnector(
                  accountId, txId, change.getTable(), bootstrap, connectorTimestamp);
          effective.add(
              TxChange.newBuilder()
                  .setTargetPointerKey(
                      Keys.connectorPointerById(accountId, connector.getResourceId().getId()))
                  .setConnector(connector)
                  .build());
          effective.add(
              change.toBuilder()
                  .setTable(enrichProvisionedTable(change.getTable(), connector))
                  .build());
          tablesWithMaterializedTablePayload.add(tableId.getId());
          continue;
        }
      }
      effective.add(change);
    }

    for (String tableId : bootstrapByTable.keySet()) {
      if (!tablesWithMaterializedTablePayload.contains(tableId)) {
        throw new IllegalArgumentException(
            "connector provisioning requires a table payload for table " + tableId);
      }
    }
    return List.copyOf(effective);
  }

  private Connector buildProvisionedConnector(
      String accountId,
      String txId,
      Table table,
      ai.floedb.floecat.transaction.rpc.ConnectorProvisioning provisioning,
      Timestamp connectorTimestamp) {
    if (provisioning == null) {
      throw new IllegalArgumentException("connector provisioning payload is required");
    }
    if (table == null || !table.hasResourceId()) {
      throw new IllegalArgumentException("connector provisioning requires table payload");
    }
    if (!table.hasCatalogId() || table.getCatalogId().getId().isBlank()) {
      throw new IllegalArgumentException("table payload missing catalog_id");
    }
    if (!table.hasNamespaceId() || table.getNamespaceId().getId().isBlank()) {
      throw new IllegalArgumentException("table payload missing namespace_id");
    }
    ResourceId tableId = table.getResourceId();
    ResourceId connectorId = deterministicConnectorId(accountId, txId, tableId);
    String connectorUri = provisioning.getConnectorUri().trim();
    if (connectorUri.isBlank()) {
      throw new IllegalArgumentException("connector provisioning missing connector_uri");
    }
    List<String> namespacePath = List.copyOf(provisioning.getSourceNamespacePathList());
    String sourceTableName = provisioning.getSourceTableName().trim();
    if (sourceTableName.isBlank()) {
      throw new IllegalArgumentException("connector provisioning missing source_table_name");
    }
    String displayName = provisioning.getDisplayName().trim();
    if (displayName.isBlank()) {
      throw new IllegalArgumentException("connector provisioning missing display_name");
    }
    String tableName = deriveBootstrapTableName(table);
    Connector.Builder connector =
        Connector.newBuilder()
            .setResourceId(connectorId)
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setSource(
                SourceSelector.newBuilder()
                    .setNamespace(NamespacePath.newBuilder().addAllSegments(namespacePath).build())
                    .setTable(sourceTableName)
                    .build())
            .setDestination(
                DestinationTarget.newBuilder()
                    .setCatalogId(table.getCatalogId())
                    .setNamespaceId(table.getNamespaceId())
                    .setTableDisplayName(tableName)
                    .setTableId(tableId)
                    .build())
            .setUri(connectorUri)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .setCreatedAt(connectorTimestamp == null ? nowTs() : connectorTimestamp)
            .setUpdatedAt(connectorTimestamp == null ? nowTs() : connectorTimestamp)
            .setState(ConnectorState.CS_PAUSED);
    if (!provisioning.getDescription().isBlank()) {
      connector.setDescription(provisioning.getDescription().trim());
    }
    provisioning.getPropertiesMap().forEach(connector::putProperties);
    return connector.build();
  }

  private Table enrichProvisionedTable(Table table, Connector connector) {
    if (table == null || !table.hasResourceId()) {
      throw new IllegalArgumentException("table payload missing resource_id");
    }
    if (connector == null || !connector.hasResourceId()) {
      return table;
    }
    if (table.hasUpstream()
        && table.getUpstream().hasConnectorId()
        && !Objects.equals(
            table.getUpstream().getConnectorId().getId(), connector.getResourceId().getId())) {
      throw new IllegalArgumentException("table payload connector_id does not match provisioning");
    }
    UpstreamRef.Builder upstream =
        table.hasUpstream()
            ? table.getUpstream().toBuilder()
            : UpstreamRef.newBuilder()
                .setFormat(TableFormat.TF_ICEBERG)
                .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID);
    upstream.setConnectorId(connector.getResourceId());
    if (connector.hasSource() && connector.getSource().hasNamespace()) {
      upstream
          .clearNamespacePath()
          .addAllNamespacePath(connector.getSource().getNamespace().getSegmentsList());
    }
    if (connector.hasDestination() && !connector.getDestination().getTableDisplayName().isBlank()) {
      upstream.setTableDisplayName(connector.getDestination().getTableDisplayName());
    }
    if (table.hasUpstream() && !table.getUpstream().getUri().isBlank()) {
      upstream.setUri(table.getUpstream().getUri());
    }
    return table.toBuilder().setUpstream(upstream).build();
  }

  private ResourceId deterministicConnectorId(String accountId, String txId, ResourceId tableId) {
    String seed = (txId == null ? "" : txId) + "|" + (tableId == null ? "" : tableId.getId());
    UUID deterministicId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId(deterministicId.toString())
        .setKind(ResourceKind.RK_CONNECTOR)
        .build();
  }

  private String deriveBootstrapTableName(Table table) {
    if (table == null) {
      throw new IllegalArgumentException("table payload is required");
    }
    if (table.hasUpstream() && !table.getUpstream().getTableDisplayName().isBlank()) {
      return table.getUpstream().getTableDisplayName();
    }
    if (!table.getDisplayName().isBlank()) {
      return table.getDisplayName();
    }
    throw new IllegalArgumentException("table payload missing display_name");
  }

  private void schedulePostCommitCaptureBootstrap(
      String accountId, String txId, List<TransactionIntent> intents) {
    if (accountId == null || accountId.isBlank() || txId == null || txId.isBlank()) {
      return;
    }
    if (intents == null || intents.isEmpty()) {
      return;
    }
    java.util.concurrent.Executor executor = postCommitExecutor;
    try {
      executor.execute(() -> runPostCommitCaptureBootstrap(accountId, txId, intents));
    } catch (RuntimeException e) {
      LOG.warnf(e, "Failed to schedule post-commit capture bootstrap tx=%s", txId);
    }
  }

  private void runPostCommitCaptureBootstrap(
      String accountId, String txId, List<TransactionIntent> intents) {
    for (PostCommitCaptureCandidate candidate : postCommitCaptureCandidates(intents)) {
      try {
        Connector connector = connectorForPostCommitCapture(candidate.connectorId());
        if (connector == null) {
          continue;
        }
        Connector activeConnector =
            ensureConnectorActiveForPostCommitCapture(
                connector, candidate.connectorCreatedInThisTransaction());
        if (activeConnector == null || activeConnector.getState() != ConnectorState.CS_ACTIVE) {
          continue;
        }
        enqueuePostCommitCapture(accountId, txId, activeConnector, candidate.tableId());
      } catch (RuntimeException e) {
        LOG.warnf(
            e,
            "Post-commit capture bootstrap failed tx=%s connector=%s table=%s",
            txId,
            candidate.connectorId().getId(),
            candidate.tableId());
      }
    }
  }

  private List<PostCommitCaptureCandidate> postCommitCaptureCandidates(
      List<TransactionIntent> intents) {
    if (intents == null || intents.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, PostCommitCaptureCandidate> candidates = new LinkedHashMap<>();
    for (TransactionIntent intent : intents) {
      if (intent == null) {
        continue;
      }
      if (isConnectorByIdPointer(intent.getTargetPointerKey())) {
        Connector connector = readConnector(intent.getBlobUri());
        if (connector == null
            || !connector.hasResourceId()
            || !isGatewayManagedCaptureOnlyIcebergConnector(connector)
            || !connector.hasDestination()
            || !connector.getDestination().hasTableId()
            || connector.getDestination().getTableId().getId().isBlank()) {
          continue;
        }
        candidates.putIfAbsent(
            connector.getResourceId().getId()
                + "|"
                + connector.getDestination().getTableId().getId(),
            new PostCommitCaptureCandidate(
                connector.getResourceId(), connector.getDestination().getTableId().getId(), true));
        continue;
      }
      if (!isTableByIdPointer(intent.getTargetPointerKey())) {
        continue;
      }
      Table table = readTable(intent.getBlobUri());
      if (table == null
          || !table.hasResourceId()
          || !table.hasUpstream()
          || !table.getUpstream().hasConnectorId()) {
        continue;
      }
      ResourceId connectorId = table.getUpstream().getConnectorId();
      if (connectorId == null
          || connectorId.getId().isBlank()
          || table.getResourceId().getId().isBlank()) {
        continue;
      }
      candidates.putIfAbsent(
          connectorId.getId() + "|" + table.getResourceId().getId(),
          new PostCommitCaptureCandidate(connectorId, table.getResourceId().getId(), false));
    }
    return List.copyOf(candidates.values());
  }

  private Connector connectorForPostCommitCapture(ResourceId connectorId) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return null;
    }
    Connector connector = connectorRepo.getById(connectorId).orElse(null);
    if (!isGatewayManagedCaptureOnlyIcebergConnector(connector)) {
      return null;
    }
    return connector;
  }

  private Connector ensureConnectorActiveForPostCommitCapture(
      Connector connector, boolean connectorCreatedInThisTransaction) {
    if (connector == null || !connector.hasResourceId()) {
      return null;
    }
    if (connector.getState() == ConnectorState.CS_ACTIVE) {
      return connector;
    }
    if (!connectorCreatedInThisTransaction) {
      return null;
    }
    if (connector.getState() != ConnectorState.CS_PAUSED) {
      LOG.warnf(
          "Skipping post-commit capture for connector=%s unsupported state=%s",
          connector.getResourceId().getId(), connector.getState().name());
      return null;
    }
    for (int attempt = 0; attempt < 3; attempt++) {
      MutationMeta meta = connectorRepo.metaForSafe(connector.getResourceId());
      if (meta == null) {
        return null;
      }
      Connector current = connectorRepo.getById(connector.getResourceId()).orElse(null);
      if (current == null) {
        return null;
      }
      if (!isGatewayManagedCaptureOnlyIcebergConnector(current)) {
        return null;
      }
      if (current.getState() == ConnectorState.CS_ACTIVE) {
        return current;
      }
      if (current.getState() != ConnectorState.CS_PAUSED) {
        return null;
      }
      Connector activated =
          current.toBuilder().setState(ConnectorState.CS_ACTIVE).setUpdatedAt(nowTs()).build();
      if (connectorRepo.update(activated, meta.getPointerVersion())) {
        return activated;
      }
    }
    LOG.warnf(
        "Failed to activate post-commit connector after retries connector=%s",
        connector.getResourceId().getId());
    return null;
  }

  private void enqueuePostCommitCapture(
      String accountId, String txId, Connector connector, String tableId) {
    if (accountId == null
        || accountId.isBlank()
        || connector == null
        || !connector.hasResourceId()
        || tableId == null
        || tableId.isBlank()) {
      return;
    }
    reconcileJobs.enqueuePlan(
        accountId,
        connector.getResourceId().getId(),
        false,
        ReconcilerService.CaptureMode.CAPTURE_ONLY,
        ReconcileScope.of(
            List.of(),
            tableId,
            List.of(),
            ReconcileCapturePolicy.of(
                List.of(),
                Set.of(
                    ReconcileCapturePolicy.Output.TABLE_STATS,
                    ReconcileCapturePolicy.Output.FILE_STATS,
                    ReconcileCapturePolicy.Output.COLUMN_STATS))),
        ReconcileExecutionPolicy.defaults(),
        "");
    LOG.infof(
        "Enqueued post-commit capture tx=%s connector=%s table=%s",
        txId, connector.getResourceId().getId(), tableId);
  }

  private boolean isGatewayManagedCaptureOnlyIcebergConnector(Connector connector) {
    if (connector == null || !connector.hasResourceId()) {
      return false;
    }
    if (connector.getKind() != ConnectorKind.CK_ICEBERG) {
      return false;
    }
    if (!CONNECTOR_MODE_CAPTURE_ONLY.equalsIgnoreCase(
        connector.getPropertiesMap().get(CONNECTOR_MODE_PROPERTY))) {
      return false;
    }
    String captureStatistics = connector.getPropertiesMap().get(CAPTURE_STATISTICS_PROPERTY);
    return captureStatistics == null || Boolean.parseBoolean(captureStatistics);
  }

  private boolean intentsAlreadyApplied(List<TransactionIntent> intents) {
    if (intents == null || intents.isEmpty()) {
      return false;
    }
    for (var intent : intents) {
      if (intent == null
          || intent.getTargetPointerKey().isBlank()
          || intent.getBlobUri().isBlank()) {
        return false;
      }
      var ptr = pointerStore.get(intent.getTargetPointerKey()).orElse(null);
      if (isDeleteSentinelBlobUri(
          intent.getAccountId(),
          intent.getTxId(),
          intent.getTargetPointerKey(),
          intent.getBlobUri())) {
        if (isTableByIdPointer(intent.getTargetPointerKey())) {
          if (!tableDeleteAlreadyApplied(intent)) {
            return false;
          }
          continue;
        }
        if (ptr != null) {
          return false;
        }
      } else if (ptr == null || !intent.getBlobUri().equals(ptr.getBlobUri())) {
        return false;
      }
    }
    return true;
  }

  private void ensurePreparedRequestMatchesExistingIntents(
      String accountId, String txId, PrepareTransactionRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("prepare request is required");
    }
    Transaction txn = getTransactionOrThrow(accountId, txId);
    List<TxChange> changes =
        materializeConnectorProvisioningChanges(
            accountId, txId, txn.getCreatedAt(), request.getChangesList());
    List<IntentFingerprint> expected = new ArrayList<>(changes.size());
    for (var change : changes) {
      PlannedIntent planned = planIntentForReplayMatch(accountId, txId, change);
      Long explicitExpectedVersion =
          change.hasPrecondition() && change.getPrecondition().hasExpectedVersion()
              ? change.getPrecondition().getExpectedVersion()
              : null;
      expected.add(
          new IntentFingerprint(
              planned.targetPointerKey(),
              planned.blobUri(),
              explicitExpectedVersion,
              planned.expectedOwnedNamePointerKey()));
    }
    expected.sort(Comparator.comparing(IntentFingerprint::targetPointerKey));

    List<TransactionIntent> existing = new ArrayList<>(intentRepo.listByTx(accountId, txId));
    existing.sort(Comparator.comparing(TransactionIntent::getTargetPointerKey));
    if (expected.size() != existing.size()) {
      throw new IllegalArgumentException(
          "prepare request does not match already-prepared transaction intents");
    }
    for (int i = 0; i < expected.size(); i++) {
      IntentFingerprint exp = expected.get(i);
      TransactionIntent actual = existing.get(i);
      if (!exp.targetPointerKey().equals(actual.getTargetPointerKey())
          || !exp.blobUri().equals(actual.getBlobUri())
          || !Objects.equals(
              exp.expectedOwnedNamePointerKey(),
              actual.hasExpectedOwnedNamePointerKey()
                  ? actual.getExpectedOwnedNamePointerKey()
                  : null)) {
        throw new IllegalArgumentException(
            "prepare request does not match already-prepared transaction intents");
      }
      // For replay matching, omitted expected_version is treated as request-shape equivalence only.
      if (exp.explicitExpectedVersion() != null
          && (!actual.hasExpectedVersion()
              || exp.explicitExpectedVersion() != actual.getExpectedVersion())) {
        throw new IllegalArgumentException(
            "prepare request does not match already-prepared transaction intents");
      }
    }
  }

  private PlannedIntent planIntent(String accountId, String txId, TxChange change) {
    ResolvedTxTarget target = resolveTarget(accountId, change);
    ResourceId tableId = target.tableId();
    String pointerKey = target.pointerKey();

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
    String expectedOwnedNamePointerKey = null;
    byte[] inlineBytes = null;
    String inlineContentType = null;
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
        if (looksLikeDeleteSentinelBlobUri(accountId, blobUri)
            && !isDeleteSentinelBlobUri(accountId, txId, pointerKey, blobUri)) {
          throw new IllegalArgumentException(
              "intended_blob_uri delete sentinel does not match target for " + pointerKey);
        }
        if (isDeleteSentinelBlobUri(accountId, txId, pointerKey, blobUri)
            && isTableByIdPointer(pointerKey)) {
          expectedOwnedNamePointerKey = expectedOwnedTableNamePointerKey(pointerKey, true);
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
        String sha = Hashing.sha256Hex(tablePayload.toByteArray());
        blobUri = Keys.tableBlobUri(accountId, tableId.getId(), sha);
        inlineBytes = tablePayload.toByteArray();
        inlineContentType = "application/x-protobuf";
      }
      case CONNECTOR -> {
        var connectorPayload = change.getConnector();
        if (!connectorPayload.hasResourceId()) {
          throw new IllegalArgumentException("connector payload missing resource_id");
        }
        if (!connectorPayload.getResourceId().getAccountId().equals(accountId)) {
          throw new IllegalArgumentException("connector payload account mismatch for target");
        }
        String expectedPointerKey =
            Keys.connectorPointerById(
                connectorPayload.getResourceId().getAccountId(),
                connectorPayload.getResourceId().getId());
        if (!expectedPointerKey.equals(pointerKey)) {
          throw new IllegalArgumentException(
              "connector payload resource_id does not match target pointer");
        }
        String sha = Hashing.sha256Hex(connectorPayload.toByteArray());
        blobUri = Keys.connectorBlobUri(accountId, connectorPayload.getResourceId().getId(), sha);
        inlineBytes = connectorPayload.toByteArray();
        inlineContentType = "application/x-protobuf";
      }
      case PAYLOAD -> {
        byte[] payload = change.getPayload().toByteArray();
        String sha = Hashing.sha256Hex(payload);
        blobUri = Keys.transactionObjectBlobUri(accountId, txId, sha);
        inlineBytes = payload;
        inlineContentType = "application/octet-stream";
      }
      case CHANGEPAYLOAD_NOT_SET -> {
        throw new IllegalArgumentException(
            "missing payload or intended_blob_uri for " + pointerKey);
      }
      default ->
          throw new IllegalArgumentException(
              "unknown payload type for " + pointerKey + ": " + change.getChangePayloadCase());
    }
    return new PlannedIntent(
        pointerKey,
        blobUri,
        expectedVersion,
        inlineBytes,
        inlineContentType,
        expectedOwnedNamePointerKey);
  }

  private PlannedIntent planIntentForReplayMatch(String accountId, String txId, TxChange change) {
    ResolvedTxTarget target = resolveTarget(accountId, change);
    ResourceId tableId = target.tableId();
    String pointerKey = target.pointerKey();

    String blobUri;
    String expectedOwnedNamePointerKey = null;
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
        if (looksLikeDeleteSentinelBlobUri(accountId, blobUri)
            && !isDeleteSentinelBlobUri(accountId, txId, pointerKey, blobUri)) {
          throw new IllegalArgumentException(
              "intended_blob_uri delete sentinel does not match target for " + pointerKey);
        }
        if (isDeleteSentinelBlobUri(accountId, txId, pointerKey, blobUri)
            && isTableByIdPointer(pointerKey)) {
          expectedOwnedNamePointerKey = expectedOwnedTableNamePointerKey(pointerKey, true);
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
        String sha = Hashing.sha256Hex(tablePayload.toByteArray());
        blobUri = Keys.tableBlobUri(accountId, tableId.getId(), sha);
      }
      case CONNECTOR -> {
        var connectorPayload = change.getConnector();
        if (!connectorPayload.hasResourceId()) {
          throw new IllegalArgumentException("connector payload missing resource_id");
        }
        if (!connectorPayload.getResourceId().getAccountId().equals(accountId)) {
          throw new IllegalArgumentException("connector payload account mismatch for target");
        }
        String expectedPointerKey =
            Keys.connectorPointerById(
                connectorPayload.getResourceId().getAccountId(),
                connectorPayload.getResourceId().getId());
        if (!expectedPointerKey.equals(pointerKey)) {
          throw new IllegalArgumentException(
              "connector payload resource_id does not match target pointer");
        }
        String sha = Hashing.sha256Hex(connectorPayload.toByteArray());
        blobUri = Keys.connectorBlobUri(accountId, connectorPayload.getResourceId().getId(), sha);
      }
      case PAYLOAD -> {
        byte[] payload = change.getPayload().toByteArray();
        String sha = Hashing.sha256Hex(payload);
        blobUri = Keys.transactionObjectBlobUri(accountId, txId, sha);
      }
      case CHANGEPAYLOAD_NOT_SET -> {
        throw new IllegalArgumentException(
            "missing payload or intended_blob_uri for " + pointerKey);
      }
      default ->
          throw new IllegalArgumentException(
              "unknown payload type for " + pointerKey + ": " + change.getChangePayloadCase());
    }
    return new PlannedIntent(pointerKey, blobUri, 0L, null, null, expectedOwnedNamePointerKey);
  }

  private boolean looksLikeDeleteSentinelBlobUri(String accountId, String blobUri) {
    if (accountId == null || accountId.isBlank() || blobUri == null || blobUri.isBlank()) {
      return false;
    }
    return blobUri.startsWith(Keys.accountRootPrefix(accountId) + "transactions/")
        && blobUri.contains("/delete/");
  }

  private boolean isDeleteSentinelBlobUri(
      String accountId, String txId, String targetPointerKey, String blobUri) {
    if (!looksLikeDeleteSentinelBlobUri(accountId, blobUri)
        || txId == null
        || txId.isBlank()
        || targetPointerKey == null
        || targetPointerKey.isBlank()) {
      return false;
    }
    try {
      return Keys.transactionDeleteSentinelUri(accountId, txId, targetPointerKey).equals(blobUri);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private boolean isTableByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/tables/by-id/");
  }

  private boolean isConnectorByIdPointer(String pointerKey) {
    return pointerKey != null && pointerKey.contains("/connectors/by-id/");
  }

  private record PostCommitCaptureCandidate(
      ResourceId connectorId, String tableId, boolean connectorCreatedInThisTransaction) {}

  private boolean tableDeleteAlreadyApplied(TransactionIntent intent) {
    if (intent == null || !isTableByIdPointer(intent.getTargetPointerKey())) {
      return false;
    }
    if (pointerStore.get(intent.getTargetPointerKey()).isPresent()) {
      return false;
    }
    String tableId = tableIdFromByIdPointer(intent.getTargetPointerKey());
    if (tableId == null || tableId.isBlank()) {
      return false;
    }
    if (!intent.hasExpectedOwnedNamePointerKey()
        || intent.getExpectedOwnedNamePointerKey().isBlank()) {
      return noOwnedNamePointerReferencesTable(intent.getAccountId(), tableId);
    }
    return ownedNamePointerCleared(intent.getExpectedOwnedNamePointerKey(), tableId);
  }

  private boolean ownedNamePointerCleared(String pointerKey, String expectedTableId) {
    var pointer = pointerStore.get(pointerKey).orElse(null);
    if (pointer == null || pointer.getBlobUri().isBlank()) {
      return true;
    }
    Table table = readTable(pointer.getBlobUri());
    if (table == null || !table.hasResourceId()) {
      return false;
    }
    return !expectedTableId.equals(table.getResourceId().getId());
  }

  private boolean noOwnedNamePointerReferencesTable(String accountId, String expectedTableId) {
    if (accountId == null
        || accountId.isBlank()
        || expectedTableId == null
        || expectedTableId.isBlank()) {
      return false;
    }
    String prefix = Keys.catalogRootPrefix(accountId);
    String token = "";
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(prefix, TABLE_NAME_REPLAY_SCAN_PAGE_SIZE, token, next);
      for (Pointer pointer : pointers) {
        String key = pointer.getKey();
        if (key == null || !key.contains(Keys.SEG_TABLES_BY_NAME)) {
          continue;
        }
        if (pointer.getBlobUri().isBlank()) {
          continue;
        }
        Table table = readTable(pointer.getBlobUri());
        if (table == null || !table.hasResourceId()) {
          return false;
        }
        if (expectedTableId.equals(table.getResourceId().getId())) {
          return false;
        }
      }
      token = next.toString();
      if (token.isEmpty()) {
        return true;
      }
    }
  }

  private String tableIdFromByIdPointer(String pointerKey) {
    if (!isTableByIdPointer(pointerKey)) {
      return null;
    }
    int idx = pointerKey.lastIndexOf("/tables/by-id/");
    if (idx < 0) {
      return null;
    }
    String encoded = pointerKey.substring(idx + "/tables/by-id/".length());
    return encoded.isBlank() ? null : URLDecoder.decode(encoded, StandardCharsets.UTF_8);
  }

  private String expectedOwnedTableNamePointerKey(
      String tableByIdPointerKey, boolean requireReadable) {
    var pointer = pointerStore.get(tableByIdPointerKey).orElse(null);
    if (pointer == null || pointer.getBlobUri().isBlank()) {
      return null;
    }
    Table table = readTable(pointer.getBlobUri());
    if (table == null || !table.hasResourceId()) {
      if (requireReadable) {
        throw new IllegalArgumentException(
            "current table pointer is unreadable for delete " + tableByIdPointerKey);
      }
      return null;
    }
    validateCurrentTablePointerTarget(tableByIdPointerKey, table);
    return Keys.tablePointerByName(
        table.getResourceId().getAccountId(),
        table.getCatalogId().getId(),
        table.getNamespaceId().getId(),
        table.getDisplayName());
  }

  private void validateCurrentTablePointerTarget(String pointerKey, Table table) {
    if (table == null || !table.hasResourceId()) {
      throw new IllegalArgumentException("current table pointer is unreadable for " + pointerKey);
    }
    String expectedPointerKey =
        Keys.tablePointerById(table.getResourceId().getAccountId(), table.getResourceId().getId());
    if (!expectedPointerKey.equals(pointerKey)) {
      throw new IllegalArgumentException(
          "current table pointer target does not match payload for " + pointerKey);
    }
  }

  private Table readTable(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return null;
    }
    try {
      byte[] bytes = blobStore.get(blobUri);
      return bytes == null ? null : Table.parseFrom(bytes);
    } catch (Exception e) {
      return null;
    }
  }

  private Connector readConnector(String blobUri) {
    if (blobUri == null || blobUri.isBlank()) {
      return null;
    }
    try {
      byte[] bytes = blobStore.get(blobUri);
      return bytes == null ? null : Connector.parseFrom(bytes);
    } catch (Exception e) {
      return null;
    }
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

  private void logCommitFailure(
      String accountId,
      Transaction txn,
      TransactionIntentApplierSupport.ApplyOutcome outcome,
      List<TransactionIntent> intents) {
    if (outcome == null) {
      return;
    }
    logCommitFailure(
        accountId,
        txn,
        outcome.errorCode(),
        outcome.errorMessage(),
        intents,
        outcome.expectedVersion(),
        outcome.actualVersion(),
        outcome.conflictOwner());
  }

  private void logCommitFailure(
      String accountId,
      Transaction txn,
      String errorCode,
      String errorMessage,
      List<TransactionIntent> intents) {
    logCommitFailure(accountId, txn, errorCode, errorMessage, intents, null, null, null);
  }

  private void logCommitFailure(
      String accountId,
      Transaction txn,
      String errorCode,
      String errorMessage,
      List<TransactionIntent> intents,
      Long expectedVersion,
      Long actualVersion,
      String conflictOwner) {
    LOG.warnf(
        "transaction commit non-applied account=%s tx=%s state=%s error_code=%s error_message=%s expected_version=%s actual_version=%s conflict_owner=%s intents=%s",
        accountId,
        txn == null ? "" : txn.getTxId(),
        txn == null ? TransactionState.TS_UNSPECIFIED : txn.getState(),
        nullToEmpty(errorCode),
        nullToEmpty(errorMessage),
        expectedVersion,
        actualVersion,
        nullToEmpty(conflictOwner),
        summarizeIntentTargets(intents));
  }

  private static String summarizeIntentTargets(List<TransactionIntent> intents) {
    if (intents == null || intents.isEmpty()) {
      return "[]";
    }
    List<String> targets = new ArrayList<>(Math.min(intents.size(), 8));
    for (var intent : intents) {
      if (intent == null) {
        continue;
      }
      String target = intent.getTargetPointerKey();
      if (target == null || target.isBlank()) {
        continue;
      }
      targets.add(target);
      if (targets.size() == 8) {
        break;
      }
    }
    String suffix = intents.size() > targets.size() ? ",..." : "";
    return "[" + String.join(",", targets) + suffix + "]";
  }

  private static String nullToEmpty(String value) {
    return value == null ? "" : value;
  }

  private void cleanupIntents(String accountId, String txId) {
    List<TransactionIntent> intents = intentRepo.listByTx(accountId, txId);
    cleanupIntentsBestEffort(intents);
  }

  private void invalidateTouchedGraphEntries(List<TransactionIntent> intents) {
    if (intents == null || intents.isEmpty() || metadataGraph == null) {
      return;
    }
    for (var intent : intents) {
      if (intent == null) {
        continue;
      }
      String tableId = tableIdFromByIdPointer(intent.getTargetPointerKey());
      if (tableId == null || tableId.isBlank()) {
        continue;
      }
      metadataGraph.invalidate(
          ResourceId.newBuilder()
              .setAccountId(intent.getAccountId())
              .setKind(ResourceKind.RK_TABLE)
              .setId(tableId)
              .build());
    }
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
    if (txn.getState() == TransactionState.TS_ABORTED
        || txn.getState() == TransactionState.TS_APPLYING
        || isTerminalNonAbortableState(txn.getState())) {
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

  private boolean isTerminalNonAbortableState(TransactionState state) {
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
      boolean targetReleased =
          intentRepo.deleteByTargetIfOwned(
              accountId, existing.getTargetPointerKey(), existing.getTxId());
      if (targetReleased) {
        intentRepo.deleteByTxIfOwned(accountId, existing.getTxId(), existing.getTargetPointerKey());
        continue;
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
    if (owner.getState() == TransactionState.TS_APPLYING) {
      return false;
    }
    if (owner.getState() == TransactionState.TS_APPLY_FAILED_RETRYABLE) {
      return isExpired(owner, now);
    }
    return isExpired(owner, now);
  }

  private record ResolvedTxTarget(String pointerKey, ResourceId tableId) {}

  private record IntentFingerprint(
      String targetPointerKey,
      String blobUri,
      Long explicitExpectedVersion,
      String expectedOwnedNamePointerKey) {}

  private record PlannedIntent(
      String targetPointerKey,
      String blobUri,
      long expectedVersion,
      byte[] inlineBytes,
      String inlineContentType,
      String expectedOwnedNamePointerKey) {}

  private record PendingBlob(String uri, byte[] bytes, String contentType) {}
}
