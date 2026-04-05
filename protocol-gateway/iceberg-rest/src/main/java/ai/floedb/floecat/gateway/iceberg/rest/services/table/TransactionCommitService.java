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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil.asStringMap;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitJournalEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergCommitOutboxEntry;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergRef;
import ai.floedb.floecat.storage.kv.Keys;
import ai.floedb.floecat.transaction.rpc.GetTransactionRequest;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitService {
  private static final int COMMIT_JOURNAL_VERSION = 1;
  private static final int COMMIT_OUTBOX_VERSION = 1;
  private static final String ICEBERG_METADATA_KEY = "iceberg";
  private static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  private static final int DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS = 6;
  private static final long DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS = 20L;
  private static final long DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS = 200L;
  private static final Set<TransactionState> CONFIRMABLE_COMMIT_STATES =
      Set.of(
          TransactionState.TS_UNSPECIFIED,
          TransactionState.TS_OPEN,
          TransactionState.TS_PREPARED,
          TransactionState.TS_APPLYING,
          TransactionState.TS_APPLY_FAILED_RETRYABLE);
  private static final Logger LOG = Logger.getLogger(TransactionCommitService.class);
  @Inject AccountContext accountContext;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitPlanner tableCommitPlanner;
  @Inject TableCreateTransactionMapper tableCreateTransactionMapper;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TableCommitMetadataMutator metadataMutator;
  @Inject TablePropertyService tablePropertyService;
  @Inject ConnectorProvisioningService connectorProvisioningService;
  @Inject TableCommitJournalService commitJournalService;
  @Inject TableCommitOutboxService commitOutboxService;
  @Inject TableCommitMaterializationService materializationService;
  @Inject GrpcServiceFacade grpcClient;

  public Response commitCreate(
      String prefix,
      String idempotencyKey,
      List<String> namespacePath,
      String tableName,
      ResourceId catalogId,
      ResourceId namespaceId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    return commitInternal(
        prefix,
        idempotencyKey,
        tableCreateTransactionMapper.buildCreateRequest(
            namespacePath, tableName, catalogId, namespaceId, request, tableSupport),
        tableSupport,
        false);
  }

  public Response commit(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    return commitInternal(prefix, idempotencyKey, request, tableSupport, true);
  }

  private Response commitInternal(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String duplicateIdentifier = firstDuplicateTableIdentifier(changes);
    if (duplicateIdentifier != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicateIdentifier);
    }
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();

    String requestHash = requestHash(changes);
    String beginIdempotency =
        firstNonBlank(idempotencyKey, "req:" + catalogName + ":" + requestHash);
    ai.floedb.floecat.transaction.rpc.BeginTransactionResponse begin;
    try {
      begin =
          grpcClient.beginTransaction(
              ai.floedb.floecat.transaction.rpc.BeginTransactionRequest.newBuilder()
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(beginIdempotency == null ? "" : beginIdempotency))
                  .putProperties(TX_REQUEST_HASH_PROPERTY, requestHash)
                  .build());
    } catch (StatusRuntimeException beginFailure) {
      return mapPreCommitFailure(beginFailure);
    } catch (RuntimeException beginFailure) {
      return preCommitStateUnknown();
    }
    String txId = begin.getTransaction().getTxId();
    if (txId == null || txId.isBlank()) {
      return IcebergErrorResponses.failure(
          "Failed to begin transaction",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    ai.floedb.floecat.transaction.rpc.GetTransactionResponse currentTxn;
    TransactionState currentState;
    try {
      currentTxn =
          grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
      currentState =
          currentTxn != null && currentTxn.hasTransaction()
              ? currentTxn.getTransaction().getState()
              : TransactionState.TS_UNSPECIFIED;
    } catch (StatusRuntimeException e) {
      abortTransactionQuietly(txId, "failed to load transaction");
      return mapPreCommitFailure(e);
    } catch (RuntimeException e) {
      abortTransactionQuietly(txId, "failed to load transaction");
      return preCommitStateUnknown();
    }
    if (currentTxn != null && currentTxn.hasTransaction()) {
      String existingRequestHash =
          currentTxn.getTransaction().getPropertiesMap().get(TX_REQUEST_HASH_PROPERTY);
      if (existingRequestHash != null
          && !existingRequestHash.isBlank()
          && !existingRequestHash.equals(requestHash)) {
        maybeAbortOpenTransaction(currentState, txId, "transaction request-hash mismatch");
        return IcebergErrorResponses.failure(
            "transaction request does not match existing transaction payload",
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }

    String idempotencyBase = firstNonBlank(idempotencyKey, txId);
    long txCreatedAtMs =
        currentTxn != null
                && currentTxn.hasTransaction()
                && currentTxn.getTransaction().hasCreatedAt()
            ? Timestamps.toMillis(currentTxn.getTransaction().getCreatedAt())
            : begin.getTransaction().hasCreatedAt()
                ? Timestamps.toMillis(begin.getTransaction().getCreatedAt())
                : clockMillis();
    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<TableCommitOutboxService.WorkItem> outboxItems = new ArrayList<>();

    if (currentState == TransactionState.TS_APPLY_FAILED_CONFLICT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    boolean alreadyApplied = currentState == TransactionState.TS_APPLIED;
    List<PlannedChange> planned = new ArrayList<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      try {
        var identifier = change.identifier();
        if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
          maybeAbortOpenTransaction(currentState, txId, "table identifier is missing");
          return IcebergErrorResponses.validation("table identifier is required");
        }
        if (change.requirements() == null) {
          maybeAbortOpenTransaction(currentState, txId, "requirements are missing");
          return IcebergErrorResponses.validation("requirements are required");
        }
        if (change.updates() == null) {
          maybeAbortOpenTransaction(currentState, txId, "updates are missing");
          return IcebergErrorResponses.validation("updates are required");
        }
        boolean assertCreateRequested =
            hasRequirementType(
                change.requirements(), CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE);
        Response requirementTypeError = validateKnownRequirementTypes(change.requirements());
        if (requirementTypeError != null) {
          maybeAbortOpenTransaction(currentState, txId, "requirements include unknown type");
          return requirementTypeError;
        }
        Response updateTypeError = validateKnownUpdateActions(change.updates());
        if (updateTypeError != null) {
          maybeAbortOpenTransaction(currentState, txId, "updates include unknown action");
          return updateTypeError;
        }
        List<String> namespacePath =
            identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
        String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
        ResourceId namespaceId =
            tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
        TableRequests.Commit commitReq =
            new TableRequests.Commit(change.requirements(), change.updates());
        var command =
            new TableCommitService.CommitCommand(
                prefix,
                namespace,
                namespacePath,
                identifier.name(),
                catalogName,
                catalogId,
                namespaceId,
                idempotencyBase,
                null,
                null,
                commitReq,
                tableSupport);
        ResourceId tableId;
        ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse;
        try {
          tableId =
              tableLifecycleService.resolveTableId(catalogName, namespacePath, identifier.name());
          tableResponse = tableLifecycleService.getTableResponse(tableId);
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.NOT_FOUND || !assertCreateRequested) {
            throw e;
          }
          tableId =
              atomicCreateTableId(
                  accountId, txId, catalogId, namespaceId, namespacePath, identifier.name());
          tableResponse = null;
        }
        Response assertCreateError =
            validateAssertCreateRequirement(change.requirements(), tableResponse);
        if (assertCreateError != null) {
          maybeAbortOpenTransaction(currentState, txId, "assert-create requirement failed");
          return assertCreateError;
        }
        ai.floedb.floecat.catalog.rpc.Table persistedTable =
            tableResponse == null || !tableResponse.hasTable()
                ? newCreateTableStub(tableId, catalogId, namespaceId, identifier.name())
                : tableResponse.getTable();
        long pointerVersion =
            tableResponse != null && tableResponse.hasMeta()
                ? tableResponse.getMeta().getPointerVersion()
                : 0L;
        Response nullRefRequirementError =
            validateNullSnapshotRefRequirements(
                tableSupport, persistedTable, change.requirements());
        if (nullRefRequirementError != null) {
          maybeAbortOpenTransaction(currentState, txId, "null snapshot-id ref requirement failed");
          return nullRefRequirementError;
        }
        ai.floedb.floecat.catalog.rpc.Table updated;
        boolean shouldPlan = !alreadyApplied;
        if (!shouldPlan) {
          // TS_APPLIED replay reloads durable outbox work from the commit journal later.
          updated = persistedTable;
        } else {
          Supplier<ai.floedb.floecat.catalog.rpc.Table> workingTableSupplier = () -> persistedTable;
          Supplier<ai.floedb.floecat.catalog.rpc.Table> requirementTableSupplier =
              () -> persistedTable;
          var plan =
              tableCommitPlanner.plan(
                  command, workingTableSupplier, requirementTableSupplier, tableId);
          if (plan.hasError()) {
            maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
            return plan.error();
          }
          updated = plan.table();
          PreMaterializedTable preMaterialized =
              preMaterializeTableBeforeCommit(
                  namespace,
                  identifier.name(),
                  tableId,
                  updated,
                  change.requirements() == null ? List.of() : List.copyOf(change.requirements()),
                  change.updates() == null ? List.of() : List.copyOf(change.updates()),
                  tableSupport,
                  preMaterializeAssertCreate);
          if (preMaterialized.error() != null) {
            maybeAbortOpenTransaction(
                currentState, txId, "metadata materialization failed before atomic commit");
            return preMaterialized.error();
          }
          updated = preMaterialized.table();
        }
        planned.add(
            new PlannedChange(
                namespacePath,
                namespaceId,
                identifier.name(),
                tableId,
                updated,
                change.updates() == null ? List.of() : List.copyOf(change.updates()),
                pointerVersion));
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          maybeAbortOpenTransaction(currentState, txId, "table not found during planning");
          return IcebergErrorResponses.noSuchTable(
              "Table not found while planning transaction change");
        }
        maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
        throw e;
      } catch (RuntimeException e) {
        maybeAbortOpenTransaction(currentState, txId, "transaction planning failed");
        throw e;
      }
    }

    for (var plan : planned) {
      var tableForTx = plan.table();
      ResourceId scopedTableId = scopeTableIdWithAccount(plan.tableId(), accountId);
      ConnectorProvisioningService.ProvisionResult connectorResolution =
          connectorProvisioningService.resolveOrCreateForCommit(
              accountId,
              txId,
              prefix,
              tableSupport,
              plan.namespacePath(),
              plan.namespaceId(),
              catalogId,
              plan.tableName(),
              scopedTableId,
              tableForTx);
      if (connectorResolution.error() != null) {
        maybeAbortOpenTransaction(currentState, txId, "connector provisioning failed");
        return connectorResolution.error();
      }
      tableForTx = connectorResolution.table();
      ResourceId connectorId = connectorResolution.connectorId();
      if (!connectorResolution.connectorTxChanges().isEmpty()) {
        txChanges.addAll(connectorResolution.connectorTxChanges());
      }
      CommitUpdateInspector.Parsed parsedUpdates =
          CommitUpdateInspector.inspectUpdates(plan.updates());
      List<Long> addedSnapshotIds = parsedUpdates.addedSnapshotIds();
      List<Long> removedSnapshotIds = parsedUpdates.removedSnapshotIds();
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTableId(plan.tableId())
              .setTable(tableForTx)
              .setPrecondition(Precondition.newBuilder().setExpectedVersion(plan.expectedVersion()))
              .build());

      SnapshotChangePlan snapshotChangePlan =
          planAtomicSnapshotChanges(
              accountId,
              txId,
              plan.tableId(),
              tableForTx,
              tableSupport,
              plan.updates(),
              removedSnapshotIds,
              alreadyApplied);
      if (snapshotChangePlan.error() != null) {
        maybeAbortOpenTransaction(currentState, txId, "snapshot metadata planning failed");
        return snapshotChangePlan.error();
      }
      if (!snapshotChangePlan.txChanges().isEmpty()) {
        txChanges.addAll(snapshotChangePlan.txChanges());
      }

      IcebergCommitJournalEntry journal =
          buildCommitJournalEntry(
              txId,
              requestHash,
              scopedTableId,
              plan.namespacePath(),
              plan.tableName(),
              connectorId,
              addedSnapshotIds,
              removedSnapshotIds,
              tableForTx,
              txCreatedAtMs);
      String pendingKey =
          Keys.tableCommitOutboxPendingPointer(
              txCreatedAtMs, accountId, scopedTableId.getId(), txId);
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(
                  Keys.tableCommitJournalPointer(accountId, scopedTableId.getId(), txId))
              .setPayload(ByteString.copyFrom(journal.toByteArray()))
              .build());
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTargetPointerKey(pendingKey)
              .setPayload(
                  ByteString.copyFrom(
                      buildCommitOutboxEntry(
                              txId, requestHash, accountId, scopedTableId.getId(), txCreatedAtMs)
                          .toByteArray()))
              .build());
      outboxItems.add(commitOutboxService.toWorkItem(pendingKey, journal));
    }

    boolean applied = isCommitAccepted(currentState);
    if (!applied) {
      if (currentState == TransactionState.TS_OPEN) {
        try {
          grpcClient.prepareTransaction(
              ai.floedb.floecat.transaction.rpc.PrepareTransactionRequest.newBuilder()
                  .setTxId(txId)
                  .addAllChanges(txChanges)
                  .setIdempotency(
                      ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                          .setKey(idempotencyBase == null ? "" : idempotencyBase + ":prepare"))
                  .build());
        } catch (RuntimeException e) {
          abortTransactionQuietly(txId, "transaction prepare failed");
          if (e instanceof StatusRuntimeException prepareFailure) {
            return mapPreCommitFailure(prepareFailure);
          }
          return preCommitStateUnknown();
        }
      }
      try {
        ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.Builder commitRequest =
            ai.floedb.floecat.transaction.rpc.CommitTransactionRequest.newBuilder().setTxId(txId);
        if (currentState != TransactionState.TS_APPLY_FAILED_RETRYABLE) {
          commitRequest.setIdempotency(
              ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder()
                  .setKey(idempotencyBase == null ? "" : idempotencyBase + ":commit"));
        }
        var commitResponse = grpcClient.commitTransaction(commitRequest.build());
        TransactionState commitState =
            commitResponse != null && commitResponse.hasTransaction()
                ? commitResponse.getTransaction().getState()
                : TransactionState.TS_UNSPECIFIED;
        if (isCommitAccepted(commitState)) {
          applied = true;
          // Commit applied; durable outbox processing happens asynchronously.
        } else {
          if (isDeterministicFailedState(commitState)) {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitFailedException",
                Response.Status.CONFLICT);
          }
          if (shouldConfirmAmbiguousCommitState(commitState) && waitForAppliedState(txId)) {
            applied = true;
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
        }
      } catch (StatusRuntimeException commitFailure) {
        if (isDeterministicCommitFailure(commitFailure)) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
        }
        if (isRetryableCommitAbort(commitFailure)) {
          if (waitForAppliedState(txId)) {
            applied = true;
            // Apply eventually succeeded; durable outbox processing happens asynchronously.
          } else {
            return IcebergErrorResponses.failure(
                "transaction commit failed",
                "CommitStateUnknownException",
                Response.Status.SERVICE_UNAVAILABLE);
          }
        } else {
          Response mapped = mapCommitFailureByStatus(commitFailure.getStatus().getCode());
          return mapped == null ? preCommitStateUnknown() : mapped;
        }
      } catch (RuntimeException commitFailure) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    if (!applied) {
      return IcebergErrorResponses.failure(
          "transaction commit did not reach applied state",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }

    return Response.noContent().build();
  }

  private record SnapshotChangePlan(
      List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges, Response error) {}

  private record PreMaterializedTable(ai.floedb.floecat.catalog.rpc.Table table, Response error) {}

  private record PlannedChange(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Map<String, Object>> updates,
      long expectedVersion) {}

  private static String firstNonBlank(String... values) {
    if (values == null) {
      return null;
    }
    for (String value : values) {
      if (value != null && !value.isBlank()) {
        return value;
      }
    }
    return null;
  }

  private PreMaterializedTable preMaterializeTableBeforeCommit(
      String namespace,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table plannedTable,
      List<Map<String, Object>> requirements,
      List<Map<String, Object>> updates,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate) {
    if (plannedTable == null || tableSupport == null) {
      return new PreMaterializedTable(plannedTable, null);
    }
    if (!preMaterializeAssertCreate
        && hasRequirementType(requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE)) {
      // Keep create-table locations empty so engines can own first metadata materialization.
      return new PreMaterializedTable(plannedTable, null);
    }
    String requestedLocation =
        CommitUpdateInspector.inspectUpdates(updates).requestedMetadataLocation();
    boolean skipMaterialization = requestedLocation != null;
    // Materialize metadata for every commit so metadata-location advances atomically with table
    // state, including snapshot-mutating updates.
    IcebergMetadata metadata = null;
    try {
      metadata = tableSupport.loadCurrentMetadata(plannedTable);
    } catch (RuntimeException e) {
      // Create/staged-create commit paths can legitimately have no readable pointer yet.
      // Continue with a metadata-free view so we can materialize and set metadata-location
      // before atomic apply.
      LOG.debugf(
          e,
          "Proceeding with pre-materialization without current metadata for tableId=%s table=%s",
          tableId == null ? "<missing>" : tableId.getId(),
          tableName);
    }
    var commitView =
        responseBuilder.buildInitialResponse(
            tableName,
            plannedTable,
            tableId,
            null,
            new TableRequests.Commit(List.of(), updates == null ? List.of() : List.copyOf(updates)),
            tableSupport,
            metadata);
    if (commitView == null || commitView.metadata() == null) {
      return new PreMaterializedTable(plannedTable, null);
    }
    var commitMetadata =
        metadataMutator.apply(
            commitView.metadata(),
            new TableRequests.Commit(
                List.of(), updates == null ? List.of() : List.copyOf(updates)));
    Table canonicalizedTable =
        tablePropertyService.applyCanonicalMetadataProperties(plannedTable, commitMetadata);
    if (skipMaterialization) {
      return new PreMaterializedTable(canonicalizedTable, null);
    }
    MaterializeMetadataResult result =
        materializationService.materializeMetadata(
            namespace,
            tableId,
            tableName,
            canonicalizedTable,
            commitMetadata,
            commitView.metadataLocation());
    if (result == null) {
      return new PreMaterializedTable(plannedTable, null);
    }
    if (result.error() != null) {
      return new PreMaterializedTable(plannedTable, result.error());
    }
    String location = result.metadataLocation();
    if (location == null || location.isBlank()) {
      return new PreMaterializedTable(canonicalizedTable, null);
    }
    return new PreMaterializedTable(
        canonicalizedTable.toBuilder().putProperties("metadata-location", location).build(), null);
  }

  private SnapshotChangePlan planAtomicSnapshotChanges(
      String accountId,
      String txId,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableGatewaySupport tableSupport,
      List<Map<String, Object>> updates,
      List<Long> removedSnapshotIds,
      boolean alreadyApplied) {
    if (alreadyApplied || tableId == null) {
      return new SnapshotChangePlan(List.of(), null);
    }
    String resolvedAccountId = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccountId == null || resolvedAccountId.isBlank()) {
      return new SnapshotChangePlan(
          List.of(),
          IcebergErrorResponses.failure(
              "missing account id for snapshot atomic changes",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE));
    }
    ResourceId scopedTableId =
        resolvedAccountId.equals(tableId.getAccountId())
            ? tableId
            : tableId.toBuilder().setAccountId(resolvedAccountId).build();
    List<ai.floedb.floecat.transaction.rpc.TxChange> out = new ArrayList<>();
    if (updates != null && !updates.isEmpty()) {
      for (Map<String, Object> update : updates) {
        String action = CommitUpdateInspector.actionOf(update);
        if (!CommitUpdateInspector.ACTION_ADD_SNAPSHOT.equals(action)) {
          continue;
        }
        Map<String, Object> snapshotMap = TableMappingUtil.asObjectMap(update.get("snapshot"));
        if (snapshotMap == null || snapshotMap.isEmpty()) {
          return new SnapshotChangePlan(
              List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot"));
        }
        Long snapshotId = TableMappingUtil.asLong(snapshotMap.get("snapshot-id"));
        if (snapshotId == null || snapshotId < 0) {
          return new SnapshotChangePlan(
              List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot-id"));
        }

        ai.floedb.floecat.catalog.rpc.Snapshot snapshot;
        try {
          snapshot =
              fetchOrBuildSnapshotPayload(
                  scopedTableId, table, tableSupport, snapshotId, snapshotMap);
        } catch (StatusRuntimeException e) {
          return new SnapshotChangePlan(List.of(), mapPreCommitFailure(e));
        } catch (RuntimeException e) {
          return new SnapshotChangePlan(
              List.of(),
              IcebergErrorResponses.failure(
                  "failed to fetch or build snapshot payload",
                  "CommitStateUnknownException",
                  Response.Status.SERVICE_UNAVAILABLE));
        }

        long upstreamCreatedMs =
            snapshot.hasUpstreamCreatedAt()
                ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
                : clockMillis();
        String byIdKey =
            Keys.snapshotPointerById(
                resolvedAccountId, scopedTableId.getId(), snapshot.getSnapshotId());
        String byTimeKey =
            Keys.snapshotPointerByTime(
                resolvedAccountId,
                scopedTableId.getId(),
                snapshot.getSnapshotId(),
                upstreamCreatedMs);
        ByteString payload = ByteString.copyFrom(snapshot.toByteArray());
        out.add(
            ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
                .setTargetPointerKey(byIdKey)
                .setPayload(payload)
                .build());
        out.add(
            ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
                .setTargetPointerKey(byTimeKey)
                .setPayload(payload)
                .build());
      }
    }
    if (removedSnapshotIds != null && !removedSnapshotIds.isEmpty()) {
      for (Long snapshotId : removedSnapshotIds) {
        if (snapshotId == null || snapshotId < 0) {
          continue;
        }
        ai.floedb.floecat.catalog.rpc.Snapshot snapshot;
        try {
          var resp =
              grpcClient.getSnapshot(
                  ai.floedb.floecat.catalog.rpc.GetSnapshotRequest.newBuilder()
                      .setTableId(scopedTableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                      .build());
          if (resp == null || !resp.hasSnapshot()) {
            continue;
          }
          snapshot = resp.getSnapshot();
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            continue;
          }
          return new SnapshotChangePlan(List.of(), mapPreCommitFailure(e));
        } catch (RuntimeException e) {
          return new SnapshotChangePlan(
              List.of(),
              IcebergErrorResponses.failure(
                  "failed to load snapshot payload for transactional delete",
                  "CommitStateUnknownException",
                  Response.Status.SERVICE_UNAVAILABLE));
        }
        long upstreamCreatedMs =
            snapshot.hasUpstreamCreatedAt()
                ? Timestamps.toMillis(snapshot.getUpstreamCreatedAt())
                : clockMillis();
        out.add(
            snapshotDeleteChange(
                resolvedAccountId,
                txId,
                scopedTableId.getId(),
                snapshot.getSnapshotId(),
                upstreamCreatedMs,
                true));
        out.add(
            snapshotDeleteChange(
                resolvedAccountId,
                txId,
                scopedTableId.getId(),
                snapshot.getSnapshotId(),
                upstreamCreatedMs,
                false));
      }
    }
    return new SnapshotChangePlan(out, null);
  }

  private ai.floedb.floecat.transaction.rpc.TxChange snapshotDeleteChange(
      String accountId,
      String txId,
      String tableId,
      long snapshotId,
      long upstreamCreatedMs,
      boolean byId) {
    String targetPointerKey =
        byId
            ? Keys.snapshotPointerById(accountId, tableId, snapshotId)
            : Keys.snapshotPointerByTime(accountId, tableId, snapshotId, upstreamCreatedMs);
    return ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
        .setTargetPointerKey(targetPointerKey)
        .setIntendedBlobUri(Keys.transactionDeleteSentinelUri(accountId, txId, targetPointerKey))
        .build();
  }

  private ai.floedb.floecat.catalog.rpc.Snapshot fetchOrBuildSnapshotPayload(
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Map<String, Object> snapshotMap) {
    try {
      var resp =
          grpcClient.getSnapshot(
              ai.floedb.floecat.catalog.rpc.GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                  .build());
      if (resp != null && resp.hasSnapshot()) {
        ai.floedb.floecat.catalog.rpc.Snapshot existing = resp.getSnapshot();
        if (!existing.getFormatMetadataMap().containsKey(ICEBERG_METADATA_KEY)) {
          IcebergMetadata snapshotIcebergMetadata =
              buildSnapshotIcebergMetadata(
                  table, tableSupport, snapshotId, existing.getSequenceNumber());
          if (snapshotIcebergMetadata != null) {
            return existing.toBuilder()
                .putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString())
                .build();
          }
        }
        return existing;
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }

    ai.floedb.floecat.catalog.rpc.Snapshot.Builder builder =
        ai.floedb.floecat.catalog.rpc.Snapshot.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(snapshotId);
    Long upstreamCreated = TableMappingUtil.asLong(snapshotMap.get("timestamp-ms"));
    if (upstreamCreated != null && upstreamCreated > 0) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreated));
    }
    Long parentId = TableMappingUtil.asLong(snapshotMap.get("parent-snapshot-id"));
    if (parentId != null) {
      builder.setParentSnapshotId(parentId);
    }
    Long sequenceNumber = TableMappingUtil.asLong(snapshotMap.get("sequence-number"));
    if (sequenceNumber != null && sequenceNumber > 0) {
      builder.setSequenceNumber(sequenceNumber);
    }
    String manifestList = TableMappingUtil.asString(snapshotMap.get("manifest-list"));
    if (manifestList != null && !manifestList.isBlank()) {
      builder.setManifestList(manifestList);
    }
    Integer schemaId = TableMappingUtil.asInteger(snapshotMap.get("schema-id"));
    if (schemaId != null && schemaId >= 0) {
      builder.setSchemaId(schemaId);
    }
    String schemaJson = TableMappingUtil.asString(snapshotMap.get("schema-json"));
    if (schemaJson != null && !schemaJson.isBlank()) {
      builder.setSchemaJson(schemaJson);
    }
    Map<String, String> summary = asStringMap(snapshotMap.get("summary"));
    String operation = TableMappingUtil.asString(snapshotMap.get("operation"));
    if (operation != null && !operation.isBlank() && !summary.containsKey("operation")) {
      summary = new LinkedHashMap<>(summary);
      summary.put("operation", operation);
    }
    if (!summary.isEmpty()) {
      builder.putAllSummary(summary);
    }
    IcebergMetadata snapshotIcebergMetadata =
        buildSnapshotIcebergMetadata(table, tableSupport, snapshotId, sequenceNumber);
    if (snapshotIcebergMetadata != null) {
      builder.putFormatMetadata(ICEBERG_METADATA_KEY, snapshotIcebergMetadata.toByteString());
    }
    if (!builder.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(clockMillis()));
    }
    return builder.build();
  }

  private IcebergMetadata buildSnapshotIcebergMetadata(
      ai.floedb.floecat.catalog.rpc.Table table,
      TableGatewaySupport tableSupport,
      long snapshotId,
      Long sequenceNumber) {
    IcebergMetadata base = null;
    if (tableSupport != null && table != null) {
      try {
        base = tableSupport.loadCurrentMetadata(table);
      } catch (RuntimeException e) {
        LOG.debugf(
            e,
            "Unable to load current metadata for snapshot format metadata tableId=%s",
            table.getResourceId().getId());
      }
    }
    IcebergMetadata.Builder builder =
        base != null ? base.toBuilder() : IcebergMetadata.newBuilder();
    if (table != null) {
      Map<String, String> props = table.getPropertiesMap();
      TableMetadataView metadata = tablePropertyService.metadataFromProperties(props);
      Integer formatVersion = metadata.formatVersion();
      if (formatVersion != null && formatVersion > 0) {
        builder.setFormatVersion(formatVersion);
      }
      String metadataLocation = metadata.metadataLocation();
      if (metadataLocation != null && !metadataLocation.isBlank()) {
        builder.setMetadataLocation(metadataLocation);
      }
      String tableUuid = metadata.tableUuid();
      if (tableUuid != null && !tableUuid.isBlank()) {
        builder.setTableUuid(tableUuid);
      }
      Integer lastColumnId = metadata.lastColumnId();
      if (lastColumnId != null && lastColumnId >= 0) {
        builder.setLastColumnId(lastColumnId);
      }
      Integer currentSchemaId = metadata.currentSchemaId();
      if (currentSchemaId != null && currentSchemaId >= 0) {
        builder.setCurrentSchemaId(currentSchemaId);
      }
      Integer defaultSpecId = metadata.defaultSpecId();
      if (defaultSpecId != null && defaultSpecId >= 0) {
        builder.setDefaultSpecId(defaultSpecId);
      }
      Integer lastPartitionId = metadata.lastPartitionId();
      if (lastPartitionId != null && lastPartitionId >= 0) {
        builder.setLastPartitionId(lastPartitionId);
      }
      Integer defaultSortOrderId = metadata.defaultSortOrderId();
      if (defaultSortOrderId != null && defaultSortOrderId >= 0) {
        builder.setDefaultSortOrderId(defaultSortOrderId);
      }
      Long lastSequenceNumber = metadata.lastSequenceNumber();
      if (lastSequenceNumber != null && lastSequenceNumber > 0) {
        builder.setLastSequenceNumber(lastSequenceNumber);
      }
      Map<String, IcebergRef> refs = decodePropertyRefs(props.get(RefPropertyUtil.PROPERTY_KEY));
      if (!refs.isEmpty()) {
        builder.clearRefs();
        builder.putAllRefs(refs);
      }
    }
    builder.setCurrentSnapshotId(snapshotId);
    if (sequenceNumber != null && sequenceNumber > 0) {
      builder.setLastSequenceNumber(sequenceNumber);
    }
    return builder.getFormatVersion() > 0 ? builder.build() : null;
  }

  private Map<String, IcebergRef> decodePropertyRefs(String encodedRefs) {
    if (encodedRefs == null || encodedRefs.isBlank()) {
      return Map.of();
    }
    Map<String, Map<String, Object>> decoded = RefPropertyUtil.decode(encodedRefs);
    if (decoded.isEmpty()) {
      return Map.of();
    }
    Map<String, IcebergRef> refs = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, Object>> entry : decoded.entrySet()) {
      if (entry == null || entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      Long snapshotId = TableMappingUtil.asLong(entry.getValue().get("snapshot-id"));
      if (snapshotId == null || snapshotId <= 0) {
        continue;
      }
      IcebergRef.Builder ref = IcebergRef.newBuilder().setSnapshotId(snapshotId);
      String type = TableMappingUtil.asString(entry.getValue().get("type"));
      if (type != null && !type.isBlank()) {
        ref.setType(type);
      }
      Long maxRefAgeMs = TableMappingUtil.asLong(entry.getValue().get("max-ref-age-ms"));
      if (maxRefAgeMs != null && maxRefAgeMs >= 0) {
        ref.setMaxReferenceAgeMs(maxRefAgeMs);
      }
      Long maxSnapshotAgeMs = TableMappingUtil.asLong(entry.getValue().get("max-snapshot-age-ms"));
      if (maxSnapshotAgeMs != null && maxSnapshotAgeMs >= 0) {
        ref.setMaxSnapshotAgeMs(maxSnapshotAgeMs);
      }
      Integer minSnapshotsToKeep =
          TableMappingUtil.asInteger(entry.getValue().get("min-snapshots-to-keep"));
      if (minSnapshotsToKeep != null && minSnapshotsToKeep >= 0) {
        ref.setMinSnapshotsToKeep(minSnapshotsToKeep);
      }
      refs.put(entry.getKey(), ref.build());
    }
    return refs.isEmpty() ? Map.of() : Map.copyOf(refs);
  }

  private ResourceId scopeTableIdWithAccount(ResourceId tableId, String accountId) {
    if (tableId == null) {
      return null;
    }
    String resolvedAccount = firstNonBlank(tableId.getAccountId(), accountId);
    if (resolvedAccount == null || resolvedAccount.isBlank()) {
      return tableId;
    }
    if (resolvedAccount.equals(tableId.getAccountId())) {
      return tableId;
    }
    return tableId.toBuilder().setAccountId(resolvedAccount).build();
  }

  private long clockMillis() {
    return System.currentTimeMillis();
  }

  private IcebergCommitJournalEntry buildCommitJournalEntry(
      String txId,
      String requestHash,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      List<Long> addedSnapshotIds,
      List<Long> removedSnapshotIds,
      ai.floedb.floecat.catalog.rpc.Table table,
      long createdAtMs) {
    IcebergCommitJournalEntry.Builder builder =
        IcebergCommitJournalEntry.newBuilder()
            .setVersion(COMMIT_JOURNAL_VERSION)
            .setTxId(txId == null ? "" : txId)
            .setRequestHash(requestHash == null ? "" : requestHash)
            .setCreatedAtMs(Math.max(0L, createdAtMs));
    if (tableId != null) {
      builder.setTableId(tableId);
    }
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.addAllNamespacePath(namespacePath);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setTableName(tableName);
    }
    if (connectorId != null && !connectorId.getId().isBlank()) {
      builder.setConnectorId(connectorId);
    }
    if (addedSnapshotIds != null && !addedSnapshotIds.isEmpty()) {
      builder.addAllAddedSnapshotIds(addedSnapshotIds);
    }
    if (removedSnapshotIds != null && !removedSnapshotIds.isEmpty()) {
      builder.addAllRemovedSnapshotIds(removedSnapshotIds);
    }
    String metadataLocation = tableMetadataLocation(table);
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      builder.setMetadataLocation(metadataLocation);
    }
    String tableUuid = tableUuid(table);
    if (tableUuid != null && !tableUuid.isBlank()) {
      builder.setTableUuid(tableUuid);
    }
    return builder.build();
  }

  private IcebergCommitOutboxEntry buildCommitOutboxEntry(
      String txId, String requestHash, String accountId, String tableId, long createdAtMs) {
    return IcebergCommitOutboxEntry.newBuilder()
        .setVersion(COMMIT_OUTBOX_VERSION)
        .setTxId(txId == null ? "" : txId)
        .setRequestHash(requestHash == null ? "" : requestHash)
        .setAccountId(accountId == null ? "" : accountId)
        .setTableId(tableId == null ? "" : tableId)
        .setCreatedAtMs(Math.max(0L, createdAtMs))
        .setAttemptCount(0)
        .setNextAttemptAtMs(0L)
        .setLastAttemptAtMs(0L)
        .setDeadLetteredAtMs(0L)
        .build();
  }

  private String firstDuplicateTableIdentifier(List<TransactionCommitRequest.TableChange> changes) {
    if (changes == null || changes.isEmpty()) {
      return null;
    }
    Set<String> seen = new java.util.LinkedHashSet<>();
    for (TransactionCommitRequest.TableChange change : changes) {
      if (change == null || change.identifier() == null) {
        continue;
      }
      String name = change.identifier().name();
      if (name == null || name.isBlank()) {
        continue;
      }
      List<String> namespacePath =
          change.identifier().namespace() == null
              ? List.of()
              : List.copyOf(change.identifier().namespace());
      String qualifiedName =
          namespacePath.isEmpty() ? name : String.join(".", namespacePath) + "." + name;
      if (!seen.add(qualifiedName)) {
        return qualifiedName;
      }
    }
    return null;
  }

  private String requestHash(List<TransactionCommitRequest.TableChange> changes) {
    List<Map<String, Object>> normalized = new ArrayList<>();
    if (changes != null) {
      for (TransactionCommitRequest.TableChange change : changes) {
        if (change == null) {
          normalized.add(Map.of());
          continue;
        }
        Map<String, Object> entry = new LinkedHashMap<>();
        var identifier = change.identifier();
        if (identifier != null) {
          Map<String, Object> idMap = new LinkedHashMap<>();
          idMap.put(
              "namespace",
              identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace()));
          idMap.put("name", identifier.name());
          entry.put("identifier", idMap);
        } else {
          entry.put("identifier", null);
        }
        entry.put(
            "requirements", change.requirements() == null ? List.of() : change.requirements());
        entry.put("updates", change.updates() == null ? List.of() : change.updates());
        normalized.add(entry);
      }
    }
    String canonical = canonicalize(normalized);
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map<?, ?> map) {
      List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
      entries.sort(java.util.Comparator.comparing(entry -> String.valueOf(entry.getKey())));
      StringBuilder out = new StringBuilder("{");
      boolean first = true;
      for (Map.Entry<?, ?> entry : entries) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(escapeJsonString(String.valueOf(entry.getKey())))
            .append(':')
            .append(canonicalize(entry.getValue()));
      }
      return out.append('}').toString();
    }
    if (value instanceof List<?> list) {
      StringBuilder out = new StringBuilder("[");
      boolean first = true;
      for (Object entry : list) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(canonicalize(entry));
      }
      return out.append(']').toString();
    }
    if (value instanceof String str) {
      return escapeJsonString(str);
    }
    if (value instanceof Number || value instanceof Boolean) {
      return String.valueOf(value);
    }
    return escapeJsonString(String.valueOf(value));
  }

  private String escapeJsonString(String input) {
    String value = input == null ? "" : input;
    StringBuilder out = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      switch (ch) {
        case '\\' -> out.append("\\\\");
        case '"' -> out.append("\\\"");
        case '\b' -> out.append("\\b");
        case '\f' -> out.append("\\f");
        case '\n' -> out.append("\\n");
        case '\r' -> out.append("\\r");
        case '\t' -> out.append("\\t");
        default -> {
          if (ch < 0x20) {
            out.append(String.format("\\u%04x", (int) ch));
          } else {
            out.append(ch);
          }
        }
      }
    }
    return out.append('"').toString();
  }

  private boolean isCommitAccepted(TransactionState state) {
    return state == TransactionState.TS_APPLIED;
  }

  private boolean isDeterministicFailedState(TransactionState state) {
    return state == TransactionState.TS_APPLY_FAILED_CONFLICT
        || state == TransactionState.TS_ABORTED;
  }

  private boolean isDeterministicCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return false;
      }
      if (detailCode == ErrorCode.MC_CONFLICT || detailCode == ErrorCode.MC_PRECONDITION_FAILED) {
        return true;
      }
      return true;
    }
    return code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS;
  }

  private boolean isRetryableCommitAbort(StatusRuntimeException failure) {
    if (failure.getStatus().getCode() != Status.Code.ABORTED) {
      return false;
    }
    return extractFloecatErrorCode(failure) == ErrorCode.MC_ABORT_RETRYABLE;
  }

  private ErrorCode extractFloecatErrorCode(StatusRuntimeException exception) {
    var statusProto = StatusProto.fromThrowable(exception);
    if (statusProto == null) {
      return null;
    }
    for (com.google.protobuf.Any detail : statusProto.getDetailsList()) {
      if (!detail.is(Error.class)) {
        continue;
      }
      try {
        return detail.unpack(Error.class).getCode();
      } catch (Exception ignored) {
        // Continue scanning details.
      }
    }
    return null;
  }

  private boolean waitForAppliedState(String txId) {
    if (txId == null || txId.isBlank()) {
      return false;
    }
    int maxAttempts =
        Math.max(
            1,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-attempts", Integer.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_ATTEMPTS));
    long sleepMillis =
        Math.max(
            0L,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.initial-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_INITIAL_SLEEP_MS));
    long maxSleepMillis =
        Math.max(
            sleepMillis,
            ConfigProvider.getConfig()
                .getOptionalValue("floecat.gateway.commit.confirm.max-sleep-ms", Long.class)
                .orElse(DEFAULT_COMMIT_CONFIRM_MAX_SLEEP_MS));

    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        var current =
            grpcClient.getTransaction(GetTransactionRequest.newBuilder().setTxId(txId).build());
        if (current != null && current.hasTransaction()) {
          TransactionState state = current.getTransaction().getState();
          if (state == TransactionState.TS_APPLIED) {
            return true;
          }
          if (state == TransactionState.TS_APPLY_FAILED_CONFLICT
              || state == TransactionState.TS_ABORTED) {
            return false;
          }
        }
      } catch (RuntimeException ignored) {
        return false;
      }
      if (attempt + 1 < maxAttempts && sleepMillis > 0L) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
        sleepMillis = Math.min(maxSleepMillis, Math.max(1L, sleepMillis * 2L));
      }
    }
    return false;
  }

  private boolean shouldConfirmAmbiguousCommitState(TransactionState state) {
    return CONFIRMABLE_COMMIT_STATES.contains(state);
  }

  private Response mapPreCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.NOT_FOUND) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "NoSuchTableException", Response.Status.NOT_FOUND);
    }
    if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.ALREADY_EXISTS) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    if (code == Status.Code.ABORTED) {
      ErrorCode detailCode = extractFloecatErrorCode(failure);
      if (detailCode == ErrorCode.MC_ABORT_RETRYABLE) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.SERVICE_UNAVAILABLE);
      }
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    Response mapped = mapCommitFailureByStatus(code);
    return mapped == null ? preCommitStateUnknown() : mapped;
  }

  private Response mapCommitFailureByStatus(Status.Code code) {
    if (code == Status.Code.INVALID_ARGUMENT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
    }
    if (code == Status.Code.UNAVAILABLE) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.SERVICE_UNAVAILABLE);
    }
    if (code == Status.Code.UNAUTHENTICATED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "UnauthorizedException", Response.Status.UNAUTHORIZED);
    }
    if (code == Status.Code.PERMISSION_DENIED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ForbiddenException", Response.Status.FORBIDDEN);
    }
    if (code == Status.Code.UNKNOWN) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "CommitStateUnknownException", Response.Status.BAD_GATEWAY);
    }
    if (code == Status.Code.DEADLINE_EXCEEDED) {
      return IcebergErrorResponses.failure(
          "transaction commit failed",
          "CommitStateUnknownException",
          Response.Status.GATEWAY_TIMEOUT);
    }
    return null;
  }

  private Response preCommitStateUnknown() {
    return IcebergErrorResponses.failure(
        "transaction commit failed",
        "CommitStateUnknownException",
        Response.Status.INTERNAL_SERVER_ERROR);
  }

  private void abortTransactionQuietly(String txId, String reason) {
    if (txId == null || txId.isBlank()) {
      return;
    }
    try {
      grpcClient.abortTransaction(
          ai.floedb.floecat.transaction.rpc.AbortTransactionRequest.newBuilder()
              .setTxId(txId)
              .setReason(reason == null ? "" : reason)
              .build());
    } catch (RuntimeException e) {
      LOG.debugf(e, "Best-effort abort failed for tx=%s", txId);
    }
  }

  private void maybeAbortOpenTransaction(
      TransactionState currentState, String txId, String reason) {
    if (currentState == TransactionState.TS_OPEN) {
      abortTransactionQuietly(txId, reason);
    }
  }

  private Response validateKnownRequirementTypes(List<Map<String, Object>> requirements) {
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return IcebergErrorResponses.validation("commit requirement entry cannot be null");
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (type == null || type.isBlank()) {
        return IcebergErrorResponses.validation("commit requirement missing type");
      }
      if (!CommitUpdateInspector.isSupportedRequirementType(type)) {
        return IcebergErrorResponses.validation("unsupported commit requirement: " + type);
      }
    }
    return null;
  }

  private Response validateAssertCreateRequirement(
      List<Map<String, Object>> requirements,
      ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse) {
    if (!hasRequirementType(requirements, CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE)) {
      return null;
    }
    if (tableResponse != null && tableResponse.hasTable()) {
      return IcebergErrorResponses.failure(
          "assert-create failed", "CommitFailedException", Response.Status.CONFLICT);
    }
    return null;
  }

  private Response validateKnownUpdateActions(List<Map<String, Object>> updates) {
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      Object actionObj = update.get("action");
      String action = actionObj instanceof String value ? value : null;
      if (action == null || action.isBlank()) {
        return IcebergErrorResponses.validation("unsupported commit update action: <missing>");
      }
      if (!CommitUpdateInspector.isSupportedUpdateAction(action)) {
        return IcebergErrorResponses.validation("unsupported commit update action: " + action);
      }
    }
    return null;
  }

  private boolean hasRequirementType(List<Map<String, Object>> requirements, String type) {
    if (requirements == null || requirements.isEmpty() || type == null || type.isBlank()) {
      return false;
    }
    for (Map<String, Object> requirement : requirements) {
      String requirementType =
          requirement == null ? null : requirement.get("type") instanceof String s ? s : null;
      if (type.equals(requirementType)) {
        return true;
      }
    }
    return false;
  }

  private ResourceId atomicCreateTableId(
      String accountId,
      String txId,
      ResourceId catalogId,
      ResourceId namespaceId,
      List<String> namespacePath,
      String tableName) {
    String catalogPart = catalogId == null ? "<catalog>" : catalogId.getId();
    String namespacePart =
        namespacePath == null || namespacePath.isEmpty()
            ? (namespaceId == null ? "<namespace>" : namespaceId.getId())
            : String.join(".", namespacePath);
    String seed =
        (txId == null ? "" : txId)
            + "|"
            + catalogPart
            + "|"
            + namespacePart
            + "|"
            + (tableName == null ? "" : tableName);
    UUID deterministicId = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8));
    return ResourceId.newBuilder()
        .setAccountId(accountId == null ? "" : accountId)
        .setId("tbl-" + deterministicId)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private ai.floedb.floecat.catalog.rpc.Table newCreateTableStub(
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId, String tableName) {
    ai.floedb.floecat.catalog.rpc.Table.Builder builder =
        ai.floedb.floecat.catalog.rpc.Table.newBuilder();
    if (tableId != null) {
      builder.setResourceId(tableId);
    }
    if (catalogId != null) {
      builder.setCatalogId(catalogId);
    }
    if (namespaceId != null) {
      builder.setNamespaceId(namespaceId);
    }
    if (tableName != null && !tableName.isBlank()) {
      builder.setDisplayName(tableName);
    }
    builder.setCreatedAt(Timestamps.fromMillis(System.currentTimeMillis()));
    builder.setUpstream(
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setColumnIdAlgorithm(ColumnIdAlgorithm.CID_FIELD_ID)
            .build());
    return builder.build();
  }

  private Response validateNullSnapshotRefRequirements(
      TableGatewaySupport tableSupport,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Map<String, Object>> requirements) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        continue;
      }
      Object typeObj = requirement.get("type");
      String type = typeObj instanceof String value ? value : null;
      if (!"assert-ref-snapshot-id".equals(type) || !requirement.containsKey("snapshot-id")) {
        continue;
      }
      if (requirement.get("snapshot-id") != null) {
        continue;
      }
      Object refObj = requirement.get("ref");
      String refName = refObj instanceof String value ? value : null;
      if (refName == null || refName.isBlank()) {
        return IcebergErrorResponses.validation("assert-ref-snapshot-id requires ref");
      }
      if (hasSnapshotRef(tableSupport, table, refName)) {
        return IcebergErrorResponses.failure(
            "assert-ref-snapshot-id failed for ref " + refName,
            "CommitFailedException",
            Response.Status.CONFLICT);
      }
    }
    return null;
  }

  private boolean hasSnapshotRef(
      TableGatewaySupport tableSupport, ai.floedb.floecat.catalog.rpc.Table table, String refName) {
    if (refName == null || refName.isBlank()) {
      return false;
    }
    try {
      var metadata = tableSupport == null ? null : tableSupport.loadCurrentMetadata(table);
      if (metadata != null) {
        if (metadata.getRefsMap().containsKey(refName)
            && metadata.getRefsOrThrow(refName).getSnapshotId() >= 0L) {
          return true;
        }
        // currentSnapshotId defaults to 0 in proto3 when unset; only positive values are
        // unambiguous here. Explicit version 0 is handled via refs/properties.
        if ("main".equals(refName) && metadata.getCurrentSnapshotId() > 0L) {
          return true;
        }
      }
    } catch (RuntimeException ignored) {
      // Fall back to table properties.
    }
    if (table == null) {
      return false;
    }
    if ("main".equals(refName)
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) != null
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) >= 0L) {
      return true;
    }
    String encodedRefs = table.getPropertiesMap().get(RefPropertyUtil.PROPERTY_KEY);
    if (encodedRefs == null || encodedRefs.isBlank()) {
      return false;
    }
    Map<String, Map<String, Object>> refs = RefPropertyUtil.decode(encodedRefs);
    if (!refs.containsKey(refName)) {
      return false;
    }
    Long snapshotId = TableMappingUtil.asLong(refs.get(refName).get("snapshot-id"));
    return snapshotId != null && snapshotId >= 0L;
  }

  private String tableMetadataLocation(ai.floedb.floecat.catalog.rpc.Table table) {
    return table == null ? null : table.getPropertiesMap().get("metadata-location");
  }

  private String tableUuid(ai.floedb.floecat.catalog.rpc.Table table) {
    return table == null ? null : table.getPropertiesMap().get("table-uuid");
  }
}
