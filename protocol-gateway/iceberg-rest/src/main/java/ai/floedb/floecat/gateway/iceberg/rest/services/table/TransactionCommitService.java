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

import ai.floedb.floecat.common.rpc.Error;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.common.RefPropertyUtil;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableMappingUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TransactionClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
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
import java.net.URLEncoder;
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
import org.jboss.logging.Logger;

@ApplicationScoped
public class TransactionCommitService {
  private static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  private static final Set<String> SUPPORTED_REQUIREMENT_TYPES =
      Set.of(
          "assert-create",
          "assert-table-uuid",
          "assert-current-schema-id",
          "assert-last-assigned-field-id",
          "assert-last-assigned-partition-id",
          "assert-default-spec-id",
          "assert-default-sort-order-id",
          "assert-ref-snapshot-id");
  private static final Set<String> SUPPORTED_UPDATE_ACTIONS =
      Set.of(
          "set-properties",
          "remove-properties",
          "set-location",
          "add-snapshot",
          "remove-snapshots",
          "set-snapshot-ref",
          "remove-snapshot-ref",
          "assign-uuid",
          "upgrade-format-version",
          "add-schema",
          "set-current-schema",
          "add-spec",
          "set-default-spec",
          "add-sort-order",
          "set-default-sort-order",
          "remove-partition-specs",
          "remove-schemas",
          "set-statistics",
          "remove-statistics",
          "set-partition-statistics",
          "remove-partition-statistics",
          "add-encryption-key",
          "remove-encryption-key");
  private static final Logger LOG = Logger.getLogger(TransactionCommitService.class);
  @Inject AccountContext accountContext;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitPlanner tableCommitPlanner;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject TableCommitMaterializationService materializationService;
  @Inject SnapshotClient snapshotClient;
  @Inject TransactionClient transactionClient;

  public Response commit(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
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
          transactionClient.beginTransaction(
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
          transactionClient.getTransaction(
              GetTransactionRequest.newBuilder().setTxId(txId).build());
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
    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    List<SyncTarget> syncTargets = new ArrayList<>();

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
        boolean assertCreateRequested = hasRequirementType(change.requirements(), "assert-create");
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
          // Replay path: skip requirement re-evaluation and rebuild side effects from current table
          // state.
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
                  change.updates() == null ? List.of() : List.copyOf(change.updates()),
                  tableSupport);
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
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTableId(plan.tableId())
              .setTable(tableForTx)
              .setPrecondition(Precondition.newBuilder().setExpectedVersion(plan.expectedVersion()))
              .build());

      SnapshotChangePlan snapshotChangePlan =
          planAtomicSnapshotChanges(accountId, plan.tableId(), plan.updates(), alreadyApplied);
      if (snapshotChangePlan.error() != null) {
        maybeAbortOpenTransaction(currentState, txId, "snapshot metadata planning failed");
        return snapshotChangePlan.error();
      }
      if (!snapshotChangePlan.txChanges().isEmpty()) {
        txChanges.addAll(snapshotChangePlan.txChanges());
      }

      syncTargets.add(
          new SyncTarget(
              plan.namespacePath(),
              plan.tableName(),
              scopeTableIdWithAccount(plan.tableId(), accountId),
              tableForTx,
              removedSnapshotIds(plan.updates())));
    }

    if (!isCommitAccepted(currentState)) {
      if (currentState == TransactionState.TS_OPEN) {
        try {
          transactionClient.prepareTransaction(
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
        var commitResponse = transactionClient.commitTransaction(commitRequest.build());
        TransactionState commitState =
            commitResponse != null && commitResponse.hasTransaction()
                ? commitResponse.getTransaction().getState()
                : TransactionState.TS_UNSPECIFIED;
        if (!isCommitAccepted(commitState)) {
          if (isDeterministicFailedState(commitState)) {
            return IcebergErrorResponses.failure(
                "transaction commit did not reach applied state",
                "CommitFailedException",
                Response.Status.CONFLICT);
          }
          if (waitForAppliedState(txId)) {
            return Response.noContent().build();
          }
          return IcebergErrorResponses.failure(
              "transaction commit did not reach applied state",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE);
        }
      } catch (StatusRuntimeException commitFailure) {
        if (commitFailure.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
        }
        if (isDeterministicCommitFailure(commitFailure)) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "CommitFailedException", Response.Status.CONFLICT);
        }
        if (isRetryableCommitAbort(commitFailure)) {
          if (waitForAppliedState(txId)) {
            return Response.noContent().build();
          }
          return IcebergErrorResponses.failure(
              "transaction commit failed",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE);
        }
        if (commitFailure.getStatus().getCode() == Status.Code.UNAVAILABLE) {
          return IcebergErrorResponses.failure(
              "transaction commit failed",
              "CommitStateUnknownException",
              Response.Status.SERVICE_UNAVAILABLE);
        }
        if (commitFailure.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "UnauthorizedException", Response.Status.UNAUTHORIZED);
        }
        if (commitFailure.getStatus().getCode() == Status.Code.PERMISSION_DENIED) {
          return IcebergErrorResponses.failure(
              "transaction commit failed", "ForbiddenException", Response.Status.FORBIDDEN);
        }
        if (commitFailure.getStatus().getCode() == Status.Code.UNKNOWN) {
          return IcebergErrorResponses.failure(
              "transaction commit failed",
              "CommitStateUnknownException",
              Response.Status.BAD_GATEWAY);
        }
        if (commitFailure.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
          return IcebergErrorResponses.failure(
              "transaction commit failed",
              "CommitStateUnknownException",
              Response.Status.GATEWAY_TIMEOUT);
        }
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.INTERNAL_SERVER_ERROR);
      } catch (RuntimeException commitFailure) {
        return IcebergErrorResponses.failure(
            "transaction commit failed",
            "CommitStateUnknownException",
            Response.Status.INTERNAL_SERVER_ERROR);
      }
    }

    for (var target : syncTargets) {
      sideEffectService.pruneRemovedSnapshots(target.tableId(), target.removedSnapshotIds());
      try {
        sideEffectService.runPostCommitStatsSyncAttempt(
            tableSupport, target.namespacePath(), target.tableName(), target.table());
      } catch (RuntimeException e) {
        LOG.warnf(
            e,
            "Post-commit stats-only connector sync failed for %s.%s in tx; backend apply already"
                + " committed",
            String.join(".", target.namespacePath()),
            target.tableName());
      }
    }

    return Response.noContent().build();
  }

  private record SyncTarget(
      List<String> namespacePath,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      List<Long> removedSnapshotIds) {}

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
      List<Map<String, Object>> updates,
      TableGatewaySupport tableSupport) {
    if (plannedTable == null || tableSupport == null) {
      return new PreMaterializedTable(plannedTable, null);
    }
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
    MaterializeMetadataResult result =
        materializationService.materializeMetadata(
            namespace,
            tableId,
            tableName,
            plannedTable,
            commitView.metadata(),
            commitView.metadataLocation());
    if (result == null) {
      return new PreMaterializedTable(plannedTable, null);
    }
    if (result.error() != null) {
      return new PreMaterializedTable(plannedTable, result.error());
    }
    String location = result.metadataLocation();
    if (location == null || location.isBlank()) {
      return new PreMaterializedTable(plannedTable, null);
    }
    return new PreMaterializedTable(
        plannedTable.toBuilder().putProperties("metadata-location", location).build(), null);
  }

  private SnapshotChangePlan planAtomicSnapshotChanges(
      String accountId,
      ResourceId tableId,
      List<Map<String, Object>> updates,
      boolean alreadyApplied) {
    if (alreadyApplied || tableId == null || updates == null || updates.isEmpty()) {
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
    for (Map<String, Object> update : updates) {
      String action = TableMappingUtil.asString(update == null ? null : update.get("action"));
      if (!"add-snapshot".equals(action)) {
        continue;
      }
      Map<String, Object> snapshotMap = TableMappingUtil.asObjectMap(update.get("snapshot"));
      if (snapshotMap == null || snapshotMap.isEmpty()) {
        return new SnapshotChangePlan(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot"));
      }
      Long snapshotId = TableMappingUtil.asLong(snapshotMap.get("snapshot-id"));
      if (snapshotId == null || snapshotId <= 0) {
        return new SnapshotChangePlan(
            List.of(), IcebergErrorResponses.validation("add-snapshot requires snapshot-id"));
      }

      ai.floedb.floecat.catalog.rpc.Snapshot snapshot;
      try {
        snapshot = fetchOrBuildSnapshotPayload(scopedTableId, snapshotId, snapshotMap);
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
          snapshotPointerById(resolvedAccountId, scopedTableId.getId(), snapshot.getSnapshotId());
      String byTimeKey =
          snapshotPointerByTime(
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
    return new SnapshotChangePlan(out, null);
  }

  private ai.floedb.floecat.catalog.rpc.Snapshot fetchOrBuildSnapshotPayload(
      ResourceId tableId, long snapshotId, Map<String, Object> snapshotMap) {
    try {
      var resp =
          snapshotClient.getSnapshot(
              ai.floedb.floecat.catalog.rpc.GetSnapshotRequest.newBuilder()
                  .setTableId(tableId)
                  .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId).build())
                  .build());
      if (resp != null && resp.hasSnapshot()) {
        return resp.getSnapshot();
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
    if (parentId != null && parentId > 0) {
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
    if (!builder.hasUpstreamCreatedAt()) {
      builder.setUpstreamCreatedAt(Timestamps.fromMillis(clockMillis()));
    }
    return builder.build();
  }

  private Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> raw) || raw.isEmpty()) {
      return Map.of();
    }
    Map<String, String> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : raw.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      out.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
    }
    return out;
  }

  private List<Long> removedSnapshotIds(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return List.of();
    }
    List<Long> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = TableMappingUtil.asString(update == null ? null : update.get("action"));
      if (!"remove-snapshots".equals(action)) {
        continue;
      }
      Object raw = update.get("snapshot-ids");
      if (!(raw instanceof List<?> ids)) {
        continue;
      }
      for (Object id : ids) {
        Long value = TableMappingUtil.asLong(id);
        if (value != null && value > 0) {
          out.add(value);
        }
      }
    }
    return out.isEmpty() ? List.of() : List.copyOf(out);
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

  private String snapshotPointerById(String accountId, String tableId, long snapshotId) {
    return "/accounts/"
        + encodePathSegment(accountId)
        + "/tables/"
        + encodePathSegment(tableId)
        + "/snapshots/by-id/"
        + String.format("%019d", snapshotId);
  }

  private String snapshotPointerByTime(
      String accountId, String tableId, long snapshotId, long upstreamCreatedAtMs) {
    long inverted = Long.MAX_VALUE - Math.max(0L, upstreamCreatedAtMs);
    return "/accounts/"
        + encodePathSegment(accountId)
        + "/tables/"
        + encodePathSegment(tableId)
        + "/snapshots/by-time/"
        + String.format("%019d-%019d", inverted, snapshotId);
  }

  private String encodePathSegment(String value) {
    return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
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
              "namespace", identifier.namespace() == null ? List.of() : identifier.namespace());
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
      return Base64.getEncoder().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 not available", e);
    }
  }

  private String canonicalize(Object value) {
    if (value == null) {
      return "null";
    }
    if (value instanceof Map<?, ?> map) {
      List<String> keys = new ArrayList<>();
      for (Object key : map.keySet()) {
        keys.add(String.valueOf(key));
      }
      keys.sort(String::compareTo);
      StringBuilder out = new StringBuilder("{");
      boolean first = true;
      for (String key : keys) {
        if (!first) {
          out.append(',');
        }
        first = false;
        out.append(escapeJsonString(key)).append(':').append(canonicalize(map.get(key)));
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
    final int maxAttempts = 3;
    final long sleepMillis = 25L;
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        var current =
            transactionClient.getTransaction(
                GetTransactionRequest.newBuilder().setTxId(txId).build());
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
      if (attempt + 1 < maxAttempts) {
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }
    return false;
  }

  private Response mapPreCommitFailure(StatusRuntimeException failure) {
    Status.Code code = failure.getStatus().getCode();
    if (code == Status.Code.INVALID_ARGUMENT) {
      return IcebergErrorResponses.failure(
          "transaction commit failed", "ValidationException", Response.Status.BAD_REQUEST);
    }
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
    return preCommitStateUnknown();
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
      transactionClient.abortTransaction(
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
      if (!SUPPORTED_REQUIREMENT_TYPES.contains(type)) {
        return IcebergErrorResponses.validation("unsupported commit requirement: " + type);
      }
    }
    return null;
  }

  private Response validateAssertCreateRequirement(
      List<Map<String, Object>> requirements,
      ai.floedb.floecat.catalog.rpc.GetTableResponse tableResponse) {
    if (!hasRequirementType(requirements, "assert-create")) {
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
      if (!SUPPORTED_UPDATE_ACTIONS.contains(action)) {
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

  private ai.floedb.floecat.catalog.rpc.Table loadPersistedTableOrDefault(ResourceId tableId) {
    try {
      return tableLifecycleService.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return ai.floedb.floecat.catalog.rpc.Table.getDefaultInstance();
      }
      throw e;
    }
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
            && metadata.getRefsOrThrow(refName).getSnapshotId() > 0L) {
          return true;
        }
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
        && TableMappingUtil.asLong(table.getPropertiesMap().get("current-snapshot-id")) > 0L) {
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
    return snapshotId == null || snapshotId > 0L;
  }
}
