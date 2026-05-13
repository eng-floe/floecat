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

package ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction;

import ai.floedb.floecat.common.rpc.Precondition;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.CatalogRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.ResourceResolver;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.ConnectorProvisioningService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.TableCreateTransactionMapper;
import ai.floedb.floecat.transaction.rpc.TransactionState;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class TransactionCommitService {
  static final String TX_REQUEST_HASH_PROPERTY = "iceberg.commit.request-hash";
  @Inject AccountContext accountContext;
  @Inject ResourceResolver resourceResolver;
  @Inject TransactionCommitExecutionSupport transactionCommitExecutionSupport;
  @Inject TransactionCommitTablePlanningSupport tablePlanningSupport;
  @Inject TransactionCommitSnapshotSupport snapshotSupport;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCreateTransactionMapper tableCreateTransactionMapper;
  @Inject ConnectorProvisioningService connectorProvisioningService;

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
        false,
        true);
  }

  public Response commit(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    return commitInternal(prefix, idempotencyKey, request, tableSupport, true, true);
  }

  public Response commitRegister(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    return commitInternal(prefix, idempotencyKey, request, tableSupport, false, true);
  }

  private Response commitInternal(
      String prefix,
      String idempotencyKey,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport,
      boolean preMaterializeAssertCreate,
      boolean allowGeneratedBeginIdempotency) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String duplicateIdentifier =
        TransactionCommitRequestSupport.firstDuplicateTableIdentifier(changes);
    if (duplicateIdentifier != null) {
      return IcebergErrorResponses.validation(
          "duplicate table identifier in table-changes: " + duplicateIdentifier);
    }
    CatalogRef catalogContext = resourceResolver.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();

    String requestHash = TransactionCommitRequestSupport.requestHash(changes);
    TransactionCommitExecutionSupport.OpenTransactionResult openResult =
        transactionCommitExecutionSupport.openTransaction(
            idempotencyKey, allowGeneratedBeginIdempotency, catalogName, requestHash);
    if (openResult.error() != null) {
      return openResult.error();
    }
    TransactionCommitExecutionSupport.OpenTransaction openTransaction = openResult.transaction();
    String txId = openTransaction.txId();
    TransactionState currentState = openTransaction.currentState();
    if (currentState == TransactionState.TS_APPLIED) {
      return Response.noContent().build();
    }
    String idempotencyBase = openTransaction.idempotencyBase();
    List<ai.floedb.floecat.transaction.rpc.TxChange> txChanges = new ArrayList<>();
    boolean reusePreparedTransaction =
        currentState == TransactionState.TS_PREPARED
            || currentState == TransactionState.TS_APPLYING
            || currentState == TransactionState.TS_APPLY_FAILED_RETRYABLE;
    List<PlannedChange> planned = new ArrayList<>();
    if (!reusePreparedTransaction) {
      for (TransactionCommitRequest.TableChange change : changes) {
        try {
          var identifier = change.identifier();
          if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "table identifier is missing");
            return IcebergErrorResponses.validation("table identifier is required");
          }
          if (change.requirements() == null) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "requirements are missing");
            return IcebergErrorResponses.validation("requirements are required");
          }
          if (change.updates() == null) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "updates are missing");
            return IcebergErrorResponses.validation("updates are required");
          }
          boolean assertCreateRequested =
              TransactionCommitRequestSupport.hasRequirementType(
                  change.requirements(), CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE);
          Response requirementTypeError =
              TransactionCommitRequestSupport.validateKnownRequirementTypes(change.requirements());
          if (requirementTypeError != null) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "requirements include unknown type");
            return requirementTypeError;
          }
          Response updateTypeError =
              TransactionCommitRequestSupport.validateKnownUpdateActions(change.updates());
          if (updateTypeError != null) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "updates include unknown action");
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
                tablePlanningSupport.reserveCreateTableId(
                    txId, namespacePath, catalogName, identifier.name());
            tableResponse = null;
          }
          Response assertCreateError =
              TransactionCommitRequestSupport.validateAssertCreateRequirement(
                  change.requirements(), tableResponse);
          if (assertCreateError != null) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "assert-create requirement failed");
            return assertCreateError;
          }
          TransactionCommitTablePlanningSupport.PlannedExistingTableChange plannedChangeResult =
              tablePlanningSupport.planExistingTableChange(
                  currentState,
                  txId,
                  command,
                  tableId,
                  catalogId,
                  namespaceId,
                  namespace,
                  identifier.name(),
                  tableResponse,
                  change.requirements(),
                  change.updates(),
                  tableSupport,
                  preMaterializeAssertCreate);
          if (plannedChangeResult.error() != null) {
            return plannedChangeResult.error();
          }
          planned.add(
              new PlannedChange(
                  namespacePath,
                  namespaceId,
                  identifier.name(),
                  tableId,
                  plannedChangeResult.table(),
                  plannedChangeResult.metadataLocation(),
                  change.updates() == null ? List.of() : List.copyOf(change.updates()),
                  plannedChangeResult.pointerVersion()));
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
            transactionCommitExecutionSupport.abortIfOpen(
                currentState, txId, "table not found during planning");
            return IcebergErrorResponses.noSuchTable(
                "Table not found while planning transaction change");
          }
          transactionCommitExecutionSupport.abortIfOpen(
              currentState, txId, "transaction planning failed");
          throw e;
        } catch (RuntimeException e) {
          transactionCommitExecutionSupport.abortIfOpen(
              currentState, txId, "transaction planning failed");
          throw e;
        }
      }
    }

    for (var plan : planned) {
      var tableForTx = tablePlanningSupport.normalizeTableIdentity(plan.table(), plan.tableId());
      ResourceId scopedTableId = scopeTableIdWithAccount(plan.tableId(), accountId);
      ConnectorProvisioningService.ProvisionResult connectorResolution =
          connectorProvisioningService.resolveOrCreateForCommit(
              prefix,
              tableSupport,
              plan.namespacePath(),
              plan.tableName(),
              scopedTableId,
              tableForTx);
      if (connectorResolution.error() != null) {
        transactionCommitExecutionSupport.abortIfOpen(
            currentState, txId, "connector provisioning failed");
        return connectorResolution.error();
      }
      tableForTx =
          tablePlanningSupport.normalizeTableIdentity(connectorResolution.table(), plan.tableId());
      if (!connectorResolution.connectorTxChanges().isEmpty()) {
        txChanges.addAll(connectorResolution.connectorTxChanges());
      }
      CommitUpdateInspector.Parsed parsedUpdates =
          CommitUpdateInspector.inspectUpdates(plan.updates());
      List<Long> removedSnapshotIds = parsedUpdates.removedSnapshotIds();
      txChanges.add(
          ai.floedb.floecat.transaction.rpc.TxChange.newBuilder()
              .setTableId(plan.tableId())
              .setTable(tableForTx)
              .setPrecondition(Precondition.newBuilder().setExpectedVersion(plan.expectedVersion()))
              .build());

      TransactionCommitSnapshotSupport.SnapshotChangePlan snapshotChangePlan =
          snapshotSupport.planAtomicSnapshotChanges(
              accountId,
              txId,
              plan.tableId(),
              tableForTx,
              tableSupport,
              plan.metadataLocation(),
              plan.updates(),
              removedSnapshotIds);
      if (snapshotChangePlan.error() != null) {
        transactionCommitExecutionSupport.abortIfOpen(
            currentState, txId, "snapshot metadata planning failed");
        return snapshotChangePlan.error();
      }
      if (!snapshotChangePlan.txChanges().isEmpty()) {
        txChanges.addAll(snapshotChangePlan.txChanges());
      }
    }

    return transactionCommitExecutionSupport.apply(openTransaction, txChanges);
  }

  private record PlannedChange(
      List<String> namespacePath,
      ResourceId namespaceId,
      String tableName,
      ResourceId tableId,
      ai.floedb.floecat.catalog.rpc.Table table,
      String metadataLocation,
      List<Map<String, Object>> updates,
      long expectedVersion) {}

  private ResourceId scopeTableIdWithAccount(ResourceId tableId, String accountId) {
    if (tableId == null) {
      return null;
    }
    if (tableId.getAccountId() == null || tableId.getAccountId().isBlank()) {
      throw new IllegalStateException("table_id.account_id must be set");
    }
    if (accountId != null && !accountId.isBlank() && !accountId.equals(tableId.getAccountId())) {
      throw new IllegalStateException("table_id.account_id mismatch with request account");
    }
    return tableId;
  }

  private String requestHash(List<TransactionCommitRequest.TableChange> changes) {
    return TransactionCommitRequestSupport.requestHash(changes);
  }

  private String canonicalize(Object value) {
    return TransactionCommitRequestSupport.canonicalizeForTests(value);
  }
}
