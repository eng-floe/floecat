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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.CommitOperationTracker.OperationKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;

@ApplicationScoped
public class TransactionCommitService {
  @Inject AccountContext accountContext;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject IcebergGatewayConfig config;
  @Inject GrpcWithHeaders grpc;
  @Inject CommitOperationTracker commitOperationTracker;

  public Response commit(
      String prefix,
      String idempotencyKey,
      String transactionId,
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
    String operationId =
        transactionId != null && !transactionId.isBlank()
            ? transactionId.trim()
            : (idempotencyKey == null ? null : idempotencyKey.trim());
    if (operationId == null || operationId.isBlank() || commitOperationTracker == null) {
      return doCommit(prefix, idempotencyKey, transactionId, request, tableSupport);
    }
    OperationKey key = new OperationKey(accountId, "transaction:" + prefix, operationId);
    return commitOperationTracker.execute(
        key,
        request,
        () -> doCommit(prefix, idempotencyKey, transactionId, request, tableSupport, key));
  }

  private Response doCommit(
      String prefix,
      String idempotencyKey,
      String transactionId,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport) {
    return doCommit(prefix, idempotencyKey, transactionId, request, tableSupport, null);
  }

  private Response doCommit(
      String prefix,
      String idempotencyKey,
      String transactionId,
      TransactionCommitRequest request,
      TableGatewaySupport tableSupport,
      OperationKey operationKey) {
    List<TransactionCommitRequest.TableChange> changes =
        request == null || request.tableChanges() == null ? List.of() : request.tableChanges();
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    ResourceId catalogId = catalogContext.catalogId();
    int index = 0;
    for (TransactionCommitRequest.TableChange change : changes) {
      var identifier = change.identifier();
      List<String> namespacePath =
          identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
      String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
      ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
      TableRequests.Commit commitReq =
          new TableRequests.Commit(change.requirements(), change.updates());
      Response tableResponse =
          tableCommitService.commit(
              new TableCommitService.CommitCommand(
                  prefix,
                  namespace,
                  namespacePath,
                  identifier.name(),
                  catalogName,
                  catalogId,
                  namespaceId,
                  idempotencyKey,
                  change.stageId(),
                  transactionId,
                  commitReq,
                  tableSupport));
      if (tableResponse.getStatus() >= 400) {
        return tableResponse;
      }
      if (commitOperationTracker != null && operationKey != null) {
        commitOperationTracker.markStep(operationKey, "TABLE_" + index + "_OK");
      }
      index++;
    }
    return Response.noContent().build();
  }
}
