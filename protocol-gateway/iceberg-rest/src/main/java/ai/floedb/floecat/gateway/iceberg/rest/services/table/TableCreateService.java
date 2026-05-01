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

import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.load.TableLoadService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.transaction.TransactionCommitService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCreateService {
  private static final Logger LOG = Logger.getLogger(TableCreateService.class);

  @Inject TableLifecycleService tableLifecycleService;
  @Inject TransactionCommitService transactionCommitService;
  @Inject TableLoadService tableLoadService;
  @Inject TableCreateSupport tableCreateSupport;

  public Response create(
      NamespaceRef namespaceContext,
      String accessDelegationMode,
      String idempotencyKey,
      String transactionId,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (request.name() == null || request.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (!hasSchema(request)) {
      return IcebergErrorResponses.validation("schema is required");
    }

    String tableName = request.name().trim();
    TableRequests.Create effectiveReq =
        tableCreateSupport.applyDefaultLocationIfMissing(namespaceContext, tableName, request);
    if (Boolean.TRUE.equals(effectiveReq.stageCreate())) {
      return tableCreateSupport.handleStageCreate(
          namespaceContext,
          tableName,
          effectiveReq,
          transactionId,
          idempotencyKey,
          accessDelegationMode,
          tableSupport);
    }

    Response txResponse;
    try {
      txResponse =
          transactionCommitService.commitCreate(
              namespaceContext.prefix(),
              idempotencyKey,
              namespaceContext.namespacePath(),
              tableName,
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              effectiveReq,
              tableSupport);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    if (txResponse == null
        || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return txResponse;
    }

    Table created;
    try {
      var tableId =
          tableLifecycleService.resolveTableId(
              namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
      created = tableLifecycleService.getTable(tableId);
    } catch (Exception e) {
      return IcebergErrorResponses.failure(
          "Failed to load created table",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    Response response;
    try {
      response =
          tableLoadService.loadResolvedTable(
              tableName, created, accessDelegationMode, tableSupport);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    Object entity = response.getEntity();
    if (!(entity instanceof LoadTableResultDto loadResult)) {
      return response;
    }
    LOG.infof(
        "Create table response namespace=%s table=%s metadata=%s location=%s configKeys=%s",
        namespaceContext.namespacePath(),
        tableName,
        loadResult.metadata() == null ? "<null>" : loadResult.metadata().metadataLocation(),
        loadResult.metadataLocation(),
        loadResult.config().keySet());
    return response;
  }

  private boolean hasSchema(TableRequests.Create request) {
    if (request == null) {
      return false;
    }
    return request.schema() != null && !request.schema().isNull();
  }
}
