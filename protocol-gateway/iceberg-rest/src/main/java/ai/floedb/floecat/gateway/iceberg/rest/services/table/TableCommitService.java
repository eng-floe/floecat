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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitService {
  private static final Logger LOG = Logger.getLogger(TableCommitService.class);

  @Inject IcebergGatewayConfig config;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject TransactionCommitService transactionCommitService;

  public Response commit(CommitCommand command) {
    if (command == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    TableRequests.Commit req = command.request();
    if (req == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }

    Table preCommitTable = loadCurrentTable(command);
    if (isDeltaReadOnlyCommitBlocked(preCommitTable)) {
      return IcebergErrorResponses.conflict(
          "Delta compatibility mode is read-only; table commits are disabled for Delta tables");
    }

    TransactionCommitRequest txRequest =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(command.namespacePath(), command.table()),
                    req.requirements(),
                    req.updates())));

    Response txResponse =
        transactionCommitService.commit(
            command.prefix(), command.idempotencyKey(), txRequest, command.tableSupport());
    if (txResponse == null
        || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return txResponse;
    }

    return buildCommitResponse(command, req);
  }

  private Response buildCommitResponse(CommitCommand command, TableRequests.Commit req) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            command.catalogName(), command.namespacePath(), command.table());
    Table committedTable = tableLifecycleService.getTable(tableId);
    TableGatewaySupport tableSupport = command.tableSupport();
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(committedTable);

    Set<Long> removedSnapshotIds = responseBuilder.removedSnapshotIds(req);
    CommitTableResponseDto initialResponse =
        responseBuilder.buildInitialResponse(
            command.table(), committedTable, tableId, null, req, tableSupport, metadata);

    CommitTableResponseDto responseDto = initialResponse;

    CommitTableResponseDto finalResponse =
        responseBuilder.buildFinalResponse(
            command.table(), committedTable, tableId, null, req, tableSupport, removedSnapshotIds);
    if (finalResponse == null
        || finalResponse.metadata() == null
        || finalResponse.metadataLocation() == null
        || finalResponse.metadataLocation().isBlank()) {
      return IcebergErrorResponses.failure(
          "Commit response missing metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    return Response.ok(finalResponse).build();
  }

  private Table loadCurrentTable(CommitCommand command) {
    try {
      ResourceId tableId =
          tableLifecycleService.resolveTableId(
              command.catalogName(), command.namespacePath(), command.table());
      return tableLifecycleService.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  public record CommitCommand(
      String prefix,
      String namespace,
      List<String> namespacePath,
      String table,
      String catalogName,
      ResourceId catalogId,
      ResourceId namespaceId,
      String idempotencyKey,
      String stageId,
      String transactionId,
      TableRequests.Commit request,
      TableGatewaySupport tableSupport) {}

  private boolean isDeltaReadOnlyCommitBlocked(Table table) {
    if (table == null || tableFormatSupport == null || config == null) {
      return false;
    }
    var deltaCompat = config.deltaCompat();
    if (deltaCompat.isEmpty()) {
      return false;
    }
    return deltaCompat.get().enabled()
        && deltaCompat.get().readOnly()
        && tableFormatSupport.isDelta(table);
  }
}
