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
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ApplicationScoped
public class TableCommitService {
  @Inject IcebergGatewayConfig config;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TableFormatSupport tableFormatSupport;
  @Inject TransactionCommitService transactionCommitService;
  @Inject TableCreateTransactionMapper tableCreateTransactionMapper;
  @Inject StagedTableService stagedTableService;
  @Inject AccountContext accountContext;

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

    Optional<StagedTableEntry> stagedEntryOpt = resolveStagedEntry(command);
    if (stagedEntryOpt.isPresent() && stagedEntryOpt.get().state() == StageState.ABORTED) {
      return IcebergErrorResponses.conflict(
          "stage " + stagedEntryOpt.get().key().stageId() + " was aborted");
    }
    TableRequests.Commit effectiveReq =
        mergeStagedCreateIntoCommit(command, req, stagedEntryOpt.orElse(null), preCommitTable);

    TransactionCommitRequest txRequest =
        new TransactionCommitRequest(
            List.of(
                new TransactionCommitRequest.TableChange(
                    new TableIdentifierDto(command.namespacePath(), command.table()),
                    effectiveReq.requirements(),
                    effectiveReq.updates())));

    Response txResponse =
        transactionCommitService.commit(
            command.prefix(), command.idempotencyKey(), txRequest, command.tableSupport());
    if (txResponse == null
        || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return txResponse;
    }
    stagedEntryOpt.ifPresent(entry -> stagedTableService.deleteStage(entry.key()));

    return buildCommitResponse(command, effectiveReq);
  }

  private Response buildCommitResponse(CommitCommand command, TableRequests.Commit req) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(
            command.catalogName(), command.namespacePath(), command.table());
    Table committedTable = tableLifecycleService.getTable(tableId);
    TableGatewaySupport tableSupport = command.tableSupport();

    CommitTableResponseDto finalResponse =
        responseBuilder.buildFinalResponse(committedTable, null, req, tableSupport);
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

  private TableRequests.Commit mergeStagedCreateIntoCommit(
      CommitCommand command,
      TableRequests.Commit req,
      StagedTableEntry stagedEntry,
      Table tableState) {
    if (req == null || stagedEntry == null) {
      return req;
    }
    if (tableState != null || callerProvidesCreateInitialization(req)) {
      return req;
    }
    TransactionCommitRequest stagedRequest =
        tableCreateTransactionMapper.buildCreateRequest(
            command.namespacePath(),
            command.table(),
            stagedEntry.catalogId(),
            stagedEntry.namespaceId(),
            stagedEntry.request(),
            command.tableSupport());
    var stagedChange = stagedRequest.tableChanges().get(0);
    List<Map<String, Object>> requirements = new ArrayList<>(stagedChange.requirements());
    if (req.requirements() != null && !req.requirements().isEmpty()) {
      requirements.addAll(req.requirements());
    }
    List<Map<String, Object>> updates = new ArrayList<>(stagedChange.updates());
    if (req.updates() != null && !req.updates().isEmpty()) {
      updates.addAll(req.updates());
    }
    return new TableRequests.Commit(List.copyOf(requirements), List.copyOf(updates));
  }

  private Optional<StagedTableEntry> resolveStagedEntry(CommitCommand command) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return Optional.empty();
    }
    String stageId = resolveStageId(command);
    if (stageId != null) {
      StagedTableKey key =
          new StagedTableKey(
              accountId, command.catalogName(), command.namespacePath(), command.table(), stageId);
      return stagedTableService.getStage(key);
    }
    return stagedTableService.findSingleStage(
        accountId, command.catalogName(), command.namespacePath(), command.table());
  }

  private String resolveStageId(CommitCommand command) {
    if (command.stageId() != null && !command.stageId().isBlank()) {
      return command.stageId();
    }
    if (command.transactionId() != null && !command.transactionId().isBlank()) {
      return command.transactionId();
    }
    return null;
  }

  private boolean callerProvidesCreateInitialization(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      if (isCreateInitializationUpdate(update)) {
        return true;
      }
    }
    return false;
  }

  private boolean isCreateInitializationUpdate(Map<String, Object> update) {
    if (update == null) {
      return false;
    }
    Object action = update.get("action");
    if (!(action instanceof String value)) {
      return false;
    }
    return CommitUpdateInspector.isCreateInitializationAction(value);
  }
}
