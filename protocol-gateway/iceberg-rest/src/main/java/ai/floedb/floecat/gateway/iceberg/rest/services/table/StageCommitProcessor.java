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

import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.common.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitException;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.IcebergMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.IcebergMetadataService.ResolvedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@ApplicationScoped
public class StageCommitProcessor {
  private static final Logger LOG = Logger.getLogger(StageCommitProcessor.class);

  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;
  @Inject StagedTableService stagedTableService;
  @Inject GrpcServiceFacade grpcClient;
  @Inject TransactionCommitService transactionCommitService;
  @Inject IcebergMetadataService icebergMetadataService;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport = new TableGatewaySupport(grpc, config, mapper, mpConfig, grpcClient);
  }

  public StageCommitResult commitStage(
      String prefix,
      String catalogName,
      String accountId,
      List<String> namespacePath,
      String tableName,
      String stageId) {
    if (accountId == null || accountId.isBlank()) {
      throw StageCommitException.validation("account context is required");
    }
    if (namespacePath == null || namespacePath.isEmpty()) {
      throw StageCommitException.validation("namespace is required");
    }
    if (tableName == null || tableName.isBlank()) {
      throw StageCommitException.validation("table name is required");
    }
    if (stageId == null || stageId.isBlank()) {
      throw StageCommitException.validation("stage-id is required");
    }
    StagedTableKey key =
        new StagedTableKey(accountId, catalogName, namespacePath, tableName, stageId);
    StagedTableEntry entry =
        stagedTableService
            .getStage(key)
            .orElseThrow(
                () -> StageCommitException.notFound("staged metadata not found for " + tableName));
    if (entry.state() == StageState.ABORTED) {
      throw StageCommitException.conflict("stage " + stageId + " was aborted");
    }
    LOG.infof(
        "Processing staged payload stageId=%s namespace=%s table=%s",
        stageId, namespacePath, tableName);

    Table existing = null;
    boolean tableExists = false;
    try {
      ResourceId existingId =
          NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
      var resp = grpcClient.getTable(GetTableRequest.newBuilder().setTableId(existingId).build());
      existing = resp.getTable();
      tableExists = true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    validateStageRequirements(
        entry.requirements(), catalogName, namespacePath, tableName, tableExists);
    Table tableRecord = existing;
    if (!tableExists) {
      Response txResponse =
          transactionCommitService.commitCreate(
              prefix,
              entry.idempotencyKey(),
              namespacePath,
              tableName,
              entry.catalogId(),
              entry.namespaceId(),
              entry.request(),
              tableSupport);
      if (txResponse == null
          || txResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
        throw mapTransactionFailure(txResponse);
      }
      ResourceId createdId =
          NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
      tableRecord =
          grpcClient
              .getTable(GetTableRequest.newBuilder().setTableId(createdId).build())
              .getTable();
    }
    LoadTableResultDto loadResult = toLoadResult(tableName, entry, tableRecord);
    LOG.infof(
        "Stage commit load result stageId=%s metadataLocation=%s tableId=%s",
        stageId,
        loadResult.metadataLocation(),
        tableRecord != null && tableRecord.hasResourceId()
            ? tableRecord.getResourceId().getId()
            : "<missing>");
    stagedTableService.deleteStage(key);
    return new StageCommitResult(tableRecord, loadResult, !tableExists);
  }

  private LoadTableResultDto toLoadResult(
      String tableName, StagedTableEntry entry, Table tableRecord) {
    Map<String, String> tableConfig = tableSupport.defaultTableConfig();
    List<StorageCredentialDto> credentials = tableSupport.defaultCredentials();
    ResolvedMetadata resolved =
        icebergMetadataService.resolveMetadata(
            tableName, tableRecord, tableSupport, java.util.List::of);
    TableMetadataView metadataView = resolved.metadataView();
    if (metadataView == null) {
      throw new IllegalStateException("failed to import canonical stage-commit metadata");
    }
    return TableResponseMapper.toLoadResult(metadataView, tableConfig, credentials);
  }

  private RuntimeException mapTransactionFailure(Response response) {
    if (response == null) {
      return new IllegalStateException("stage commit transaction failed");
    }
    String message = "stage commit transaction failed";
    if (response.getEntity() instanceof IcebergErrorResponse errorResponse
        && errorResponse.error() != null
        && errorResponse.error().message() != null
        && !errorResponse.error().message().isBlank()) {
      message = errorResponse.error().message();
    }
    return switch (response.getStatus()) {
      case 400 -> StageCommitException.validation(message);
      case 404 -> StageCommitException.notFound(message);
      case 409 -> StageCommitException.conflict(message);
      default -> new IllegalStateException(message);
    };
  }

  void validateStageRequirements(
      List<Map<String, Object>> requirements,
      String catalogName,
      List<String> namespacePath,
      String tableName,
      boolean tableExists) {
    if (requirements == null || requirements.isEmpty()) {
      return;
    }
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        throw StageCommitException.validation("stage requirement cannot be null");
      }
      String type = requirement.get("type") instanceof String s ? s : null;
      if (type == null || type.isBlank()) {
        throw StageCommitException.validation("stage requirement missing type");
      }
      if (CommitUpdateInspector.REQUIREMENT_ASSERT_CREATE.equals(type)) {
        if (tableExists) {
          throw StageCommitException.conflict("assert-create failed");
        }
        try {
          NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
          throw StageCommitException.conflict("assert-create failed");
        } catch (StatusRuntimeException e) {
          if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
            throw e;
          }
        }
        continue;
      }
      throw StageCommitException.validation("unsupported stage requirement: " + type);
    }
  }

  public static final class StageCommitResult {
    private final Table table;
    private final LoadTableResultDto loadResult;
    private final boolean tableCreated;

    public StageCommitResult(Table table, LoadTableResultDto loadResult) {
      this(table, loadResult, false);
    }

    public StageCommitResult(Table table, LoadTableResultDto loadResult, boolean tableCreated) {
      this.table = table;
      this.loadResult = loadResult;
      this.tableCreated = tableCreated;
    }

    public Table table() {
      return table;
    }

    public LoadTableResultDto loadResult() {
      return loadResult;
    }

    public boolean tableCreated() {
      return tableCreated;
    }
  }
}
