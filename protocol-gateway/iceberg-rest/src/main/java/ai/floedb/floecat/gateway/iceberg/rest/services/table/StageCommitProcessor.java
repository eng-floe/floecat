package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.common.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitException;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ConnectorClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.SnapshotClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
  @Inject TableClient tableClient;
  @Inject SnapshotClient snapshotClient;
  @Inject ConnectorClient connectorClient;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport =
        new TableGatewaySupport(
            grpc, config, mapper, mpConfig, tableClient, snapshotClient, connectorClient);
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
    String stagedMetadata =
        entry.request().properties() == null
            ? null
            : entry.request().properties().get("metadata-location");
    LOG.infof(
        "Processing staged payload stageId=%s namespace=%s table=%s metadata=%s",
        stageId, namespacePath, tableName, stagedMetadata);

    Table existing = null;
    boolean tableExists = false;
    try {
      ResourceId existingId =
          NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
      var resp = tableClient.getTable(GetTableRequest.newBuilder().setTableId(existingId).build());
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
      CreateTableRequest.Builder createRequest =
          CreateTableRequest.newBuilder().setSpec(entry.spec());
      if (entry.idempotencyKey() != null && !entry.idempotencyKey().isBlank()) {
        createRequest.setIdempotency(IdempotencyKey.newBuilder().setKey(entry.idempotencyKey()));
      }
      tableRecord = tableClient.createTable(createRequest.build()).getTable();
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
    return new StageCommitResult(tableRecord, loadResult);
  }

  private LoadTableResultDto toLoadResult(
      String tableName, StagedTableEntry entry, Table tableRecord) {
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(tableRecord);
    Map<String, String> tableConfig = tableSupport.defaultTableConfig();
    List<StorageCredentialDto> credentials = tableSupport.defaultCredentials();
    List<Snapshot> snapshots = loadSnapshots(tableRecord.getResourceId());
    if (metadata == null) {
      return TableResponseMapper.toLoadResultFromCreate(
          tableName, tableRecord, entry.request(), tableConfig, credentials);
    }
    return TableResponseMapper.toLoadResult(
        tableName, tableRecord, metadata, snapshots, tableConfig, credentials);
  }

  private List<Snapshot> loadSnapshots(ResourceId tableId) {
    try {
      return snapshotClient
          .listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build())
          .getSnapshotsList();
    } catch (StatusRuntimeException e) {
      return List.of();
    }
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
      if ("assert-create".equals(type)) {
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

    public StageCommitResult(Table table, LoadTableResultDto loadResult) {
      this.table = table;
      this.loadResult = loadResult;
    }

    public Table table() {
      return table;
    }

    public LoadTableResultDto loadResult() {
      return loadResult;
    }
  }
}
