package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.Config;

/** Shared stage commit workflow for table and transaction endpoints. */
@ApplicationScoped
public class StageCommitProcessor {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;
  @Inject StagedTableService stagedTableService;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport = new TableGatewaySupport(grpc, config, mapper, mpConfig);
  }

  public StageCommitResult commitStage(
      String prefix,
      String catalogName,
      String tenantId,
      List<String> namespacePath,
      String tableName,
      String stageId) {
    if (tenantId == null || tenantId.isBlank()) {
      throw StageCommitException.validation("tenant context is required");
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
        new StagedTableKey(tenantId, catalogName, namespacePath, tableName, stageId);
    StagedTableEntry entry =
        stagedTableService
            .getStage(key)
            .orElseThrow(
                () -> StageCommitException.notFound("staged metadata not found for " + tableName));
    if (entry.state() == StageState.ABORTED) {
      throw StageCommitException.conflict("stage " + stageId + " was aborted");
    }

    TableServiceGrpc.TableServiceBlockingStub tableStub = grpc.withHeaders(grpc.raw().table());
    Table existing = null;
    boolean tableExists = false;
    try {
      ResourceId existingId =
          NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
      var resp =
          tableStub.getTable(GetTableRequest.newBuilder().setTableId(existingId).build());
      existing = resp.getTable();
      tableExists = true;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    validateStageRequirements(entry.requirements(), catalogName, namespacePath, tableName, tableExists);
    Table tableRecord = existing;
    if (!tableExists) {
      CreateTableRequest.Builder createRequest =
          CreateTableRequest.newBuilder().setSpec(entry.spec());
      if (entry.idempotencyKey() != null && !entry.idempotencyKey().isBlank()) {
        createRequest.setIdempotency(IdempotencyKey.newBuilder().setKey(entry.idempotencyKey()));
      }
      tableRecord = tableStub.createTable(createRequest.build()).getTable();
    }
    LoadTableResultDto loadResult = toLoadResult(tableName, entry, tableRecord);
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
    SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub =
        grpc.withHeaders(grpc.raw().snapshot());
    try {
      return snapshotStub
          .listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build())
          .getSnapshotsList();
    } catch (StatusRuntimeException e) {
      return List.of();
    }
  }

  private void validateStageRequirements(
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
          continue;
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

    StageCommitResult(Table table, LoadTableResultDto loadResult) {
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
