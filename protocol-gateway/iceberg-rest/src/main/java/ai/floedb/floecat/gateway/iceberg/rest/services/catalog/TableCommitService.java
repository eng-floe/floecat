package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitService {
  private static final Logger LOG = Logger.getLogger(TableCommitService.class);

  @Inject GrpcWithHeaders grpc;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject StageMaterializationService stageMaterializationService;
  @Inject CommitStageResolver stageResolver;
  @Inject TableUpdatePlanner tableUpdatePlanner;

  public Response commit(CommitCommand command) {
    String prefix = command.prefix();
    String namespace = command.namespace();
    List<String> namespacePath = command.namespacePath();
    String table = command.table();
    String catalogName = command.catalogName();
    ResourceId catalogId = command.catalogId();
    ResourceId namespaceId = command.namespaceId();
    String idempotencyKey = command.idempotencyKey();
    String transactionId = command.transactionId();
    TableRequests.Commit req = command.request();
    TableGatewaySupport tableSupport = command.tableSupport();

    CommitStageResolver.StageResolution stageResolution = stageResolver.resolve(command);
    if (stageResolution.hasError()) {
      return stageResolution.error();
    }
    ResourceId tableId = stageResolution.tableId();
    Supplier<Table> tableSupplier = createTableSupplier(stageResolution.stagedTable(), tableId);

    TableUpdatePlanner.UpdatePlan updatePlan =
        tableUpdatePlanner.planUpdates(command, tableSupplier, tableId);
    if (updatePlan.hasError()) {
      return updatePlan.error();
    }

    Table committedTable =
        applyTableUpdates(tableSupplier, tableId, updatePlan.spec(), updatePlan.mask());
    StageCommitResult stageMaterialization = stageResolution.stageCommitResult();
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(committedTable);
    String requestedMetadataOverride = resolveRequestedMetadataLocation(req);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(grpc, tableId, SnapshotLister.Mode.ALL, metadata);
    CommitTableResponseDto initialResponse =
        TableResponseMapper.toCommitResponse(table, committedTable, metadata, snapshotList);
    CommitTableResponseDto stageAwareResponse =
        preferStageMetadata(initialResponse, stageMaterialization);
    stageAwareResponse =
        normalizeMetadataLocation(tableSupport, committedTable, stageAwareResponse);
    stageAwareResponse =
        preferRequestedMetadata(tableSupport, stageAwareResponse, requestedMetadataOverride);
    var sideEffects =
        sideEffectService.finalizeCommitResponse(
            namespace, table, tableId, committedTable, stageAwareResponse, false);
    if (sideEffects.hasError()) {
      return sideEffects.error();
    }
    CommitTableResponseDto responseDto =
        preferStageMetadata(sideEffects.response(), stageMaterialization);
    Response.ResponseBuilder builder = Response.ok(responseDto);
    LOG.infof(
        "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
        namespace,
        table,
        committedTable.hasResourceId() ? committedTable.getResourceId().getId() : "<missing>",
        metadata != null ? metadata.getCurrentSnapshotId() : "<null>",
        snapshotList == null ? 0 : snapshotList.size());
    if (responseDto != null && responseDto.metadataLocation() != null) {
      builder.tag(responseDto.metadataLocation());
    }
    logStageCommit(
        stageMaterialization,
        nonBlank(
            stageResolution.materializedStageId(),
            stageMaterializationService.resolveStageId(req, transactionId)),
        namespace,
        table,
        responseDto == null ? null : responseDto.metadata());
    ResourceId connectorId =
        sideEffectService.synchronizeConnector(
            tableSupport,
            prefix,
            namespacePath,
            namespaceId,
            catalogId,
            table,
            committedTable,
            responseDto == null ? null : responseDto.metadata(),
            responseDto == null ? null : responseDto.metadataLocation(),
            idempotencyKey);
    runConnectorSync(tableSupport, connectorId, namespacePath, table);
    return builder.build();
  }

  private void logStageCommit(
      StageCommitResult stageMaterialization,
      String stageId,
      String namespace,
      String table,
      TableMetadataView metadata) {
    if (stageMaterialization == null) {
      return;
    }
    Table staged = stageMaterialization.table();
    String tableId =
        staged != null && staged.hasResourceId() ? staged.getResourceId().getId() : "<missing>";
    String snapshotStr =
        metadata == null || metadata.currentSnapshotId() == null
            ? "<null>"
            : metadata.currentSnapshotId().toString();
    int snapshotCount =
        metadata == null || metadata.snapshots() == null ? 0 : metadata.snapshots().size();
    LOG.infof(
        "Stage commit satisfied for %s.%s tableId=%s stageId=%s currentSnapshot=%s snapshotCount=%d",
        namespace, table, tableId, stageId, snapshotStr, snapshotCount);
  }

  private CommitTableResponseDto preferStageMetadata(
      CommitTableResponseDto response, StageCommitResult stageMaterialization) {
    if (stageMaterialization == null || stageMaterialization.loadResult() == null) {
      return response;
    }
    LoadTableResultDto staged = stageMaterialization.loadResult();
    TableMetadataView stagedMetadata = staged.metadata();
    String stagedLocation = staged.metadataLocation();
    if ((stagedLocation == null || stagedLocation.isBlank())
        && stagedMetadata != null
        && stagedMetadata.metadataLocation() != null
        && !stagedMetadata.metadataLocation().isBlank()) {
      stagedLocation = stagedMetadata.metadataLocation();
    }
    if (stagedLocation == null || stagedLocation.isBlank()) {
      return response;
    }
    String originalLocation = response == null ? "<null>" : response.metadataLocation();
    LOG.infof(
        "Stage metadata evaluation stagedLocation=%s originalLocation=%s",
        stagedLocation, originalLocation);
    if (stagedMetadata != null) {
      stagedMetadata = stagedMetadata.withMetadataLocation(stagedLocation);
    }
    if (response != null
        && stagedLocation.equals(response.metadataLocation())
        && Objects.equals(stagedMetadata, response.metadata())) {
      return response;
    }
    LOG.infof("Preferring staged metadata location %s over %s", stagedLocation, originalLocation);
    return new CommitTableResponseDto(stagedLocation, stagedMetadata);
  }

  private CommitTableResponseDto preferRequestedMetadata(
      TableGatewaySupport tableSupport,
      CommitTableResponseDto response,
      String requestedMetadataLocation) {
    if (requestedMetadataLocation == null || requestedMetadataLocation.isBlank()) {
      return response;
    }
    String resolved = tableSupport.stripMetadataMirrorPrefix(requestedMetadataLocation);
    if (resolved == null || resolved.isBlank()) {
      return response;
    }
    TableMetadataView metadata = response == null ? null : response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(resolved);
    }
    if (response != null && resolved.equals(response.metadataLocation())) {
      if (Objects.equals(metadata, response.metadata())) {
        return response;
      }
    }
    return new CommitTableResponseDto(resolved, metadata);
  }

  private CommitTableResponseDto normalizeMetadataLocation(
      TableGatewaySupport tableSupport, Table tableRecord, CommitTableResponseDto response) {
    if (response == null || response.metadataLocation() == null) {
      return response;
    }
    String metadataLocation = response.metadataLocation();
    if (!tableSupport.isMirrorMetadataLocation(metadataLocation)) {
      return response;
    }
    String resolvedLocation = tableSupport.stripMetadataMirrorPrefix(metadataLocation);
    TableMetadataView metadata = response.metadata();
    if (metadata != null) {
      metadata = metadata.withMetadataLocation(resolvedLocation);
    }
    return new CommitTableResponseDto(resolvedLocation, metadata);
  }

  private String unsupportedUpdateAction(TableRequests.Commit req) {
    if (req == null || req.updates() == null) {
      return null;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = asString(update == null ? null : update.get("action"));
      if (action == null) {
        continue;
      }
      if (!"set-properties".equals(action)
          && !"remove-properties".equals(action)
          && !"set-location".equals(action)
          && !"add-snapshot".equals(action)
          && !"remove-snapshots".equals(action)
          && !"set-snapshot-ref".equals(action)
          && !"remove-snapshot-ref".equals(action)
          && !"assign-uuid".equals(action)
          && !"upgrade-format-version".equals(action)
          && !"add-schema".equals(action)
          && !"set-current-schema".equals(action)
          && !"add-spec".equals(action)
          && !"set-default-spec".equals(action)
          && !"add-sort-order".equals(action)
          && !"set-default-sort-order".equals(action)
          && !"remove-partition-specs".equals(action)
          && !"remove-schemas".equals(action)
          && !"set-statistics".equals(action)
          && !"remove-statistics".equals(action)
          && !"set-partition-statistics".equals(action)
          && !"remove-partition-statistics".equals(action)
          && !"add-encryption-key".equals(action)
          && !"remove-encryption-key".equals(action)) {
        return action;
      }
    }
    return null;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private String resolveRequestedMetadataLocation(TableRequests.Commit req) {
    if (req == null) {
      return null;
    }
    String location = MetadataLocationUtil.metadataLocation(req.properties());
    String updateLocation = metadataLocationFromUpdates(req.updates());
    return nonBlank(updateLocation, location);
  }

  private String metadataLocationFromUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if (!"set-properties".equals(action)) {
        continue;
      }
      Map<String, String> toSet = asStringMap(update.get("updates"));
      if (toSet.isEmpty()) {
        continue;
      }
      String candidate = MetadataLocationUtil.metadataLocation(toSet);
      if (candidate != null && !candidate.isBlank()) {
        location = candidate;
      }
    }
    return location;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map) || map.isEmpty()) {
      return Map.of();
    }
    Map<String, String> converted = new LinkedHashMap<>();
    map.forEach(
        (k, v) -> {
          String key = asString(k);
          String strValue = asString(v);
          if (key != null && strValue != null) {
            converted.put(key, strValue);
          }
        });
    return converted;
  }

  public MaterializeMetadataResult materializeMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      Table tableRecord,
      TableMetadataView metadata,
      String metadataLocation) {
    return sideEffectService.materializeMetadata(
        namespace, tableId, table, tableRecord, metadata, metadataLocation);
  }

  public void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String tableName) {
    try {
      sideEffectService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      String namespace =
          namespacePath == null
              ? "<missing>"
              : (namespacePath.isEmpty() ? "<empty>" : String.join(".", namespacePath));
      LOG.warnf(
          e,
          "Post-commit connector sync failed for %s.%s",
          namespace,
          tableName == null ? "<missing>" : tableName);
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
      String transactionId,
      TableRequests.Commit request,
      TableGatewaySupport tableSupport) {}

  private Supplier<Table> createTableSupplier(Table stagedTable, ResourceId tableId) {
    return new Supplier<>() {
      private Table cached = stagedTable;

      @Override
      public Table get() {
        if (cached == null) {
          cached = tableLifecycleService.getTable(tableId);
        }
        return cached;
      }
    };
  }

  private Table applyTableUpdates(
      Supplier<Table> tableSupplier,
      ResourceId tableId,
      TableSpec.Builder spec,
      FieldMask.Builder mask) {
    if (mask.getPathsCount() == 0) {
      return tableSupplier.get();
    }
    UpdateTableRequest.Builder updateRequest =
        UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).setUpdateMask(mask);
    return tableLifecycleService.updateTable(updateRequest.build());
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }
}
