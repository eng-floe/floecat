package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.StageCommitProcessor.StageCommitResult;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergRef;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitService {
  private static final Logger LOG = Logger.getLogger(TableCommitService.class);

  @Inject GrpcWithHeaders grpc;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject TablePropertyService tablePropertyService;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject StageMaterializationService stageMaterializationService;
  @Inject CommitRequirementService commitRequirementService;

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

    final Table[] stagedTableHolder = new Table[1];
    StageCommitResult stageMaterialization = null;
    String materializedStageId = null;

    ResourceId resolvedTableId = null;
    try {
      resolvedTableId = tableLifecycleService.resolveTableId(catalogName, namespacePath, table);
      StageMaterializationService.StageMaterializationResult explicitStage =
          stageMaterializationService.materializeExplicitStage(
              prefix, catalogName, namespacePath, table, req, transactionId);
      if (explicitStage != null) {
        stageMaterialization = explicitStage.result();
        materializedStageId = explicitStage.stageId();
        stagedTableHolder[0] = explicitStage.table();
        resolvedTableId = explicitStage.table().getResourceId();
      }
    } catch (io.grpc.StatusRuntimeException e) {
      StageMaterializationService.StageMaterializationResult materialization;
      try {
        materialization =
            stageMaterializationService.materializeIfTableMissing(
                e, prefix, catalogName, namespacePath, table, req, transactionId);
      } catch (StageCommitException sce) {
        return sce.toResponse();
      }
      if (materialization != null) {
        stageMaterialization = materialization.result();
        materializedStageId = materialization.stageId();
        stagedTableHolder[0] = stageMaterialization.table();
        resolvedTableId = stageMaterialization.table().getResourceId();
      } else {
        throw e;
      }
    }

    if (resolvedTableId == null) {
      throw new IllegalStateException("table resolution failed");
    }
    final ResourceId tableId = resolvedTableId;
    Supplier<Table> tableSupplier =
        new Supplier<>() {
          private Table cached = stagedTableHolder[0];

          @Override
          public Table get() {
            if (cached == null) {
              cached = tableLifecycleService.getTable(tableId);
            }
            return cached;
          }
        };

    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    SnapshotUpdateContext snapshotContext = new SnapshotUpdateContext();
    if (req != null) {
      Response requirementError =
          commitRequirementService.validateRequirements(
              tableSupport,
              req.requirements(),
              tableSupplier,
              this::validationError,
              this::conflictError);
      if (requirementError != null) {
        return requirementError;
      }
      if (req.name() != null) {
        spec.setDisplayName(req.name());
        mask.addPaths("display_name");
      }
      if (req.namespace() != null && !req.namespace().isEmpty()) {
        var targetNs =
            tableLifecycleService.resolveNamespaceId(catalogName, new ArrayList<>(req.namespace()));
        spec.setNamespaceId(targetNs);
        mask.addPaths("namespace_id");
      }
      if (req.schemaJson() != null && !req.schemaJson().isBlank()) {
        spec.setSchemaJson(req.schemaJson());
        mask.addPaths("schema_json");
      }
      Map<String, String> mergedProps = null;
      if (req.properties() != null && !req.properties().isEmpty()) {
        mergedProps = new LinkedHashMap<>(req.properties());
        tablePropertyService.stripMetadataLocation(mergedProps);
        if (mergedProps.isEmpty()) {
          mergedProps = null;
        }
      }
      if (tablePropertyService.hasPropertyUpdates(req)) {
        if (mergedProps == null) {
          mergedProps = new LinkedHashMap<>(tableSupplier.get().getPropertiesMap());
        }
        Response updateError =
            tablePropertyService.applyPropertyUpdates(mergedProps, req.updates());
        if (updateError != null) {
          return updateError;
        }
      }
      Response locationError =
          tablePropertyService.applyLocationUpdate(spec, mask, tableSupplier, req.updates());
      if (locationError != null) {
        return locationError;
      }
      String unsupported = unsupportedUpdateAction(req);
      if (unsupported != null) {
        return validationError("unsupported commit update action: " + unsupported);
      }
      Response snapshotError =
          snapshotMetadataService.applySnapshotUpdates(
              tableSupport,
              tableId,
              namespacePath,
              table,
              tableSupplier,
              req.updates(),
              idempotencyKey,
              snapshotContext);
      if (snapshotError != null) {
        return snapshotError;
      }
      mergedProps =
          materializePendingSnapshotMetadataIfNeeded(
              namespace, table, tableId, tableSupplier, mergedProps, snapshotContext);
      if (mergedProps != null) {
        spec.clearProperties().putAllProperties(mergedProps);
        mask.addPaths("properties");
      }
    }

    if (mask.getPathsCount() == 0) {
      Table current = tableSupplier.get();
      IcebergMetadata metadata = tableSupport.loadCurrentMetadata(current);
      List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, metadata);
      CommitTableResponseDto initialResponse =
          TableResponseMapper.toCommitResponse(table, current, metadata, snapshotList);
      var sideEffects =
          sideEffectService.finalizeCommitResponse(
              namespace,
              table,
              tableId,
              current,
              initialResponse,
              false,
              tableSupport,
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              idempotencyKey);
      if (sideEffects.hasError()) {
        return sideEffects.error();
      }
      CommitTableResponseDto responseDto = sideEffects.response();
      Response.ResponseBuilder builder = Response.ok(responseDto);
      LOG.infof(
          "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
          namespace,
          table,
          current.hasResourceId() ? current.getResourceId().getId() : "<missing>",
          metadata != null ? metadata.getCurrentSnapshotId() : "<null>",
          snapshotList == null ? 0 : snapshotList.size());
      if (responseDto != null && responseDto.metadataLocation() != null) {
        builder.tag(responseDto.metadataLocation());
      }
      logStageCommit(
          stageMaterialization,
          nonBlank(materializedStageId, resolveStageId(req, transactionId)),
          namespace,
          table,
          responseDto == null ? null : responseDto.metadata());
      runConnectorSync(tableSupport, sideEffects.connectorId(), namespacePath, namespace, table);
      return builder.build();
    }

    UpdateTableRequest.Builder updateRequest =
        UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).setUpdateMask(mask);
    Table updated = tableLifecycleService.updateTable(updateRequest.build());
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(updated);
    List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, metadata);
    CommitTableResponseDto initialResponse =
        TableResponseMapper.toCommitResponse(table, updated, metadata, snapshotList);
    boolean skipMaterialization = snapshotContext.hasMaterializedMetadata();
    CommitTableResponseDto responseDto = initialResponse;
    if (skipMaterialization) {
      String location = snapshotContext.materializedMetadataLocation;
      responseDto =
          new CommitTableResponseDto(
              location,
              responseDto.metadata() == null
                  ? null
                  : responseDto.metadata().withMetadataLocation(location));
    }
    var sideEffects =
        sideEffectService.finalizeCommitResponse(
            namespace,
            table,
            tableId,
            updated,
            responseDto,
            skipMaterialization,
            tableSupport,
            prefix,
            namespacePath,
            namespaceId,
            catalogId,
            idempotencyKey);
    if (sideEffects.hasError()) {
      return sideEffects.error();
    }
    responseDto = sideEffects.response();
    Response.ResponseBuilder builder = Response.ok(responseDto);
    LOG.infof(
        "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
        namespace,
        table,
        updated.hasResourceId() ? updated.getResourceId().getId() : "<missing>",
        metadata != null ? metadata.getCurrentSnapshotId() : "<null>",
        snapshotList == null ? 0 : snapshotList.size());
    if (responseDto != null && responseDto.metadataLocation() != null) {
      builder.tag(responseDto.metadataLocation());
    }
    logStageCommit(
        stageMaterialization,
        nonBlank(materializedStageId, resolveStageId(req, transactionId)),
        namespace,
        table,
        responseDto == null ? null : responseDto.metadata());
    runConnectorSync(tableSupport, sideEffects.connectorId(), namespacePath, namespace, table);
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

  private String resolveStageId(TableRequests.Commit req, String headerStageId) {
    if (req != null && req.stageId() != null && !req.stageId().isBlank()) {
      return req.stageId();
    }
    if (headerStageId != null && !headerStageId.isBlank()) {
      return headerStageId;
    }
    return null;
  }

  private Table tableWithPropertyOverrides(
      Supplier<Table> tableSupplier, Map<String, String> propertyOverrides) {
    return tablePropertyService.tableWithPropertyOverrides(tableSupplier, propertyOverrides);
  }

  private void runConnectorSync(
      TableGatewaySupport tableSupport,
      ResourceId connectorId,
      List<String> namespacePath,
      String namespace,
      String tableName) {
    try {
      sideEffectService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
    } catch (Throwable e) {
      LOG.warnf(
          e,
          "Post-commit connector sync failed for %s.%s",
          namespace == null ? "<missing>" : namespace,
          tableName == null ? "<missing>" : tableName);
    }
  }

  public List<Snapshot> fetchSnapshots(
      ResourceId tableId, SnapshotMode mode, IcebergMetadata metadata) {
    SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub =
        grpc.withHeaders(grpc.raw().snapshot());
    try {
      var resp =
          snapshotStub.listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build());
      List<Snapshot> snapshots = resp.getSnapshotsList();
      if (mode == SnapshotMode.REFS) {
        if (metadata == null || metadata.getRefsCount() == 0) {
          return List.of();
        }
        Set<Long> refIds =
            metadata.getRefsMap().values().stream()
                .map(IcebergRef::getSnapshotId)
                .collect(Collectors.toSet());
        return snapshots.stream()
            .filter(s -> refIds.contains(s.getSnapshotId()))
            .collect(Collectors.toList());
      }
      return snapshots;
    } catch (io.grpc.StatusRuntimeException e) {
      return List.of();
    }
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

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(
            new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse(
                new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError(
                    message, "ValidationException", 400)))
        .build();
  }

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(
            new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse(
                new ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError(
                    message, "CommitFailedException", 409)))
        .build();
  }

  private MaterializeMetadataResult materializePendingSnapshotMetadata(
      String namespace,
      String tableName,
      ResourceId tableId,
      Supplier<Table> tableSupplier,
      Map<String, String> mergedProps,
      Long snapshotIdForMaterialization) {
    if (snapshotIdForMaterialization == null) {
      return null;
    }
    IcebergMetadata snapshotMetadata =
        snapshotMetadataService.loadSnapshotMetadata(tableId, snapshotIdForMaterialization);
    if (snapshotMetadata == null) {
      return null;
    }
    Table tableForMetadata = tableWithPropertyOverrides(tableSupplier, mergedProps);
    List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, snapshotMetadata);
    CommitTableResponseDto pendingResponse =
        TableResponseMapper.toCommitResponse(
            tableName, tableForMetadata, snapshotMetadata, snapshotList);
    return sideEffectService.materializeMetadata(
        namespace,
        null,
        tableName,
        tableForMetadata,
        pendingResponse.metadata(),
        pendingResponse.metadataLocation());
  }

  private Map<String, String> materializePendingSnapshotMetadataIfNeeded(
      String namespace,
      String tableName,
      ResourceId tableId,
      Supplier<Table> tableSupplier,
      Map<String, String> mergedProps,
      SnapshotUpdateContext snapshotContext) {
    if (snapshotContext == null || snapshotContext.lastSnapshotId == null) {
      return mergedProps;
    }
    Map<String, String> propsForMaterialization =
        tablePropertyService.ensurePropertyMap(tableSupplier, mergedProps);
    MaterializeMetadataResult pendingMaterialization =
        materializePendingSnapshotMetadata(
            namespace,
            tableName,
            tableId,
            tableSupplier,
            propsForMaterialization,
            snapshotContext.lastSnapshotId);
    if (pendingMaterialization != null) {
      TableMetadataView pendingMetadata =
          pendingMaterialization.metadata() != null ? pendingMaterialization.metadata() : null;
      String resolvedLocation =
          nonBlank(
              pendingMaterialization.metadataLocation(),
              pendingMetadata != null ? pendingMetadata.metadataLocation() : null);
      Map<String, String> updatedProps =
          pendingMetadata != null && pendingMetadata.properties() != null
              ? new LinkedHashMap<>(pendingMetadata.properties())
              : propsForMaterialization;
      if (updatedProps != null && resolvedLocation != null && !resolvedLocation.isBlank()) {
        updatedProps.put("metadata-location", resolvedLocation);
        updatedProps.put("metadata_location", resolvedLocation);
        snapshotContext.materializedMetadataLocation = resolvedLocation;
      }
      return updatedProps;
    }
    return mergedProps;
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
    sideEffectService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
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

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }

  public enum SnapshotMode {
    ALL,
    REFS
  }
}
