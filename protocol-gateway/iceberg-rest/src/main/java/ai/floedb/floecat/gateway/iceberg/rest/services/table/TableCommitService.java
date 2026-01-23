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
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CommitTableResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.CommitStageResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.table.StageCommitProcessor.StageCommitResult;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  @Inject CommitResponseBuilder responseBuilder;
  @Inject TableUpdatePlanner tableUpdatePlanner;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject TableMetadataImportService tableMetadataImportService;

  public Response commit(CommitCommand command) {
    String prefix = command.prefix();
    String namespace = command.namespace();
    List<String> namespacePath = command.namespacePath();
    String table = command.table();
    ResourceId catalogId = command.catalogId();
    ResourceId namespaceId = command.namespaceId();
    String idempotencyKey = command.idempotencyKey();
    String stageId = command.stageId();
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
    Map<String, String> ioProps =
        committedTable == null
            ? Map.of()
            : FileIoFactory.filterIoProperties(committedTable.getPropertiesMap());
    LOG.infof("Commit table io props namespace=%s.%s props=%s", namespace, table, ioProps);
    StageCommitResult stageMaterialization = stageResolution.stageCommitResult();
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(committedTable);
    Set<Long> removedSnapshotIds = responseBuilder.removedSnapshotIds(req);
    CommitTableResponseDto stageAwareResponse =
        responseBuilder.buildInitialResponse(
            table, committedTable, tableId, stageMaterialization, req, tableSupport, metadata);
    if (LOG.isDebugEnabled() && stageAwareResponse != null) {
      TableMetadataView debugMeta = stageAwareResponse.metadata();
      Long debugLastSeq = debugMeta == null ? null : debugMeta.lastSequenceNumber();
      String debugPropSeq =
          debugMeta == null || debugMeta.properties() == null
              ? null
              : debugMeta.properties().get("last-sequence-number");
      LOG.debugf(
          "Commit response sequence debug namespace=%s table=%s reqSeq=%s metaSeq=%s propSeq=%s",
          namespace, table, responseBuilder.maxSequenceNumber(req), debugLastSeq, debugPropSeq);
    }
    var sideEffects =
        sideEffectService.finalizeCommitResponse(
            namespace, table, tableId, committedTable, stageAwareResponse, false);
    if (sideEffects.hasError()) {
      return sideEffects.error();
    }
    CommitTableResponseDto responseDto = sideEffects.response();

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
    syncExternalSnapshotsIfNeeded(
        tableSupport,
        tableId,
        namespacePath,
        table,
        committedTable,
        responseDto,
        req,
        idempotencyKey);
    syncSnapshotMetadataFromCommit(
        tableSupport, tableId, namespacePath, table, committedTable, responseDto, idempotencyKey);
    runConnectorSync(tableSupport, connectorId, namespacePath, table);

    CommitTableResponseDto finalResponse =
        responseBuilder.buildFinalResponse(
            table,
            committedTable,
            tableId,
            stageMaterialization,
            req,
            tableSupport,
            removedSnapshotIds);
    if (responseDto != null && responseDto.metadataLocation() != null) {
      finalResponse =
          responseBuilder.preferRequestedMetadata(finalResponse, responseDto.metadataLocation());
    }
    if (finalResponse == null
        || finalResponse.metadata() == null
        || finalResponse.metadataLocation() == null
        || finalResponse.metadataLocation().isBlank()) {
      return IcebergErrorResponses.failure(
          "Commit response missing metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }

    Response.ResponseBuilder builder = Response.ok(finalResponse);
    TableMetadataView finalMetadata = finalResponse == null ? null : finalResponse.metadata();
    Long finalSnapshotId = finalMetadata == null ? null : finalMetadata.currentSnapshotId();
    int finalSnapshotCount =
        finalMetadata == null || finalMetadata.snapshots() == null
            ? 0
            : finalMetadata.snapshots().size();
    LOG.infof(
        "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
        namespace,
        table,
        committedTable.hasResourceId() ? committedTable.getResourceId().getId() : "<missing>",
        finalSnapshotId == null ? "<null>" : finalSnapshotId,
        finalSnapshotCount);
    logStageCommit(
        stageMaterialization,
        nonBlank(
            stageResolution.materializedStageId(),
            stageMaterializationService.resolveStageId(stageId, transactionId)),
        namespace,
        table,
        finalResponse == null ? null : finalResponse.metadata());
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
        "Stage commit satisfied for %s.%s tableId=%s stageId=%s currentSnapshot=%s"
            + " snapshotCount=%d",
        namespace, table, tableId, stageId, snapshotStr, snapshotCount);
  }

  private void syncExternalSnapshotsIfNeeded(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Table committedTable,
      CommitTableResponseDto responseDto,
      TableRequests.Commit req,
      String idempotencyKey) {
    String metadataLocation = responseDto == null ? null : responseDto.metadataLocation();
    if (!isExternalLocationTable(committedTable, metadataLocation)) {
      return;
    }
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    String previousLocation =
        committedTable == null
            ? null
            : MetadataLocationUtil.metadataLocation(committedTable.getPropertiesMap());
    if (previousLocation != null && metadataLocation.equals(previousLocation)) {
      return;
    }
    try {
      Map<String, String> ioProps =
          committedTable == null
              ? Map.of()
              : FileIoFactory.filterIoProperties(committedTable.getPropertiesMap());
      var imported = tableMetadataImportService.importMetadata(metadataLocation, ioProps);
      snapshotMetadataService.syncSnapshotsFromImportedMetadata(
          tableSupport,
          tableId,
          namespacePath,
          tableName,
          () -> committedTable,
          imported,
          idempotencyKey,
          true);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Snapshot sync from metadata failed for %s.%s (metadata=%s)",
          namespacePath,
          tableName,
          metadataLocation);
    }
  }

  private void syncSnapshotMetadataFromCommit(
      TableGatewaySupport tableSupport,
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Table committedTable,
      CommitTableResponseDto responseDto,
      String idempotencyKey) {
    if (tableId == null || responseDto == null) {
      return;
    }
    String metadataLocation = responseDto.metadataLocation();
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    Map<String, String> ioProps =
        committedTable == null
            ? Map.of()
            : FileIoFactory.filterIoProperties(committedTable.getPropertiesMap());
    try {
      var imported = tableMetadataImportService.importMetadata(metadataLocation, ioProps);
      snapshotMetadataService.syncSnapshotsFromImportedMetadata(
          tableSupport,
          tableId,
          namespacePath,
          tableName,
          () -> committedTable,
          imported,
          idempotencyKey,
          false);
    } catch (Exception e) {
      LOG.warnf(
          e,
          "Snapshot sync from commit metadata failed for %s.%s (metadata=%s)",
          namespacePath,
          tableName,
          metadataLocation);
    }
  }

  private boolean isExternalLocationTable(Table table, String metadataLocation) {
    if (table == null) {
      return false;
    }
    if (!table.hasUpstream()) {
      return metadataLocation != null && !metadataLocation.isBlank();
    }
    String uri = table.getUpstream().getUri();
    return uri != null && !uri.isBlank();
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
    if (LOG.isDebugEnabled()) {
      String namespace =
          namespacePath == null
              ? "<missing>"
              : (namespacePath.isEmpty() ? "<empty>" : String.join(".", namespacePath));
      String connector =
          connectorId == null || connectorId.getId().isBlank() ? "<missing>" : connectorId.getId();
      LOG.debugf(
          "Connector sync request namespace=%s table=%s connectorId=%s",
          namespace, tableName == null ? "<missing>" : tableName, connector);
    }
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
      String stageId,
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
