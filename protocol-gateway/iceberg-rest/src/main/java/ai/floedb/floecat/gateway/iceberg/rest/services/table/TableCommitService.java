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
import ai.floedb.floecat.gateway.iceberg.rest.common.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.compat.TableFormatSupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableCommitService {
  private static final Logger LOG = Logger.getLogger(TableCommitService.class);

  @Inject IcebergGatewayConfig config;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitSideEffectService sideEffectService;
  @Inject TableCommitMaterializationService materializationService;
  @Inject CommitResponseBuilder responseBuilder;
  @Inject SnapshotMetadataService snapshotMetadataService;
  @Inject TableMetadataImportService tableMetadataImportService;
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

    PreCommitMaterializationResult preMaterialization =
        preMaterializeForAtomicCommit(command, req, preCommitTable);
    if (preMaterialization.error() != null) {
      return preMaterialization.error();
    }
    TableRequests.Commit effectiveReq = preMaterialization.request();

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

    return buildCommitResponse(command, effectiveReq);
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
    if (!responseBuilder.containsSnapshotUpdates(req)) {
      syncExternalSnapshotsIfNeeded(
          tableSupport,
          tableId,
          command.namespacePath(),
          command.table(),
          committedTable,
          responseDto,
          req,
          command.idempotencyKey());
      syncSnapshotMetadataFromCommit(
          tableSupport,
          tableId,
          command.namespacePath(),
          command.table(),
          committedTable,
          responseDto,
          command.idempotencyKey());
    }

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

  private PreCommitMaterializationResult preMaterializeForAtomicCommit(
      CommitCommand command, TableRequests.Commit req, Table preCommitTable) {
    if (command == null
        || req == null
        || command.tableSupport() == null
        || preCommitTable == null) {
      return new PreCommitMaterializationResult(req, null);
    }
    if (!preCommitTable.hasResourceId()) {
      return new PreCommitMaterializationResult(req, null);
    }
    ResourceId tableId = preCommitTable.getResourceId();
    IcebergMetadata metadata = command.tableSupport().loadCurrentMetadata(preCommitTable);
    CommitTableResponseDto initialResponse =
        responseBuilder.buildInitialResponse(
            command.table(), preCommitTable, tableId, null, req, command.tableSupport(), metadata);
    if (initialResponse == null || initialResponse.metadata() == null) {
      return new PreCommitMaterializationResult(req, null);
    }
    MaterializeMetadataResult materialized =
        materializationService.materializeMetadata(
            command.namespace(),
            tableId,
            command.table(),
            preCommitTable,
            initialResponse.metadata(),
            initialResponse.metadataLocation());
    if (materialized == null) {
      return new PreCommitMaterializationResult(req, null);
    }
    if (materialized.error() != null) {
      return new PreCommitMaterializationResult(null, materialized.error());
    }
    String resolvedLocation = materialized.metadataLocation();
    if (resolvedLocation == null || resolvedLocation.isBlank()) {
      return new PreCommitMaterializationResult(req, null);
    }
    return new PreCommitMaterializationResult(
        withMetadataLocationProperty(req, resolvedLocation), null);
  }

  private TableRequests.Commit withMetadataLocationProperty(
      TableRequests.Commit req, String metadataLocation) {
    if (req == null || metadataLocation == null || metadataLocation.isBlank()) {
      return req;
    }
    List<Map<String, Object>> updates =
        req.updates() == null ? List.of() : List.copyOf(req.updates());
    List<Map<String, Object>> merged = new java.util.ArrayList<>();
    boolean hasMetadataLocation = false;
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      Object actionObj = update.get("action");
      String action = actionObj instanceof String s ? s : null;
      if (!"set-properties".equals(action)) {
        merged.add(update);
        continue;
      }
      Object rawProps = update.get("updates");
      Map<String, Object> props = new LinkedHashMap<>();
      if (rawProps instanceof Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (entry.getKey() != null && entry.getValue() != null) {
            props.put(String.valueOf(entry.getKey()), entry.getValue());
          }
        }
      }
      Object existing = props.put("metadata-location", metadataLocation);
      hasMetadataLocation = true;
      if (existing != null && metadataLocation.equals(String.valueOf(existing))) {
        merged.add(update);
      } else {
        Map<String, Object> rewritten = new LinkedHashMap<>(update);
        rewritten.put("updates", Map.copyOf(props));
        merged.add(Map.copyOf(rewritten));
      }
    }
    if (!hasMetadataLocation) {
      Map<String, Object> setProps = new LinkedHashMap<>();
      setProps.put("action", "set-properties");
      setProps.put("updates", Map.of("metadata-location", metadataLocation));
      merged.add(Map.copyOf(setProps));
    }
    return new TableRequests.Commit(req.requirements(), List.copyOf(merged));
  }

  private record PreCommitMaterializationResult(TableRequests.Commit request, Response error) {}

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
      Map<String, String> ioProps = resolveCommitIoProperties(tableSupport, committedTable);
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
    Map<String, String> ioProps = resolveCommitIoProperties(tableSupport, committedTable);
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

  private Map<String, String> resolveCommitIoProperties(
      TableGatewaySupport tableSupport, Table committedTable) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (tableSupport != null) {
      merged.putAll(tableSupport.defaultFileIoProperties());
    }
    if (committedTable != null) {
      merged.putAll(FileIoFactory.filterIoProperties(committedTable.getPropertiesMap()));
    }
    return merged.isEmpty() ? Map.of() : Map.copyOf(merged);
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
