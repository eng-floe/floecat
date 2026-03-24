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

package ai.floedb.floecat.gateway.iceberg.rest.table;

import static ai.floedb.floecat.gateway.iceberg.rest.support.TableMappingUtil.firstNonBlank;

import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.NamespaceRef;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.support.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.CommitUpdateInspector;
import ai.floedb.floecat.gateway.iceberg.rest.support.FileIoFactory;
import ai.floedb.floecat.gateway.iceberg.rest.support.GrpcServiceFacade;
import ai.floedb.floecat.gateway.iceberg.rest.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.table.IcebergMetadataService.ImportedSnapshot;
import ai.floedb.floecat.gateway.iceberg.rest.table.transaction.TransactionCommitService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;

@ApplicationScoped
public class TableLifecycleService {
  private static final Logger LOG = Logger.getLogger(TableLifecycleService.class);
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      CommitUpdateInspector.assertCreateRequirements();

  @Inject AccountContext accountContext;
  @Inject ObjectMapper mapper;
  @Inject TableGatewaySupport tableGatewaySupport;
  @Inject IcebergMetadataService icebergMetadataService;
  @Inject StagedTableRepository stagedTableRepository;
  @Inject GrpcServiceFacade grpcClient;
  @Inject TransactionCommitService transactionCommitService;

  public Response createTable(
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
        applyDefaultLocationIfMissing(namespaceContext, tableName, request);
    if (Boolean.TRUE.equals(effectiveReq.stageCreate())) {
      return handleStageCreate(
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

    ai.floedb.floecat.catalog.rpc.Table created;
    try {
      ResourceId tableId =
          tableGatewaySupport.resolveTableId(
              namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
      created = tableGatewaySupport.getTable(tableId);
    } catch (Exception e) {
      return IcebergErrorResponses.failure(
          "Failed to load created table",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    try {
      storePendingCreateBootstrap(
          namespaceContext, tableName, effectiveReq, created, idempotencyKey, tableSupport);
    } catch (JsonProcessingException e) {
      return IcebergErrorResponses.failure(
          "Failed to persist pending create bootstrap",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    List<StorageCredentialDto> credentials;
    try {
      credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    try {
      var resolved =
          icebergMetadataService.resolveMetadata(
              tableName,
              created,
              tableSupport,
              () ->
                  SnapshotLister.fetchSnapshots(
                      grpcClient, created.getResourceId(), SnapshotLister.Mode.ALL, null));
      String metadataLocation =
          resolved.metadataView() == null ? null : resolved.metadataView().metadataLocation();
      if ((metadataLocation == null || metadataLocation.isBlank())
          && effectiveReq.location() != null
          && !effectiveReq.location().isBlank()) {
        String tableUuid =
            firstNonBlank(
                resolved.metadataView() == null ? null : resolved.metadataView().tableUuid(),
                created.getPropertiesMap().get("table-uuid"),
                created.hasResourceId() ? created.getResourceId().getId() : null);
        if (tableUuid != null && !tableUuid.isBlank()) {
          String reservedMetadataLocation =
              icebergMetadataService.reserveCreateMetadataLocation(
                  effectiveReq.location(),
                  tableUuid,
                  stageCreateFileIoProperties(effectiveReq, tableSupport));
          resolved =
              new IcebergMetadataService.ResolvedMetadata(
                  resolved.tableMetadata(),
                  resolved.metadataView() == null
                      ? null
                      : resolved.metadataView().withMetadataLocation(reservedMetadataLocation),
                  resolved.icebergMetadata());
        }
      }
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResult(
              resolved.metadataView(), tableSupport.defaultTableConfig(), credentials);
      LOG.infof(
          "Create table response namespace=%s table=%s metadata=%s location=%s configKeys=%s",
          namespaceContext.namespacePath(),
          tableName,
          loadResult.metadata() == null ? "<null>" : loadResult.metadata().metadataLocation(),
          loadResult.metadataLocation(),
          loadResult.config().keySet());
      return Response.ok(loadResult).build();
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private void storePendingCreateBootstrap(
      NamespaceRef namespaceContext,
      String tableName,
      TableRequests.Create effectiveReq,
      ai.floedb.floecat.catalog.rpc.Table created,
      String idempotencyKey,
      TableGatewaySupport tableSupport)
      throws JsonProcessingException {
    if (namespaceContext == null
        || effectiveReq == null
        || created == null
        || tableSupport == null
        || hasCurrentSnapshot(created)) {
      return;
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return;
    }
    TableSpec spec =
        tableSupport
            .buildCreateSpec(
                namespaceContext.catalogId(),
                namespaceContext.namespaceId(),
                tableName,
                effectiveReq)
            .build();
    String stageId =
        firstNonBlank(
            idempotencyKey, created.getResourceId().getId(), UUID.randomUUID().toString());
    stagedTableRepository.saveStage(
        new StagedTableEntry(
            new StagedTableKey(
                accountId,
                namespaceContext.catalogName(),
                namespaceContext.namespacePath(),
                tableName,
                stageId),
            namespaceContext.catalogId(),
            namespaceContext.namespaceId(),
            effectiveReq,
            spec,
            STAGE_CREATE_REQUIREMENTS,
            StageState.STAGED,
            null,
            null,
            idempotencyKey));
  }

  private boolean hasCurrentSnapshot(ai.floedb.floecat.catalog.rpc.Table table) {
    if (table == null || table.getPropertiesMap().isEmpty()) {
      return false;
    }
    String currentSnapshotId = table.getPropertiesMap().get("current-snapshot-id");
    return currentSnapshotId != null && !currentSnapshotId.isBlank();
  }

  public Response registerTable(
      NamespaceRef namespaceContext,
      String idempotencyKey,
      TableRequests.Register req,
      TableGatewaySupport tableSupport) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String tableName = req.name().trim();

    Map<String, String> ioProperties =
        tableSupport.resolveRegisterFileIoProperties(req.properties());
    ImportedMetadata importedMetadata;
    try {
      importedMetadata = icebergMetadataService.importMetadata(metadataLocation, ioProperties);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    boolean overwrite = Boolean.TRUE.equals(req.overwrite());
    ai.floedb.floecat.catalog.rpc.Table existing = null;
    if (overwrite) {
      try {
        ResourceId existingTableId =
            tableGatewaySupport.resolveTableId(
                namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
        existing = tableGatewaySupport.getTable(existingTableId);
      } catch (StatusRuntimeException e) {
        if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
          throw e;
        }
      }
    }
    Response commitResponse =
        commitRegisteredTable(
            namespaceContext,
            tableName,
            metadataLocation,
            idempotencyKey,
            ioProperties,
            importedMetadata,
            existing,
            tableSupport);
    if (commitResponse != null) {
      return commitResponse;
    }

    ResourceId tableId =
        tableGatewaySupport.resolveTableId(
            namespaceContext.catalogName(), namespaceContext.namespacePath(), tableName);
    tableGatewaySupport.getTable(tableId);

    if (importedMetadata.metadataView() == null) {
      return IcebergErrorResponses.failure(
          "Failed to load canonical registered table metadata",
          "InternalServerError",
          Response.Status.INTERNAL_SERVER_ERROR);
    }
    return Response.ok(
            TableResponseMapper.toLoadResult(
                importedMetadata.metadataView(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()))
        .build();
  }

  private Response handleStageCreate(
      NamespaceRef namespaceContext,
      String tableName,
      TableRequests.Create request,
      String transactionId,
      String idempotencyKey,
      String accessDelegationMode,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      return IcebergErrorResponses.validation("stage-create requires a request body");
    }
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(namespaceContext, tableName, request);
    if (effectiveReq.location() == null || effectiveReq.location().isBlank()) {
      LOG.warnf(
          "Stage-create request missing location prefix=%s namespace=%s table=%s payload=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          safeSerializeCreate(request));
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    String stageId = firstNonBlank(transactionId, idempotencyKey);
    if (stageId == null || stageId.isBlank()) {
      stageId = UUID.randomUUID().toString();
    }
    try {
      LOG.infof(
          "Stage-create request payload prefix=%s namespace=%s table=%s stageId=%s location=%s"
              + " properties=%s",
          namespaceContext.prefix(),
          namespaceContext.namespacePath(),
          tableName,
          stageId,
          effectiveReq.location(),
          effectiveReq.properties());
      ai.floedb.floecat.catalog.rpc.Table stubTable =
          ai.floedb.floecat.catalog.rpc.Table.newBuilder()
              .setCatalogId(namespaceContext.catalogId())
              .setNamespaceId(namespaceContext.namespaceId())
              .setDisplayName(tableName)
              .build();
      BootstrapResult bootstrap =
          bootstrapStageCreate(tableName, stubTable, effectiveReq, tableSupport);
      effectiveReq = bootstrap.request();
      TableSpec spec =
          tableSupport
              .buildCreateSpec(
                  namespaceContext.catalogId(),
                  namespaceContext.namespaceId(),
                  tableName,
                  effectiveReq)
              .build();
      StagedTableEntry entry =
          new StagedTableEntry(
              new StagedTableKey(
                  accountId,
                  namespaceContext.catalogName(),
                  namespaceContext.namespacePath(),
                  tableName,
                  stageId),
              namespaceContext.catalogId(),
              namespaceContext.namespaceId(),
              effectiveReq,
              spec,
              STAGE_CREATE_REQUIREMENTS,
              StageState.STAGED,
              null,
              null,
              idempotencyKey);
      StagedTableEntry stored = stagedTableRepository.saveStage(entry);
      LOG.infof(
          "Stored stage-create payload account=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          accountContext.getAccountId(),
          namespaceContext.catalogName(),
          namespaceContext.namespacePath(),
          tableName,
          stored.key().stageId(),
          transactionId);
      List<StorageCredentialDto> credentials;
      try {
        credentials = tableSupport.credentialsForAccessDelegation(accessDelegationMode);
      } catch (IllegalArgumentException e) {
        stagedTableRepository.deleteStage(stored.key());
        return IcebergErrorResponses.validation(e.getMessage());
      }
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResult(
              bootstrap.metadataView().withMetadataLocation(bootstrap.metadataLocation()),
              tableSupport.defaultTableConfig(),
              credentials);
      LOG.infof(
          "Stage-create metadata resolved stageId=%s location=%s",
          stored.key().stageId(), loadResult.metadataLocation());
      LOG.infof(
          "Stage-create response stageId=%s metadataLocation=%s configKeys=%s",
          stored.key().stageId(), loadResult.metadataLocation(), loadResult.config().keySet());
      return Response.ok(loadResult).build();
    } catch (IllegalStateException e) {
      return IcebergErrorResponses.conflict(e.getMessage());
    } catch (IllegalArgumentException | com.fasterxml.jackson.core.JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  private TableRequests.Create applyDefaultLocationIfMissing(
      NamespaceRef namespaceContext, String tableName, TableRequests.Create request) {
    if (request == null) {
      return null;
    }
    if (request.location() != null && !request.location().isBlank()) {
      return request;
    }
    String resolved = tableGatewaySupport.defaultTableLocation(namespaceContext, tableName);
    if (resolved == null || resolved.isBlank()) {
      return request;
    }
    return new TableRequests.Create(
        request.name(),
        request.schema(),
        resolved,
        request.properties(),
        request.partitionSpec(),
        request.writeOrder(),
        request.stageCreate());
  }

  private String safeSerializeCreate(TableRequests.Create request) {
    if (request == null) {
      return "<null>";
    }
    try {
      return mapper.writeValueAsString(request);
    } catch (Exception e) {
      return String.valueOf(request);
    }
  }

  private boolean hasSchema(TableRequests.Create request) {
    return request != null && request.schema() != null && !request.schema().isNull();
  }

  private BootstrapResult bootstrapStageCreate(
      String tableName,
      ai.floedb.floecat.catalog.rpc.Table table,
      TableRequests.Create request,
      TableGatewaySupport tableSupport) {
    if (request == null) {
      throw new IllegalArgumentException("create request is required");
    }
    String tableLocation = request.location();
    if (tableLocation == null || tableLocation.isBlank()) {
      throw new IllegalArgumentException("stage-create requires a table location");
    }
    Map<String, String> props = new LinkedHashMap<>();
    if (request.properties() != null && !request.properties().isEmpty()) {
      props.putAll(request.properties());
    }
    props.putIfAbsent("table-uuid", UUID.randomUUID().toString());
    String tableUuid = props.get("table-uuid");
    if (!props.containsKey(MetadataLocationUtil.PRIMARY_KEY)) {
      String reservedMetadataLocation =
          icebergMetadataService.reserveCreateMetadataLocation(
              tableLocation, tableUuid, stageCreateFileIoProperties(request, tableSupport));
      MetadataLocationUtil.setMetadataLocation(props, reservedMetadataLocation);
    }
    String metadataLocation = MetadataLocationUtil.metadataLocation(props);
    TableRequests.Create effectiveRequest =
        new TableRequests.Create(
            request.name(),
            request.schema(),
            request.location(),
            Map.copyOf(props),
            request.partitionSpec(),
            request.writeOrder(),
            request.stageCreate());
    TableMetadataView metadataView =
        TableMetadataBuilder.fromCreateRequest(tableName, table, effectiveRequest)
            .withMetadataLocation(metadataLocation);
    return new BootstrapResult(effectiveRequest, metadataView, metadataLocation);
  }

  private Map<String, String> stageCreateFileIoProperties(
      TableRequests.Create request, TableGatewaySupport tableSupport) {
    Map<String, String> ioProps =
        tableSupport == null
            ? new LinkedHashMap<>()
            : new LinkedHashMap<>(tableSupport.defaultFileIoProperties());
    if (request != null && request.properties() != null && !request.properties().isEmpty()) {
      ioProps.putAll(FileIoFactory.filterIoProperties(request.properties()));
    }
    return ioProps;
  }

  private Response commitRegisteredTable(
      NamespaceRef namespaceContext,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      Map<String, String> ioProperties,
      ImportedMetadata importedMetadata,
      ai.floedb.floecat.catalog.rpc.Table existing,
      TableGatewaySupport tableSupport) {
    boolean assertCreate = existing == null;
    List<Long> existingSnapshotIds =
        existing == null ? List.of() : listSnapshotIds(existing.getResourceId());
    Map<String, String> props =
        mergeRegisteredProperties(
            existing == null ? null : existing.getPropertiesMap(),
            importedMetadata,
            metadataLocation,
            ioProperties);
    Response response =
        transactionCommitService.commit(
            namespaceContext.prefix(),
            idempotencyKey,
            buildRegisterCommitRequest(
                namespaceContext.namespacePath(),
                tableName,
                props,
                importedMetadata,
                existingSnapshotIds,
                assertCreate),
            tableSupport);
    if (response == null || response.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
      return null;
    }
    if (assertCreate && response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
      return IcebergErrorResponses.conflict("Table already exists");
    }
    return response;
  }

  private TransactionCommitRequest buildRegisterCommitRequest(
      List<String> namespacePath,
      String tableName,
      Map<String, String> mergedProps,
      ImportedMetadata importedMetadata,
      List<Long> existingSnapshotIds,
      boolean assertCreate) {
    List<Map<String, Object>> requirements = new ArrayList<>();
    if (assertCreate) {
      requirements.addAll(CommitUpdateInspector.assertCreateRequirements());
    }
    List<Map<String, Object>> updates = new ArrayList<>();
    Map<String, String> props = mergedProps == null ? Map.of() : new LinkedHashMap<>(mergedProps);
    if (!props.isEmpty()) {
      updates.add(Map.of("action", CommitUpdateInspector.ACTION_SET_PROPERTIES, "updates", props));
    }
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      updates.add(
          Map.of("action", CommitUpdateInspector.ACTION_SET_LOCATION, "location", location));
    }
    List<Long> importedSnapshotIds = new ArrayList<>();
    for (ImportedSnapshot snapshot : snapshotsToImport(importedMetadata)) {
      Map<String, Object> snapshotMap = toSnapshotUpdate(snapshot, importedMetadata);
      if (!snapshotMap.isEmpty()) {
        updates.add(
            Map.of("action", CommitUpdateInspector.ACTION_ADD_SNAPSHOT, "snapshot", snapshotMap));
      }
      if (snapshot != null && snapshot.snapshotId() != null) {
        importedSnapshotIds.add(snapshot.snapshotId());
      }
    }
    if (existingSnapshotIds != null
        && !existingSnapshotIds.isEmpty()
        && !importedSnapshotIds.isEmpty()) {
      List<Long> removals =
          existingSnapshotIds.stream().filter(id -> !importedSnapshotIds.contains(id)).toList();
      if (!removals.isEmpty()) {
        updates.add(
            Map.of(
                "action", CommitUpdateInspector.ACTION_REMOVE_SNAPSHOTS, "snapshot-ids", removals));
      }
    }
    return new TransactionCommitRequest(
        List.of(
            new TransactionCommitRequest.TableChange(
                new TableIdentifierDto(namespacePath, tableName), requirements, updates)));
  }

  private List<ImportedSnapshot> snapshotsToImport(ImportedMetadata importedMetadata) {
    if (importedMetadata == null) {
      return List.of();
    }
    if (importedMetadata.snapshots() != null) {
      return importedMetadata.snapshots();
    }
    if (importedMetadata.currentSnapshot() == null) {
      return List.of();
    }
    return List.of(importedMetadata.currentSnapshot());
  }

  private Map<String, Object> toSnapshotUpdate(
      ImportedSnapshot snapshot, ImportedMetadata importedMetadata) {
    if (snapshot == null) {
      return Map.of();
    }
    return snapshot.toSnapshotUpdate(
        importedMetadata == null ? null : importedMetadata.schemaJson());
  }

  private List<Long> listSnapshotIds(ResourceId tableId) {
    if (tableId == null) {
      return List.of();
    }
    try {
      return grpcClient
          .listSnapshots(ListSnapshotsRequest.newBuilder().setTableId(tableId).build())
          .getSnapshotsList()
          .stream()
          .map(Snapshot::getSnapshotId)
          .toList();
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return List.of();
      }
      throw e;
    }
  }

  private Map<String, String> mergeRegisteredProperties(
      Map<String, String> existing,
      ImportedMetadata importedMetadata,
      String metadataLocation,
      Map<String, String> registerIoProperties) {
    Map<String, String> merged = new LinkedHashMap<>();
    if (existing != null && !existing.isEmpty()) {
      merged.putAll(existing);
    }
    if (importedMetadata != null && importedMetadata.properties() != null) {
      merged.putAll(importedMetadata.properties());
    }
    if (importedMetadata != null
        && importedMetadata.tableLocation() != null
        && !importedMetadata.tableLocation().isBlank()) {
      merged.put("location", importedMetadata.tableLocation());
    }
    if (registerIoProperties != null && !registerIoProperties.isEmpty()) {
      merged.putAll(registerIoProperties);
    }
    MetadataLocationUtil.setMetadataLocation(merged, metadataLocation);
    return merged;
  }

  private record BootstrapResult(
      TableRequests.Create request, TableMetadataView metadataView, String metadataLocation) {}
}
