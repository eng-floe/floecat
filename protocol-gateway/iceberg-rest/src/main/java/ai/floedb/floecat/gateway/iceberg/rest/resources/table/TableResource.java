package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.CredentialsResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.PlanResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StageCreateResponseDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TaskRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.MaterializeMetadataResult;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.MetadataLocationSync;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotLister;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.SnapshotMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableDropCleanupService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.floecat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.floecat.gateway.iceberg.rest.services.planning.PlanTaskManager;
import ai.floedb.floecat.gateway.iceberg.rest.services.planning.TablePlanService;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.floecat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.floecat.gateway.iceberg.rest.support.MetadataLocationUtil;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.floecat.gateway.iceberg.rpc.IcebergMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

@Path("/v1/{prefix}/namespaces/{namespace}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  private static final Logger LOG = Logger.getLogger(TableResource.class);
  private static final List<Map<String, Object>> STAGE_CREATE_REQUIREMENTS =
      List.of(Map.of("type", "assert-create"));

  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;
  @Inject StagedTableService stagedTableService;
  @Inject AccountContext accountContext;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableDropCleanupService tableDropCleanupService;
  @Inject TableCommitService tableCommitService;
  @Inject TablePlanService tablePlanService;
  @Inject PlanTaskManager planTaskManager;
  @Inject TableMetadataImportService tableMetadataImportService;
  @Inject SnapshotMetadataService snapshotMetadataService;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport = new TableGatewaySupport(grpc, config, mapper, mpConfig);
  }

  @GET
  @Path("/tables")
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    try {
      TableLifecycleService.ListTablesResult result =
          tableLifecycleService.listTables(catalogName, namespace, pageSize, pageToken);
      Map<String, Object> body = new LinkedHashMap<>();
      body.put("identifiers", result.identifiers());
      if (result.nextPageToken() != null) {
        body.put("next-page-token", result.nextPageToken());
      }
      return Response.ok(body).build();
    } catch (StatusRuntimeException e) {
      return IcebergErrorResponses.grpcError(e);
    }
  }

  @POST
  @Path("/tables")
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Create req) {
    if (req == null) {
      return IcebergErrorResponses.validation("Request body is required");
    }
    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    if (!hasSchema(req)) {
      return IcebergErrorResponses.validation("schema is required");
    }
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);

    String tableName = req.name().trim();
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(prefix, namespacePath, tableName, req);
    if (Boolean.TRUE.equals(effectiveReq.stageCreate())) {
      return handleStageCreate(
          prefix,
          catalogName,
          catalogId,
          namespaceId,
          namespacePath,
          tableName,
          effectiveReq,
          transactionId,
          idempotencyKey);
    }

    TableSpec.Builder spec;
    try {
      spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, effectiveReq);
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    Table created = tableLifecycleService.createTable(spec, idempotencyKey);
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(created);
    Map<String, String> tableConfig = tableSupport.defaultTableConfig();
    List<StorageCredentialDto> credentials = tableSupport.defaultCredentials();
    LoadTableResultDto loadResult;
    if (metadata == null && req != null) {
      try {
        loadResult =
            TableResponseMapper.toLoadResultFromCreate(
                tableName, created, req, tableConfig, credentials);
      } catch (IllegalArgumentException e) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
    } else {
      loadResult =
          TableResponseMapper.toLoadResult(
              tableName, created, metadata, List.of(), tableConfig, credentials);
    }

    MaterializeMetadataResult materializationResult =
        tableCommitService.materializeMetadata(
            namespace,
            created.getResourceId(),
            tableName,
            created,
            loadResult.metadata(),
            loadResult.metadataLocation());
    if (materializationResult.error() != null) {
      return materializationResult.error();
    }
    TableMetadataView responseMetadata =
        materializationResult.metadata() != null
            ? materializationResult.metadata()
            : loadResult.metadata();
    String responseMetadataLocation =
        nonBlank(materializationResult.metadataLocation(), loadResult.metadataLocation());
    String preferredMetadataLocation =
        nonBlank(
            req == null ? null : tableSupport.metadataLocationFromCreate(req),
            metadataLocation(created, metadata));
    if (preferredMetadataLocation != null && !preferredMetadataLocation.isBlank()) {
      responseMetadataLocation = preferredMetadataLocation;
    }
    if (responseMetadata != null && responseMetadataLocation != null) {
      responseMetadata = responseMetadata.withMetadataLocation(responseMetadataLocation);
    }
    loadResult =
        new LoadTableResultDto(
            responseMetadataLocation,
            responseMetadata,
            loadResult.config(),
            loadResult.storageCredentials());

    String requestedLocation = req != null ? req.location() : null;
    String metadataForLocation =
        nonBlank(loadResult.metadataLocation(), tableSupport.metadataLocationFromCreate(req));
    String externalUri = tableSupport.resolveTableLocation(requestedLocation, metadataForLocation);
    configureConnectorAndSync(
        prefix,
        namespacePath,
        namespaceId,
        catalogId,
        tableName,
        created.getResourceId(),
        metadataForLocation,
        externalUri,
        null,
        idempotencyKey);

    Response.ResponseBuilder builder = Response.ok(loadResult);
    String etagValue = metadataLocation(created, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private Response handleStageCreate(
      String prefix,
      String catalogName,
      ResourceId catalogId,
      ResourceId namespaceId,
      List<String> namespacePath,
      String tableName,
      TableRequests.Create req,
      String transactionId,
      String idempotencyKey) {
    if (req == null) {
      return IcebergErrorResponses.validation("stage-create requires a request body");
    }
    TableRequests.Create effectiveReq =
        applyDefaultLocationIfMissing(prefix, namespacePath, tableName, req);
    if (effectiveReq.location() == null || effectiveReq.location().isBlank()) {
      LOG.warnf(
          "Stage-create request missing location prefix=%s namespace=%s table=%s payload=%s",
          prefix, namespacePath, tableName, safeSerializeCreate(req));
      return IcebergErrorResponses.validation("location is required");
    }
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    String stageId =
        (transactionId == null || transactionId.isBlank())
            ? UUID.randomUUID().toString()
            : transactionId.trim();
    try {
      TableSpec.Builder spec =
          tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, effectiveReq);
      StagedTableEntry entry =
          new StagedTableEntry(
              new StagedTableKey(accountId, catalogName, namespacePath, tableName, stageId),
              catalogId,
              namespaceId,
              effectiveReq,
              spec.build(),
              STAGE_CREATE_REQUIREMENTS,
              StageState.STAGED,
              null,
              null,
              idempotencyKey);
      StagedTableEntry stored = stagedTableService.saveStage(entry);
      LOG.infof(
          "Stored stage-create payload account=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          accountContext.getAccountId(),
          catalogName,
          namespacePath,
          tableName,
          stored.key().stageId(),
          transactionId);
      Table stubTable =
          Table.newBuilder()
              .setCatalogId(catalogId)
              .setNamespaceId(namespaceId)
              .setDisplayName(tableName)
              .build();
      LoadTableResultDto loadResult =
          TableResponseMapper.toLoadResultFromCreate(
              tableName,
              stubTable,
              effectiveReq,
              tableSupport.defaultTableConfig(),
              tableSupport.defaultCredentials());
      return Response.ok(
              new StageCreateResponseDto(
                  loadResult.metadataLocation(),
                  loadResult.metadata(),
                  loadResult.config(),
                  loadResult.storageCredentials(),
                  stored.requirements(),
                  stored.key().stageId()))
          .build();
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
  }

  @Path("/tables/{table}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots,
      @HeaderParam("If-None-Match") String ifNoneMatch) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespace, table);
    Table tableRecord = tableLifecycleService.getTable(tableId);
    SnapshotLister.Mode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(tableRecord);
    List<Snapshot> snapshotList =
        SnapshotLister.fetchSnapshots(grpc, tableId, snapshotMode, metadata);
    String etagValue = metadataLocation(tableRecord, metadata);
    if (etagMatches(etagValue, ifNoneMatch)) {
      return Response.status(Response.Status.NOT_MODIFIED).tag(etagValue).build();
    }
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                table,
                tableRecord,
                metadata,
                snapshotList,
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  @Path("/tables/{table}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    tableLifecycleService.resolveTableId(catalogName, namespace, table);
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("purgeRequested") Boolean purgeRequested) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespacePath, table);
    ResourceId connectorId = null;
    Table existing = null;
    try {
      existing = tableLifecycleService.getTable(tableId);
      if (existing.hasUpstream() && existing.getUpstream().hasConnectorId()) {
        connectorId = existing.getUpstream().getConnectorId();
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    boolean purge = Boolean.TRUE.equals(purgeRequested);
    if (purge) {
      tableDropCleanupService.purgeTableData(catalogName, namespace, table, existing);
    }
    tableLifecycleService.deleteTable(tableId, purge);
    if (connectorId != null) {
      tableSupport.deleteConnector(connectorId);
    }
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TableRequests.Commit req) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
    return tableCommitService.commit(
        new TableCommitService.CommitCommand(
            prefix,
            namespace,
            namespacePath,
            table,
            catalogName,
            catalogId,
            namespaceId,
            idempotencyKey,
            transactionId,
            req,
            tableSupport));
  }

  @Path("/tables/{table}/plan")
  @POST
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      PlanRequests.Plan rawRequest) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespace, table);
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (startSnapshotId != null && endSnapshotId != null && startSnapshotId >= endSnapshotId) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id must be less than end-snapshot-id");
    }
    Long resolvedSnapshotId = endSnapshotId != null ? endSnapshotId : snapshotId;
    if (startSnapshotId != null && resolvedSnapshotId == null) {
      return IcebergErrorResponses.validation(
          "start-snapshot-id requires snapshot-id or end-snapshot-id");
    }
    boolean caseSensitive =
        request.caseSensitive() == null || Boolean.TRUE.equals(request.caseSensitive());
    boolean useSnapshotSchema = Boolean.TRUE.equals(request.useSnapshotSchema());
    try {
      var handle =
          tablePlanService.startPlan(
              catalogName,
              tableId,
              copyOfOrNull(request.select()),
              startSnapshotId,
              endSnapshotId,
              snapshotId,
              copyOfOrNull(request.statsFields()),
              request.filter(),
              caseSensitive,
              useSnapshotSchema,
              request.minRowsRequested());
      PlanResponseDto planned =
          tablePlanService.fetchPlan(handle.queryId(), tableSupport.defaultCredentials());
      List<FileScanTaskDto> fileScanTasks = copyOfOrEmpty(planned.fileScanTasks());
      List<ContentFileDto> deleteFiles = copyOfOrEmptyContent(planned.deleteFiles());
      PlanTaskManager.PlanDescriptor descriptor =
          planTaskManager.registerCompletedPlan(
              handle.queryId(),
              namespace,
              table,
              fileScanTasks,
              deleteFiles,
              planned.storageCredentials());
      return Response.ok(
              new PlanResponseDto(
                  descriptor.status().value(),
                  descriptor.planId(),
                  descriptor.planTasks(),
                  fileScanTasks,
                  deleteFiles,
                  descriptor.credentials()))
          .build();
    } catch (IllegalArgumentException ex) {
      return IcebergErrorResponses.validation(ex.getMessage());
    }
  }

  @Path("/tables/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    tableLifecycleService.resolveTableId(
        CatalogResolver.resolveCatalog(config, prefix), namespace, table);
    return planTaskManager
        .findPlan(planId)
        .map(
            descriptor ->
                Response.ok(
                        new PlanResponseDto(
                            descriptor.status().value(),
                            descriptor.planId(),
                            descriptor.planTasks(),
                            descriptor.fileScanTasks(),
                            descriptor.deleteFiles(),
                            descriptor.credentials()))
                    .build())
        .orElseGet(() -> IcebergErrorResponses.notFound("plan " + planId + " not found"));
  }

  @Path("/tables/{table}/plan/{planId}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    tableLifecycleService.resolveTableId(
        CatalogResolver.resolveCatalog(config, prefix), namespace, table);
    planTaskManager.cancelPlan(planId);
    tablePlanService.cancelPlan(planId);
    return Response.noContent().build();
  }

  @Path("/tables/{table}/tasks")
  @POST
  public Response scanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      TaskRequests.Fetch request) {
    if (request == null || request.planTask() == null || request.planTask().isBlank()) {
      return IcebergErrorResponses.validation("plan-task is required");
    }
    tableLifecycleService.resolveTableId(
        CatalogResolver.resolveCatalog(config, prefix), namespace, table);
    return planTaskManager
        .consumeTask(namespace, table, request.planTask().trim())
        .map(response -> Response.ok(response).build())
        .orElseGet(() -> IcebergErrorResponses.notFound("plan-task not found"));
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    tableLifecycleService.resolveTableId(catalogName, namespace, table);
    return Response.ok(new CredentialsResponseDto(tableSupport.defaultCredentials())).build();
  }

  @Path("/tables/{table}/metrics")
  @POST
  public Response publishMetrics(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      MetricsRequests.Report request) {
    if (request == null || request.snapshotId() == null) {
      return IcebergErrorResponses.validation("snapshot-id is required");
    }
    try {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          namespace, table, mapper.writeValueAsString(request));
    } catch (JsonProcessingException e) {
      LOG.infof(
          "Received metrics report namespace=%s table=%s payload=%s",
          namespace, table, String.valueOf(request));
    }
    return Response.noContent().build();
  }

  @Path("/register")
  @POST
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Register req) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return IcebergErrorResponses.validation("metadata-location is required");
    }
    String metadataLocation = req.metadataLocation().trim();
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);

    if (req.name() == null || req.name().isBlank()) {
      return IcebergErrorResponses.validation("name is required");
    }
    String tableName = req.name().trim();

    Map<String, String> ioProperties =
        req != null && req.properties() != null
            ? new LinkedHashMap<>(req.properties())
            : new LinkedHashMap<>();
    ImportedMetadata importedMetadata;
    try {
      importedMetadata = tableMetadataImportService.importMetadata(metadataLocation, ioProperties);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    TableSpec.Builder spec = tableSupport.baseTableSpec(catalogId, namespaceId, tableName);
    if (importedMetadata.schemaJson() != null && !importedMetadata.schemaJson().isBlank()) {
      spec.setSchemaJson(importedMetadata.schemaJson());
    }
    Map<String, String> mergedProps =
        mergeImportedProperties(null, importedMetadata, metadataLocation);
    if (!mergedProps.isEmpty()) {
      spec.putAllProperties(mergedProps);
    }
    tableSupport.addMetadataLocationProperties(spec, metadataLocation);

    Table created;
    try {
      created = tableLifecycleService.createTable(spec, idempotencyKey);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.ALREADY_EXISTS) {
        if (Boolean.TRUE.equals(req.overwrite())) {
          return overwriteRegisteredTable(
              prefix,
              catalogName,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              metadataLocation,
              idempotencyKey,
              importedMetadata);
        }
        return IcebergErrorResponses.conflict("Table already exists");
      }
      throw e;
    }
    final Table initialCreated = created;
    Supplier<Table> createdSupplier = () -> initialCreated;
    Response snapshotBootstrap =
        snapshotMetadataService.ensureImportedCurrentSnapshot(
            tableSupport,
            created.getResourceId(),
            namespacePath,
            tableName,
            createdSupplier,
            importedMetadata,
            idempotencyKey);
    if (snapshotBootstrap != null) {
      return snapshotBootstrap;
    }
    Table ensuredCreated =
        MetadataLocationSync.ensureMetadataLocation(
            tableLifecycleService,
            tableSupport,
            initialCreated.getResourceId(),
            initialCreated,
            metadataLocation);
    if (ensuredCreated != null) {
      created = ensuredCreated;
    }

    String resolvedLocation =
        tableSupport.resolveTableLocation(
            importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
    configureConnectorAndSync(
        prefix,
        namespacePath,
        namespaceId,
        catalogId,
        tableName,
        created.getResourceId(),
        metadataLocation,
        resolvedLocation,
        null,
        idempotencyKey);

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(created);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                created,
                metadata,
                List.of(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(created, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private Response overwriteRegisteredTable(
      String prefix,
      String catalogName,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      String metadataLocation,
      String idempotencyKey,
      ImportedMetadata importedMetadata) {
    ResourceId tableId =
        tableLifecycleService.resolveTableId(catalogName, namespacePath, tableName);
    Table existing;
    try {
      existing = tableLifecycleService.getTable(tableId);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return IcebergErrorResponses.notFound(
            "Table " + String.join(".", namespacePath) + "." + tableName + " not found");
      }
      throw e;
    }

    Map<String, String> props =
        mergeImportedProperties(existing.getPropertiesMap(), importedMetadata, metadataLocation);
    LOG.infof(
        "Register overwrite merged metadata namespace=%s.%s merged=%s imported=%s",
        String.join(".", namespacePath),
        tableName,
        props.get("metadata-location"),
        metadataLocation);

    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("properties");
    TableSpec.Builder updateSpec = TableSpec.newBuilder().putAllProperties(props);
    tableSupport.addMetadataLocationProperties(updateSpec, metadataLocation);
    if (importedMetadata != null
        && importedMetadata.schemaJson() != null
        && !importedMetadata.schemaJson().isBlank()) {
      updateSpec.setSchemaJson(importedMetadata.schemaJson());
      mask.addPaths("schema_json");
    }
    Table updated =
        tableLifecycleService.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(updateSpec)
                .setUpdateMask(mask)
                .build());
    LOG.infof(
        "Register overwrite update response namespace=%s.%s metadata=%s",
        String.join(".", namespacePath),
        tableName,
        updated.getPropertiesMap().get("metadata-location"));
    Long currentSnapshotId = propertyLong(props, "current-snapshot-id");
    if (currentSnapshotId != null && currentSnapshotId > 0) {
      snapshotMetadataService.updateSnapshotMetadataLocation(
          tableId, currentSnapshotId, metadataLocation);
    }

    Table snapshotTable = updated;
    Response snapshotBootstrap =
        snapshotMetadataService.ensureImportedCurrentSnapshot(
            tableSupport,
            tableId,
            namespacePath,
            tableName,
            () -> snapshotTable,
            importedMetadata,
            idempotencyKey);
    if (snapshotBootstrap != null) {
      return snapshotBootstrap;
    }
    Table ensuredUpdated =
        MetadataLocationSync.ensureMetadataLocation(
            tableLifecycleService, tableSupport, tableId, updated, metadataLocation);
    if (ensuredUpdated != null) {
      updated = ensuredUpdated;
    }

    ResourceId connectorId =
        existing.hasUpstream() && existing.getUpstream().hasConnectorId()
            ? existing.getUpstream().getConnectorId()
            : null;
    String resolvedLocation =
        tableSupport.resolveTableLocation(
            importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
    if (connectorId == null) {
      configureConnectorAndSync(
          prefix,
          namespacePath,
          namespaceId,
          catalogId,
          tableName,
          tableId,
          metadataLocation,
          resolvedLocation,
          null,
          idempotencyKey);
    } else {
      tableSupport.updateConnectorMetadata(connectorId, metadataLocation);
      String existingUri =
          existing.hasUpstream() && existing.getUpstream().getUri() != null
              ? existing.getUpstream().getUri()
              : null;
      if (resolvedLocation != null
          && (existingUri == null || !resolvedLocation.equals(existingUri))) {
        tableSupport.updateTableUpstream(
            tableId, namespacePath, tableName, connectorId, resolvedLocation);
      }
      tableCommitService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
    }

    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(updated);
    Response.ResponseBuilder builder =
        Response.ok(
            TableResponseMapper.toLoadResult(
                tableName,
                updated,
                metadata,
                List.of(),
                tableSupport.defaultTableConfig(),
                tableSupport.defaultCredentials()));
    String etagValue = metadataLocation(updated, metadata);
    if (etagValue != null) {
      builder.tag(etagValue);
    }
    return builder.build();
  }

  private void configureConnectorAndSync(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String resolvedTableLocation,
      String existingUpstreamUri,
      String idempotencyKey) {
    var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
    ResourceId connectorId = null;
    String upstreamUri = null;
    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      connectorId =
          tableSupport.createTemplateConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              tableId,
              connectorTemplate,
              idempotencyKey);
      upstreamUri = connectorTemplate.uri();
    } else if (resolvedTableLocation != null && !resolvedTableLocation.isBlank()) {
      String metadata = nonBlank(metadataLocation, resolvedTableLocation);
      connectorId =
          tableSupport.createExternalConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              tableId,
              metadata,
              resolvedTableLocation,
              idempotencyKey);
      upstreamUri = resolvedTableLocation;
    }
    if (connectorId == null || upstreamUri == null || upstreamUri.isBlank()) {
      return;
    }
    if (existingUpstreamUri == null || !existingUpstreamUri.equals(upstreamUri)) {
      tableSupport.updateTableUpstream(tableId, namespacePath, tableName, connectorId, upstreamUri);
    }
    tableCommitService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
  }

  private Map<String, String> mergeImportedProperties(
      Map<String, String> existing, ImportedMetadata importedMetadata, String metadataLocation) {
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
    MetadataLocationUtil.setMetadataLocation(merged, metadataLocation);
    return merged;
  }

  private SnapshotLister.Mode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotLister.Mode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotLister.Mode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private boolean etagMatches(String etagValue, String ifNoneMatch) {
    if (etagValue == null || ifNoneMatch == null) {
      return false;
    }
    String token = ifNoneMatch.trim();
    if (token.startsWith("\"") && token.endsWith("\"") && token.length() >= 2) {
      token = token.substring(1, token.length() - 1);
    }
    return token.equals(etagValue);
  }

  private String metadataLocation(Table table, IcebergMetadata metadata) {
    Map<String, String> props =
        table == null || table.getPropertiesMap() == null ? Map.of() : table.getPropertiesMap();
    String propertyLocation = MetadataLocationUtil.metadataLocation(props);
    if (propertyLocation != null
        && !propertyLocation.isBlank()
        && !MetadataLocationUtil.isPointer(propertyLocation)) {
      return propertyLocation;
    }
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    if (propertyLocation != null && !propertyLocation.isBlank()) {
      return propertyLocation;
    }
    return table != null && table.hasResourceId() ? table.getResourceId().getId() : null;
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }

  private String safeSerializeCreate(TableRequests.Create req) {
    if (req == null) {
      return "<null>";
    }
    try {
      return mapper.writeValueAsString(req);
    } catch (JsonProcessingException e) {
      return String.valueOf(req);
    }
  }

  private static List<String> copyOfOrNull(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return List.copyOf(values);
  }

  private static List<FileScanTaskDto> copyOfOrEmpty(List<FileScanTaskDto> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    return List.copyOf(values);
  }

  private static List<ContentFileDto> copyOfOrEmptyContent(List<ContentFileDto> values) {
    if (values == null || values.isEmpty()) {
      return List.of();
    }
    return List.copyOf(values);
  }

  private TableRequests.Create applyDefaultLocationIfMissing(
      String prefix, List<String> namespacePath, String tableName, TableRequests.Create req) {
    if (req == null) {
      return null;
    }
    if (req.location() != null && !req.location().isBlank()) {
      return req;
    }
    String base = config.defaultWarehousePath().orElse(null);
    if (base == null || base.isBlank()) {
      return req;
    }
    String resolvedName = (tableName == null || tableName.isBlank()) ? "table" : tableName.trim();
    StringBuilder builder = new StringBuilder(ensureEndsWithSlash(base.trim()));
    if (prefix != null && !prefix.isBlank()) {
      builder.append(trimSlashes(prefix)).append('/');
    }
    if (namespacePath != null && !namespacePath.isEmpty()) {
      builder.append(String.join("/", namespacePath)).append('/');
    }
    builder.append(resolvedName);
    String computedLocation = builder.toString();
    return new TableRequests.Create(
        req.name(),
        req.schemaJson(),
        req.schema(),
        computedLocation,
        req.properties(),
        req.partitionSpec(),
        req.writeOrder(),
        req.stageCreate());
  }

  private static String ensureEndsWithSlash(String base) {
    if (base == null || base.isBlank()) {
      return "";
    }
    return base.endsWith("/") ? base : base + "/";
  }

  private static String trimSlashes(String text) {
    if (text == null) {
      return "";
    }
    String trimmed = text.trim();
    while (trimmed.startsWith("/")) {
      trimmed = trimmed.substring(1);
    }
    while (trimmed.endsWith("/") && !trimmed.isEmpty()) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private Long propertyLong(Map<String, String> props, String key) {
    if (props == null || props.isEmpty()) {
      return null;
    }
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      return null;
    }
  }

  private boolean hasSchema(TableRequests.Create req) {
    if (req == null) {
      return false;
    }
    if (req.schemaJson() != null && !req.schemaJson().isBlank()) {
      return true;
    }
    return req.schema() != null && !req.schema().isNull();
  }
}
