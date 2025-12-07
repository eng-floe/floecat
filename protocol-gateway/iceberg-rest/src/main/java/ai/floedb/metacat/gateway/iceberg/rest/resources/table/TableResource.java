package ai.floedb.metacat.gateway.iceberg.rest.resources.table;

import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.ContentFileDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.CredentialsResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.FileScanTaskDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.LoadTableResultDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.PlanResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.StageCreateResponseDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.StorageCredentialDto;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.MetricsRequests;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.PlanRequests;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.TaskRequests;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.metacat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.MaterializeMetadataResult;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.TableMetadataImportService;
import ai.floedb.metacat.gateway.iceberg.rest.services.metadata.TableMetadataImportService.ImportedMetadata;
import ai.floedb.metacat.gateway.iceberg.rest.services.planning.PlanTaskManager;
import ai.floedb.metacat.gateway.iceberg.rest.services.planning.TablePlanService;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergMetadata;
import ai.floedb.metacat.gateway.iceberg.rpc.IcebergRef;
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
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
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
  @Inject TenantContext tenantContext;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject TablePlanService tablePlanService;
  @Inject PlanTaskManager planTaskManager;
  @Inject TableMetadataImportService tableMetadataImportService;

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
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);

    String tableName = req != null && req.name() != null ? req.name() : "table";
    TableRequests.Create effectiveReq = req;
    if (effectiveReq != null) {
      try {
        System.out.println(
            "CreateTable request payload: " + mapper.writeValueAsString(effectiveReq));
      } catch (JsonProcessingException ignored) {
        System.out.println("CreateTable request payload: " + effectiveReq);
      }
      effectiveReq = applyDefaultLocationIfMissing(prefix, namespacePath, tableName, effectiveReq);
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
        req != null && tableSupport.metadataLocationFromCreate(req) != null
            ? tableSupport.metadataLocationFromCreate(req)
            : metadataLocation(created, metadata);
    if (preferredMetadataLocation != null && !preferredMetadataLocation.isBlank()) {
      responseMetadata =
          responseMetadata != null
              ? responseMetadata.withMetadataLocation(preferredMetadataLocation)
              : null;
      responseMetadataLocation = preferredMetadataLocation;
    }
    loadResult =
        new LoadTableResultDto(
            responseMetadataLocation,
            responseMetadata,
            loadResult.config(),
            loadResult.storageCredentials());

    var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
    ResourceId connectorId = null;
    String upstreamUri = null;
    String requestedLocation = req != null ? req.location() : null;
    String metadataForLocation =
        nonBlank(loadResult.metadataLocation(), tableSupport.metadataLocationFromCreate(req));
    String externalUri = tableSupport.resolveTableLocation(requestedLocation, metadataForLocation);

    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      connectorId =
          tableSupport.createTemplateConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getResourceId(),
              connectorTemplate,
              idempotencyKey);
      upstreamUri = connectorTemplate.uri();
    } else {
      if (externalUri != null && !externalUri.isBlank()) {
        connectorId =
            tableSupport.createExternalConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                tableName,
                created.getResourceId(),
                loadResult.metadataLocation(),
                externalUri,
                idempotencyKey);
        upstreamUri = externalUri;
      }
    }

    if (connectorId != null) {
      tableSupport.updateTableUpstream(
          created.getResourceId(), namespacePath, tableName, connectorId, upstreamUri);
      tableCommitService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);
    }

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
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null || tenantId.isBlank()) {
      return IcebergErrorResponses.validation("tenant context is required");
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
              new StagedTableKey(tenantId, catalogName, namespacePath, tableName, stageId),
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
          "Stored stage-create payload tenant=%s catalog=%s namespace=%s table=%s stageId=%s"
              + " txnHeader=%s",
          tenantContext.getTenantId(),
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
    SnapshotMode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(tableRecord);
    List<Snapshot> snapshotList = fetchSnapshots(tableId, snapshotMode, metadata);
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
  @PUT
  public Response update(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      TableRequests.Update req) {
    return commit(
        prefix,
        namespace,
        table,
        null,
        null,
        req == null
            ? null
            : new TableRequests.Commit(
                req.name(), req.namespace(), req.schemaJson(), req.properties(), null, null, null));
  }

  @Path("/tables/{table}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespacePath, table);
    ResourceId connectorId = null;
    try {
      Table existing = tableLifecycleService.getTable(tableId);
      if (existing.hasUpstream() && existing.getUpstream().hasConnectorId()) {
        connectorId = existing.getUpstream().getConnectorId();
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
        throw e;
      }
    }
    tableLifecycleService.deleteTable(tableId);
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
      PlanTaskManager.PlanDescriptor descriptor =
          planTaskManager.registerCompletedPlan(
              handle.queryId(),
              namespace,
              table,
              copyOfOrEmpty(planned.fileScanTasks()),
              copyOfOrEmptyContent(planned.deleteFiles()),
              planned.storageCredentials());
      return Response.ok(
              new PlanResponseDto(
                  descriptor.status().value(),
                  descriptor.planId(),
                  descriptor.planTasks(),
                  null,
                  null,
                  planned.storageCredentials()))
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
                            null,
                            null,
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

    var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
    ResourceId connectorId;
    String upstreamUri;
    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      connectorId =
          tableSupport.createTemplateConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getResourceId(),
              connectorTemplate,
              idempotencyKey);
      upstreamUri = connectorTemplate.uri();
    } else {
      String resolvedLocation =
          tableSupport.resolveTableLocation(
              importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
      connectorId =
          tableSupport.createExternalConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getResourceId(),
              metadataLocation,
              resolvedLocation,
              idempotencyKey);
      upstreamUri = resolvedLocation;
    }

    tableSupport.updateTableUpstream(
        created.getResourceId(), namespacePath, tableName, connectorId, upstreamUri);
    tableCommitService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);

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

    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("properties");
    TableSpec.Builder updateSpec = TableSpec.newBuilder().putAllProperties(props);
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

    ResourceId connectorId =
        existing.hasUpstream() && existing.getUpstream().hasConnectorId()
            ? existing.getUpstream().getConnectorId()
            : null;
    String resolvedLocation =
        tableSupport.resolveTableLocation(
            importedMetadata != null ? importedMetadata.tableLocation() : null, metadataLocation);
    if (connectorId == null) {
      var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
      String upstreamUri;
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
      } else {
        connectorId =
            tableSupport.createExternalConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                tableName,
                tableId,
                metadataLocation,
                resolvedLocation,
                idempotencyKey);
        upstreamUri = resolvedLocation;
      }
      if (connectorId != null) {
        tableSupport.updateTableUpstream(
            tableId, namespacePath, tableName, connectorId, upstreamUri);
      }
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
    }
    tableCommitService.runConnectorSync(tableSupport, connectorId, namespacePath, tableName);

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
    if (metadataLocation != null && !metadataLocation.isBlank()) {
      merged.put("metadata-location", metadataLocation);
      merged.put("metadata_location", metadataLocation);
    }
    return merged;
  }

  private SnapshotMode parseSnapshotMode(String raw) {
    if (raw == null || raw.isBlank() || raw.equalsIgnoreCase("all")) {
      return SnapshotMode.ALL;
    }
    if ("refs".equalsIgnoreCase(raw)) {
      return SnapshotMode.REFS;
    }
    throw new IllegalArgumentException("snapshots must be one of [all, refs]");
  }

  private List<Snapshot> fetchSnapshots(
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
    } catch (StatusRuntimeException e) {
      return List.of();
    }
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
    if (metadata != null
        && metadata.getMetadataLocation() != null
        && !metadata.getMetadataLocation().isBlank()) {
      return metadata.getMetadataLocation();
    }
    Map<String, String> props = table.getPropertiesMap();
    String location = props.getOrDefault("metadata-location", props.get("metadata_location"));
    if (location != null && !location.isBlank()) {
      return location;
    }
    return table.hasResourceId() ? table.getResourceId().getId() : null;
  }

  private enum SnapshotMode {
    ALL,
    REFS
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
}
