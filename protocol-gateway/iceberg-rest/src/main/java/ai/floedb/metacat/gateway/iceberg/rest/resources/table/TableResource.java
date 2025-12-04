package ai.floedb.metacat.gateway.iceberg.rest.resources.table;

import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergRef;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.*;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.metacat.gateway.iceberg.rest.api.metadata.TableMetadataView;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.*;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.MirrorMetadataResult;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.metacat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.metacat.gateway.iceberg.rest.services.planning.TablePlanService;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StageState;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableEntry;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableKey;
import ai.floedb.metacat.gateway.iceberg.rest.services.staging.StagedTableService;
import ai.floedb.metacat.gateway.iceberg.rest.services.tenant.TenantContext;
import ai.floedb.metacat.gateway.iceberg.rest.support.mapper.TableResponseMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Optional;
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
    String catalogName = resolveCatalog(prefix);
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
      return mapGrpcError(e);
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
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);

    String tableName = req != null && req.name() != null ? req.name() : "table";
    if (req != null) {
      try {
        System.out.println("CreateTable request payload: " + mapper.writeValueAsString(req));
      } catch (JsonProcessingException ignored) {
        System.out.println("CreateTable request payload: " + req);
      }
      if (Boolean.TRUE.equals(req.stageCreate())) {
        return handleStageCreate(
            prefix,
            catalogName,
            catalogId,
            namespaceId,
            namespacePath,
            tableName,
            req,
            transactionId,
            idempotencyKey);
      }
    }

    TableSpec.Builder spec;
    try {
      spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, req);
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return validationError(e.getMessage());
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
        return validationError(e.getMessage());
      }
    } else {
      loadResult =
          TableResponseMapper.toLoadResult(
              tableName, created, metadata, List.of(), tableConfig, credentials);
    }

    MirrorMetadataResult mirrorResult =
        tableCommitService.mirrorMetadata(
            namespace,
            created.getResourceId(),
            tableName,
            loadResult.metadata(),
            loadResult.metadataLocation());
    if (mirrorResult.error() != null) {
      return mirrorResult.error();
    }
    TableMetadataView responseMetadata =
        mirrorResult.metadata() != null ? mirrorResult.metadata() : loadResult.metadata();
    String responseMetadataLocation =
        nonBlank(mirrorResult.metadataLocation(), loadResult.metadataLocation());
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
      tableCommitService.runConnectorSync(
          tableSupport, connectorId, namespacePath, tableName);
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
      return validationError("stage-create requires a request body");
    }
    if (req.location() == null || req.location().isBlank()) {
      return validationError("location is required");
    }
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null || tenantId.isBlank()) {
      return validationError("tenant context is required");
    }
    String stageId =
        (transactionId == null || transactionId.isBlank())
            ? UUID.randomUUID().toString()
            : transactionId.trim();
    try {
      TableSpec.Builder spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, req);
      StagedTableEntry entry =
          new StagedTableEntry(
              new StagedTableKey(tenantId, catalogName, namespacePath, tableName, stageId),
              catalogId,
              namespaceId,
              req,
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
              req,
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
      return validationError(e.getMessage());
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespace, table);
    Table tableRecord = tableLifecycleService.getTable(tableId);
    SnapshotMode snapshotMode;
    try {
      snapshotMode = parseSnapshotMode(snapshots);
    } catch (IllegalArgumentException e) {
      return validationError(e.getMessage());
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
    String catalogName = resolveCatalog(prefix);
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
    String catalogName = resolveCatalog(prefix);
    tableLifecycleService.deleteTable(catalogName, namespace, table);
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
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId = tableLifecycleService.resolveTableId(catalogName, namespace, table);
    Long startSnapshotId = request.startSnapshotId();
    Long endSnapshotId = request.endSnapshotId();
    Long snapshotId = request.snapshotId();
    if (startSnapshotId != null && endSnapshotId != null && startSnapshotId >= endSnapshotId) {
      return validationError("start-snapshot-id must be less than end-snapshot-id");
    }
    Long resolvedSnapshotId = endSnapshotId != null ? endSnapshotId : snapshotId;
    if (startSnapshotId != null && resolvedSnapshotId == null) {
      return validationError("start-snapshot-id requires snapshot-id or end-snapshot-id");
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
      return Response.ok(
              new PlanResponseDto(
                  "in-progress",
                  handle.queryId(),
                  List.of(handle.queryId()),
                  null,
                  null,
                  tableSupport.defaultCredentials()))
          .build();
    } catch (IllegalArgumentException ex) {
      return validationError(ex.getMessage());
    }
  }

  @Path("/tables/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    tableLifecycleService.resolveTableId(resolveCatalog(prefix), namespace, table);
    try {
      return Response.ok(tablePlanService.fetchPlan(planId, tableSupport.defaultCredentials()))
          .build();
    } catch (IllegalArgumentException ex) {
      return validationError(ex.getMessage());
    }
  }

  @Path("/tables/{table}/plan/{planId}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    tableLifecycleService.resolveTableId(resolveCatalog(prefix), namespace, table);
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
      return validationError("plan-task is required");
    }
    tableLifecycleService.resolveTableId(resolveCatalog(prefix), namespace, table);
    try {
      return Response.ok(tablePlanService.fetchTasks(request.planTask().trim())).build();
    } catch (IllegalArgumentException ex) {
      return validationError(ex.getMessage());
    }
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
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
      return validationError("snapshot-id is required");
    }
    LOG.infof(
        "Received metrics report for %s.%s snapshot=%d (deferred)",
        namespace, table, request.snapshotId());
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @Path("/register")
  @POST
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Register req) {
    if (req == null || req.metadataLocation() == null || req.metadataLocation().isBlank()) {
      return validationError("metadata-location is required");
    }
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);

    if (req.name() == null || req.name().isBlank()) {
      return validationError("name is required");
    }
    String tableName = req.name().trim();

    TableSpec.Builder spec = tableSupport.baseTableSpec(catalogId, namespaceId, tableName);
    tableSupport.addMetadataLocationProperties(spec, req.metadataLocation());

    Table created = tableLifecycleService.createTable(spec, idempotencyKey);

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
      connectorId =
          tableSupport.createExternalConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getResourceId(),
              req.metadataLocation(),
              tableSupport.resolveTableLocation(null, req.metadataLocation()),
              idempotencyKey);
      upstreamUri = tableSupport.resolveTableLocation(null, req.metadataLocation());
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

  private String resolveCatalog(String prefix) {
    Map<String, String> mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
  }

  private ResourceId resolveCatalogId(String prefix) {
    return tableLifecycleService.resolveCatalogId(resolveCatalog(prefix));
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private Response mapGrpcError(StatusRuntimeException exception) {
    var status = exception.getStatus();
    Response.Status httpStatus;
    String type;
    switch (status.getCode()) {
      case NOT_FOUND -> {
        httpStatus = Response.Status.NOT_FOUND;
        type = "NoSuchObjectException";
      }
      case INVALID_ARGUMENT -> {
        httpStatus = Response.Status.BAD_REQUEST;
        type = "ValidationException";
      }
      case PERMISSION_DENIED -> {
        httpStatus = Response.Status.FORBIDDEN;
        type = "ForbiddenException";
      }
      case UNAUTHENTICATED -> {
        httpStatus = Response.Status.UNAUTHORIZED;
        type = "UnauthorizedException";
      }
      default -> {
        httpStatus = Response.Status.INTERNAL_SERVER_ERROR;
        type = status.getCode().name();
      }
    }
    String message =
        status.getDescription() == null ? status.getCode().name() : status.getDescription();
    return Response.status(httpStatus)
        .entity(
            new IcebergErrorResponse(new IcebergError(message, type, httpStatus.getStatusCode())))
        .build();
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }

  private static List<String> copyOfOrNull(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return List.copyOf(values);
  }
}
