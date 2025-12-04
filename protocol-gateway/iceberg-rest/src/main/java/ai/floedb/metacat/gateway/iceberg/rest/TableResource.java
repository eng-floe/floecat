package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.IcebergBlobMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergEncryptedKey;
import ai.floedb.metacat.catalog.rpc.IcebergMetadata;
import ai.floedb.metacat.catalog.rpc.IcebergPartitionStatisticsFile;
import ai.floedb.metacat.catalog.rpc.IcebergRef;
import ai.floedb.metacat.catalog.rpc.IcebergSchema;
import ai.floedb.metacat.catalog.rpc.IcebergSortField;
import ai.floedb.metacat.catalog.rpc.IcebergSortOrder;
import ai.floedb.metacat.catalog.rpc.IcebergStatisticsFile;
import ai.floedb.metacat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.execution.rpc.ScanBundle;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.StageCommitProcessor.StageCommitResult;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.DescribeInputsRequest;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.GetQueryRequest;
import ai.floedb.metacat.query.rpc.Operator;
import ai.floedb.metacat.query.rpc.Predicate;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.metacat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.protobuf.util.Timestamps;
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
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

  private final ConcurrentMap<String, PlanContext> planContexts = new ConcurrentHashMap<>();

  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;
  @Inject StagedTableService stagedTableService;
  @Inject TenantContext tenantContext;
  @Inject StageCommitProcessor stageCommitProcessor;
  @Inject MetadataMirrorService metadataMirrorService;

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
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));

    ListTablesRequest.Builder req = ListTablesRequest.newBuilder().setNamespaceId(namespaceId);
    if (pageToken != null || pageSize != null) {
      PageRequest.Builder page = PageRequest.newBuilder();
      if (pageToken != null) {
        page.setPageToken(pageToken);
      }
      if (pageSize != null) {
        page.setPageSize(pageSize);
      }
      req.setPage(page);
    }

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    var resp = stub.listTables(req.build());
    List<TableIdentifierDto> identifiers =
        resp.getTablesList().stream()
            .map(t -> new TableIdentifierDto(NamespacePaths.split(namespace), t.getDisplayName()))
            .collect(Collectors.toList());
    Map<String, Object> body = new LinkedHashMap<>();
    body.put("identifiers", identifiers);

    String nextToken = flattenPageToken(resp.getPage());
    if (nextToken != null) {
      body.put("next-page-token", nextToken);
    }
    return Response.ok(body).build();
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
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, namespacePath);

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
      if (req.location() == null || req.location().isBlank()) {
        return validationError("location is required");
      }
    }

    TableSpec.Builder spec;
    try {
      spec = tableSupport.buildCreateSpec(catalogId, namespaceId, tableName, req);
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return validationError(e.getMessage());
    }

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = stub.createTable(request.build()).getTable();
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
        mirrorMetadata(
            namespace,
            created.getResourceId(),
            tableName,
            loadResult.metadata(),
            loadResult.metadataLocation());
    if (mirrorResult.error() != null) {
      return mirrorResult.error();
    }
    loadResult =
        new LoadTableResultDto(
            mirrorResult.metadataLocation(),
            mirrorResult.metadata() != null ? mirrorResult.metadata() : loadResult.metadata(),
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
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    var resp = stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
    Table tableRecord = resp.getTable();
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
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    return Response.noContent().build();
  }

  @Path("/tables/{table}")
  @PUT
  public Response update(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      TableRequests.Update req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
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
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    stub.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build());
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
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, namespacePath);
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    final Table[] stagedTableHolder = new Table[1];
    StageCommitResult stageMaterialization = null;
    String bodyStageId = req == null ? null : req.stageId();
    String effectiveStageId = resolveStageId(req, transactionId);
    if (effectiveStageId != null) {
      LOG.infof(
          "Commit request referenced stage payload namespace=%s table=%s stageId=%s (body=%s"
              + " header=%s)",
          namespace, table, effectiveStageId, bodyStageId, transactionId);
    }
    ResourceId resolvedTableId = null;
    try {
      resolvedTableId = NameResolution.resolveTable(grpc, catalogName, namespacePath, table);
    } catch (StatusRuntimeException e) {
      String stageIdToUse = effectiveStageId;
      if (stageIdToUse == null && tenantContext.getTenantId() != null) {
        Optional<StagedTableEntry> latest =
            stagedTableService.findLatestStage(
                tenantContext.getTenantId(), catalogName, namespacePath, table);
        if (latest.isPresent()) {
          stageIdToUse = latest.get().key().stageId();
          LOG.infof(
              "Found staged payload without explicit stage id namespace=%s table=%s stageId=%s",
              namespace, table, stageIdToUse);
        }
      }
      if (stageIdToUse != null && e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        LOG.infof(
            "Table not found for commit, attempting staged materialization namespace=%s table=%s"
                + " stageId=%s",
            namespace, table, stageIdToUse);
        try {
          stageMaterialization =
              stageCommitProcessor.commitStage(
                  prefix,
                  catalogName,
                  tenantContext.getTenantId(),
                  namespacePath,
                  table,
                  stageIdToUse);
          stagedTableHolder[0] = stageMaterialization.table();
          resolvedTableId = stagedTableHolder[0].getResourceId();
        } catch (StageCommitException sce) {
          return sce.toResponse();
        }
      } else {
        if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
          LOG.warnf(
              "Commit request table not found and no stage id supplied namespace=%s table=%s"
                  + " bodyStageId=%s headerStageId=%s",
              namespace, table, bodyStageId, transactionId);
        }
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
              cached =
                  stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build())
                      .getTable();
            }
            return cached;
          }
        };

    if (stageMaterialization != null) {
      List<Map<String, Object>> snapshotAdds =
          addSnapshotUpdates(req == null ? null : req.updates());
      if (!snapshotAdds.isEmpty()) {
        Response snapshotError =
            applySnapshotUpdates(
                tableId, namespacePath, table, tableSupplier, snapshotAdds, idempotencyKey);
        if (snapshotError != null) {
          return snapshotError;
        }
      }
      Table current = tableSupplier.get();
      IcebergMetadata metadata = tableSupport.loadCurrentMetadata(current);
      List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, metadata);
      CommitTableResponseDto responseDto =
          TableResponseMapper.toCommitResponse(table, current, metadata, snapshotList);
      MirrorMetadataResult mirrorResult =
          mirrorMetadata(
              namespace, tableId, table, responseDto.metadata(), responseDto.metadataLocation());
      if (mirrorResult.error() != null) {
        return mirrorResult.error();
      }
      responseDto = applyMirrorResult(responseDto, mirrorResult);
      ResourceId connectorId =
          synchronizeConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              table,
              current,
              responseDto.metadataLocation(),
              idempotencyKey);
      Response.ResponseBuilder builder = Response.ok(responseDto);
      if (responseDto.metadataLocation() != null) {
        builder.tag(responseDto.metadataLocation());
      }
      TableMetadataView meta = responseDto.metadata();
      LOG.infof(
          "Stage commit satisfied for %s.%s tableId=%s stageId=%s currentSnapshot=%s"
              + " snapshotCount=%d",
          namespace,
          table,
          stageMaterialization.table().hasResourceId()
              ? stageMaterialization.table().getResourceId().getId()
              : "<missing>",
          resolveStageId(req, transactionId),
          meta == null ? "<null>" : meta.currentSnapshotId(),
          meta == null || meta.snapshots() == null ? 0 : meta.snapshots().size());
      runConnectorSyncIfPossible(connectorId, namespacePath, table);
      return builder.build();
    }

    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
      Response requirementError = validateRequirements(req.requirements(), tableSupplier);
      if (requirementError != null) {
        return requirementError;
      }
      if (req.name() != null) {
        spec.setDisplayName(req.name());
        mask.addPaths("display_name");
      }
      if (req.namespace() != null && !req.namespace().isEmpty()) {
        var targetNs =
            NameResolution.resolveNamespace(grpc, catalogName, new ArrayList<>(req.namespace()));
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
        stripMetadataLocation(mergedProps);
        if (mergedProps.isEmpty()) {
          mergedProps = null;
        }
      }
      if (hasPropertyUpdates(req)) {
        if (mergedProps == null) {
          mergedProps = new LinkedHashMap<>(tableSupplier.get().getPropertiesMap());
        }
        Response updateError = applyPropertyUpdates(mergedProps, req.updates());
        if (updateError != null) {
          return updateError;
        }
      }
      Response snapshotError =
          applySnapshotUpdates(
              tableId, namespacePath, table, tableSupplier, req.updates(), idempotencyKey);
      if (snapshotError != null) {
        return snapshotError;
      }
      Response locationError = applyLocationUpdate(spec, mask, tableSupplier, req.updates());
      if (locationError != null) {
        return locationError;
      }
      String unsupported = unsupportedUpdateAction(req);
      if (unsupported != null) {
        return validationError("unsupported commit update action: " + unsupported);
      }
      if (mergedProps != null) {
        spec.clearProperties().putAllProperties(mergedProps);
        mask.addPaths("properties");
      }
    }
    if (mask.getPathsCount() == 0) {
      Table current = tableSupplier.get();
      IcebergMetadata metadata = tableSupport.loadCurrentMetadata(current);
      List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, metadata);
      CommitTableResponseDto responseDto =
          TableResponseMapper.toCommitResponse(table, current, metadata, snapshotList);
      MirrorMetadataResult mirrorResult =
          mirrorMetadata(
              namespace, tableId, table, responseDto.metadata(), responseDto.metadataLocation());
      if (mirrorResult.error() != null) {
        return mirrorResult.error();
      }
      responseDto = applyMirrorResult(responseDto, mirrorResult);
      ResourceId connectorId =
          synchronizeConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              table,
              current,
              responseDto.metadataLocation(),
              idempotencyKey);
      Response.ResponseBuilder builder = Response.ok(responseDto);
      LOG.infof(
          "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
          namespace,
          table,
          current.hasResourceId() ? current.getResourceId().getId() : "<missing>",
          metadata != null ? metadata.getCurrentSnapshotId() : "<null>",
          snapshotList == null ? 0 : snapshotList.size());
      if (responseDto.metadataLocation() != null) {
        builder.tag(responseDto.metadataLocation());
      }
      runConnectorSyncIfPossible(connectorId, namespacePath, table);
      return builder.build();
    }

    UpdateTableRequest.Builder updateRequest =
        UpdateTableRequest.newBuilder().setTableId(tableId).setSpec(spec).setUpdateMask(mask);
    var resp = stub.updateTable(updateRequest.build());
    Table updated = resp.getTable();
    IcebergMetadata metadata = tableSupport.loadCurrentMetadata(updated);
    List<Snapshot> snapshotList = fetchSnapshots(tableId, SnapshotMode.ALL, metadata);
    CommitTableResponseDto responseDto =
        TableResponseMapper.toCommitResponse(table, updated, metadata, snapshotList);
    MirrorMetadataResult mirrorResult =
        mirrorMetadata(
            namespace, tableId, table, responseDto.metadata(), responseDto.metadataLocation());
    if (mirrorResult.error() != null) {
      return mirrorResult.error();
    }
    responseDto = applyMirrorResult(responseDto, mirrorResult);
    ResourceId connectorId =
        synchronizeConnector(
            prefix,
            namespacePath,
            namespaceId,
            catalogId,
            table,
            updated,
            responseDto.metadataLocation(),
            idempotencyKey);
    Response.ResponseBuilder builder = Response.ok(responseDto);
    LOG.infof(
        "Commit response for %s.%s tableId=%s currentSnapshot=%s snapshotCount=%d",
        namespace,
        table,
        updated.hasResourceId() ? updated.getResourceId().getId() : "<missing>",
        metadata != null ? metadata.getCurrentSnapshotId() : "<null>",
        snapshotList == null ? 0 : snapshotList.size());
    if (responseDto.metadataLocation() != null) {
      builder.tag(responseDto.metadataLocation());
    }
    runConnectorSyncIfPossible(connectorId, namespacePath, table);
    return builder.build();
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

  @Path("/tables/{table}/plan")
  @POST
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      PlanRequests.Plan rawRequest) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
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
    List<Predicate> predicates;
    try {
      predicates = buildPredicates(request.filter(), request.caseSensitive());
    } catch (IllegalArgumentException ex) {
      return validationError(ex.getMessage());
    }
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var begin = queryStub.beginQuery(BeginQueryRequest.newBuilder().build());
    String queryId = begin.getQuery().getQueryId();
    registerPlanInput(queryId, tableId, resolvedSnapshotId);
    planContexts.put(
        queryId,
        new PlanContext(
            tableId,
            copyOfOrNull(request.select()),
            startSnapshotId,
            resolvedSnapshotId,
            predicates,
            copyOfOrNull(request.statsFields()),
            request.useSnapshotSchema(),
            request.caseSensitive(),
            request.minRowsRequested()));
    return Response.ok(
            new PlanResponseDto(
                "in-progress",
                queryId,
                List.of(queryId),
                null,
                null,
                tableSupport.defaultCredentials()))
        .build();
  }

  @Path("/tables/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    PlanContext ctx =
        planContexts.getOrDefault(
            planId, new PlanContext(tableId, null, null, null, List.of(), null, null, null, null));
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var resp = queryStub.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, ctx, resp.getQuery().getQueryId());
    return Response.ok(toCompletedPlanResult(resp.getQuery(), planId, bundle)).build();
  }

  @Path("/tables/{table}/plan/{planId}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    queryStub.endQuery(EndQueryRequest.newBuilder().setQueryId(planId).setCommit(false).build());
    planContexts.remove(planId);
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    String planTask = request.planTask().trim();
    PlanContext ctx =
        planContexts.getOrDefault(
            planTask,
            new PlanContext(tableId, null, null, null, List.of(), null, null, null, null));
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, ctx, planTask);
    planContexts.remove(planTask);
    return Response.ok(toScanTasksDto(bundle)).build();
  }

  @Path("/tables/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
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
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, namespacePath);

    if (req.name() == null || req.name().isBlank()) {
      return validationError("name is required");
    }
    String tableName = req.name().trim();

    TableSpec.Builder spec = tableSupport.baseTableSpec(catalogId, namespaceId, tableName);
    tableSupport.addMetadataLocationProperties(spec, req.metadataLocation());

    TableServiceGrpc.TableServiceBlockingStub tableStub = grpc.withHeaders(grpc.raw().table());
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = tableStub.createTable(request.build()).getTable();

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

  private String flattenPageToken(PageResponse page) {
    String token = page.getNextPageToken();
    return token == null || token.isBlank() ? null : token;
  }

  private ResourceId resolveCatalogId(String prefix) {
    return NameResolution.resolveCatalog(grpc, resolveCatalog(prefix));
  }

  private QueryInput buildPlanInput(ResourceId tableId, Long snapshotId) {
    QueryInput.Builder input = QueryInput.newBuilder().setTableId(tableId);
    if (snapshotId != null) {
      input.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(snapshotId));
    } else {
      input.setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT));
    }
    return input.build();
  }

  private void registerPlanInput(String queryId, ResourceId tableId, Long snapshotId) {
    QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaStub =
        grpc.withHeaders(grpc.raw().querySchema());
    schemaStub.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(buildPlanInput(tableId, snapshotId))
            .build());
  }

  private PlanResponseDto toCompletedPlanResult(
      QueryDescriptor descriptor, String planIdOverride, ScanBundle bundle) {
    ScanTasksResponseDto scanTasks = toScanTasksDto(bundle);
    String queryId = descriptor.getQueryId();
    String planId =
        (queryId == null || queryId.isBlank())
            ? (planIdOverride == null ? "" : planIdOverride)
            : queryId;
    List<String> planTasks =
        (scanTasks.fileScanTasks() == null || scanTasks.fileScanTasks().isEmpty())
            ? List.of()
            : List.of(planId);
    return new PlanResponseDto(
        "completed",
        planId,
        planTasks,
        scanTasks.fileScanTasks(),
        scanTasks.deleteFiles(),
        tableSupport.defaultCredentials());
  }

  private ScanBundle fetchScanBundle(
      QueryScanServiceGrpc.QueryScanServiceBlockingStub stub, PlanContext ctx, String queryId) {
    FetchScanBundleRequest.Builder builder =
        FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(ctx.tableId());
    if (ctx.requiredColumns() != null && !ctx.requiredColumns().isEmpty()) {
      builder.addAllRequiredColumns(ctx.requiredColumns());
    }
    if (ctx.predicates() != null && !ctx.predicates().isEmpty()) {
      builder.addAllPredicates(ctx.predicates());
    }
    return stub.fetchScanBundle(builder.build()).getBundle();
  }

  private ScanTasksResponseDto toScanTasksDto(ScanBundle bundle) {
    List<ContentFileDto> deleteFiles = new ArrayList<>();
    List<FileScanTaskDto> tasks = new ArrayList<>();
    if (bundle != null) {
      for (ScanFile delete : bundle.getDeleteFilesList()) {
        deleteFiles.add(toContentFile(delete));
      }
      for (ScanFile file : bundle.getDataFilesList()) {
        tasks.add(new FileScanTaskDto(toContentFile(file), List.of(), null));
      }
    }
    return new ScanTasksResponseDto(List.of(), tasks, deleteFiles);
  }

  private ContentFileDto toContentFile(ScanFile file) {
    String content =
        switch (file.getFileContent()) {
          case SCAN_FILE_CONTENT_EQUALITY_DELETES -> "equality-deletes";
          case SCAN_FILE_CONTENT_POSITION_DELETES -> "position-deletes";
          default -> "data";
        };
    List<Object> partition = parsePartition(file.getPartitionDataJson());
    List<Integer> equality =
        file.getEqualityFieldIdsList().isEmpty() ? null : file.getEqualityFieldIdsList();
    return new ContentFileDto(
        content,
        file.getFilePath(),
        file.getFileFormat(),
        file.getPartitionSpecId(),
        partition,
        file.getFileSizeInBytes(),
        file.getRecordCount(),
        null,
        List.of(),
        null,
        equality);
  }

  private List<Object> parsePartition(String json) {
    if (json == null || json.isBlank()) {
      return List.of();
    }
    try {
      JsonNode node = mapper.readTree(json);
      if (node.has("partitionValues") && node.get("partitionValues").isArray()) {
        List<Object> values = new ArrayList<>();
        for (JsonNode entry : node.get("partitionValues")) {
          JsonNode valueNode = entry.get("value");
          values.add(extractValue(valueNode));
        }
        return values;
      }
      if (node.isArray()) {
        List<Object> values = new ArrayList<>();
        for (JsonNode entry : node) {
          values.add(extractValue(entry));
        }
        return values;
      }
    } catch (Exception ignored) {
      // fall back to empty partition representation
    }
    return List.of();
  }

  private Object extractValue(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (node.isNumber()) {
      if (node.isIntegralNumber()) {
        return node.longValue();
      }
      return node.doubleValue();
    }
    if (node.isBoolean()) {
      return node.booleanValue();
    }
    return node.asText();
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }

  private Response serverError(String message) {
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(new IcebergErrorResponse(new IcebergError(message, "CommitFailedException", 500)))
        .build();
  }

  private MirrorMetadataResult mirrorMetadata(
      String namespace,
      ResourceId tableId,
      String table,
      TableMetadataView metadata,
      String metadataLocation) {
    if (metadata == null) {
      return MirrorMetadataResult.success(null, metadataLocation);
    }
    try {
      MetadataMirrorService.MirrorResult mirrorResult =
          metadataMirrorService.mirror(namespace, table, metadata, metadataLocation);
      String resolvedLocation = nonBlank(mirrorResult.metadataLocation(), metadataLocation);
      TableMetadataView resolvedMetadata =
          mirrorResult.metadata() != null ? mirrorResult.metadata() : metadata;
      if (tableId != null) {
        updateTableMetadataProperties(tableId, resolvedMetadata, resolvedLocation);
      }
      return MirrorMetadataResult.success(resolvedMetadata, resolvedLocation);
    } catch (MetadataMirrorException e) {
      LOG.errorf(
          e,
          "Failed to mirror Iceberg metadata for %s.%s to %s",
          namespace,
          table,
          metadataLocation);
      return MirrorMetadataResult.failure(serverError("Failed to persist Iceberg metadata files"));
    }
  }

  private void updateTableMetadataProperties(
      ResourceId tableId, TableMetadataView metadata, String resolvedLocation) {
    if (tableId == null || metadata == null) {
      return;
    }
    Map<String, String> props =
        metadata.properties() != null
            ? new LinkedHashMap<>(metadata.properties())
            : new LinkedHashMap<>();
    if (resolvedLocation != null && !resolvedLocation.isBlank()) {
      props.put("metadata-location", resolvedLocation);
      props.put("metadata_location", resolvedLocation);
    }
    if (props.isEmpty()) {
      return;
    }
    TableSpec spec = TableSpec.newBuilder().putAllProperties(props).build();
    UpdateTableRequest request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build();
    try {
      grpc.withHeaders(grpc.raw().table()).updateTable(request);
    } catch (Exception e) {
      LOG.warnf(e, "Failed to update metadata properties for tableId=%s", tableId.getId());
    }
  }

  private ResourceId synchronizeConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String table,
      Table tableRecord,
      String metadataLocation,
      String idempotencyKey) {
    if (tableRecord == null || metadataLocation == null || metadataLocation.isBlank()) {
      return resolveConnectorId(tableRecord);
    }
    ResourceId connectorId = resolveConnectorId(tableRecord);
    if (connectorId == null) {
      var connectorTemplate = tableSupport.connectorTemplateFor(prefix);
      if (connectorTemplate != null && connectorTemplate.uri() != null) {
        connectorId =
            tableSupport.createTemplateConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                table,
                tableRecord.getResourceId(),
                connectorTemplate,
                idempotencyKey);
        tableSupport.updateTableUpstream(
            tableRecord.getResourceId(),
            namespacePath,
            table,
            connectorId,
            connectorTemplate.uri());
      } else {
        String baseLocation = tableLocation(tableRecord);
        String resolvedLocation =
            tableSupport.resolveTableLocation(baseLocation, metadataLocation);
        connectorId =
            tableSupport.createExternalConnector(
                prefix,
                namespacePath,
                namespaceId,
                catalogId,
                table,
                tableRecord.getResourceId(),
                metadataLocation,
                resolvedLocation,
                idempotencyKey);
        tableSupport.updateTableUpstream(
            tableRecord.getResourceId(),
            namespacePath,
            table,
            connectorId,
            resolvedLocation);
      }
    } else {
      tableSupport.updateConnectorMetadata(connectorId, metadataLocation);
    }
    return connectorId;
  }

  private String tableLocation(Table tableRecord) {
    if (tableRecord == null) {
      return null;
    }
    if (tableRecord.hasUpstream()) {
      String uri = tableRecord.getUpstream().getUri();
      if (uri != null && !uri.isBlank()) {
        return uri;
      }
    }
    Map<String, String> props = tableRecord.getPropertiesMap();
    String location = props.get("location");
    if (location != null && !location.isBlank()) {
      return location;
    }
    return null;
  }

  private CommitTableResponseDto applyMirrorResult(
      CommitTableResponseDto responseDto, MirrorMetadataResult mirrorResult) {
    if (responseDto == null || mirrorResult == null) {
      return responseDto;
    }
    TableMetadataView updatedMetadata =
        mirrorResult.metadata() != null ? mirrorResult.metadata() : responseDto.metadata();
    String updatedLocation =
        nonBlank(mirrorResult.metadataLocation(), responseDto.metadataLocation());
    if (updatedMetadata == responseDto.metadata()
        && Objects.equals(updatedLocation, responseDto.metadataLocation())) {
      return responseDto;
    }
    return new CommitTableResponseDto(updatedLocation, updatedMetadata);
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }

  private record MirrorMetadataResult(
      Response error, TableMetadataView metadata, String metadataLocation) {
    static MirrorMetadataResult success(TableMetadataView metadata, String location) {
      return new MirrorMetadataResult(null, metadata, location);
    }

    static MirrorMetadataResult failure(Response error) {
      return new MirrorMetadataResult(error, null, null);
    }
  }

  private String toJson(Object value) {
    if (value == null) {
      return null;
    }
    try {
      return mapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      return null;
    }
  }

  private String schemaJsonFromMetadata(IcebergMetadata metadata, Integer schemaId) {
    if (metadata == null || metadata.getSchemasCount() == 0) {
      return null;
    }
    Integer targetId = schemaId != null ? schemaId : metadata.getCurrentSchemaId();
    if (targetId != null && targetId > 0) {
      for (IcebergSchema schema : metadata.getSchemasList()) {
        if (schema.getSchemaId() == targetId) {
          return schema.getSchemaJson();
        }
      }
    }
    return metadata.getSchemasList().get(0).getSchemaJson();
  }

  private List<Map<String, Object>> addSnapshotUpdates(List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if ("add-snapshot".equals(action)) {
        out.add(update);
      }
    }
    return out;
  }

  private Response specNotImplemented(String operation) {
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            new IcebergErrorResponse(
                new IcebergError(
                    operation + " not implemented", "UnsupportedOperationException", 501)))
        .build();
  }

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(new IcebergErrorResponse(new IcebergError(message, "CommitFailedException", 409)))
        .build();
  }

  private Response validateRequirements(
      List<Map<String, Object>> requirements, Supplier<Table> tableSupplier) {
    if (requirements == null || requirements.isEmpty()) {
      return null;
    }
    Table table = tableSupplier.get();
    Map<String, String> props = table.getPropertiesMap();
    IcebergMetadata[] metadataHolder = new IcebergMetadata[1];
    Supplier<IcebergMetadata> metadataSupplier =
        () -> {
          if (metadataHolder[0] == null) {
            metadataHolder[0] = tableSupport.loadCurrentMetadata(table);
          }
          return metadataHolder[0];
        };
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return validationError("commit requirement entry cannot be null");
      }
      String type = asString(requirement.get("type"));
      if (type == null || type.isBlank()) {
        return validationError("commit requirement missing type");
      }
      switch (type) {
        case "assert-create" -> {
          // Spec requirement for staged create operations. We don't currently stage create tables,
          // so treat this as satisfied when the table exists.
        }
        case "assert-table-uuid" -> {
          String expected = asString(requirement.get("uuid"));
          if (expected == null || expected.isBlank()) {
            return validationError("assert-table-uuid requires uuid");
          }
          String actual =
              Optional.ofNullable(props.get("table-uuid"))
                  .orElse(table.hasResourceId() ? table.getResourceId().getId() : "");
          if (!expected.equals(actual)) {
            return conflictError("assert-table-uuid failed");
          }
        }
        case "assert-current-schema-id" -> {
          Integer expected = asInteger(requirement.get("current-schema-id"));
          if (expected == null) {
            return validationError("assert-current-schema-id requires current-schema-id");
          }
          Integer actual = propertyInt(props, "current-schema-id");
          if (!expected.equals(actual)) {
            return conflictError("assert-current-schema-id failed");
          }
        }
        case "assert-last-assigned-field-id" -> {
          Integer expected = asInteger(requirement.get("last-assigned-field-id"));
          if (expected == null) {
            return validationError("assert-last-assigned-field-id requires last-assigned-field-id");
          }
          Integer actual = propertyInt(props, "last-assigned-field-id");
          if (!expected.equals(actual)) {
            return conflictError("assert-last-assigned-field-id failed");
          }
        }
        case "assert-last-assigned-partition-id" -> {
          Integer expected = asInteger(requirement.get("last-assigned-partition-id"));
          if (expected == null) {
            return validationError(
                "assert-last-assigned-partition-id requires last-assigned-partition-id");
          }
          Integer actual = propertyInt(props, "last-assigned-partition-id");
          if (!expected.equals(actual)) {
            return conflictError("assert-last-assigned-partition-id failed");
          }
        }
        case "assert-default-spec-id" -> {
          Integer expected = asInteger(requirement.get("default-spec-id"));
          if (expected == null) {
            return validationError("assert-default-spec-id requires default-spec-id");
          }
          Integer actual = propertyInt(props, "default-spec-id");
          if (!expected.equals(actual)) {
            return conflictError("assert-default-spec-id failed");
          }
        }
        case "assert-default-sort-order-id" -> {
          Integer expected = asInteger(requirement.get("default-sort-order-id"));
          if (expected == null) {
            return validationError("assert-default-sort-order-id requires default-sort-order-id");
          }
          Integer actual = propertyInt(props, "default-sort-order-id");
          if (!expected.equals(actual)) {
            return conflictError("assert-default-sort-order-id failed");
          }
        }
        case "assert-ref-snapshot-id" -> {
          String refName = asString(requirement.get("ref"));
          Long expected = asLong(requirement.get("snapshot-id"));
          if (refName == null || refName.isBlank()) {
            return validationError("assert-ref-snapshot-id requires ref");
          }
          if (expected == null) {
            LOG.debugf(
                "Skipping assert-ref-snapshot-id requirement for table %s ref %s because"
                    + " snapshot-id was not provided",
                table.hasResourceId() ? table.getResourceId().getId() : "<unknown>", refName);
            continue;
          }
          Long actual = null;
          IcebergMetadata metadata = metadataSupplier.get();
          if (metadata != null && metadata.getRefsMap().containsKey(refName)) {
            actual = metadata.getRefsMap().get(refName).getSnapshotId();
          } else if ("main".equals(refName)) {
            actual = propertyLong(props, "current-snapshot-id");
          }
          if (!Objects.equals(actual, expected)) {
            return conflictError("assert-ref-snapshot-id failed for ref " + refName);
          }
        }
        default -> {
          return validationError("unsupported commit requirement: " + type);
        }
      }
    }
    return null;
  }

  private boolean hasPropertyUpdates(TableRequests.Commit req) {
    if (req == null || req.updates() == null || req.updates().isEmpty()) {
      return false;
    }
    for (Map<String, Object> update : req.updates()) {
      String action = asString(update == null ? null : update.get("action"));
      if ("set-properties".equals(action) || "remove-properties".equals(action)) {
        return true;
      }
    }
    return false;
  }

  private Response applyLocationUpdate(
      TableSpec.Builder spec,
      FieldMask.Builder mask,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    String location = null;
    for (Map<String, Object> update : updates) {
      String action = asString(update == null ? null : update.get("action"));
      if (!"set-location".equals(action)) {
        continue;
      }
      if (location != null) {
        return validationError("set-location may only be specified once");
      }
      String value = asString(update.get("location"));
      if (value == null || value.isBlank()) {
        return validationError("set-location requires location");
      }
      location = value;
    }
    if (location == null) {
      return null;
    }
    Table existing = tableSupplier.get();
    UpstreamRef upstream = existing.hasUpstream() ? existing.getUpstream() : null;
    UpstreamRef.Builder builder =
        upstream != null
            ? upstream.toBuilder()
            : UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG);
    builder.setUri(location);
    spec.setUpstream(builder.build());
    mask.addPaths("upstream");
    return null;
  }

  private Response applySnapshotUpdates(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Supplier<Table> tableSupplier,
      List<Map<String, Object>> updates,
      String idempotencyKey) {
    if (updates == null || updates.isEmpty()) {
      return null;
    }
    Table existing = null;
    Long lastSnapshotId = null;
    SnapshotMetadataChanges metadataChanges = new SnapshotMetadataChanges();
    for (Map<String, Object> update : updates) {
      if (update == null) {
        continue;
      }
      String action = asString(update.get("action"));
      if ("add-snapshot".equals(action)) {
        @SuppressWarnings("unchecked")
        Map<String, Object> snapshot =
            update.get("snapshot") instanceof Map<?, ?> m ? (Map<String, Object>) m : null;
        if (snapshot == null || snapshot.isEmpty()) {
          return validationError("add-snapshot requires snapshot");
        }
        if (existing == null) {
          existing = tableSupplier.get();
        }
        Response error =
            createSnapshotPlaceholder(
                tableId, namespacePath, tableName, existing, snapshot, idempotencyKey);
        if (error != null) {
          return error;
        }
        Long snapshotId = asLong(snapshot.get("snapshot-id"));
        if (snapshotId != null) {
          lastSnapshotId = snapshotId;
        }
      } else if ("remove-snapshots".equals(action)) {
        List<Long> ids = asLongList(update.get("snapshot-ids"));
        if (ids.isEmpty()) {
          return validationError("remove-snapshots requires snapshot-ids");
        }
        deleteSnapshots(tableId, ids);
      } else if ("set-snapshot-ref".equals(action)) {
        String refName = asString(update.get("ref-name"));
        if (refName == null || refName.isBlank()) {
          return validationError("set-snapshot-ref requires ref-name");
        }
        String type = asString(update.get("type"));
        if (type == null || type.isBlank()) {
          return validationError("set-snapshot-ref requires type");
        }
        Long pointedSnapshot = asLong(update.get("snapshot-id"));
        if (pointedSnapshot == null) {
          return validationError("set-snapshot-ref requires snapshot-id");
        }
        IcebergRef.Builder refBuilder =
            IcebergRef.newBuilder().setSnapshotId(pointedSnapshot).setType(type);
        Long maxRefAge =
            asLong(firstNonNull(update.get("max-ref-age-ms"), update.get("max_ref_age_ms")));
        if (maxRefAge != null) {
          refBuilder.setMaxReferenceAgeMs(maxRefAge);
        }
        Long maxSnapshotAge =
            asLong(
                firstNonNull(update.get("max-snapshot-age-ms"), update.get("max_snapshot_age_ms")));
        if (maxSnapshotAge != null) {
          refBuilder.setMaxSnapshotAgeMs(maxSnapshotAge);
        }
        Integer minSnapshots =
            asInteger(
                firstNonNull(
                    update.get("min-snapshots-to-keep"), update.get("min_snapshots_to_keep")));
        if (minSnapshots != null) {
          refBuilder.setMinSnapshotsToKeep(minSnapshots);
        }
        metadataChanges.refsToSet.put(refName, refBuilder.build());
      } else if ("remove-snapshot-ref".equals(action)) {
        String ref = asString(update.get("ref-name"));
        if (ref == null || ref.isBlank()) {
          return validationError("remove-snapshot-ref requires ref-name");
        }
        metadataChanges.refsToRemove.add(ref);
      } else if ("assign-uuid".equals(action)) {
        String uuid = asString(update.get("uuid"));
        if (uuid == null || uuid.isBlank()) {
          return validationError("assign-uuid requires uuid");
        }
        metadataChanges.tableUuid = uuid;
      } else if ("upgrade-format-version".equals(action)) {
        Integer version = asInteger(update.get("format-version"));
        if (version == null) {
          return validationError("upgrade-format-version requires format-version");
        }
        metadataChanges.formatVersion = version;
      } else if ("add-schema".equals(action)) {
        Map<String, Object> schemaMap = asObjectMap(update.get("schema"));
        if (schemaMap == null || schemaMap.isEmpty()) {
          return validationError("add-schema requires schema");
        }
        try {
          metadataChanges.schemasToAdd.add(buildIcebergSchema(schemaMap, update));
        } catch (IllegalArgumentException | JsonProcessingException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-current-schema".equals(action)) {
        Integer schemaId = asInteger(update.get("schema-id"));
        if (schemaId == null) {
          return validationError("set-current-schema requires schema-id");
        }
        if (schemaId == -1) {
          metadataChanges.setCurrentSchemaLast = true;
        } else {
          metadataChanges.currentSchemaId = schemaId;
        }
      } else if ("add-spec".equals(action)) {
        Map<String, Object> specMap = asObjectMap(update.get("spec"));
        if (specMap == null || specMap.isEmpty()) {
          return validationError("add-spec requires spec");
        }
        try {
          metadataChanges.partitionSpecsToAdd.add(buildPartitionSpec(specMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-default-spec".equals(action)) {
        Integer specId = asInteger(update.get("spec-id"));
        if (specId == null) {
          return validationError("set-default-spec requires spec-id");
        }
        if (specId == -1) {
          metadataChanges.setDefaultSpecLast = true;
        } else {
          metadataChanges.defaultSpecId = specId;
        }
      } else if ("remove-partition-specs".equals(action)) {
        List<Integer> specIds = asIntegerList(update.get("spec-ids"));
        if (specIds.isEmpty()) {
          return validationError("remove-partition-specs requires spec-ids");
        }
        metadataChanges.partitionSpecIdsToRemove.addAll(specIds);
      } else if ("add-sort-order".equals(action)) {
        Map<String, Object> orderMap = asObjectMap(update.get("sort-order"));
        if (orderMap == null || orderMap.isEmpty()) {
          return validationError("add-sort-order requires sort-order");
        }
        try {
          metadataChanges.sortOrdersToAdd.add(buildSortOrder(orderMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("set-default-sort-order".equals(action)) {
        Integer orderId = asInteger(update.get("sort-order-id"));
        if (orderId == null) {
          return validationError("set-default-sort-order requires sort-order-id");
        }
        if (orderId == -1) {
          metadataChanges.setDefaultSortOrderLast = true;
        } else {
          metadataChanges.defaultSortOrderId = orderId;
        }
      } else if ("set-statistics".equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-statistics requires statistics");
        }
        try {
          metadataChanges.statisticsToAdd.add(buildStatisticsFile(statsMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-statistics".equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-statistics requires snapshot-id");
        }
        metadataChanges.statisticsSnapshotsToRemove.add(snapId);
      } else if ("set-partition-statistics".equals(action)) {
        Map<String, Object> statsMap = asObjectMap(update.get("partition-statistics"));
        if (statsMap == null || statsMap.isEmpty()) {
          return validationError("set-partition-statistics requires partition-statistics");
        }
        try {
          metadataChanges.partitionStatisticsToAdd.add(buildPartitionStatisticsFile(statsMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-partition-statistics".equals(action)) {
        Long snapId = asLong(update.get("snapshot-id"));
        if (snapId == null) {
          return validationError("remove-partition-statistics requires snapshot-id");
        }
        metadataChanges.partitionStatisticsSnapshotsToRemove.add(snapId);
      } else if ("add-encryption-key".equals(action)) {
        Map<String, Object> keyMap = asObjectMap(update.get("encryption-key"));
        if (keyMap == null || keyMap.isEmpty()) {
          return validationError("add-encryption-key requires encryption-key");
        }
        try {
          metadataChanges.encryptionKeysToAdd.add(buildEncryptionKey(keyMap));
        } catch (IllegalArgumentException e) {
          return validationError(e.getMessage());
        }
      } else if ("remove-encryption-key".equals(action)) {
        String keyId = asString(update.get("key-id"));
        if (keyId == null || keyId.isBlank()) {
          return validationError("remove-encryption-key requires key-id");
        }
        metadataChanges.encryptionKeysToRemove.add(keyId);
      } else if ("remove-schemas".equals(action)) {
        List<Integer> schemaIds = asIntegerList(update.get("schema-ids"));
        if (schemaIds.isEmpty()) {
          return validationError("remove-schemas requires schema-ids");
        }
        metadataChanges.schemaIdsToRemove.addAll(schemaIds);
      }
    }
    if (metadataChanges.hasChanges()) {
      if (existing == null) {
        existing = tableSupplier.get();
      }
      Response error =
          applySnapshotMetadataUpdates(
              tableId, tableSupplier, existing, lastSnapshotId, metadataChanges);
      if (error != null) {
        return error;
      }
    }
    return null;
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

  private Response applyPropertyUpdates(
      Map<String, String> properties, List<Map<String, Object>> updates) {
    if (updates == null) {
      return null;
    }
    for (Map<String, Object> update : updates) {
      if (update == null) {
        return validationError("commit update entry cannot be null");
      }
      String action = asString(update.get("action"));
      if (action == null) {
        return validationError("commit update missing action");
      }
      switch (action) {
        case "set-properties" -> {
          Map<String, String> toSet = new LinkedHashMap<>(asStringMap(update.get("updates")));
          if (toSet.isEmpty()) {
            return validationError("set-properties requires updates");
          }
          stripMetadataLocation(toSet);
          if (!toSet.isEmpty()) {
            properties.putAll(toSet);
          }
        }
        case "remove-properties" -> {
          List<String> removals = asStringList(update.get("removals"));
          if (removals.isEmpty()) {
            return validationError("remove-properties requires removals");
          }
          removals.forEach(properties::remove);
        }
        default -> {
          // handled separately
        }
      }
    }
    return null;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  private static void stripMetadataLocation(Map<String, String> props) {
    if (props == null || props.isEmpty()) {
      return;
    }
    boolean removed = false;
    if (props.remove("metadata-location") != null) {
      removed = true;
    }
    if (props.remove("metadata_location") != null) {
      removed = true;
    }
    if (removed) {
      LOG.debug("Ignored commit metadata-location property override");
    }
  }

  private static List<String> copyOfOrNull(List<String> values) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    return List.copyOf(values);
  }

  private List<Predicate> buildPredicates(Map<String, Object> filter, Boolean caseSensitiveFlag) {
    if (filter == null || filter.isEmpty()) {
      return List.of();
    }
    boolean caseSensitive = caseSensitiveFlag == null || caseSensitiveFlag;
    List<Predicate> out = new ArrayList<>();
    parseFilterExpression(filter, caseSensitive, out);
    return List.copyOf(out);
  }

  private void parseFilterExpression(
      Object expression, boolean caseSensitive, List<Predicate> out) {
    Map<String, Object> expr = asObjectMap(expression);
    if (expr == null || expr.isEmpty()) {
      throw new IllegalArgumentException("filter expression must be an object");
    }
    String type = normalizeExpressionType(asString(expr.get("type")));
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("filter expression missing type");
    }
    switch (type) {
      case "and" -> {
        List<Map<String, Object>> children = expressionChildren(expr);
        if (children.isEmpty()) {
          throw new IllegalArgumentException("and expression requires child expressions");
        }
        for (Map<String, Object> child : children) {
          parseFilterExpression(child, caseSensitive, out);
        }
      }
      case "always_true" -> {
        // No-op
      }
      case "always_false", "or", "not" -> {
        throw new IllegalArgumentException("filter type " + type + " is not supported");
      }
      default -> out.add(buildLeafPredicate(type, expr, caseSensitive));
    }
  }

  private Predicate buildLeafPredicate(
      String type, Map<String, Object> expr, boolean caseSensitive) {
    String column = resolveColumn(expr.get("term"), caseSensitive);
    if (column == null || column.isBlank()) {
      throw new IllegalArgumentException("filter expression requires a term reference");
    }
    Operator op;
    List<String> values = List.of();
    switch (type) {
      case "eq", "equal", "equals", "==" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_EQ;
      }
      case "neq", "not_equal", "!=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_NEQ;
      }
      case "lt", "less_than", "<" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LT;
      }
      case "lte", "less_than_or_equal", "<=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_LTE;
      }
      case "gt", "greater_than", ">" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_GT;
      }
      case "gte", "greater_than_or_equal", ">=" -> {
        values = literalValues(expr, "literal", "value");
        op = Operator.OP_GTE;
      }
      case "between" -> {
        values = literalValues(expr, "literals", "values", "literal");
        if (values.size() < 2) {
          List<String> bounds = new ArrayList<>();
          String lower = literalValue(expr.get("lower"));
          String upper = literalValue(expr.get("upper"));
          if (lower != null) {
            bounds.add(lower);
          }
          if (upper != null) {
            bounds.add(upper);
          }
          values = bounds;
        }
        if (values.size() < 2) {
          throw new IllegalArgumentException("between expression requires two bounds");
        }
        values = List.of(values.get(0), values.get(1));
        op = Operator.OP_BETWEEN;
      }
      case "is_null", "is-null" -> {
        op = Operator.OP_IS_NULL;
        values = List.of();
      }
      case "is_not_null", "not_null", "not-null" -> {
        op = Operator.OP_IS_NOT_NULL;
        values = List.of();
      }
      case "in" -> {
        values = literalValues(expr, "literals", "values");
        op = Operator.OP_IN;
      }
      default -> throw new IllegalArgumentException("filter type " + type + " is not supported");
    }
    if (requiresLiteral(op) && values.isEmpty()) {
      throw new IllegalArgumentException(type + " filter requires a literal value");
    }
    Predicate.Builder builder = Predicate.newBuilder().setColumn(column).setOp(op);
    if (!values.isEmpty()) {
      builder.addAllValues(values);
    }
    return builder.build();
  }

  private boolean requiresLiteral(Operator op) {
    return switch (op) {
      case OP_EQ, OP_NEQ, OP_LT, OP_LTE, OP_GT, OP_GTE, OP_BETWEEN, OP_IN -> true;
      default -> false;
    };
  }

  private List<Map<String, Object>> expressionChildren(Map<String, Object> expr) {
    List<Map<String, Object>> expressions = asMapList(expr.get("expressions"));
    if (!expressions.isEmpty()) {
      return expressions;
    }
    List<Map<String, Object>> out = new ArrayList<>();
    Map<String, Object> left = asObjectMap(expr.get("left"));
    if (left != null && !left.isEmpty()) {
      out.add(left);
    }
    Map<String, Object> right = asObjectMap(expr.get("right"));
    if (right != null && !right.isEmpty()) {
      out.add(right);
    }
    return out;
  }

  private String normalizeExpressionType(String raw) {
    if (raw == null) {
      return null;
    }
    return raw.trim().toLowerCase(Locale.ROOT).replace('-', '_');
  }

  private String resolveColumn(Object termNode, boolean caseSensitive) {
    String column = null;
    if (termNode instanceof String str) {
      column = str;
    } else {
      Map<String, Object> term = asObjectMap(termNode);
      if (term != null) {
        column = asString(term.get("ref"));
        if (column == null || column.isBlank()) {
          column = asString(term.get("name"));
        }
        if (column == null || column.isBlank()) {
          column = asString(term.get("column"));
        }
      }
    }
    if (column == null || column.isBlank()) {
      return null;
    }
    return caseSensitive ? column : column.toLowerCase(Locale.ROOT);
  }

  private List<String> literalValues(Map<String, Object> expr, String... keys) {
    for (String key : keys) {
      if (expr.containsKey(key)) {
        List<String> values = literalList(expr.get(key));
        if (!values.isEmpty()) {
          return values;
        }
      }
    }
    return List.of();
  }

  private List<String> literalList(Object node) {
    if (node == null) {
      return List.of();
    }
    if (node instanceof List<?> list) {
      List<String> out = new ArrayList<>();
      for (Object item : list) {
        String value = literalValue(item);
        if (value != null) {
          out.add(value);
        }
      }
      return out;
    }
    Map<String, Object> map = asObjectMap(node);
    if (map != null && !map.isEmpty()) {
      if (map.containsKey("values")) {
        return literalList(map.get("values"));
      }
      if (map.containsKey("literals")) {
        return literalList(map.get("literals"));
      }
      if (map.containsKey("literal")) {
        return literalList(map.get("literal"));
      }
      if (map.containsKey("value")) {
        return literalList(map.get("value"));
      }
    }
    String single = literalValue(node);
    return single == null ? List.of() : List.of(single);
  }

  private String literalValue(Object node) {
    if (node == null) {
      return null;
    }
    Map<String, Object> map = asObjectMap(node);
    if (map != null && !map.isEmpty()) {
      Object value =
          firstPresent(
              map.get("value"),
              map.get("literal"),
              map.get("string"),
              map.get("long"),
              map.get("int"),
              map.get("double"),
              map.get("float"),
              map.get("boolean"));
      return value == null ? null : value.toString();
    }
    return node.toString();
  }

  private Object firstPresent(Object... values) {
    for (Object value : values) {
      if (value != null) {
        return value;
      }
    }
    return null;
  }

  private static Integer asInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.intValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Integer propertyInt(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Long propertyLong(Map<String, String> props, String key) {
    String value = props.get(key);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static Object firstNonNull(Object first, Object second) {
    return first != null ? first : second;
  }

  private static Map<String, Object> asObjectMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return null;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null) {
        continue;
      }
      out.put(entry.getKey().toString(), entry.getValue());
    }
    return out;
  }

  private static List<Integer> asIntegerList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Integer> out = new ArrayList<>();
    for (Object item : list) {
      Integer val = asInteger(item);
      if (val != null) {
        out.add(val);
      }
    }
    return out;
  }

  private static List<Map<String, Object>> asMapList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Map<String, Object>> out = new ArrayList<>();
    for (Object item : list) {
      Map<String, Object> map = asObjectMap(item);
      if (map != null && !map.isEmpty()) {
        out.add(map);
      }
    }
    return out;
  }

  private IcebergSchema buildIcebergSchema(
      Map<String, Object> schemaMap, Map<String, Object> update) throws JsonProcessingException {
    Integer schemaId = asInteger(schemaMap.get("schema-id"));
    if (schemaId == null) {
      throw new IllegalArgumentException("add-schema requires schema.schema-id");
    }
    String schemaJson = mapper.writeValueAsString(schemaMap);
    IcebergSchema.Builder builder =
        IcebergSchema.newBuilder().setSchemaId(schemaId).setSchemaJson(schemaJson);
    Integer lastColumnId =
        asInteger(firstNonNull(update.get("last-column-id"), schemaMap.get("last-column-id")));
    if (lastColumnId != null) {
      builder.setLastColumnId(lastColumnId);
    }
    List<Integer> identifierIds = asIntegerList(schemaMap.get("identifier-field-ids"));
    if (!identifierIds.isEmpty()) {
      builder.addAllIdentifierFieldIds(identifierIds);
    }
    return builder.build();
  }

  private PartitionSpecInfo buildPartitionSpec(Map<String, Object> specMap) {
    Integer specId = asInteger(specMap.get("spec-id"));
    if (specId == null) {
      throw new IllegalArgumentException("add-spec requires spec.spec-id");
    }
    PartitionSpecInfo.Builder builder = PartitionSpecInfo.newBuilder().setSpecId(specId);
    String specName = asString(specMap.get("name"));
    builder.setSpecName(specName == null || specName.isBlank() ? "spec-" + specId : specName);
    List<Map<String, Object>> fields = asMapList(specMap.get("fields"));
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("add-spec requires spec.fields");
    }
    for (Map<String, Object> field : fields) {
      String name = asString(field.get("name"));
      Integer fieldId = asInteger(firstNonNull(field.get("field-id"), field.get("source-id")));
      String transform = asString(field.get("transform"));
      if (name == null || fieldId == null || transform == null || transform.isBlank()) {
        throw new IllegalArgumentException(
            "add-spec fields require name, field-id/source-id, and transform");
      }
      builder.addFields(
          PartitionField.newBuilder()
              .setFieldId(fieldId)
              .setName(name)
              .setTransform(transform)
              .build());
    }
    return builder.build();
  }

  private IcebergSortOrder buildSortOrder(Map<String, Object> orderMap) {
    Integer orderId =
        asInteger(firstNonNull(orderMap.get("sort-order-id"), orderMap.get("order-id")));
    if (orderId == null) {
      throw new IllegalArgumentException("add-sort-order requires sort-order.sort-order-id");
    }
    IcebergSortOrder.Builder builder = IcebergSortOrder.newBuilder().setSortOrderId(orderId);
    List<Map<String, Object>> fields = asMapList(orderMap.get("fields"));
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("add-sort-order requires sort-order.fields");
    }
    for (Map<String, Object> field : fields) {
      Integer sourceId = asInteger(field.get("source-id"));
      if (sourceId == null) {
        throw new IllegalArgumentException("sort-order.fields require source-id");
      }
      String transform = asString(field.get("transform"));
      if (transform == null || transform.isBlank()) {
        transform = "identity";
      }
      String direction = asString(field.get("direction"));
      if (direction == null || direction.isBlank()) {
        direction = "ASC";
      }
      String nullOrder = asString(field.get("null-order"));
      if (nullOrder == null || nullOrder.isBlank()) {
        nullOrder = "NULLS_FIRST";
      }
      builder.addFields(
          IcebergSortField.newBuilder()
              .setSourceFieldId(sourceId)
              .setTransform(transform)
              .setDirection(direction)
              .setNullOrder(nullOrder)
              .build());
    }
    return builder.build();
  }

  private IcebergStatisticsFile buildStatisticsFile(Map<String, Object> statsMap) {
    Long snapshotId = asLong(statsMap.get("snapshot-id"));
    String path = asString(statsMap.get("statistics-path"));
    Long size = asLong(statsMap.get("file-size-in-bytes"));
    Long footerSize = asLong(statsMap.get("file-footer-size-in-bytes"));
    List<Map<String, Object>> blobMaps = asMapList(statsMap.get("blob-metadata"));
    if (snapshotId == null || path == null || path.isBlank() || size == null) {
      throw new IllegalArgumentException(
          "set-statistics requires snapshot-id, statistics-path, and file-size-in-bytes");
    }
    IcebergStatisticsFile.Builder builder =
        IcebergStatisticsFile.newBuilder()
            .setSnapshotId(snapshotId)
            .setStatisticsPath(path)
            .setFileSizeInBytes(size);
    if (footerSize != null) {
      builder.setFileFooterSizeInBytes(footerSize);
    }
    if (!blobMaps.isEmpty()) {
      for (Map<String, Object> blobMap : blobMaps) {
        builder.addBlobMetadata(buildBlobMetadata(blobMap));
      }
    }
    return builder.build();
  }

  private IcebergBlobMetadata buildBlobMetadata(Map<String, Object> blobMap) {
    String type = asString(blobMap.get("type"));
    Long snapshotId = asLong(blobMap.get("snapshot-id"));
    Long sequenceNumber = asLong(blobMap.get("sequence-number"));
    List<Integer> fields = asIntegerList(blobMap.get("fields"));
    if (type == null
        || type.isBlank()
        || snapshotId == null
        || sequenceNumber == null
        || fields.isEmpty()) {
      throw new IllegalArgumentException(
          "statistics blob-metadata requires type, snapshot-id, sequence-number, and fields");
    }
    IcebergBlobMetadata.Builder builder =
        IcebergBlobMetadata.newBuilder()
            .setType(type)
            .setSnapshotId(snapshotId)
            .setSequenceNumber(sequenceNumber)
            .addAllFields(fields);
    Map<String, String> props = asStringMap(blobMap.get("properties"));
    if (!props.isEmpty()) {
      builder.putAllProperties(props);
    }
    return builder.build();
  }

  private IcebergPartitionStatisticsFile buildPartitionStatisticsFile(
      Map<String, Object> statsMap) {
    Long snapshotId = asLong(statsMap.get("snapshot-id"));
    String path = asString(statsMap.get("statistics-path"));
    Long size = asLong(statsMap.get("file-size-in-bytes"));
    if (snapshotId == null || path == null || path.isBlank() || size == null) {
      throw new IllegalArgumentException(
          "set-partition-statistics requires snapshot-id, statistics-path, and file-size-in-bytes");
    }
    return IcebergPartitionStatisticsFile.newBuilder()
        .setSnapshotId(snapshotId)
        .setStatisticsPath(path)
        .setFileSizeInBytes(size)
        .build();
  }

  private IcebergEncryptedKey buildEncryptionKey(Map<String, Object> keyMap) {
    String keyId = asString(keyMap.get("key-id"));
    String metadataB64 = asString(keyMap.get("encrypted-key-metadata"));
    if (keyId == null || keyId.isBlank() || metadataB64 == null || metadataB64.isBlank()) {
      throw new IllegalArgumentException(
          "add-encryption-key requires key-id and encrypted-key-metadata");
    }
    byte[] decoded;
    try {
      decoded = Base64.getDecoder().decode(metadataB64);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("encrypted-key-metadata must be valid base64");
    }
    IcebergEncryptedKey.Builder builder =
        IcebergEncryptedKey.newBuilder()
            .setKeyId(keyId)
            .setEncryptedKeyMetadata(ByteString.copyFrom(decoded));
    String encryptedBy = asString(keyMap.get("encrypted-by-id"));
    if (encryptedBy != null && !encryptedBy.isBlank()) {
      builder.setEncryptedById(encryptedBy);
    }
    return builder.build();
  }

  private static Integer lastSchemaId(IcebergMetadata.Builder builder) {
    int count = builder.getSchemasCount();
    if (count == 0) {
      return null;
    }
    return builder.getSchemas(count - 1).getSchemaId();
  }

  private static Integer lastPartitionSpecId(IcebergMetadata.Builder builder) {
    int count = builder.getPartitionSpecsCount();
    if (count == 0) {
      return null;
    }
    return builder.getPartitionSpecs(count - 1).getSpecId();
  }

  private static Integer lastSortOrderId(IcebergMetadata.Builder builder) {
    int count = builder.getSortOrdersCount();
    if (count == 0) {
      return null;
    }
    return builder.getSortOrders(count - 1).getSortOrderId();
  }

  private static final class SnapshotMetadataChanges {
    String tableUuid;
    Integer formatVersion;
    final List<IcebergSchema> schemasToAdd = new ArrayList<>();
    final Set<Integer> schemaIdsToRemove = new LinkedHashSet<>();
    Integer currentSchemaId;
    boolean setCurrentSchemaLast;
    final List<PartitionSpecInfo> partitionSpecsToAdd = new ArrayList<>();
    final Set<Integer> partitionSpecIdsToRemove = new LinkedHashSet<>();
    Integer defaultSpecId;
    boolean setDefaultSpecLast;
    final List<IcebergSortOrder> sortOrdersToAdd = new ArrayList<>();
    Integer defaultSortOrderId;
    boolean setDefaultSortOrderLast;
    final List<IcebergStatisticsFile> statisticsToAdd = new ArrayList<>();
    final Set<Long> statisticsSnapshotsToRemove = new LinkedHashSet<>();
    final List<IcebergPartitionStatisticsFile> partitionStatisticsToAdd = new ArrayList<>();
    final Set<Long> partitionStatisticsSnapshotsToRemove = new LinkedHashSet<>();
    final List<IcebergEncryptedKey> encryptionKeysToAdd = new ArrayList<>();
    final Set<String> encryptionKeysToRemove = new LinkedHashSet<>();
    final Map<String, IcebergRef> refsToSet = new LinkedHashMap<>();
    final Set<String> refsToRemove = new LinkedHashSet<>();

    boolean hasChanges() {
      return tableUuid != null
          || formatVersion != null
          || !schemasToAdd.isEmpty()
          || !schemaIdsToRemove.isEmpty()
          || currentSchemaId != null
          || setCurrentSchemaLast
          || !partitionSpecsToAdd.isEmpty()
          || !partitionSpecIdsToRemove.isEmpty()
          || defaultSpecId != null
          || setDefaultSpecLast
          || !sortOrdersToAdd.isEmpty()
          || defaultSortOrderId != null
          || setDefaultSortOrderLast
          || !statisticsToAdd.isEmpty()
          || !statisticsSnapshotsToRemove.isEmpty()
          || !partitionStatisticsToAdd.isEmpty()
          || !partitionStatisticsSnapshotsToRemove.isEmpty()
          || !encryptionKeysToAdd.isEmpty()
          || !encryptionKeysToRemove.isEmpty()
          || !refsToSet.isEmpty()
          || !refsToRemove.isEmpty();
    }
  }

  private static Map<String, String> asStringMap(Object value) {
    if (!(value instanceof Map<?, ?> map)) {
      return Map.of();
    }
    Map<String, String> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getKey() == null || entry.getValue() == null) {
        continue;
      }
      result.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return result;
  }

  private static List<String> asStringList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (Object item : list) {
      if (item != null) {
        out.add(item.toString());
      }
    }
    return out;
  }

  private Response createSnapshotPlaceholder(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      Table existing,
      Map<String, Object> snapshot,
      String idempotencyKey) {
    Long snapshotId = asLong(snapshot.get("snapshot-id"));
    if (snapshotId == null) {
      return validationError("add-snapshot requires snapshot.snapshot-id");
    }
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    SnapshotSpec.Builder spec =
        SnapshotSpec.newBuilder().setTableId(tableId).setSnapshotId(snapshotId);
    Long upstreamCreated = asLong(snapshot.get("timestamp-ms"));
    if (upstreamCreated != null) {
      spec.setUpstreamCreatedAt(Timestamps.fromMillis(upstreamCreated));
    }
    Long parentId = asLong(snapshot.get("parent-snapshot-id"));
    if (parentId != null) {
      spec.setParentSnapshotId(parentId);
    }
    Long sequenceNumber = asLong(snapshot.get("sequence-number"));
    if (sequenceNumber != null) {
      spec.setSequenceNumber(sequenceNumber);
    }
    String manifestList = asString(snapshot.get("manifest-list"));
    if (manifestList != null && !manifestList.isBlank()) {
      spec.setManifestList(manifestList);
    }
    Map<String, String> summary = asStringMap(snapshot.get("summary"));
    if (!summary.isEmpty()) {
      spec.putAllSummary(summary);
    }
    Integer schemaId = asInteger(snapshot.get("schema-id"));
    IcebergMetadata metadata = null;
    if (schemaId == null) {
      schemaId = propertyInt(existing.getPropertiesMap(), "current-schema-id");
    }
    if (schemaId == null) {
      metadata = tableSupport.loadCurrentMetadata(existing);
      if (metadata != null && metadata.getCurrentSchemaId() > 0) {
        schemaId = metadata.getCurrentSchemaId();
      }
    } else {
      metadata = tableSupport.loadCurrentMetadata(existing);
    }
    if (schemaId == null) {
      schemaId = 0;
    }
    spec.setSchemaId(schemaId);
    String schemaJson = asString(snapshot.get("schema-json"));
    if (schemaJson == null || schemaJson.isBlank()) {
      schemaJson = existing.getSchemaJson();
    }
    if ((schemaJson == null || schemaJson.isBlank()) && metadata != null) {
      schemaJson = schemaJsonFromMetadata(metadata, schemaId);
    }
    if (schemaJson != null && !schemaJson.isBlank()) {
      spec.setSchemaJson(schemaJson);
    }
    CreateSnapshotRequest.Builder request =
        CreateSnapshotRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":snapshot:" + snapshotId).build());
    }
    stub.createSnapshot(request.build());
    return null;
  }

  private void deleteSnapshots(ResourceId tableId, List<Long> snapshotIds) {
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    for (Long id : snapshotIds) {
      if (id == null) {
        continue;
      }
      stub.deleteSnapshot(
          DeleteSnapshotRequest.newBuilder().setTableId(tableId).setSnapshotId(id).build());
    }
  }

  private ResourceId resolveConnectorId(Table tableRecord) {
    if (tableRecord == null
        || !tableRecord.hasUpstream()
        || !tableRecord.getUpstream().hasConnectorId()) {
      return null;
    }
    ResourceId connectorId = tableRecord.getUpstream().getConnectorId();
    if (connectorId == null || connectorId.getId().isBlank()) {
      return null;
    }
    return connectorId;
  }

  private void runConnectorSyncIfPossible(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    if (connectorId == null || connectorId.getId().isBlank()) {
      return;
    }
    tableSupport.triggerScopedReconcile(connectorId, namespacePath, tableName);
  }

  private static Long asLong(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number number) {
      return number.longValue();
    }
    String text = value.toString();
    if (text.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(text);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static List<Long> asLongList(Object value) {
    if (!(value instanceof List<?> list)) {
      return List.of();
    }
    List<Long> out = new ArrayList<>();
    for (Object item : list) {
      Long val = asLong(item);
      if (val != null) {
        out.add(val);
      }
    }
    return out;
  }

  private Response applySnapshotMetadataUpdates(
      ResourceId tableId,
      Supplier<Table> tableSupplier,
      Table existingTable,
      Long preferredSnapshotId,
      SnapshotMetadataChanges changes) {
    if (!changes.hasChanges()) {
      return null;
    }
    Table table = existingTable != null ? existingTable : tableSupplier.get();
    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    Snapshot snapshot;
    Long targetSnapshotId = preferredSnapshotId;
    if (targetSnapshotId == null) {
      try {
        snapshot =
            stub.getSnapshot(
                    GetSnapshotRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshot(
                            SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
                        .build())
                .getSnapshot();
        targetSnapshotId = snapshot.getSnapshotId();
      } catch (StatusRuntimeException e) {
        Long propertySnapshot = propertyLong(table.getPropertiesMap(), "current-snapshot-id");
        targetSnapshotId = propertySnapshot;
        snapshot = null;
      }
    } else {
      snapshot = null;
    }

    if (targetSnapshotId == null || targetSnapshotId <= 0) {
      return validationError("snapshot metadata updates require current snapshot id");
    }

    if (snapshot == null) {
      snapshot =
          stub.getSnapshot(
                  GetSnapshotRequest.newBuilder()
                      .setTableId(tableId)
                      .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(targetSnapshotId))
                      .build())
              .getSnapshot();
    }

    IcebergMetadata.Builder iceberg =
        snapshot.hasIceberg() ? snapshot.getIceberg().toBuilder() : IcebergMetadata.newBuilder();
    boolean mutated = false;

    if (changes.tableUuid != null) {
      iceberg.setTableUuid(changes.tableUuid);
      mutated = true;
    }
    if (changes.formatVersion != null) {
      iceberg.setFormatVersion(changes.formatVersion);
      mutated = true;
    }
    if (!changes.schemaIdsToRemove.isEmpty()) {
      List<IcebergSchema> filtered = new ArrayList<>();
      for (IcebergSchema schema : iceberg.getSchemasList()) {
        if (changes.schemaIdsToRemove.contains(schema.getSchemaId())) {
          mutated = true;
          continue;
        }
        filtered.add(schema);
      }
      if (filtered.size() != iceberg.getSchemasCount()) {
        iceberg.clearSchemas();
        iceberg.addAllSchemas(filtered);
      }
    }
    if (!changes.schemasToAdd.isEmpty()) {
      iceberg.addAllSchemas(changes.schemasToAdd);
      mutated = true;
    }
    if (changes.setCurrentSchemaLast) {
      Integer lastSchema = lastSchemaId(iceberg);
      if (lastSchema == null) {
        return validationError("set-current-schema requires at least one schema");
      }
      iceberg.setCurrentSchemaId(lastSchema);
      mutated = true;
    } else if (changes.currentSchemaId != null) {
      iceberg.setCurrentSchemaId(changes.currentSchemaId);
      mutated = true;
    }
    if (!changes.partitionSpecIdsToRemove.isEmpty()) {
      List<PartitionSpecInfo> filtered = new ArrayList<>();
      for (PartitionSpecInfo spec : iceberg.getPartitionSpecsList()) {
        if (changes.partitionSpecIdsToRemove.contains(spec.getSpecId())) {
          mutated = true;
          continue;
        }
        filtered.add(spec);
      }
      if (filtered.size() != iceberg.getPartitionSpecsCount()) {
        iceberg.clearPartitionSpecs();
        iceberg.addAllPartitionSpecs(filtered);
      }
    }
    if (!changes.partitionSpecsToAdd.isEmpty()) {
      iceberg.addAllPartitionSpecs(changes.partitionSpecsToAdd);
      mutated = true;
    }
    if (changes.setDefaultSpecLast) {
      Integer lastSpec = lastPartitionSpecId(iceberg);
      if (lastSpec == null) {
        return validationError("set-default-spec requires at least one partition spec");
      }
      iceberg.setDefaultSpecId(lastSpec);
      mutated = true;
    } else if (changes.defaultSpecId != null) {
      iceberg.setDefaultSpecId(changes.defaultSpecId);
      mutated = true;
    }
    if (!changes.sortOrdersToAdd.isEmpty()) {
      iceberg.addAllSortOrders(changes.sortOrdersToAdd);
      mutated = true;
    }
    if (changes.setDefaultSortOrderLast) {
      Integer lastSortOrder = lastSortOrderId(iceberg);
      if (lastSortOrder == null) {
        return validationError("set-default-sort-order requires at least one sort order");
      }
      iceberg.setDefaultSortOrderId(lastSortOrder);
      mutated = true;
    } else if (changes.defaultSortOrderId != null) {
      iceberg.setDefaultSortOrderId(changes.defaultSortOrderId);
      mutated = true;
    }

    if (!changes.statisticsSnapshotsToRemove.isEmpty() || !changes.statisticsToAdd.isEmpty()) {
      var replace =
          changes.statisticsToAdd.stream()
              .map(IcebergStatisticsFile::getSnapshotId)
              .collect(Collectors.toSet());
      List<IcebergStatisticsFile> filtered = new ArrayList<>();
      for (IcebergStatisticsFile file : iceberg.getStatisticsList()) {
        long snapId = file.getSnapshotId();
        if (changes.statisticsSnapshotsToRemove.contains(snapId) || replace.contains(snapId)) {
          mutated = true;
          continue;
        }
        filtered.add(file);
      }
      if (!changes.statisticsToAdd.isEmpty()) {
        filtered.addAll(changes.statisticsToAdd);
        mutated = true;
      }
      if (mutated) {
        iceberg.clearStatistics();
        iceberg.addAllStatistics(filtered);
      }
    }
    if (!changes.partitionStatisticsSnapshotsToRemove.isEmpty()
        || !changes.partitionStatisticsToAdd.isEmpty()) {
      var replace =
          changes.partitionStatisticsToAdd.stream()
              .map(IcebergPartitionStatisticsFile::getSnapshotId)
              .collect(Collectors.toSet());
      List<IcebergPartitionStatisticsFile> filtered = new ArrayList<>();
      for (IcebergPartitionStatisticsFile file : iceberg.getPartitionStatisticsList()) {
        long snapId = file.getSnapshotId();
        if (changes.partitionStatisticsSnapshotsToRemove.contains(snapId)
            || replace.contains(snapId)) {
          mutated = true;
          continue;
        }
        filtered.add(file);
      }
      if (!changes.partitionStatisticsToAdd.isEmpty()) {
        filtered.addAll(changes.partitionStatisticsToAdd);
        mutated = true;
      }
      if (mutated) {
        iceberg.clearPartitionStatistics();
        iceberg.addAllPartitionStatistics(filtered);
      }
    }
    if (!changes.encryptionKeysToRemove.isEmpty() || !changes.encryptionKeysToAdd.isEmpty()) {
      Map<String, IcebergEncryptedKey> keys = new LinkedHashMap<>();
      for (IcebergEncryptedKey key : iceberg.getEncryptionKeysList()) {
        if (changes.encryptionKeysToRemove.contains(key.getKeyId())) {
          mutated = true;
          continue;
        }
        keys.put(key.getKeyId(), key);
      }
      for (IcebergEncryptedKey key : changes.encryptionKeysToAdd) {
        keys.put(key.getKeyId(), key);
        mutated = true;
      }
      iceberg.clearEncryptionKeys();
      iceberg.addAllEncryptionKeys(keys.values());
    }

    if (!changes.refsToRemove.isEmpty() || !changes.refsToSet.isEmpty()) {
      var refs = new LinkedHashMap<>(iceberg.getRefsMap());
      if (!changes.refsToRemove.isEmpty()) {
        mutated = refs.keySet().removeAll(changes.refsToRemove) || mutated;
      }
      if (!changes.refsToSet.isEmpty()) {
        refs.putAll(changes.refsToSet);
        mutated = true;
      }
      iceberg.clearRefs();
      iceberg.putAllRefs(refs);
    }

    if (!mutated) {
      return null;
    }

    SnapshotSpec.Builder spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(targetSnapshotId)
            .setIceberg(iceberg);
    FieldMask mask = FieldMask.newBuilder().addPaths("iceberg").build();

    stub.updateSnapshot(
        UpdateSnapshotRequest.newBuilder().setSpec(spec).setUpdateMask(mask).build());
    return null;
  }

  private record PlanContext(
      ResourceId tableId,
      List<String> requiredColumns,
      Long startSnapshotId,
      Long snapshotId,
      List<Predicate> predicates,
      List<String> statsFields,
      Boolean useSnapshotSchema,
      Boolean caseSensitive,
      Long minRowsRequested) {}
}
