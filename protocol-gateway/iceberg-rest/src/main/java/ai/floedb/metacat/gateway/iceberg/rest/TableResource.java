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
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.PartitionField;
import ai.floedb.metacat.catalog.rpc.PartitionSpecInfo;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotSpec;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.connector.rpc.AuthConfig;
import ai.floedb.metacat.connector.rpc.ConnectorKind;
import ai.floedb.metacat.connector.rpc.ConnectorSpec;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.connector.rpc.CreateConnectorRequest;
import ai.floedb.metacat.connector.rpc.CreateConnectorResponse;
import ai.floedb.metacat.connector.rpc.NamespacePath;
import ai.floedb.metacat.connector.rpc.SourceSelector;
import ai.floedb.metacat.connector.rpc.TriggerReconcileRequest;
import ai.floedb.metacat.execution.rpc.ScanBundle;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.DescribeInputsRequest;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.FetchScanBundleRequest;
import ai.floedb.metacat.query.rpc.GetQueryRequest;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class TableResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ObjectMapper mapper;

  @GET
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
  public Response create(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Create req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));

    String tableName = req != null && req.name() != null ? req.name() : "table";

    TableSpec.Builder spec = buildCreateSpec(catalogId, namespaceId, tableName, req);

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = stub.createTable(request.build());
    return Response.ok(TableResponseMapper.toLoadResult(tableName, created.getTable())).build();
  }

  @Path("/{table}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("snapshots") String snapshots) {
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    var resp = stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build());
    return Response.ok(TableResponseMapper.toLoadResult(table, resp.getTable())).build();
  }

  @Path("/{table}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    return Response.noContent().build();
  }

  @Path("/{table}")
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
        req == null
            ? null
            : new TableRequests.Commit(
                req.name(), req.namespace(), req.schemaJson(), req.properties(), null, null));
  }

  @Path("/{table}")
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

  @Path("/{table}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      TableRequests.Commit req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    Supplier<Table> tableSupplier =
        new Supplier<>() {
          private Table cached;

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

    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
      List<String> namespacePath = NamespacePaths.split(namespace);
      Response requirementError = validateRequirements(req.requirements(), tableSupplier);
      if (requirementError != null) {
        return requirementError;
      }
      if (req.name() != null) {
        spec.setDisplayName(req.name());
        mask.addPaths("display_name");
      }
      if (req.namespace() != null) {
        var targetNs =
            NameResolution.resolveNamespace(
                grpc, catalogName, NamespacePaths.split(req.namespace()));
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
        return specNotImplemented("commit update action " + unsupported);
      }
      if (mergedProps != null) {
        spec.clearProperties().putAllProperties(mergedProps);
        mask.addPaths("properties");
      }
    }
    var resp =
        stub.updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    return Response.ok(TableResponseMapper.toCommitResponse(table, resp.getTable())).build();
  }

  @Path("/{table}/plan")
  @POST
  public Response planTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      PlanRequests.Plan rawRequest) {
    PlanRequests.Plan request = rawRequest == null ? PlanRequests.Plan.empty() : rawRequest;
    if (request.startSnapshotId() != null || request.endSnapshotId() != null) {
      return validationError("Incremental scans are not supported yet");
    }
    if (request.filter() != null && !request.filter().isEmpty()) {
      return validationError("Filter expressions are not supported yet");
    }
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var begin = queryStub.beginQuery(BeginQueryRequest.newBuilder().build());
    String queryId = begin.getQuery().getQueryId();
    registerPlanInput(queryId, tableId, request);
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, tableId, queryId, request.select());
    return Response.ok(toPlanResult(begin.getQuery(), null, bundle)).build();
  }

  @Path("/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    QueryServiceGrpc.QueryServiceBlockingStub queryStub = grpc.withHeaders(grpc.raw().query());
    var resp = queryStub.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle = fetchScanBundle(scanStub, tableId, resp.getQuery().getQueryId(), null);
    return Response.ok(toPlanResult(resp.getQuery(), planId, bundle)).build();
  }

  @Path("/{table}/plan/{planId}")
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
    return Response.noContent().build();
  }

  @Path("/{table}/tasks")
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
    QueryScanServiceGrpc.QueryScanServiceBlockingStub scanStub =
        grpc.withHeaders(grpc.raw().queryScan());
    ScanBundle bundle =
        fetchScanBundle(scanStub, tableId, request.planTask().trim(), /*requiredColumns*/ null);
    return Response.ok(toScanTasksDto(bundle)).build();
  }

  @Path("/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @QueryParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    return Response.ok(new CredentialsResponseDto(List.of())).build();
  }

  @Path("/{table}/metrics")
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
    String catalogName = resolveCatalog(prefix);
    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stub =
        grpc.withHeaders(grpc.raw().stats());

    TableStats.Builder stats =
        TableStats.newBuilder().setTableId(tableId).setSnapshotId(request.snapshotId());
    if (request.metadata() != null) {
      stats.putAllProperties(request.metadata());
    }
    if (request.reportType() != null && !request.reportType().isBlank()) {
      stats.putProperties("report-type", request.reportType());
    }
    if (request.operation() != null && !request.operation().isBlank()) {
      stats.putProperties("operation", request.operation());
    }
    if (request.sequenceNumber() != null) {
      stats.putProperties("sequence-number", request.sequenceNumber().toString());
    }
    if (request.schemaId() != null) {
      stats.putProperties("schema-id", request.schemaId().toString());
    }
    if (request.projectedFieldIds() != null && !request.projectedFieldIds().isEmpty()) {
      stats.putProperties(
          "projected-field-ids",
          request.projectedFieldIds().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(",")));
    }
    if (request.projectedFieldNames() != null && !request.projectedFieldNames().isEmpty()) {
      stats.putProperties("projected-field-names", String.join(",", request.projectedFieldNames()));
    }
    if (request.metrics() != null && !request.metrics().isEmpty()) {
      String metricsJson = toJson(request.metrics());
      if (metricsJson == null) {
        return validationError("invalid metrics payload");
      }
      stats.putProperties("metrics", metricsJson);
    }

    var put =
        ai.floedb.metacat.catalog.rpc.PutTableStatsRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(request.snapshotId())
            .setStats(stats);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      put.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    stub.putTableStats(put.build());
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

    TableSpec.Builder spec = baseTableSpec(catalogId, namespaceId, tableName);
    addMetadataLocationProperties(spec, req.metadataLocation());

    TableServiceGrpc.TableServiceBlockingStub tableStub = grpc.withHeaders(grpc.raw().table());
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = tableStub.createTable(request.build());

    var connectorTemplate = connectorTemplateFor(prefix);
    ResourceId connectorId;
    String upstreamUri;
    if (connectorTemplate != null && connectorTemplate.uri() != null) {
      connectorId =
          createTemplateConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getTable().getResourceId(),
              connectorTemplate,
              idempotencyKey);
      upstreamUri = connectorTemplate.uri();
    } else {
      connectorId =
          createExternalConnector(
              prefix,
              namespacePath,
              namespaceId,
              catalogId,
              tableName,
              created.getTable().getResourceId(),
              req.metadataLocation(),
              idempotencyKey);
      upstreamUri = req.metadataLocation();
    }

    updateTableUpstream(
        created.getTable().getResourceId(), namespacePath, tableName, connectorId, upstreamUri);

    triggerScopedReconcile(connectorId, namespacePath, tableName);

    return Response.ok(TableResponseMapper.toLoadResult(tableName, created.getTable())).build();
  }

  private TableSpec.Builder buildCreateSpec(
      ResourceId catalogId, ResourceId namespaceId, String tableName, TableRequests.Create req) {
    TableSpec.Builder spec = baseTableSpec(catalogId, namespaceId, tableName);
    if (req != null) {
      if (req.schemaJson() != null) {
        spec.setSchemaJson(req.schemaJson());
      }
      if (req.properties() != null) {
        spec.putAllProperties(req.properties());
      }
    }
    return spec;
  }

  private TableSpec.Builder baseTableSpec(
      ResourceId catalogId, ResourceId namespaceId, String tableName) {
    return TableSpec.newBuilder()
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(tableName)
        .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build());
  }

  private void addMetadataLocationProperties(TableSpec.Builder spec, String metadataLocation) {
    if (metadataLocation == null || metadataLocation.isBlank()) {
      return;
    }
    spec.putProperties("metadata-location", metadataLocation);
    spec.putProperties("metadata_location", metadataLocation);
  }

  private void triggerScopedReconcile(
      ResourceId connectorId, List<String> namespacePath, String tableName) {
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    TriggerReconcileRequest.Builder request =
        TriggerReconcileRequest.newBuilder()
            .setConnectorId(connectorId)
            .setFullRescan(false)
            .setDestinationTableDisplayName(tableName);
    if (namespacePath != null && !namespacePath.isEmpty()) {
      request.addDestinationNamespacePaths(
          NamespacePath.newBuilder().addAllSegments(namespacePath).build());
    }
    stub.triggerReconcile(request.build());
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

  private QueryInput buildPlanInput(ResourceId tableId, PlanRequests.Plan request) {
    QueryInput.Builder input = QueryInput.newBuilder().setTableId(tableId);
    if (request.snapshotId() != null) {
      input.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(request.snapshotId()));
    } else {
      input.setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT));
    }
    return input.build();
  }

  private void registerPlanInput(String queryId, ResourceId tableId, PlanRequests.Plan request) {
    QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemaStub =
        grpc.withHeaders(grpc.raw().querySchema());
    schemaStub.describeInputs(
        DescribeInputsRequest.newBuilder()
            .setQueryId(queryId)
            .addInputs(buildPlanInput(tableId, request))
            .build());
  }

  private PlanResponseDto toPlanResult(
      QueryDescriptor descriptor, String planIdOverride, ScanBundle bundle) {
    ScanTasksResponseDto scanTasks = toScanTasksDto(bundle);
    String queryId = descriptor.getQueryId();
    String planId =
        (queryId == null || queryId.isBlank())
            ? (planIdOverride == null ? "" : planIdOverride)
            : queryId;
    return new PlanResponseDto(
        "completed",
        planId,
        scanTasks.planTasks(),
        scanTasks.fileScanTasks(),
        scanTasks.deleteFiles(),
        null);
  }

  private ScanBundle fetchScanBundle(
      QueryScanServiceGrpc.QueryScanServiceBlockingStub stub,
      ResourceId tableId,
      String queryId,
      List<String> requiredColumns) {
    FetchScanBundleRequest.Builder builder =
        FetchScanBundleRequest.newBuilder().setQueryId(queryId).setTableId(tableId);
    if (requiredColumns != null && !requiredColumns.isEmpty()) {
      builder.addAllRequiredColumns(requiredColumns);
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
    for (Map<String, Object> requirement : requirements) {
      if (requirement == null) {
        return validationError("commit requirement entry cannot be null");
      }
      String type = asString(requirement.get("type"));
      if (type == null || type.isBlank()) {
        return validationError("commit requirement missing type");
      }
      switch (type) {
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
        default -> {
          return specNotImplemented("commit requirement " + type);
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
          Map<String, String> toSet = asStringMap(update.get("updates"));
          if (toSet.isEmpty()) {
            return validationError("set-properties requires updates");
          }
          properties.putAll(toSet);
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
    if (snapshotId == null
        || path == null
        || path.isBlank()
        || size == null
        || footerSize == null
        || blobMaps.isEmpty()) {
      throw new IllegalArgumentException(
          "set-statistics requires snapshot-id, statistics-path, file-size-in-bytes,"
              + " file-footer-size-in-bytes, and blob-metadata");
    }
    IcebergStatisticsFile.Builder builder =
        IcebergStatisticsFile.newBuilder()
            .setSnapshotId(snapshotId)
            .setStatisticsPath(path)
            .setFileSizeInBytes(size)
            .setFileFooterSizeInBytes(footerSize);
    for (Map<String, Object> blobMap : blobMaps) {
      builder.addBlobMetadata(buildBlobMetadata(blobMap));
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
    if (schemaId != null) {
      spec.setSchemaId(schemaId);
    }
    if (snapshot.containsKey("schema-json")) {
      String schemaJson = asString(snapshot.get("schema-json"));
      if (schemaJson != null && !schemaJson.isBlank()) {
        spec.setSchemaJson(schemaJson);
      }
    }
    CreateSnapshotRequest.Builder request =
        CreateSnapshotRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":snapshot:" + snapshotId).build());
    }
    stub.createSnapshot(request.build());
    triggerSnapshotReconcile(existing, namespacePath, tableName);
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

  private void triggerSnapshotReconcile(Table table, List<String> namespacePath, String tableName) {
    if (table == null || !table.hasUpstream() || !table.getUpstream().hasConnectorId()) {
      return;
    }
    triggerScopedReconcile(table.getUpstream().getConnectorId(), namespacePath, tableName);
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
    Long targetSnapshotId =
        preferredSnapshotId != null
            ? preferredSnapshotId
            : propertyLong(table.getPropertiesMap(), "current-snapshot-id");
    if (targetSnapshotId == null || targetSnapshotId <= 0) {
      return validationError("snapshot metadata updates require current snapshot id");
    }

    SnapshotServiceGrpc.SnapshotServiceBlockingStub stub = grpc.withHeaders(grpc.raw().snapshot());
    var snapshot =
        stub.getSnapshot(
                GetSnapshotRequest.newBuilder()
                    .setTableId(tableId)
                    .setSnapshot(SnapshotRef.newBuilder().setSnapshotId(targetSnapshotId))
                    .build())
            .getSnapshot();

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

  private IcebergGatewayConfig.RegisterConnectorTemplate connectorTemplateFor(String prefix) {
    Map<String, IcebergGatewayConfig.RegisterConnectorTemplate> templates =
        config.registerConnectors();
    if (templates == null || templates.isEmpty()) {
      return null;
    }
    IcebergGatewayConfig.RegisterConnectorTemplate direct = templates.get(prefix);
    if (direct != null) {
      return direct;
    }
    String resolved = resolveCatalog(prefix);
    return templates.get(resolved);
  }

  private ResourceId createTemplateConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      IcebergGatewayConfig.RegisterConnectorTemplate template,
      String idempotencyKey) {
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        ai.floedb.metacat.connector.rpc.DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();

    String displayName = template.displayName().orElse(null);
    if (displayName == null || displayName.isBlank()) {
      displayName = prefix + ":" + String.join(".", namespacePath) + "." + tableName;
    }

    ConnectorSpec.Builder spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri(template.uri())
            .setSource(source)
            .setDestination(dest);
    spec.setAuth(
        template
            .auth()
            .map(this::toAuthConfig)
            .orElse(AuthConfig.newBuilder().setScheme("none").build()));
    if (template.properties() != null && !template.properties().isEmpty()) {
      spec.putAllProperties(template.properties());
    }
    template.description().ifPresent(spec::setDescription);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    CreateConnectorResponse response = stub.createConnector(request.build());
    return response.getConnector().getResourceId();
  }

  private ResourceId createExternalConnector(
      String prefix,
      List<String> namespacePath,
      ResourceId namespaceId,
      ResourceId catalogId,
      String tableName,
      ResourceId tableId,
      String metadataLocation,
      String idempotencyKey) {
    ConnectorsGrpc.ConnectorsBlockingStub stub = grpc.withHeaders(grpc.raw().connectors());
    NamespacePath nsPath = NamespacePath.newBuilder().addAllSegments(namespacePath).build();
    SourceSelector source =
        SourceSelector.newBuilder().setNamespace(nsPath).setTable(tableName).build();
    var dest =
        ai.floedb.metacat.connector.rpc.DestinationTarget.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setTableId(tableId)
            .setTableDisplayName(tableName)
            .build();

    String namespaceFq = String.join(".", namespacePath);
    String displayName =
        "register:" + prefix + (namespaceFq.isBlank() ? "" : ":" + namespaceFq) + "." + tableName;

    ConnectorSpec.Builder spec =
        ConnectorSpec.newBuilder()
            .setDisplayName(displayName)
            .setKind(ConnectorKind.CK_ICEBERG)
            .setUri(metadataLocation)
            .setSource(source)
            .setDestination(dest)
            .setAuth(AuthConfig.newBuilder().setScheme("none").build())
            .putProperties("external.metadata-location", metadataLocation)
            .putProperties("external.table-name", tableName)
            .putProperties("external.namespace", namespaceFq);

    CreateConnectorRequest.Builder request =
        CreateConnectorRequest.newBuilder().setSpec(spec.build());
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          IdempotencyKey.newBuilder().setKey(idempotencyKey + ":connector").build());
    }
    return stub.createConnector(request.build()).getConnector().getResourceId();
  }

  private AuthConfig toAuthConfig(IcebergGatewayConfig.AuthTemplate template) {
    AuthConfig.Builder builder =
        AuthConfig.newBuilder()
            .setScheme(
                template.scheme() == null || template.scheme().isBlank()
                    ? "none"
                    : template.scheme())
            .putAllProperties(template.properties())
            .putAllHeaderHints(template.headerHints());
    template.secretRef().ifPresent(builder::setSecretRef);
    return builder.build();
  }

  private void updateTableUpstream(
      ResourceId tableId,
      List<String> namespacePath,
      String tableName,
      ResourceId connectorId,
      String connectorUri) {
    TableServiceGrpc.TableServiceBlockingStub tableStub = grpc.withHeaders(grpc.raw().table());
    UpstreamRef.Builder upstream =
        UpstreamRef.newBuilder()
            .setFormat(TableFormat.TF_ICEBERG)
            .setConnectorId(connectorId)
            .addAllNamespacePath(namespacePath)
            .setTableDisplayName(tableName);
    if (connectorUri != null && !connectorUri.isBlank()) {
      upstream.setUri(connectorUri);
    }
    UpdateTableRequest request =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setUpstream(upstream).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("upstream").build())
            .build();
    tableStub.updateTable(request);
  }
}
