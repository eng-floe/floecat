package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.SnapshotRef;
import ai.floedb.metacat.common.rpc.SpecialSnapshot;
import ai.floedb.metacat.execution.rpc.ScanFile;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.query.rpc.BeginQueryRequest;
import ai.floedb.metacat.query.rpc.EndQueryRequest;
import ai.floedb.metacat.query.rpc.GetQueryRequest;
import ai.floedb.metacat.query.rpc.QueryDescriptor;
import ai.floedb.metacat.query.rpc.QueryInput;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(tableName)
            .setUpstream(UpstreamRef.newBuilder().setFormat(TableFormat.TF_ICEBERG).build());
    if (req != null) {
      if (req.schemaJson() != null) {
        spec.setSchemaJson(req.schemaJson());
      }
      if (req.properties() != null) {
        spec.putAllProperties(req.properties());
      }
    }

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
                req.name(), req.namespace(), req.schemaJson(), req.properties()));
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

    TableSpec.Builder spec = TableSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
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
      if (req.properties() != null && !req.properties().isEmpty()) {
        spec.putAllProperties(req.properties());
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
    QueryServiceGrpc.QueryServiceBlockingStub stub = grpc.withHeaders(grpc.raw().query());
    BeginQueryRequest planReq = buildPlanRequest(tableId, request);
    var resp = stub.beginQuery(planReq);
    return Response.ok(toPlanResult(resp.getQuery())).build();
  }

  @Path("/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveTable(grpc, catalogName, NamespacePaths.split(namespace), table);
    QueryServiceGrpc.QueryServiceBlockingStub stub = grpc.withHeaders(grpc.raw().query());
    var resp = stub.getQuery(GetQueryRequest.newBuilder().setQueryId(planId).build());
    return Response.ok(toPlanResult(resp.getQuery(), planId)).build();
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
    QueryServiceGrpc.QueryServiceBlockingStub stub = grpc.withHeaders(grpc.raw().query());
    stub.endQuery(EndQueryRequest.newBuilder().setQueryId(planId).setCommit(false).build());
    return Response.noContent().build();
  }

  @Path("/{table}/tasks")
  @POST
  public Response scanTasks(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    return specNotImplemented("fetchScanTasks");
  }

  @Path("/{table}/credentials")
  @GET
  public Response loadCredentials(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table) {
    return specNotImplemented("loadCredentials");
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
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    return specNotImplemented("registerTable");
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

  private BeginQueryRequest buildPlanRequest(ResourceId tableId, PlanRequests.Plan request) {
    QueryInput.Builder input = QueryInput.newBuilder().setTableId(tableId);
    if (request.snapshotId() != null) {
      input.setSnapshot(SnapshotRef.newBuilder().setSnapshotId(request.snapshotId()));
    } else {
      input.setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT));
    }
    BeginQueryRequest.Builder builder = BeginQueryRequest.newBuilder().addInputs(input);
    if (request.select() != null && !request.select().isEmpty()) {
      builder.addAllRequiredColumns(request.select());
    }
    builder.setIncludeSchema(false);
    return builder.build();
  }

  private PlanResponseDto toPlanResult(QueryDescriptor descriptor) {
    return toPlanResult(descriptor, descriptor.getQueryId());
  }

  private PlanResponseDto toPlanResult(QueryDescriptor descriptor, String planIdOverride) {
    List<ContentFileDto> deleteFiles = new ArrayList<>();
    for (ScanFile delete : descriptor.getDeleteFilesList()) {
      deleteFiles.add(toContentFile(delete));
    }
    List<FileScanTaskDto> tasks = new ArrayList<>();
    for (ScanFile file : descriptor.getDataFilesList()) {
      tasks.add(new FileScanTaskDto(toContentFile(file), List.of(), null));
    }
    String planId = descriptor.getQueryId().isBlank() ? planIdOverride : descriptor.getQueryId();
    return new PlanResponseDto("completed", planId, List.of(), tasks, deleteFiles, null);
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
}
