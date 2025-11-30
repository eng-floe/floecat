package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.catalog.rpc.UpstreamRef;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.PageResponse;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
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
        req == null ? null : new TableRequests.Commit(
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
      @PathParam("table") String table) {
    return specNotImplemented("planTableScan");
  }

  @Path("/{table}/plan/{planId}")
  @GET
  public Response fetchPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    return specNotImplemented("fetchPlanningResult");
  }

  @Path("/{table}/plan/{planId}")
  @DELETE
  public Response cancelPlan(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @PathParam("planId") String planId) {
    return specNotImplemented("cancelPlanning");
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
      @PathParam("table") String table) {
    return specNotImplemented("publishMetrics");
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

  private Response specNotImplemented(String operation) {
    return Response.status(Response.Status.NOT_IMPLEMENTED)
        .entity(
            new IcebergErrorResponse(
                new IcebergError(operation + " not implemented", "UnsupportedOperationException", 501)))
        .build();
  }

}
