package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.CreateViewRequest;
import ai.floedb.metacat.catalog.rpc.DeleteViewRequest;
import ai.floedb.metacat.catalog.rpc.GetViewRequest;
import ai.floedb.metacat.catalog.rpc.ListViewsRequest;
import ai.floedb.metacat.catalog.rpc.UpdateViewRequest;
import ai.floedb.metacat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ViewSpec;
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

@Path("/v1/{prefix}/namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));

    ListViewsRequest.Builder req = ListViewsRequest.newBuilder().setNamespaceId(namespaceId);
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

    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    var resp = stub.listViews(req.build());
    List<TableIdentifierDto> identifiers =
        resp.getViewsList().stream()
            .map(v -> new TableIdentifierDto(NamespacePaths.split(namespace), v.getDisplayName()))
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
      ViewRequests.Create req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId catalogId = resolveCatalogId(prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));

    String viewName = req != null && req.name() != null ? req.name() : "view";

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(viewName);
    if (req != null) {
      if (req.sql() != null) {
        spec.setSql(req.sql());
      }
      if (req.properties() != null) {
        spec.putAllProperties(req.properties());
      }
    }

    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          ai.floedb.metacat.common.rpc.IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = stub.createView(request.build());
    return Response.ok(ViewResponseMapper.toLoadResult(namespace, viewName, created.getView()))
        .build();
  }

  @Path("/{view}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = resolveCatalog(prefix);
    ResourceId viewId =
        NameResolution.resolveView(grpc, catalogName, NamespacePaths.split(namespace), view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    var resp = stub.getView(GetViewRequest.newBuilder().setViewId(viewId).build());
    return Response.ok(ViewResponseMapper.toLoadResult(namespace, view, resp.getView())).build();
  }

  @Path("/{view}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = resolveCatalog(prefix);
    NameResolution.resolveView(grpc, catalogName, NamespacePaths.split(namespace), view);
    return Response.noContent().build();
  }

  @Path("/{view}")
  @PUT
  public Response update(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      ViewRequests.Update req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId viewId =
        NameResolution.resolveView(grpc, catalogName, NamespacePaths.split(namespace), view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());

    ViewSpec.Builder spec = ViewSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
      if (req.name() != null) {
        spec.setDisplayName(req.name());
        mask.addPaths("display_name");
      }
      if (req.namespace() != null && !req.namespace().isEmpty()) {
        var targetNs =
            NameResolution.resolveNamespace(grpc, catalogName, new java.util.ArrayList<>(req.namespace()));
        spec.setNamespaceId(targetNs);
        mask.addPaths("namespace_id");
      }
      if (req.sql() != null) {
        spec.setSql(req.sql());
        mask.addPaths("sql");
      }
      if (req.properties() != null && !req.properties().isEmpty()) {
        spec.putAllProperties(req.properties());
        mask.addPaths("properties");
      }
    }
    var resp =
        stub.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    return Response.ok(ViewResponseMapper.toLoadResult(namespace, view, resp.getView())).build();
  }

  @Path("/{view}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = resolveCatalog(prefix);
    ResourceId viewId =
        NameResolution.resolveView(grpc, catalogName, NamespacePaths.split(namespace), view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    stub.deleteView(DeleteViewRequest.newBuilder().setViewId(viewId).build());
    return Response.noContent().build();
  }

  @Path("/{view}")
  @POST
  public Response commit(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      ViewRequests.Commit req) {
    String catalogName = resolveCatalog(prefix);
    ResourceId viewId =
        NameResolution.resolveView(grpc, catalogName, NamespacePaths.split(namespace), view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());

    ViewSpec.Builder spec = ViewSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
      if (req.namespace() != null && !req.namespace().isEmpty()) {
        var targetNs =
            NameResolution.resolveNamespace(grpc, catalogName, new java.util.ArrayList<>(req.namespace()));
        spec.setNamespaceId(targetNs);
        mask.addPaths("namespace_id");
      }
      if (req.sql() != null) {
        spec.setSql(req.sql());
        mask.addPaths("sql");
      }
      if (req.summary() != null && !req.summary().isEmpty()) {
        spec.putAllProperties(req.summary());
        mask.addPaths("properties");
      }
    }
    var resp =
        stub.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    return Response.ok(ViewResponseMapper.toLoadResult(namespace, view, resp.getView())).build();
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
}
