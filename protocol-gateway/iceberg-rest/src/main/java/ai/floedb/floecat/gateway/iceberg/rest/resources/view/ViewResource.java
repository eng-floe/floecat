package ai.floedb.floecat.gateway.iceberg.rest.resources.view;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.PageResponse;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.ViewRequests;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.PageRequestHelper;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService;
import ai.floedb.floecat.gateway.iceberg.rest.services.view.ViewMetadataService.MetadataContext;
import ai.floedb.floecat.gateway.iceberg.rest.support.mapper.ViewResponseMapper;
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
import java.util.stream.Collectors;

@Path("/v1/{prefix}/namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class ViewResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ViewMetadataService viewMetadataService;

  @GET
  public Response list(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @QueryParam("pageToken") String pageToken,
      @QueryParam("pageSize") Integer pageSize) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, NamespacePaths.split(namespace));

    ListViewsRequest.Builder req = ListViewsRequest.newBuilder().setNamespaceId(namespaceId);
    PageRequest.Builder page = PageRequestHelper.builder(pageToken, pageSize);
    if (page != null) {
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
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, namespacePath);

    String viewName = req != null && req.name() != null ? req.name() : "view";

    MetadataContext metadataContext;
    try {
      metadataContext = viewMetadataService.fromCreate(namespacePath, viewName, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(viewName);
    spec.setSql(metadataContext.sql());
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(metadataContext));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(
          ai.floedb.floecat.common.rpc.IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    var created = stub.createView(request.build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, viewName, created.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespace, viewName, created.getView(), responseContext.metadata()))
        .build();
  }

  @Path("/{view}")
  @GET
  public Response get(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId viewId = NameResolution.resolveView(grpc, catalogName, namespacePath, view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    var resp = stub.getView(GetViewRequest.newBuilder().setViewId(viewId).build());
    MetadataContext context = viewMetadataService.fromView(namespacePath, view, resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(namespace, view, resp.getView(), context.metadata()))
        .build();
  }

  @Path("/{view}")
  @HEAD
  public Response exists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
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
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId viewId = NameResolution.resolveView(grpc, catalogName, namespacePath, view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    View current = stub.getView(GetViewRequest.newBuilder().setViewId(viewId).build()).getView();
    MetadataContext metadataContext = viewMetadataService.fromView(namespacePath, view, current);
    MetadataContext workingContext = metadataContext;
    boolean propertiesDirty = false;

    ViewSpec.Builder spec = ViewSpec.newBuilder();
    FieldMask.Builder mask = FieldMask.newBuilder();
    if (req != null) {
      if (req.name() != null) {
        spec.setDisplayName(req.name());
        mask.addPaths("display_name");
      }
      if (req.namespace() != null && !req.namespace().isEmpty()) {
        var targetNs =
            NameResolution.resolveNamespace(
                grpc, catalogName, new java.util.ArrayList<>(req.namespace()));
        spec.setNamespaceId(targetNs);
        mask.addPaths("namespace_id");
      }
      if (req.sql() != null) {
        spec.setSql(req.sql());
        mask.addPaths("sql");
        try {
          workingContext = viewMetadataService.withSql(workingContext, req.sql());
          propertiesDirty = true;
        } catch (IllegalArgumentException e) {
          return IcebergErrorResponses.validation(e.getMessage());
        }
      }
      if (req.properties() != null && !req.properties().isEmpty()) {
        try {
          workingContext = viewMetadataService.withUserProperties(workingContext, req.properties());
          propertiesDirty = true;
        } catch (IllegalArgumentException e) {
          return IcebergErrorResponses.validation(e.getMessage());
        }
      }
    }
    if (propertiesDirty) {
      try {
        spec.putAllProperties(viewMetadataService.buildPropertyMap(workingContext));
      } catch (IllegalArgumentException e) {
        return IcebergErrorResponses.validation(e.getMessage());
      }
      mask.addPaths("properties");
    }
    var resp =
        stub.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, view, resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespace, view, resp.getView(), responseContext.metadata()))
        .build();
  }

  @Path("/{view}")
  @DELETE
  public Response delete(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
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
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId viewId = NameResolution.resolveView(grpc, catalogName, namespacePath, view);
    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    View current = stub.getView(GetViewRequest.newBuilder().setViewId(viewId).build()).getView();
    MetadataContext baseContext = viewMetadataService.fromView(namespacePath, view, current);
    MetadataContext updated;
    try {
      updated = viewMetadataService.applyCommit(namespacePath, baseContext, req);
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    ViewSpec.Builder spec = ViewSpec.newBuilder().setSql(updated.sql());
    FieldMask.Builder mask = FieldMask.newBuilder().addPaths("sql").addPaths("properties");
    try {
      spec.putAllProperties(viewMetadataService.buildPropertyMap(updated));
    } catch (IllegalArgumentException e) {
      return IcebergErrorResponses.validation(e.getMessage());
    }

    var resp =
        stub.updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(viewId)
                .setSpec(spec)
                .setUpdateMask(mask)
                .build());
    MetadataContext responseContext =
        viewMetadataService.fromView(namespacePath, view, resp.getView());
    return Response.ok(
            ViewResponseMapper.toLoadResult(
                namespace, view, resp.getView(), responseContext.metadata()))
        .build();
  }

  private String flattenPageToken(PageResponse page) {
    String token = page.getNextPageToken();
    return token == null || token.isBlank() ? null : token;
  }
}
