package ai.floedb.floecat.gateway.iceberg.rest.resources.view;

import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.ViewClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import com.google.protobuf.FieldMask;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/v1/{prefix}/views")
@Produces(MediaType.APPLICATION_JSON)
public class ViewAdminResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject ViewClient viewClient;

  @Path("/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @NotNull @Valid RenameRequest request) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId viewId =
        NameResolution.resolveView(
            grpc, catalogName, request.source().namespace(), request.source().name());
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, request.destination().namespace());

    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    viewClient.updateView(
        UpdateViewRequest.newBuilder().setViewId(viewId).setSpec(spec).setUpdateMask(mask).build());
    return Response.noContent().build();
  }
}
