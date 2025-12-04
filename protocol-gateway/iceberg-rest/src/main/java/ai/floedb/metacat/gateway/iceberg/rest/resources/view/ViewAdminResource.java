package ai.floedb.metacat.gateway.iceberg.rest.resources.view;

import ai.floedb.metacat.catalog.rpc.UpdateViewRequest;
import ai.floedb.metacat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ViewSpec;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergError;
import ai.floedb.metacat.gateway.iceberg.rest.api.error.IcebergErrorResponse;
import ai.floedb.metacat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NameResolution;
import com.google.protobuf.FieldMask;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Optional;

@Path("/v1/{prefix}/views")
@Produces(MediaType.APPLICATION_JSON)
public class ViewAdminResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;

  @Path("/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      RenameRequest request) {
    if (request == null || request.source() == null || request.destination() == null) {
      return validationError("source and destination are required");
    }
    if (request.source().namespace() == null
        || request.source().name() == null
        || request.destination().namespace() == null
        || request.destination().name() == null) {
      return validationError("namespace and name must be provided");
    }
    String catalogName = resolveCatalog(prefix);
    ResourceId viewId =
        NameResolution.resolveView(
            grpc, catalogName, request.source().namespace(), request.source().name());
    ResourceId namespaceId =
        NameResolution.resolveNamespace(grpc, catalogName, request.destination().namespace());

    ViewServiceGrpc.ViewServiceBlockingStub stub = grpc.withHeaders(grpc.raw().view());
    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    stub.updateView(
        UpdateViewRequest.newBuilder().setViewId(viewId).setSpec(spec).setUpdateMask(mask).build());
    return Response.noContent().build();
  }

  private String resolveCatalog(String prefix) {
    var mapping = config.catalogMapping();
    return Optional.ofNullable(mapping == null ? null : mapping.get(prefix)).orElse(prefix);
  }

  private Response validationError(String message) {
    return Response.status(Response.Status.BAD_REQUEST)
        .entity(new IcebergErrorResponse(new IcebergError(message, "ValidationException", 400)))
        .build();
  }
}
