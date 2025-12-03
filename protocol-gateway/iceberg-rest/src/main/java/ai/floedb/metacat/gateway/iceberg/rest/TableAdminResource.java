package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.StageCommitException;
import ai.floedb.metacat.gateway.iceberg.rest.StageCommitProcessor;
import ai.floedb.metacat.gateway.iceberg.rest.StageCommitProcessor.StageCommitResult;
import com.google.protobuf.FieldMask;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Path("/v1/{prefix}/tables")
@Produces(MediaType.APPLICATION_JSON)
public class TableAdminResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject StagedTableService stagedTableService;
  @Inject TenantContext tenantContext;
  @Inject StageCommitProcessor stageCommitProcessor;

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
    var sourcePath = request.source().namespace();
    var destinationPath = request.destination().namespace();

    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, sourcePath, request.source().name());
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, destinationPath);

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    stub.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.noContent().build();
  }

  @Path("/transactions/commit")
  @POST
  public Response commitTransaction(
      @PathParam("prefix") String prefix, TransactionCommitRequest request) {
    if (request == null
        || request.stagedRefUpdates() == null
        || request.stagedRefUpdates().isEmpty()) {
      return validationError("staged-ref-updates are required");
    }
    String tenantId = tenantContext.getTenantId();
    if (tenantId == null || tenantId.isBlank()) {
      return validationError("tenant context is required");
    }
    String catalogName = resolveCatalog(prefix);
    List<TransactionCommitResponse.TransactionCommitResult> results = new java.util.ArrayList<>();
    for (TransactionCommitRequest.StagedRefUpdate update : request.stagedRefUpdates()) {
      try {
        StageCommitProcessor.StageCommitResult stageResult =
            stageCommitProcessor.commitStage(
                prefix,
                catalogName,
                tenantId,
                update.table().namespace(),
                update.table().name(),
                update.stageId());
        results.add(
            new TransactionCommitResponse.TransactionCommitResult(
                update.table(),
                update.stageId(),
                stageResult.loadResult().metadataLocation(),
                stageResult.loadResult().metadata(),
                stageResult.loadResult().config(),
                stageResult.loadResult().storageCredentials()));
      } catch (StageCommitException e) {
        return e.toResponse();
      }
    }
    stagedTableService.expireStages();
    return Response.ok(new TransactionCommitResponse(results)).build();
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

  private Response conflictError(String message) {
    return Response.status(Response.Status.CONFLICT)
        .entity(new IcebergErrorResponse(new IcebergError(message, "CommitFailedException", 409)))
        .build();
  }

  private Response notFound(String message) {
    return Response.status(Response.Status.NOT_FOUND)
        .entity(new IcebergErrorResponse(new IcebergError(message, "NotFoundException", 404)))
        .build();
  }
}
