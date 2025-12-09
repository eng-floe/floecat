package ai.floedb.floecat.gateway.iceberg.rest.resources.table;

import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TableRequests;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.TransactionCommitRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.CatalogResolver;
import ai.floedb.floecat.gateway.iceberg.rest.resources.support.IcebergErrorResponses;
import ai.floedb.floecat.gateway.iceberg.rest.services.account.AccountContext;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableCommitService;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableLifecycleService;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.FieldMask;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.List;
import org.eclipse.microprofile.config.Config;

@Path("/v1/{prefix}")
@Produces(MediaType.APPLICATION_JSON)
public class TableAdminResource {
  @Inject GrpcWithHeaders grpc;
  @Inject IcebergGatewayConfig config;
  @Inject AccountContext accountContext;
  @Inject TableLifecycleService tableLifecycleService;
  @Inject TableCommitService tableCommitService;
  @Inject ObjectMapper mapper;
  @Inject Config mpConfig;

  private TableGatewaySupport tableSupport;

  @PostConstruct
  void initSupport() {
    this.tableSupport = new TableGatewaySupport(grpc, config, mapper, mpConfig);
  }

  @Path("/tables/rename")
  @POST
  public Response rename(
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      RenameRequest request) {
    if (request == null || request.source() == null || request.destination() == null) {
      return IcebergErrorResponses.validation("source and destination are required");
    }
    if (request.source().namespace() == null
        || request.source().name() == null
        || request.destination().namespace() == null
        || request.destination().name() == null) {
      return IcebergErrorResponses.validation("namespace and name must be provided");
    }
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
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
      @PathParam("prefix") String prefix,
      @HeaderParam("Idempotency-Key") String idempotencyKey,
      @HeaderParam("Iceberg-Transaction-Id") String transactionId,
      TransactionCommitRequest request) {
    String accountId = accountContext.getAccountId();
    if (accountId == null || accountId.isBlank()) {
      return IcebergErrorResponses.validation("account context is required");
    }
    List<TransactionCommitRequest.TableChange> changes =
        request == null ? List.of() : request.resolvedTableChanges();
    if (changes.isEmpty()) {
      return IcebergErrorResponses.validation("table-changes are required");
    }
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = CatalogResolver.resolveCatalogId(grpc, config, prefix);
    for (TransactionCommitRequest.TableChange change : changes) {
      var identifier = change.identifier();
      if (identifier == null || identifier.name() == null || identifier.name().isBlank()) {
        return IcebergErrorResponses.validation("table identifier is required");
      }
      List<String> namespacePath =
          identifier.namespace() == null ? List.of() : List.copyOf(identifier.namespace());
      String namespace = namespacePath.isEmpty() ? "" : String.join(".", namespacePath);
      ResourceId namespaceId = tableLifecycleService.resolveNamespaceId(catalogName, namespacePath);
      TableRequests.Commit commitReq =
          new TableRequests.Commit(
              null,
              namespacePath,
              null,
              null,
              change.stageId(),
              change.requirements(),
              change.updates());
      Response tableResponse =
          tableCommitService.commit(
              new TableCommitService.CommitCommand(
                  prefix,
                  namespace,
                  namespacePath,
                  identifier.name(),
                  catalogName,
                  catalogId,
                  namespaceId,
                  idempotencyKey,
                  transactionId,
                  commitReq,
                  tableSupport));
      if (tableResponse.getStatus() >= 400) {
        return tableResponse;
      }
    }
    return Response.noContent().build();
  }

  private static String nonBlank(String primary, String fallback) {
    return primary != null && !primary.isBlank() ? primary : fallback;
  }
}
