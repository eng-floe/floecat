package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.request.RenameRequest;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.CatalogRequestContext;
import ai.floedb.floecat.gateway.iceberg.rest.resources.common.RequestContextFactory;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import com.google.protobuf.FieldMask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;

@ApplicationScoped
public class TableRenameService {
  @Inject GrpcWithHeaders grpc;
  @Inject RequestContextFactory requestContextFactory;
  @Inject TableClient tableClient;

  public Response rename(String prefix, RenameRequest request) {
    CatalogRequestContext catalogContext = requestContextFactory.catalog(prefix);
    String catalogName = catalogContext.catalogName();
    var sourcePath = request.source().namespace();
    var destinationPath = request.destination().namespace();

    ResourceId tableId =
        NameResolution.resolveTable(grpc, catalogName, sourcePath, request.source().name());
    ResourceId namespaceId = NameResolution.resolveNamespace(grpc, catalogName, destinationPath);

    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setNamespaceId(namespaceId)
            .setDisplayName(request.destination().name());
    FieldMask mask =
        FieldMask.newBuilder().addPaths("namespace_id").addPaths("display_name").build();
    tableClient.updateTable(
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(spec)
            .setUpdateMask(mask)
            .build());
    return Response.noContent().build();
  }
}
