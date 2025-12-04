package ai.floedb.metacat.gateway.iceberg.rest.services.catalog;

import ai.floedb.metacat.catalog.rpc.CreateTableRequest;
import ai.floedb.metacat.catalog.rpc.DeleteTableRequest;
import ai.floedb.metacat.catalog.rpc.GetTableRequest;
import ai.floedb.metacat.catalog.rpc.ListTablesRequest;
import ai.floedb.metacat.catalog.rpc.Table;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableSpec;
import ai.floedb.metacat.catalog.rpc.UpdateTableRequest;
import ai.floedb.metacat.common.rpc.IdempotencyKey;
import ai.floedb.metacat.common.rpc.PageRequest;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.metacat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.metacat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles catalog lifecycle operations (list/create/delete) so {@code TableResource} can delegate
 * to a focused service instead of issuing gRPC calls directly.
 */
@ApplicationScoped
public class TableLifecycleService {
  @Inject GrpcWithHeaders grpc;

  public record ListTablesResult(List<TableIdentifierDto> identifiers, String nextPageToken) {}

  public ListTablesResult listTables(
      String catalogName, String namespace, Integer pageSize, String pageToken) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = resolveNamespaceId(catalogName, namespacePath);

    ListTablesRequest.Builder request =
        ListTablesRequest.newBuilder().setNamespaceId(namespaceId);
    if (pageToken != null || pageSize != null) {
      PageRequest.Builder page = PageRequest.newBuilder();
      if (pageToken != null) {
        page.setPageToken(pageToken);
      }
      if (pageSize != null) {
        page.setPageSize(pageSize);
      }
      request.setPage(page);
    }

    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    var resp = stub.listTables(request.build());
    if (resp == null) {
      resp = ai.floedb.metacat.catalog.rpc.ListTablesResponse.getDefaultInstance();
    }
    List<TableIdentifierDto> identifiers =
        resp.getTablesList().stream()
            .map(table -> new TableIdentifierDto(namespacePath, table.getDisplayName()))
            .collect(Collectors.toList());
    String nextToken = flattenPageToken(resp.getPage());
    return new ListTablesResult(identifiers, nextToken);
  }

  private String flattenPageToken(ai.floedb.metacat.common.rpc.PageResponse page) {
    if (page == null) {
      return null;
    }
    String token = page.getNextPageToken();
    return (token == null || token.isBlank()) ? null : token;
  }

  public ResourceId resolveNamespaceId(String catalogName, String namespace) {
    return resolveNamespaceId(catalogName, NamespacePaths.split(namespace));
  }

  public ResourceId resolveNamespaceId(String catalogName, List<String> namespacePath) {
    return NameResolution.resolveNamespace(grpc, catalogName, namespacePath);
  }

  public ResourceId resolveTableId(String catalogName, String namespace, String tableName) {
    return resolveTableId(catalogName, NamespacePaths.split(namespace), tableName);
  }

  public ResourceId resolveTableId(
      String catalogName, List<String> namespacePath, String tableName) {
    return NameResolution.resolveTable(grpc, catalogName, namespacePath, tableName);
  }

  public Table createTable(TableSpec.Builder spec, String idempotencyKey) {
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return stub.createTable(request.build()).getTable();
  }

  public Table getTable(ResourceId tableId) {
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    return stub.getTable(GetTableRequest.newBuilder().setTableId(tableId).build()).getTable();
  }

  public Table updateTable(UpdateTableRequest request) {
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    return stub.updateTable(request).getTable();
  }

  public void deleteTable(String catalogName, String namespace, String tableName) {
    ResourceId tableId = resolveTableId(catalogName, namespace, tableName);
    deleteTable(tableId);
  }

  public void deleteTable(ResourceId tableId) {
    if (tableId == null) {
      return;
    }
    TableServiceGrpc.TableServiceBlockingStub stub = grpc.withHeaders(grpc.raw().table());
    stub.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build());
  }

  public ResourceId resolveCatalogId(String catalogName) {
    return NameResolution.resolveCatalog(grpc, catalogName);
  }
}
