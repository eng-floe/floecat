/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.gateway.iceberg.rest.services.catalog;

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.api.dto.TableIdentifierDto;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.TableClient;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NameResolution;
import ai.floedb.floecat.gateway.iceberg.rest.services.resolution.NamespacePaths;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class TableLifecycleService {
  @Inject GrpcWithHeaders grpc;
  @Inject TableClient tableClient;

  public record ListTablesResult(List<TableIdentifierDto> identifiers, String nextPageToken) {}

  public ListTablesResult listTables(
      String catalogName, String namespace, Integer pageSize, String pageToken) {
    List<String> namespacePath = NamespacePaths.split(namespace);
    ResourceId namespaceId = resolveNamespaceId(catalogName, namespacePath);

    ListTablesRequest.Builder request = ListTablesRequest.newBuilder().setNamespaceId(namespaceId);
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

    var resp = tableClient.listTables(request.build());
    if (resp == null) {
      resp = ListTablesResponse.getDefaultInstance();
    }
    List<TableIdentifierDto> identifiers =
        resp.getTablesList().stream()
            .map(table -> new TableIdentifierDto(namespacePath, table.getDisplayName()))
            .collect(Collectors.toList());
    String nextToken = null;
    if (resp.hasPage()) {
      String token = resp.getPage().getNextPageToken();
      if (token != null && !token.isBlank()) {
        nextToken = token;
      }
    }
    return new ListTablesResult(identifiers, nextToken);
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
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return tableClient.createTable(request.build()).getTable();
  }

  public Table getTable(ResourceId tableId) {
    return tableClient
        .getTable(GetTableRequest.newBuilder().setTableId(tableId).build())
        .getTable();
  }

  public Table updateTable(UpdateTableRequest request) {
    return tableClient.updateTable(request).getTable();
  }

  public void deleteTable(String catalogName, String namespace, String tableName) {
    deleteTable(catalogName, namespace, tableName, false);
  }

  public void deleteTable(
      String catalogName, String namespace, String tableName, boolean purgeRequested) {
    ResourceId tableId = resolveTableId(catalogName, namespace, tableName);
    deleteTable(tableId, purgeRequested);
  }

  public void deleteTable(ResourceId tableId) {
    deleteTable(tableId, false);
  }

  public void deleteTable(ResourceId tableId, boolean purgeRequested) {
    if (tableId == null) {
      return;
    }
    DeleteTableRequest.Builder request = DeleteTableRequest.newBuilder().setTableId(tableId);
    tableClient.deleteTable(request.build());
  }

  public ResourceId resolveCatalogId(String catalogName) {
    return NameResolution.resolveCatalog(grpc, catalogName);
  }
}
