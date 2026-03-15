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

package ai.floedb.floecat.gateway.iceberg.minimal.services.table;

import ai.floedb.floecat.catalog.rpc.CreateTableRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.ListTablesRequest;
import ai.floedb.floecat.catalog.rpc.ListTablesResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.CatalogResolver;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GrpcTableBackend implements TableBackend {
  private final GrpcWithHeaders grpc;
  private final MinimalGatewayConfig config;

  @Inject
  public GrpcTableBackend(GrpcWithHeaders grpc, MinimalGatewayConfig config) {
    this.grpc = grpc;
    this.config = config;
  }

  @Override
  public ListTablesResponse list(
      String prefix, List<String> namespacePath, Integer pageSize, String pageToken) {
    ListTablesRequest.Builder request =
        ListTablesRequest.newBuilder().setNamespaceId(resolveNamespaceId(prefix, namespacePath));
    if (pageSize != null || (pageToken != null && !pageToken.isBlank())) {
      PageRequest.Builder page = PageRequest.newBuilder();
      if (pageSize != null) {
        page.setPageSize(pageSize);
      }
      if (pageToken != null && !pageToken.isBlank()) {
        page.setPageToken(pageToken);
      }
      request.setPage(page);
    }
    return tableStub().listTables(request.build());
  }

  @Override
  public Table get(String prefix, List<String> namespacePath, String tableName) {
    return tableStub()
        .getTable(
            GetTableRequest.newBuilder()
                .setTableId(resolveTableId(prefix, namespacePath, tableName))
                .build())
        .getTable();
  }

  @Override
  public void exists(String prefix, List<String> namespacePath, String tableName) {
    resolveTableId(prefix, namespacePath, tableName);
  }

  @Override
  public void delete(String prefix, List<String> namespacePath, String tableName) {
    tableStub()
        .deleteTable(
            DeleteTableRequest.newBuilder()
                .setTableId(resolveTableId(prefix, namespacePath, tableName))
                .build());
  }

  @Override
  public Snapshot currentSnapshot(String prefix, List<String> namespacePath, String tableName) {
    ResourceId tableId = resolveTableId(prefix, namespacePath, tableName);
    return snapshotStub()
        .getSnapshot(
            GetSnapshotRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshot(
                    SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT).build())
                .build())
        .getSnapshot();
  }

  @Override
  public ListSnapshotsResponse listSnapshots(
      String prefix, List<String> namespacePath, String tableName) {
    ResourceId tableId = resolveTableId(prefix, namespacePath, tableName);
    ListSnapshotsResponse.Builder merged = ListSnapshotsResponse.newBuilder();
    String token = "";
    while (true) {
      ListSnapshotsResponse response =
          snapshotStub()
              .listSnapshots(
                  ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest.newBuilder()
                      .setTableId(tableId)
                      .setPage(
                          PageRequest.newBuilder().setPageSize(1000).setPageToken(token).build())
                      .build());
      if (response == null) {
        break;
      }
      merged.addAllSnapshots(response.getSnapshotsList());
      if (!response.hasPage()) {
        break;
      }
      String nextToken = response.getPage().getNextPageToken();
      if (nextToken == null || nextToken.isBlank() || nextToken.equals(token)) {
        break;
      }
      token = nextToken;
    }
    return merged.build();
  }

  @Override
  public Table create(
      String prefix,
      List<String> namespacePath,
      String tableName,
      JsonNode schema,
      String location,
      Map<String, String> properties,
      String idempotencyKey) {
    ResourceId namespaceId = resolveNamespaceId(prefix, namespacePath);
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId catalogId = resolveCatalogId(catalogName);
    Map<String, String> mergedProperties = new LinkedHashMap<>();
    if (properties != null) {
      mergedProperties.putAll(properties);
    }
    if (location != null && !location.isBlank()) {
      mergedProperties.putIfAbsent("storage_location", location);
    }
    TableSpec.Builder spec =
        TableSpec.newBuilder()
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName(tableName)
            .setSchemaJson(schema == null ? "" : schema.toString())
            .putAllProperties(mergedProperties);
    CreateTableRequest.Builder request = CreateTableRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return tableStub().createTable(request.build()).getTable();
  }

  @Override
  public void rename(
      String prefix,
      List<String> sourceNamespacePath,
      String sourceTableName,
      List<String> destinationNamespacePath,
      String destinationTableName) {
    ResourceId tableId = resolveTableId(prefix, sourceNamespacePath, sourceTableName);
    ResourceId namespaceId = resolveNamespaceId(prefix, destinationNamespacePath);
    tableStub()
        .updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(tableId)
                .setSpec(
                    TableSpec.newBuilder()
                        .setNamespaceId(namespaceId)
                        .setDisplayName(destinationTableName))
                .setUpdateMask(
                    FieldMask.newBuilder()
                        .addPaths("namespace_id")
                        .addPaths("display_name")
                        .build())
                .build());
  }

  @Override
  public Table commitProperties(
      String prefix,
      List<String> namespacePath,
      String tableName,
      List<Map<String, Object>> updates) {
    Table current = get(prefix, namespacePath, tableName);
    Map<String, String> properties = new LinkedHashMap<>(current.getPropertiesMap());
    if (updates != null) {
      for (Map<String, Object> update : updates) {
        if (update == null) {
          continue;
        }
        Object action = update.get("action");
        if (!"set-properties".equals(action)
            && !"remove-properties".equals(action)
            && !"set-location".equals(action)) {
          continue;
        }
        if ("set-properties".equals(action)) {
          Object raw = update.get("updates");
          if (raw instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
              if (entry.getKey() != null && entry.getValue() != null) {
                properties.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
              }
            }
          }
        } else if ("remove-properties".equals(action)) {
          Object raw = update.get("removals");
          if (raw instanceof List<?> removals) {
            for (Object removal : removals) {
              if (removal != null) {
                properties.remove(String.valueOf(removal));
              }
            }
          }
        } else if ("set-location".equals(action)) {
          Object value = update.get("location");
          if (value != null) {
            properties.put("location", String.valueOf(value));
          }
        }
      }
    }
    return tableStub()
        .updateTable(
            UpdateTableRequest.newBuilder()
                .setTableId(current.getResourceId())
                .setSpec(
                    TableSpec.newBuilder()
                        .setCatalogId(current.getCatalogId())
                        .setNamespaceId(current.getNamespaceId())
                        .setDisplayName(current.getDisplayName())
                        .setSchemaJson(current.getSchemaJson())
                        .putAllProperties(properties))
                .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
                .build())
        .getTable();
  }

  private ResourceId resolveCatalogId(String catalogName) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    ResourceId id =
        directoryStub()
            .resolveCatalog(
                ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest.newBuilder()
                    .setRef(ref)
                    .build())
            .getResourceId();
    if (id == null || id.getId().isBlank()) {
      throw Status.NOT_FOUND
          .withDescription("No such catalog: " + catalogName)
          .asRuntimeException();
    }
    return id;
  }

  private ResourceId resolveNamespaceId(String prefix, List<String> namespacePath) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).addAllPath(namespacePath).build();
    ResourceId id =
        directoryStub()
            .resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build())
            .getResourceId();
    if (id == null || id.getId().isBlank()) {
      throw Status.NOT_FOUND
          .withDescription("Namespace " + String.join(".", namespacePath) + " not found")
          .asRuntimeException();
    }
    return id;
  }

  private ResourceId resolveTableId(String prefix, List<String> namespacePath, String tableName) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(namespacePath)
            .setName(tableName)
            .build();
    ResourceId id =
        directoryStub()
            .resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build())
            .getResourceId();
    if (id == null || id.getId().isBlank()) {
      throw Status.NOT_FOUND
          .withDescription(
              "Table " + String.join(".", namespacePath) + "." + tableName + " not found")
          .asRuntimeException();
    }
    return id;
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub() {
    return grpc.withHeaders(grpc.raw().directory());
  }

  private TableServiceGrpc.TableServiceBlockingStub tableStub() {
    return grpc.withHeaders(grpc.raw().table());
  }

  private SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshotStub() {
    return grpc.withHeaders(grpc.raw().snapshot());
  }
}
