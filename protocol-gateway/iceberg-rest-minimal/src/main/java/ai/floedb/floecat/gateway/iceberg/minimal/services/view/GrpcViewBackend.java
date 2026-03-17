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

package ai.floedb.floecat.gateway.iceberg.minimal.services.view;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.CatalogResolver;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GrpcViewBackend implements ViewBackend {
  private final GrpcWithHeaders grpc;
  private final MinimalGatewayConfig config;

  @Inject
  public GrpcViewBackend(GrpcWithHeaders grpc, MinimalGatewayConfig config) {
    this.grpc = grpc;
    this.config = config;
  }

  @Override
  public ListViewsResponse list(
      String prefix, List<String> namespacePath, Integer pageSize, String pageToken) {
    ListViewsRequest.Builder request =
        ListViewsRequest.newBuilder().setNamespaceId(resolveNamespaceId(prefix, namespacePath));
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
    return viewStub().listViews(request.build());
  }

  @Override
  public View get(String prefix, List<String> namespacePath, String viewName) {
    return viewStub()
        .getView(
            GetViewRequest.newBuilder()
                .setViewId(resolveViewId(prefix, namespacePath, viewName))
                .build())
        .getView();
  }

  @Override
  public void exists(String prefix, List<String> namespacePath, String viewName) {
    resolveViewId(prefix, namespacePath, viewName);
  }

  @Override
  public void delete(String prefix, List<String> namespacePath, String viewName) {
    viewStub()
        .deleteView(
            DeleteViewRequest.newBuilder()
                .setViewId(resolveViewId(prefix, namespacePath, viewName))
                .build());
  }

  @Override
  public View create(
      String prefix,
      List<String> namespacePath,
      String viewName,
      String sql,
      String dialect,
      List<String> creationSearchPath,
      List<SchemaColumn> outputColumns,
      Map<String, String> properties,
      String idempotencyKey) {
    ViewSpec.Builder spec =
        ViewSpec.newBuilder()
            .setCatalogId(resolveCatalogId(prefix))
            .setNamespaceId(resolveNamespaceId(prefix, namespacePath))
            .setDisplayName(viewName)
            .setSql(sql == null ? "" : sql)
            .setDialect(dialect == null ? "" : dialect)
            .addAllCreationSearchPath(creationSearchPath == null ? List.of() : creationSearchPath)
            .addAllOutputColumns(outputColumns == null ? List.of() : outputColumns)
            .putAllProperties(properties == null ? Map.of() : properties);
    CreateViewRequest.Builder request = CreateViewRequest.newBuilder().setSpec(spec);
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return viewStub().createView(request.build()).getView();
  }

  @Override
  public View update(
      String prefix,
      List<String> namespacePath,
      String viewName,
      String sql,
      Map<String, String> properties) {
    return viewStub()
        .updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(resolveViewId(prefix, namespacePath, viewName))
                .setSpec(
                    ViewSpec.newBuilder()
                        .setSql(sql == null ? "" : sql)
                        .putAllProperties(properties == null ? Map.of() : properties))
                .setUpdateMask(
                    FieldMask.newBuilder().addPaths("sql").addPaths("properties").build())
                .build())
        .getView();
  }

  @Override
  public void rename(
      String prefix,
      List<String> sourceNamespacePath,
      String sourceViewName,
      List<String> destinationNamespacePath,
      String destinationViewName) {
    viewStub()
        .updateView(
            UpdateViewRequest.newBuilder()
                .setViewId(resolveViewId(prefix, sourceNamespacePath, sourceViewName))
                .setSpec(
                    ViewSpec.newBuilder()
                        .setNamespaceId(resolveNamespaceId(prefix, destinationNamespacePath))
                        .setDisplayName(destinationViewName))
                .setUpdateMask(
                    FieldMask.newBuilder()
                        .addPaths("namespace_id")
                        .addPaths("display_name")
                        .build())
                .build());
  }

  private ResourceId resolveCatalogId(String prefix) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId id =
        directoryStub()
            .resolveCatalog(
                ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest.newBuilder()
                    .setRef(NameRef.newBuilder().setCatalog(catalogName).build())
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
    ResourceId id =
        directoryStub()
            .resolveNamespace(
                ResolveNamespaceRequest.newBuilder()
                    .setRef(
                        NameRef.newBuilder()
                            .setCatalog(catalogName)
                            .addAllPath(namespacePath)
                            .build())
                    .build())
            .getResourceId();
    if (id == null || id.getId().isBlank()) {
      throw Status.NOT_FOUND
          .withDescription("Namespace " + String.join(".", namespacePath) + " not found")
          .asRuntimeException();
    }
    return id;
  }

  private ResourceId resolveViewId(String prefix, List<String> namespacePath, String viewName) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    ResourceId id =
        directoryStub()
            .resolveView(
                ResolveViewRequest.newBuilder()
                    .setRef(
                        NameRef.newBuilder()
                            .setCatalog(catalogName)
                            .addAllPath(namespacePath)
                            .setName(viewName)
                            .build())
                    .build())
            .getResourceId();
    if (id == null || id.getId().isBlank()) {
      throw Status.NOT_FOUND
          .withDescription(
              "View " + String.join(".", namespacePath) + "." + viewName + " not found")
          .asRuntimeException();
    }
    return id;
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub() {
    return grpc.withHeaders(grpc.raw().directory());
  }

  private ViewServiceGrpc.ViewServiceBlockingStub viewStub() {
    return grpc.withHeaders(grpc.raw().view());
  }
}
