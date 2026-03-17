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

package ai.floedb.floecat.gateway.iceberg.minimal.services.namespace;

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceSpec;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.common.rpc.IdempotencyKey;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.minimal.resources.common.CatalogResolver;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class GrpcNamespaceBackend implements NamespaceBackend {
  private final GrpcWithHeaders grpc;
  private final MinimalGatewayConfig config;

  @Inject
  public GrpcNamespaceBackend(GrpcWithHeaders grpc, MinimalGatewayConfig config) {
    this.grpc = grpc;
    this.config = config;
  }

  @Override
  public ListNamespacesResponse list(
      String prefix, List<String> parentPath, Integer pageSize, String pageToken) {
    ListNamespacesRequest.Builder request =
        ListNamespacesRequest.newBuilder().setChildrenOnly(true);
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
    if (parentPath == null || parentPath.isEmpty()) {
      request.setCatalogId(resolveCatalogId(prefix));
    } else {
      request.setNamespaceId(resolveNamespaceId(prefix, parentPath));
    }
    return namespaceStub().listNamespaces(request.build());
  }

  @Override
  public Namespace get(String prefix, List<String> namespacePath) {
    ResourceId namespaceId = resolveNamespaceId(prefix, namespacePath);
    return namespaceStub()
        .getNamespace(GetNamespaceRequest.newBuilder().setNamespaceId(namespaceId).build())
        .getNamespace();
  }

  @Override
  public void exists(String prefix, List<String> namespacePath) {
    resolveNamespaceId(prefix, namespacePath);
  }

  @Override
  public Namespace create(
      String prefix,
      List<String> namespacePath,
      Map<String, String> properties,
      String idempotencyKey) {
    if (namespacePath == null || namespacePath.isEmpty()) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Namespace name must be provided")
          .asRuntimeException();
    }
    CreateNamespaceRequest.Builder request =
        CreateNamespaceRequest.newBuilder()
            .setSpec(
                NamespaceSpec.newBuilder()
                    .setCatalogId(resolveCatalogId(prefix))
                    .addAllPath(namespacePath.subList(0, namespacePath.size() - 1))
                    .setDisplayName(namespacePath.get(namespacePath.size() - 1))
                    .putAllProperties(properties == null ? Map.of() : properties));
    if (idempotencyKey != null && !idempotencyKey.isBlank()) {
      request.setIdempotency(IdempotencyKey.newBuilder().setKey(idempotencyKey));
    }
    return namespaceStub().createNamespace(request.build()).getNamespace();
  }

  @Override
  public Namespace updateProperties(
      String prefix,
      List<String> namespacePath,
      Map<String, String> properties,
      String idempotencyKey) {
    Namespace existing = get(prefix, namespacePath);
    ResourceId namespaceId = existing.getResourceId();
    NamespaceSpec spec =
        NamespaceSpec.newBuilder()
            .setCatalogId(existing.getCatalogId())
            .setDisplayName(existing.getDisplayName())
            .addAllPath(existing.getParentsList())
            .putAllProperties(properties == null ? Map.of() : properties)
            .build();
    return namespaceStub()
        .updateNamespace(
            UpdateNamespaceRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setSpec(spec)
                .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
                .build())
        .getNamespace();
  }

  @Override
  public void delete(String prefix, List<String> namespacePath) {
    namespaceStub()
        .deleteNamespace(
            DeleteNamespaceRequest.newBuilder()
                .setNamespaceId(resolveNamespaceId(prefix, namespacePath))
                .setRequireEmpty(true)
                .build());
  }

  private ResourceId resolveCatalogId(String prefix) {
    String catalogName = CatalogResolver.resolveCatalog(config, prefix);
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    ResourceId id =
        directoryStub()
            .resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build())
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

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub directoryStub() {
    return grpc.withHeaders(grpc.raw().directory());
  }

  private NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaceStub() {
    return grpc.withHeaders(grpc.raw().namespace());
  }
}
