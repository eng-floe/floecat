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

package ai.floedb.floecat.gateway.iceberg.rest.services.resolution;

import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.DirectoryClient;
import io.grpc.Status;
import java.util.List;

public final class NameResolution {
  private NameResolution() {}

  public static ResourceId resolveCatalog(GrpcWithHeaders grpc, String catalogName) {
    return resolveCatalog(new DirectoryClient(grpc), catalogName);
  }

  public static ResourceId resolveCatalog(DirectoryClient client, String catalogName) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    var response = client.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    return requireId(response == null ? null : response.getResourceId(), "catalog", catalogName);
  }

  public static ResourceId resolveNamespace(
      GrpcWithHeaders grpc, String catalogName, List<String> path) {
    return resolveNamespace(new DirectoryClient(grpc), catalogName, path);
  }

  public static ResourceId resolveNamespace(
      DirectoryClient client, String catalogName, List<String> path) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).build();
    var response =
        client.resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build());
    return requireId(
        response == null ? null : response.getResourceId(), "namespace", catalogName, path, null);
  }

  public static ResourceId resolveTable(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String tableName) {
    return resolveTable(new DirectoryClient(grpc), catalogName, path, tableName);
  }

  public static ResourceId resolveTable(
      DirectoryClient client, String catalogName, List<String> path, String tableName) {
    NameRef ref =
        NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).setName(tableName).build();
    var response = client.resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build());
    return requireId(
        response == null ? null : response.getResourceId(), "table", catalogName, path, tableName);
  }

  public static ResourceId resolveView(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String viewName) {
    return resolveView(new DirectoryClient(grpc), catalogName, path, viewName);
  }

  public static ResourceId resolveView(
      DirectoryClient client, String catalogName, List<String> path, String viewName) {
    NameRef ref =
        NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).setName(viewName).build();
    var response = client.resolveView(ResolveViewRequest.newBuilder().setRef(ref).build());
    return requireId(
        response == null ? null : response.getResourceId(), "view", catalogName, path, viewName);
  }

  private static ResourceId requireId(
      ResourceId resolved, String resourceType, String catalogName) {
    return requireId(resolved, resourceType, catalogName, null, null);
  }

  private static ResourceId requireId(
      ResourceId resolved,
      String resourceType,
      String catalogName,
      List<String> path,
      String leafName) {
    if (resolved != null && !resolved.getId().isBlank()) {
      return resolved;
    }
    StringBuilder message = new StringBuilder("No such ").append(resourceType).append(": ");
    boolean appended = false;
    if (catalogName != null && !catalogName.isBlank()) {
      message.append(catalogName);
      appended = true;
    }
    if (path != null && !path.isEmpty()) {
      if (appended) {
        message.append('.');
      }
      message.append(String.join(".", path));
      appended = true;
    }
    if (leafName != null && !leafName.isBlank()) {
      if (appended) {
        message.append('.');
      }
      message.append(leafName);
    }
    throw Status.NOT_FOUND.withDescription(message.toString()).asRuntimeException();
  }
}
