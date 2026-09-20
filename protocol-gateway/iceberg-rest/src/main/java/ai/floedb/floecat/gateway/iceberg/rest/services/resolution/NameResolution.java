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

import ai.floedb.floecat.catalog.rpc.Relation;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.GrpcServiceFacade;
import io.grpc.Status;
import java.util.List;

public final class NameResolution {
  private NameResolution() {}

  public static ResourceId resolveCatalog(GrpcWithHeaders grpc, String catalogName) {
    return resolveCatalog(new GrpcServiceFacade(grpc), catalogName);
  }

  public static ResourceId resolveCatalog(GrpcServiceFacade client, String catalogName) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    var response = client.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    return requireId(response == null ? null : response.getResourceId(), "catalog", catalogName);
  }

  public static ResourceId resolveNamespace(
      GrpcWithHeaders grpc, String catalogName, List<String> path) {
    return resolveNamespace(new GrpcServiceFacade(grpc), catalogName, path);
  }

  public static ResourceId resolveNamespace(
      GrpcServiceFacade client, String catalogName, List<String> path) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).build();
    var response =
        client.resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build());
    return requireId(
        response == null ? null : response.getResourceId(), "namespace", catalogName, path, null);
  }

  public static ResourceId resolveTable(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String tableName) {
    return resolveTable(new GrpcServiceFacade(grpc), catalogName, path, tableName);
  }

  public static ResourceId resolveTable(
      GrpcServiceFacade client, String catalogName, List<String> path, String tableName) {
    NameRef ref =
        NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).setName(tableName).build();
    var response = resolveRelation(client, ref);
    return requireId(
        response != null && response.hasTable() ? response.getResourceId() : null,
        "table",
        catalogName,
        path,
        tableName);
  }

  public static ResourceId resolveView(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String viewName) {
    return resolveView(new GrpcServiceFacade(grpc), catalogName, path, viewName);
  }

  public static ResourceId resolveView(
      GrpcServiceFacade client, String catalogName, List<String> path, String viewName) {
    NameRef ref =
        NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).setName(viewName).build();
    var response = resolveRelation(client, ref);
    return requireId(
        response != null && response.hasView() ? response.getResourceId() : null,
        "view",
        catalogName,
        path,
        viewName);
  }

  private static Relation resolveRelation(GrpcServiceFacade client, NameRef ref) {
    var response =
        client.resolveRelations(
            ResolveRelationsRequest.newBuilder()
                .addReferences(RelationReference.newBuilder().addCandidates(ref))
                .build());
    if (response == null || response.getResultsCount() == 0) {
      return null;
    }
    var result = response.getResults(0);
    return result.hasRelation() ? result.getRelation() : null;
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
