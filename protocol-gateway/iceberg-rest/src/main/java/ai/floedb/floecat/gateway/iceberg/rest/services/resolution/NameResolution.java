package ai.floedb.floecat.gateway.iceberg.rest.services.resolution;

import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import ai.floedb.floecat.gateway.iceberg.rest.services.client.DirectoryClient;
import java.util.ArrayList;
import java.util.List;

public final class NameResolution {
  private NameResolution() {}

  public static ResourceId resolveCatalog(GrpcWithHeaders grpc, String catalogName) {
    return resolveCatalog(new DirectoryClient(grpc), catalogName);
  }

  public static ResourceId resolveCatalog(DirectoryClient client, String catalogName) {
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    var response = client.resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build());
    return coalesceId(
        response == null ? null : response.getResourceId(),
        syntheticId(ResourceKind.RK_CATALOG, catalogName, List.of(), null));
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
    return coalesceId(
        response == null ? null : response.getResourceId(),
        syntheticId(ResourceKind.RK_NAMESPACE, catalogName, path, null));
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
    return coalesceId(
        response == null ? null : response.getResourceId(),
        syntheticId(ResourceKind.RK_TABLE, catalogName, path, tableName));
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
    return coalesceId(
        response == null ? null : response.getResourceId(),
        syntheticId(ResourceKind.RK_VIEW, catalogName, path, viewName));
  }

  private static ResourceId coalesceId(ResourceId resolved, ResourceId fallback) {
    if (resolved == null || resolved.getId().isBlank()) {
      return fallback;
    }
    return resolved;
  }

  private static ResourceId syntheticId(
      ResourceKind kind, String catalogName, List<String> path, String leafName) {
    List<String> segments = new ArrayList<>();
    segments.add("cat");
    if (catalogName != null && !catalogName.isBlank()) {
      segments.add(catalogName);
    }
    if (path != null) {
      for (String part : path) {
        if (part != null && !part.isBlank()) {
          segments.add(part);
        }
      }
    }
    if (leafName != null && !leafName.isBlank()) {
      segments.add(leafName);
    }
    String id = String.join(":", segments);
    return ResourceId.newBuilder().setId(id).setKind(kind).build();
  }
}
