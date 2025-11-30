package ai.floedb.metacat.gateway.iceberg.rest;

import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.metacat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.metacat.catalog.rpc.ResolveTableRequest;
import ai.floedb.metacat.catalog.rpc.ResolveViewRequest;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.gateway.iceberg.grpc.GrpcWithHeaders;
import java.util.List;

/**
 * Resolves human-readable identifiers into Metacat ResourceIds via DirectoryService.
 */
public final class NameResolution {
  private NameResolution() {}

  public static ResourceId resolveCatalog(GrpcWithHeaders grpc, String catalogName) {
    DirectoryServiceGrpc.DirectoryServiceBlockingStub dir =
        grpc.withHeaders(grpc.raw().directory());
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).build();
    return dir
        .resolveCatalog(ResolveCatalogRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  public static ResourceId resolveNamespace(
      GrpcWithHeaders grpc, String catalogName, List<String> path) {
    DirectoryServiceGrpc.DirectoryServiceBlockingStub dir =
        grpc.withHeaders(grpc.raw().directory());
    NameRef ref = NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).build();
    return dir.resolveNamespace(ResolveNamespaceRequest.newBuilder().setRef(ref).build())
        .getResourceId();
  }

  public static ResourceId resolveTable(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String tableName) {
    DirectoryServiceGrpc.DirectoryServiceBlockingStub dir =
        grpc.withHeaders(grpc.raw().directory());
    NameRef ref =
        NameRef.newBuilder()
            .setCatalog(catalogName)
            .addAllPath(path)
            .setName(tableName)
            .build();
    return dir.resolveTable(ResolveTableRequest.newBuilder().setRef(ref).build()).getResourceId();
  }

  public static ResourceId resolveView(
      GrpcWithHeaders grpc, String catalogName, List<String> path, String viewName) {
    DirectoryServiceGrpc.DirectoryServiceBlockingStub dir =
        grpc.withHeaders(grpc.raw().directory());
    NameRef ref =
        NameRef.newBuilder().setCatalog(catalogName).addAllPath(path).setName(viewName).build();
    return dir.resolveView(ResolveViewRequest.newBuilder().setRef(ref).build()).getResourceId();
  }
}
