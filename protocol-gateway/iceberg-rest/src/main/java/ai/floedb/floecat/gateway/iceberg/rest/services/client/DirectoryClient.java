package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;

public class DirectoryClient {
  private final GrpcWithHeaders grpc;

  public DirectoryClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ResolveCatalogResponse resolveCatalog(ResolveCatalogRequest request) {
    return stub().resolveCatalog(request);
  }

  public ResolveNamespaceResponse resolveNamespace(ResolveNamespaceRequest request) {
    return stub().resolveNamespace(request);
  }

  public ResolveTableResponse resolveTable(ResolveTableRequest request) {
    return stub().resolveTable(request);
  }

  public ResolveViewResponse resolveView(ResolveViewRequest request) {
    return stub().resolveView(request);
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().directory());
  }
}
