package ai.floedb.metacat.reconciler.impl;

import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;

@ApplicationScoped
public class GrpcClients {
  @GrpcClient("directory") DirectoryGrpc.DirectoryBlockingStub directory;
  @GrpcClient("resource-mutation") ResourceMutationGrpc.ResourceMutationBlockingStub mutation;
  @GrpcClient("connectors") ConnectorsGrpc.ConnectorsBlockingStub connector;

  public DirectoryGrpc.DirectoryBlockingStub directory() {
    return directory;
  }

  public ResourceMutationGrpc.ResourceMutationBlockingStub mutation() {
    return mutation;
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return connector;
  }
}
