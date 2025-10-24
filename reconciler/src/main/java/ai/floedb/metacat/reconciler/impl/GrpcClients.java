package ai.floedb.metacat.reconciler.impl;

import ai.floedb.metacat.catalog.rpc.*;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class GrpcClients {
  @GrpcClient("directory")
  DirectoryGrpc.DirectoryBlockingStub directory;

  @GrpcClient("catalog-service")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("namespace-service")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("table-service")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("snapshot-service")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("connectors")
  ConnectorsGrpc.ConnectorsBlockingStub connector;

  public DirectoryGrpc.DirectoryBlockingStub directory() {
    return directory;
  }

  public CatalogServiceGrpc.CatalogServiceBlockingStub catalog() { return catalog; }

  public NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace() { return namespace; }

  public TableServiceGrpc.TableServiceBlockingStub table() { return table; }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot() { return snapshot; }

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return connector;
  }
}
