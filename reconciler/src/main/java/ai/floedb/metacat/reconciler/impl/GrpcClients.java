package ai.floedb.metacat.reconciler.impl;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.DirectoryGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
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

  @GrpcClient("table-statistics-service")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @GrpcClient("connectors")
  ConnectorsGrpc.ConnectorsBlockingStub connector;

  public DirectoryGrpc.DirectoryBlockingStub directory() {
    return directory;
  }

  public CatalogServiceGrpc.CatalogServiceBlockingStub catalog() {
    return catalog;
  }

  public NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace() {
    return namespace;
  }

  public TableServiceGrpc.TableServiceBlockingStub table() {
    return table;
  }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot() {
    return snapshot;
  }

  public TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics() {
    return statistics;
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return connector;
  }
}
