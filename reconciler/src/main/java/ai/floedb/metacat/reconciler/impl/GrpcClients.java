package ai.floedb.metacat.reconciler.impl;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class GrpcClients {
  @GrpcClient("metacat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("metacat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("metacat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("metacat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("metacat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("metacat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @GrpcClient("metacat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny;

  @GrpcClient("metacat")
  ConnectorsGrpc.ConnectorsBlockingStub connector;

  public DirectoryServiceGrpc.DirectoryServiceBlockingStub directory() {
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

  public MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny() {
    return statisticsMutiny;
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return connector;
  }
}
