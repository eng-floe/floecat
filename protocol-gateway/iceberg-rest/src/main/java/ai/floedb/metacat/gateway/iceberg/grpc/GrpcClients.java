package ai.floedb.metacat.gateway.iceberg.grpc;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.metacat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.metacat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.metacat.query.rpc.QueryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class GrpcClients implements AutoCloseable {
  private final ManagedChannel channel;
  private final CatalogServiceGrpc.CatalogServiceBlockingStub catalog;
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;
  private final TableServiceGrpc.TableServiceBlockingStub table;
  private final ViewServiceGrpc.ViewServiceBlockingStub view;
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;
  private final SchemaServiceGrpc.SchemaServiceBlockingStub schema;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats;
  private final QueryServiceGrpc.QueryServiceBlockingStub query;

  public GrpcClients(IcebergGatewayConfig config) {
    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(config.upstreamTarget());
    if (config.upstreamPlaintext()) {
      builder.usePlaintext();
    }
    this.channel = builder.build();
    this.catalog = CatalogServiceGrpc.newBlockingStub(channel);
    this.namespace = NamespaceServiceGrpc.newBlockingStub(channel);
    this.table = TableServiceGrpc.newBlockingStub(channel);
    this.view = ViewServiceGrpc.newBlockingStub(channel);
    this.snapshot = SnapshotServiceGrpc.newBlockingStub(channel);
    this.schema = SchemaServiceGrpc.newBlockingStub(channel);
    this.directory = DirectoryServiceGrpc.newBlockingStub(channel);
    this.stats = TableStatisticsServiceGrpc.newBlockingStub(channel);
    this.query = QueryServiceGrpc.newBlockingStub(channel);
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

  public ViewServiceGrpc.ViewServiceBlockingStub view() {
    return view;
  }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot() {
    return snapshot;
  }

  public SchemaServiceGrpc.SchemaServiceBlockingStub schema() {
    return schema;
  }

  public DirectoryServiceGrpc.DirectoryServiceBlockingStub directory() {
    return directory;
  }

  public TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub stats() {
    return stats;
  }

  public QueryServiceGrpc.QueryServiceBlockingStub query() {
    return query;
  }

  @PreDestroy
  @Override
  public void close() {
    channel.shutdownNow();
  }
}
