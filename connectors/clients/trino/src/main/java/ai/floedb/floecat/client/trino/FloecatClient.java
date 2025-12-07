package ai.floedb.floecat.client.trino;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import io.grpc.ManagedChannel;
import jakarta.inject.Inject;
import java.io.Closeable;
import java.util.Objects;

public final class FloecatClient implements Closeable {

  private final ManagedChannel channel;
  private final TableServiceGrpc.TableServiceBlockingStub tables;
  private final ConnectorsGrpc.ConnectorsBlockingStub connectors;
  private final CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final QueryServiceGrpc.QueryServiceBlockingStub queries;
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub scans;
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots;

  @Inject
  public FloecatClient(FloecatConfig cfg, ManagedChannel channel) {
    Objects.requireNonNull(cfg, "cfg");
    this.channel = Objects.requireNonNull(channel, "channel");
    this.tables = TableServiceGrpc.newBlockingStub(channel);
    this.connectors = ConnectorsGrpc.newBlockingStub(channel);
    this.catalogs = CatalogServiceGrpc.newBlockingStub(channel);
    this.namespaces = NamespaceServiceGrpc.newBlockingStub(channel);
    this.directory = DirectoryServiceGrpc.newBlockingStub(channel);
    this.queries = QueryServiceGrpc.newBlockingStub(channel);
    this.scans = QueryScanServiceGrpc.newBlockingStub(channel);
    this.snapshots = SnapshotServiceGrpc.newBlockingStub(channel);
  }

  public TableServiceGrpc.TableServiceBlockingStub tables() {
    return tables;
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connectors() {
    return connectors;
  }

  public CatalogServiceGrpc.CatalogServiceBlockingStub catalogs() {
    return catalogs;
  }

  public NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces() {
    return namespaces;
  }

  public DirectoryServiceGrpc.DirectoryServiceBlockingStub directory() {
    return directory;
  }

  public QueryServiceGrpc.QueryServiceBlockingStub queries() {
    return queries;
  }

  public QueryScanServiceGrpc.QueryScanServiceBlockingStub scans() {
    return scans;
  }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots() {
    return snapshots;
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}