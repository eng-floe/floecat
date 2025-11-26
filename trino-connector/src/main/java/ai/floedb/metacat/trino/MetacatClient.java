package ai.floedb.metacat.trino;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.connector.rpc.ConnectorsGrpc;
import ai.floedb.metacat.planning.rpc.PlanningExGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.inject.Inject;
import java.io.Closeable;
import java.util.Objects;

public final class MetacatClient implements Closeable {

  private final ManagedChannel channel;
  private final TableServiceGrpc.TableServiceBlockingStub tables;
  private final ConnectorsGrpc.ConnectorsBlockingStub connectors;
  private final CatalogServiceGrpc.CatalogServiceBlockingStub catalogs;
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespaces;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final PlanningExGrpc.PlanningExBlockingStub planningEx;
  private final SchemaServiceGrpc.SchemaServiceBlockingStub schemas;

  @Inject
  public MetacatClient(MetacatConfig cfg) {
    Objects.requireNonNull(cfg, "cfg");
    this.channel =
        ManagedChannelBuilder.forTarget(cfg.getMetacatUri())
            .usePlaintext() // TODO: TLS + auth once wiring is added
            .build();
    this.tables = TableServiceGrpc.newBlockingStub(channel);
    this.connectors = ConnectorsGrpc.newBlockingStub(channel);
    this.catalogs = CatalogServiceGrpc.newBlockingStub(channel);
    this.namespaces = NamespaceServiceGrpc.newBlockingStub(channel);
    this.directory = DirectoryServiceGrpc.newBlockingStub(channel);
    this.planningEx = PlanningExGrpc.newBlockingStub(channel);
    this.schemas = SchemaServiceGrpc.newBlockingStub(channel);
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

  public PlanningExGrpc.PlanningExBlockingStub planning() {
    return planningEx;
  }

  public SchemaServiceGrpc.SchemaServiceBlockingStub schemas() {
    return schemas;
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}
