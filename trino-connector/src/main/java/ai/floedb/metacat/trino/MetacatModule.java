package ai.floedb.metacat.trino;

import static io.airlift.configuration.ConfigBinder.configBinder;

import ai.floedb.metacat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.metacat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.metacat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.metacat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.metacat.catalog.rpc.TableServiceGrpc;
import ai.floedb.metacat.query.rpc.PlanningExGrpc;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.manager.FileSystemModule;
import io.trino.orc.OrcReaderOptions;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.iceberg.IcebergFileSystemFactory;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.TypeManager;

public class MetacatModule extends AbstractConfigurationAwareModule {

  private final TypeManager typeManager;
  private final NodeManager nodeManager;
  private final CatalogName catalogName;
  private final CatalogHandle catalogHandle;
  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final boolean coordinatorFileCaching;

  public MetacatModule(
      String catalogName,
      CatalogHandle catalogHandle,
      TypeManager typeManager,
      NodeManager nodeManager,
      OpenTelemetry openTelemetry,
      Tracer tracer,
      boolean coordinatorFileCaching) {
    this.typeManager = typeManager;
    this.nodeManager = nodeManager;
    this.catalogName = new CatalogName(catalogName);
    this.catalogHandle = catalogHandle;
    this.openTelemetry = openTelemetry;
    this.tracer = tracer;
    this.coordinatorFileCaching = coordinatorFileCaching;
  }

  @Override
  protected void setup(Binder binder) {

    // Configuration class
    configBinder(binder).bindConfig(MetacatConfig.class);

    // gRPC client holder
    binder.bind(MetacatClient.class).in(Scopes.SINGLETON);

    // Core connector classes
    binder.bind(MetacatConnector.class).in(Scopes.SINGLETON);
    binder.bind(MetacatMetadata.class).in(Scopes.SINGLETON);
    binder.bind(MetacatSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(MetacatPageSourceProvider.class).in(Scopes.SINGLETON);
    binder.bind(MetacatSessionProperties.class).in(Scopes.SINGLETON);

    binder
        .bind(ConnectorPageSourceProvider.class)
        .to(MetacatPageSourceProvider.class)
        .in(Scopes.SINGLETON);

    // Install Trino's filesystem module to provide TrinoFileSystemFactory and related bindings.
    super.install(
        new FileSystemModule(
            catalogName.toString(), nodeManager, openTelemetry, coordinatorFileCaching));

    // Minimal Iceberg FS binding; use the default implementation.
    binder
        .bind(IcebergFileSystemFactory.class)
        .to(DefaultIcebergFileSystemFactory.class)
        .in(Scopes.SINGLETON);

    // Trino services from the engine / connector context
    binder.bind(TypeManager.class).toInstance(typeManager);
    binder.bind(NodeManager.class).toInstance(nodeManager);
    binder.bind(CatalogName.class).toInstance(catalogName);
    binder.bind(CatalogHandle.class).toInstance(catalogHandle);
  }

  @Provides
  @Singleton
  public OpenTelemetry openTelemetry() {
    return openTelemetry;
  }

  // ----- gRPC channel and stubs -----

  @Provides
  @Singleton
  public ManagedChannel createGrpcChannel(MetacatConfig config) {
    // Plaintext for now; swap to TLS + auth later
    return ManagedChannelBuilder.forTarget(config.getMetacatUri()).usePlaintext().build();
  }

  @Provides
  @Singleton
  public TableServiceGrpc.TableServiceBlockingStub createTableStub(ManagedChannel channel) {
    return TableServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public DirectoryServiceGrpc.DirectoryServiceBlockingStub createDirectoryStub(
      ManagedChannel channel) {
    return DirectoryServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public NamespaceServiceGrpc.NamespaceServiceBlockingStub createNamespaceStub(
      ManagedChannel channel) {
    return NamespaceServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public CatalogServiceGrpc.CatalogServiceBlockingStub createCatalogStub(ManagedChannel channel) {
    return CatalogServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public PlanningExGrpc.PlanningExBlockingStub createPlanningStub(ManagedChannel channel) {
    return PlanningExGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public SchemaServiceGrpc.SchemaServiceBlockingStub createSchemaStub(ManagedChannel channel) {
    return SchemaServiceGrpc.newBlockingStub(channel);
  }

  // ----- Iceberg / file formats (local utilities only, no FS wiring yet) -----

  @Provides
  @Singleton
  public FileFormatDataSourceStats fileFormatDataSourceStats() {
    return new FileFormatDataSourceStats();
  }

  @Provides
  @Singleton
  public OrcReaderOptions orcReaderOptions() {
    return new OrcReaderOptions();
  }

  @Provides
  @Singleton
  public ParquetReaderOptions parquetReaderOptions() {
    return ParquetReaderOptions.builder().build();
  }

  @Provides
  @Singleton
  public io.trino.plugin.iceberg.IcebergPageSourceProvider icebergPageSourceProvider(
      IcebergFileSystemFactory fsFactory,
      FileFormatDataSourceStats stats,
      OrcReaderOptions orcOptions,
      ParquetReaderOptions parquetOptions,
      TypeManager typeManager) {
    // Construct the engine's Iceberg page source provider with the bound dependencies.
    return new io.trino.plugin.iceberg.IcebergPageSourceProvider(
        fsFactory, stats, orcOptions, parquetOptions, typeManager);
  }

  @Provides
  @Singleton
  public Tracer tracer() {
    return tracer;
  }
}
