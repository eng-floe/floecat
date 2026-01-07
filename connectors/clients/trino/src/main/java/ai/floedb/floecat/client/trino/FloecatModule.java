/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.client.trino;

import static io.airlift.configuration.ConfigBinder.configBinder;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
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
import io.trino.plugin.iceberg.IcebergPageSourceProvider;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.PreDestroy;

public class FloecatModule extends AbstractConfigurationAwareModule {

  private final TypeManager typeManager;
  private final NodeManager nodeManager;
  private final CatalogName catalogName;
  private final CatalogHandle catalogHandle;
  private final OpenTelemetry openTelemetry;
  private final Tracer tracer;
  private final boolean coordinatorFileCaching;

  public FloecatModule(
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
    configBinder(binder).bindConfig(FloecatConfig.class);

    binder.bind(FloecatClient.class).in(Scopes.SINGLETON);
    binder.bind(FloecatConnector.class).in(Scopes.SINGLETON);
    binder.bind(FloecatMetadata.class).in(Scopes.SINGLETON);
    binder.bind(FloecatSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(FloecatPageSourceProvider.class).in(Scopes.SINGLETON);
    binder.bind(FloecatSessionProperties.class).in(Scopes.SINGLETON);
    binder.bind(ManagedChannelCloser.class).asEagerSingleton();

    binder
        .bind(ConnectorPageSourceProvider.class)
        .to(FloecatPageSourceProvider.class)
        .in(Scopes.SINGLETON);

    super.install(
        new FileSystemModule(
            catalogName.toString(), nodeManager, openTelemetry, coordinatorFileCaching));

    binder
        .bind(IcebergFileSystemFactory.class)
        .to(DefaultIcebergFileSystemFactory.class)
        .in(Scopes.SINGLETON);

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

  @Provides
  @Singleton
  public ManagedChannel createGrpcChannel(FloecatConfig config) {
    return ManagedChannelBuilder.forTarget(config.getFloecatUri()).usePlaintext().build();
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
  public SnapshotServiceGrpc.SnapshotServiceBlockingStub createSnapshotStub(
      ManagedChannel channel) {
    return SnapshotServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public CatalogServiceGrpc.CatalogServiceBlockingStub createCatalogStub(ManagedChannel channel) {
    return CatalogServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public QueryServiceGrpc.QueryServiceBlockingStub createQueryStub(ManagedChannel channel) {
    return QueryServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public QueryScanServiceGrpc.QueryScanServiceBlockingStub createQueryScanStub(
      ManagedChannel channel) {
    return QueryScanServiceGrpc.newBlockingStub(channel);
  }

  @Provides
  @Singleton
  public QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub createQuerySchemaStub(
      ManagedChannel channel) {
    return QuerySchemaServiceGrpc.newBlockingStub(channel);
  }

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
  public IcebergPageSourceProvider icebergPageSourceProvider(
      IcebergFileSystemFactory fsFactory,
      FileFormatDataSourceStats stats,
      OrcReaderOptions orcOptions,
      ParquetReaderOptions parquetOptions,
      TypeManager typeManager) {
    return new IcebergPageSourceProvider(fsFactory, stats, orcOptions, parquetOptions, typeManager);
  }

  @Provides
  @Singleton
  public Tracer tracer() {
    return tracer;
  }

  @Singleton
  public static class ManagedChannelCloser implements AutoCloseable {
    private final ManagedChannel channel;

    @com.google.inject.Inject
    public ManagedChannelCloser(ManagedChannel channel) {
      this.channel = channel;
    }

    @PreDestroy
    @Override
    public void close() {
      channel.shutdownNow();
    }
  }
}
