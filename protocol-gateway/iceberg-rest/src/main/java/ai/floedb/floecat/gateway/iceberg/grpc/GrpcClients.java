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

package ai.floedb.floecat.gateway.iceberg.grpc;

import ai.floedb.floecat.account.rpc.AccountServiceGrpc;
import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SchemaServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.gateway.iceberg.config.IcebergGatewayConfig;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
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
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan;
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema;
  private final ConnectorsGrpc.ConnectorsBlockingStub connectors;
  private final AccountServiceGrpc.AccountServiceBlockingStub account;

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
    this.queryScan = QueryScanServiceGrpc.newBlockingStub(channel);
    this.querySchema = QuerySchemaServiceGrpc.newBlockingStub(channel);
    this.connectors = ConnectorsGrpc.newBlockingStub(channel);
    this.account = AccountServiceGrpc.newBlockingStub(channel);
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

  public QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan() {
    return queryScan;
  }

  public QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema() {
    return querySchema;
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connectors() {
    return connectors;
  }

  public AccountServiceGrpc.AccountServiceBlockingStub account() {
    return account;
  }

  @PreDestroy
  @Override
  public void close() {
    channel.shutdownNow();
  }
}
