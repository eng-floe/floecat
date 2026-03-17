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

package ai.floedb.floecat.gateway.iceberg.minimal.grpc;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.gateway.iceberg.minimal.config.MinimalGatewayConfig;
import ai.floedb.floecat.query.rpc.QueryScanServiceGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
import ai.floedb.floecat.query.rpc.QueryServiceGrpc;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.transaction.rpc.TransactionsGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class GrpcClients implements AutoCloseable {
  private final ManagedChannel channel;
  private final DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;
  private final NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;
  private final TableServiceGrpc.TableServiceBlockingStub table;
  private final SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;
  private final ViewServiceGrpc.ViewServiceBlockingStub view;
  private final QueryServiceGrpc.QueryServiceBlockingStub query;
  private final QueryScanServiceGrpc.QueryScanServiceBlockingStub queryScan;
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub querySchema;
  private final ConnectorsGrpc.ConnectorsBlockingStub connector;
  private final ReconcileControlGrpc.ReconcileControlBlockingStub reconcile;
  private final TransactionsGrpc.TransactionsBlockingStub transactions;

  public GrpcClients(MinimalGatewayConfig config) {
    ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(config.upstreamTarget());
    if (config.upstreamPlaintext()) {
      builder.usePlaintext();
    }
    this.channel = builder.build();
    this.directory = DirectoryServiceGrpc.newBlockingStub(channel);
    this.namespace = NamespaceServiceGrpc.newBlockingStub(channel);
    this.table = TableServiceGrpc.newBlockingStub(channel);
    this.snapshot = SnapshotServiceGrpc.newBlockingStub(channel);
    this.view = ViewServiceGrpc.newBlockingStub(channel);
    this.query = QueryServiceGrpc.newBlockingStub(channel);
    this.queryScan = QueryScanServiceGrpc.newBlockingStub(channel);
    this.querySchema = QuerySchemaServiceGrpc.newBlockingStub(channel);
    this.connector = ConnectorsGrpc.newBlockingStub(channel);
    this.reconcile = ReconcileControlGrpc.newBlockingStub(channel);
    this.transactions = TransactionsGrpc.newBlockingStub(channel);
  }

  public DirectoryServiceGrpc.DirectoryServiceBlockingStub directory() {
    return directory;
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

  public ViewServiceGrpc.ViewServiceBlockingStub view() {
    return view;
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

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return connector;
  }

  public ReconcileControlGrpc.ReconcileControlBlockingStub reconcile() {
    return reconcile;
  }

  public TransactionsGrpc.TransactionsBlockingStub transactions() {
    return transactions;
  }

  @PreDestroy
  @Override
  public void close() {
    channel.shutdownNow();
  }
}
