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

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import ai.floedb.floecat.query.rpc.QuerySchemaServiceGrpc;
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
  private final QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemas;
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
    this.schemas = QuerySchemaServiceGrpc.newBlockingStub(channel);
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

  public QuerySchemaServiceGrpc.QuerySchemaServiceBlockingStub schemas() {
    return schemas;
  }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshots() {
    return snapshots;
  }

  @Override
  public void close() {
    channel.shutdownNow();
  }
}