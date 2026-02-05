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

package ai.floedb.floecat.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.CatalogServiceGrpc;
import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.MutinyTableStatisticsServiceGrpc;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableServiceGrpc;
import ai.floedb.floecat.catalog.rpc.TableStatisticsServiceGrpc;
import ai.floedb.floecat.connector.rpc.ConnectorsGrpc;
import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;
import io.quarkus.grpc.GrpcClient;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class GrpcClients {
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private final Optional<String> headerName;
  private final Optional<String> staticToken;

  public GrpcClients(
      @ConfigProperty(name = "floecat.reconciler.authorization.header") Optional<String> headerName,
      @ConfigProperty(name = "floecat.reconciler.authorization.token")
          Optional<String> staticToken) {
    this.headerName = headerName.map(String::trim).filter(v -> !v.isBlank());
    this.staticToken = staticToken.map(String::trim).filter(v -> !v.isBlank());
  }

  @GrpcClient("floecat")
  DirectoryServiceGrpc.DirectoryServiceBlockingStub directory;

  @GrpcClient("floecat")
  CatalogServiceGrpc.CatalogServiceBlockingStub catalog;

  @GrpcClient("floecat")
  NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace;

  @GrpcClient("floecat")
  TableServiceGrpc.TableServiceBlockingStub table;

  @GrpcClient("floecat")
  SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot;

  @GrpcClient("floecat")
  TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics;

  @GrpcClient("floecat")
  MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny;

  @GrpcClient("floecat")
  ConnectorsGrpc.ConnectorsBlockingStub connector;

  public DirectoryServiceGrpc.DirectoryServiceBlockingStub directory() {
    return withAuth(directory);
  }

  public CatalogServiceGrpc.CatalogServiceBlockingStub catalog() {
    return withAuth(catalog);
  }

  public NamespaceServiceGrpc.NamespaceServiceBlockingStub namespace() {
    return withAuth(namespace);
  }

  public TableServiceGrpc.TableServiceBlockingStub table() {
    return withAuth(table);
  }

  public SnapshotServiceGrpc.SnapshotServiceBlockingStub snapshot() {
    return withAuth(snapshot);
  }

  public TableStatisticsServiceGrpc.TableStatisticsServiceBlockingStub statistics() {
    return withAuth(statistics);
  }

  public MutinyTableStatisticsServiceGrpc.MutinyTableStatisticsServiceStub statisticsMutiny() {
    return withAuth(statisticsMutiny);
  }

  public ConnectorsGrpc.ConnectorsBlockingStub connector() {
    return withAuth(connector);
  }

  private Metadata.Key<String> authHeaderKey() {
    if (headerName.isPresent() && !"authorization".equalsIgnoreCase(headerName.get())) {
      return Metadata.Key.of(headerName.get(), Metadata.ASCII_STRING_MARSHALLER);
    }
    return AUTHORIZATION;
  }

  private String withBearerPrefix(String token) {
    if (token.regionMatches(true, 0, "bearer ", 0, 7)) {
      return token;
    }
    return "Bearer " + token;
  }

  private String authorizationToken() {
    String contextToken = ReconcilerAuthContext.AUTHORIZATION_HEADER_VALUE_KEY.get();
    if (contextToken != null && !contextToken.isBlank()) {
      return contextToken;
    }
    return staticToken.orElse(null);
  }

  private <T extends AbstractStub<T>> T withAuth(T stub) {
    String token = authorizationToken();
    if (token == null || token.isBlank()) {
      return stub;
    }
    Metadata metadata = new Metadata();
    metadata.put(authHeaderKey(), withBearerPrefix(token));
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));
  }
}
