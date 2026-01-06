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

package ai.floedb.floecat.gateway.iceberg.rest.services.client;

import ai.floedb.floecat.catalog.rpc.DirectoryServiceGrpc;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogRequest;
import ai.floedb.floecat.catalog.rpc.ResolveCatalogResponse;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.ResolveNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ResolveTableRequest;
import ai.floedb.floecat.catalog.rpc.ResolveTableResponse;
import ai.floedb.floecat.catalog.rpc.ResolveViewRequest;
import ai.floedb.floecat.catalog.rpc.ResolveViewResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;

public class DirectoryClient {
  private final GrpcWithHeaders grpc;

  public DirectoryClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ResolveCatalogResponse resolveCatalog(ResolveCatalogRequest request) {
    return stub().resolveCatalog(request);
  }

  public ResolveNamespaceResponse resolveNamespace(ResolveNamespaceRequest request) {
    return stub().resolveNamespace(request);
  }

  public ResolveTableResponse resolveTable(ResolveTableRequest request) {
    return stub().resolveTable(request);
  }

  public ResolveViewResponse resolveView(ResolveViewRequest request) {
    return stub().resolveView(request);
  }

  private DirectoryServiceGrpc.DirectoryServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().directory());
  }
}
