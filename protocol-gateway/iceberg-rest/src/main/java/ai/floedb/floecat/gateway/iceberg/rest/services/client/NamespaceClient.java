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

import ai.floedb.floecat.catalog.rpc.CreateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.CreateNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.DeleteNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.GetNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.GetNamespaceResponse;
import ai.floedb.floecat.catalog.rpc.ListNamespacesRequest;
import ai.floedb.floecat.catalog.rpc.ListNamespacesResponse;
import ai.floedb.floecat.catalog.rpc.NamespaceServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceRequest;
import ai.floedb.floecat.catalog.rpc.UpdateNamespaceResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class NamespaceClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public NamespaceClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListNamespacesResponse listNamespaces(ListNamespacesRequest request) {
    return stub().listNamespaces(request);
  }

  public GetNamespaceResponse getNamespace(GetNamespaceRequest request) {
    return stub().getNamespace(request);
  }

  public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
    return stub().createNamespace(request);
  }

  public DeleteNamespaceResponse deleteNamespace(DeleteNamespaceRequest request) {
    return stub().deleteNamespace(request);
  }

  public UpdateNamespaceResponse updateNamespace(UpdateNamespaceRequest request) {
    return stub().updateNamespace(request);
  }

  private NamespaceServiceGrpc.NamespaceServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().namespace());
  }
}
