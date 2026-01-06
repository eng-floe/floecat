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

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.CreateViewResponse;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewResponse;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewResponse;
import ai.floedb.floecat.catalog.rpc.ListViewsRequest;
import ai.floedb.floecat.catalog.rpc.ListViewsResponse;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewResponse;
import ai.floedb.floecat.catalog.rpc.ViewServiceGrpc;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ViewClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public ViewClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListViewsResponse listViews(ListViewsRequest request) {
    return stub().listViews(request);
  }

  public GetViewResponse getView(GetViewRequest request) {
    return stub().getView(request);
  }

  public CreateViewResponse createView(CreateViewRequest request) {
    return stub().createView(request);
  }

  public DeleteViewResponse deleteView(DeleteViewRequest request) {
    return stub().deleteView(request);
  }

  public UpdateViewResponse updateView(UpdateViewRequest request) {
    return stub().updateView(request);
  }

  private ViewServiceGrpc.ViewServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().view());
  }
}
