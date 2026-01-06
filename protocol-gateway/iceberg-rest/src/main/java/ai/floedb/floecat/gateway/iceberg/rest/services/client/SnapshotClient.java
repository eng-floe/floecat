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

import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.CreateSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.DeleteSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotResponse;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsRequest;
import ai.floedb.floecat.catalog.rpc.ListSnapshotsResponse;
import ai.floedb.floecat.catalog.rpc.SnapshotServiceGrpc;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.UpdateSnapshotResponse;
import ai.floedb.floecat.gateway.iceberg.grpc.GrpcWithHeaders;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class SnapshotClient {
  private final GrpcWithHeaders grpc;

  @Inject
  public SnapshotClient(GrpcWithHeaders grpc) {
    this.grpc = grpc;
  }

  public ListSnapshotsResponse listSnapshots(ListSnapshotsRequest request) {
    return stub().listSnapshots(request);
  }

  public GetSnapshotResponse getSnapshot(GetSnapshotRequest request) {
    return stub().getSnapshot(request);
  }

  public CreateSnapshotResponse createSnapshot(CreateSnapshotRequest request) {
    return stub().createSnapshot(request);
  }

  public UpdateSnapshotResponse updateSnapshot(UpdateSnapshotRequest request) {
    return stub().updateSnapshot(request);
  }

  public DeleteSnapshotResponse deleteSnapshot(DeleteSnapshotRequest request) {
    return stub().deleteSnapshot(request);
  }

  private SnapshotServiceGrpc.SnapshotServiceBlockingStub stub() {
    return grpc.withHeaders(grpc.raw().snapshot());
  }
}
