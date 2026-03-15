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

package ai.floedb.floecat.gateway.iceberg.minimal.services.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcClients;
import ai.floedb.floecat.gateway.iceberg.minimal.grpc.GrpcWithHeaders;
import ai.floedb.floecat.reconciler.rpc.ReconcileControlGrpc;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.reconciler.rpc.StartCaptureResponse;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TableCommitSideEffectServiceTest {
  private final GrpcWithHeaders grpc = Mockito.mock(GrpcWithHeaders.class);
  private final GrpcClients clients = Mockito.mock(GrpcClients.class);
  private final ReconcileControlGrpc.ReconcileControlBlockingStub reconcile =
      Mockito.mock(ReconcileControlGrpc.ReconcileControlBlockingStub.class);

  private final TableCommitSideEffectService service = new TableCommitSideEffectService(grpc);

  @Test
  void schedulesStatsOnlyCaptureWithStartCapture() {
    when(grpc.raw()).thenReturn(clients);
    when(clients.reconcile()).thenReturn(reconcile);
    when(grpc.withHeaders(reconcile)).thenReturn(reconcile);
    when(reconcile.startCapture(any()))
        .thenReturn(StartCaptureResponse.newBuilder().setJobId("job-123").build());

    service.schedulePostCommitStatsSync(
        ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1").build(),
        List.of("iceberg"),
        "orders",
        List.of(101L, 102L));
    ArgumentCaptor<StartCaptureRequest> captor = ArgumentCaptor.forClass(StartCaptureRequest.class);
    Mockito.verify(reconcile).startCapture(captor.capture());
    StartCaptureRequest request = captor.getValue();
    assertEquals("conn-1", request.getScope().getConnectorId().getId());
    assertEquals(
        List.of("iceberg"), request.getScope().getDestinationNamespacePaths(0).getSegmentsList());
    assertEquals("orders", request.getScope().getDestinationTableDisplayName());
    assertEquals(List.of(101L, 102L), request.getScope().getDestinationSnapshotIdsList());
    assertEquals(ai.floedb.floecat.reconciler.rpc.CaptureMode.CM_STATS_ONLY, request.getMode());
    assertEquals(true, request.getFullRescan());
  }

  @Test
  void skipsWhenNoSnapshotsAreProvided() {
    service.schedulePostCommitStatsSync(
        ResourceId.newBuilder().setAccountId("acct-1").setId("conn-1").build(),
        List.of("iceberg"),
        "orders",
        List.of());

    Mockito.verifyNoInteractions(grpc, clients, reconcile);
  }
}
