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

package ai.floedb.floecat.service.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.rpc.Connector;
import ai.floedb.floecat.connector.rpc.NamespacePath;
import ai.floedb.floecat.reconciler.impl.ReconcileCancellationRegistry;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.rpc.CaptureNowRequest;
import ai.floedb.floecat.reconciler.rpc.CaptureScope;
import ai.floedb.floecat.reconciler.rpc.StartCaptureRequest;
import ai.floedb.floecat.service.reconciler.jobs.ReconcilerSettingsStore;
import ai.floedb.floecat.service.repo.impl.ConnectorRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReconcileControlImplTest {
  private ReconcileControlImpl service;

  @BeforeEach
  void setUp() {
    service = new ReconcileControlImpl();
    service.connectorRepo = mock(ConnectorRepository.class);
    service.principalProvider = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.jobs = mock(ReconcileJobStore.class);
    service.reconcilerService = mock(ReconcilerService.class);
    service.cancellations = mock(ReconcileCancellationRegistry.class);
    service.settings = mock(ReconcilerSettingsStore.class);

    PrincipalContext principalContext = mock(PrincipalContext.class);
    when(service.principalProvider.get()).thenReturn(principalContext);
    when(principalContext.getCorrelationId()).thenReturn("corr");
    when(principalContext.getAccountId()).thenReturn("acct");
    doNothing().when(service.authz).require(any(), anyString());

    ResourceId connectorId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("connector-1")
            .setKind(ResourceKind.RK_CONNECTOR)
            .build();
    when(service.connectorRepo.getById(any()))
        .thenReturn(Optional.of(Connector.newBuilder().setResourceId(connectorId).build()));
  }

  @Test
  void captureNowRejectsSnapshotScopeWithoutSingleTableTarget() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .captureNow(
                        CaptureNowRequest.newBuilder()
                            .setScope(
                                CaptureScope.newBuilder()
                                    .setConnectorId(connectorId())
                                    .addDestinationSnapshotIds(10L)
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  @Test
  void startCaptureRejectsSnapshotScopeWithoutSingleNamespacePath() {
    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .startCapture(
                        StartCaptureRequest.newBuilder()
                            .setScope(
                                CaptureScope.newBuilder()
                                    .setConnectorId(connectorId())
                                    .setDestinationTableDisplayName("orders")
                                    .addDestinationSnapshotIds(10L)
                                    .addDestinationNamespacePaths(
                                        NamespacePath.newBuilder().addSegments("db1").build())
                                    .addDestinationNamespacePaths(
                                        NamespacePath.newBuilder().addSegments("db2").build())
                                    .build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  private static ResourceId connectorId() {
    return ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();
  }
}
