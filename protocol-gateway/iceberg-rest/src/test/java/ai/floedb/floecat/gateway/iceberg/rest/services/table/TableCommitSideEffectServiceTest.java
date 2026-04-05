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

package ai.floedb.floecat.gateway.iceberg.rest.services.table;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableCommitSideEffectServiceTest {
  private final TableCommitSideEffectService service = new TableCommitSideEffectService();

  @Test
  void runPostCommitStatsSyncSkipsWhenConnectorIntegrationDisabled() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    ResourceId connectorId =
        ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();

    service.runPostCommitStatsSyncAttempt(
        tableSupport, connectorId, List.of("db"), "orders", List.of(101L));

    verify(tableSupport, never())
        .runSyncStatisticsCapture(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void runPostCommitStatsSyncTargetsCommittedSnapshots() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    ResourceId connectorId =
        ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();

    service.runPostCommitStatsSyncAttempt(
        tableSupport, connectorId, List.of("db"), "orders", List.of(101L, 102L));

    verify(tableSupport)
        .runSyncStatisticsCapture(
            eq(connectorId), eq(List.of("db")), eq("orders"), eq(List.of(101L, 102L)), eq(false));
  }

  @Test
  void runPostCommitStatsSyncSkipsWhenNoSnapshotsWereCommitted() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    ResourceId connectorId =
        ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();

    service.runPostCommitStatsSyncAttempt(
        tableSupport, connectorId, List.of("db"), "orders", List.of());

    verify(tableSupport, never())
        .runSyncStatisticsCapture(any(), any(), any(), any(), anyBoolean());
  }

  @Test
  void runPostCommitStatsSyncSupportsDirectConnectorIdReplayInputs() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    ResourceId connectorId =
        ResourceId.newBuilder().setId("connector-1").setKind(ResourceKind.RK_CONNECTOR).build();

    service.runPostCommitStatsSyncAttempt(
        tableSupport, connectorId, List.of("db"), "orders", List.of(101L, 102L));

    verify(tableSupport)
        .runSyncStatisticsCapture(
            eq(connectorId), eq(List.of("db")), eq("orders"), eq(List.of(101L, 102L)), eq(false));
  }
}
