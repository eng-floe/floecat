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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.gateway.iceberg.rest.services.catalog.TableGatewaySupport;
import java.util.List;
import org.junit.jupiter.api.Test;

class TableCommitSideEffectServiceTest {
  private final TableCommitSideEffectService service = new TableCommitSideEffectService();

  @Test
  void runConnectorSyncTriggersCaptureAndReconcile() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(true);
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-1").build();

    service.runConnectorSync(tableSupport, connectorId, List.of("db"), "orders");

    verify(tableSupport).runSyncMetadataCapture(connectorId, List.of("db"), "orders");
    verify(tableSupport).triggerScopedReconcile(connectorId, List.of("db"), "orders");
  }

  @Test
  void runConnectorSyncSkipsBlankConnectorIds() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    ResourceId emptyId = ResourceId.newBuilder().setId("").build();

    service.runConnectorSync(tableSupport, emptyId, List.of("db"), "orders");

    verify(tableSupport, never()).runSyncMetadataCapture(any(), any(), any());
    verify(tableSupport, never()).triggerScopedReconcile(any(), any(), any());
  }

  @Test
  void runConnectorStatsSyncSkipsWhenConnectorIntegrationDisabled() {
    TableGatewaySupport tableSupport = mock(TableGatewaySupport.class);
    when(tableSupport.connectorIntegrationEnabled()).thenReturn(false);
    ResourceId connectorId = ResourceId.newBuilder().setId("connector-1").build();

    service.runConnectorStatsSyncAttempt(tableSupport, connectorId, List.of("db"), "orders");

    verify(tableSupport, never()).runSyncStatisticsCapture(any(), any(), any());
  }
}
