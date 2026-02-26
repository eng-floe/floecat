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

package ai.floedb.floecat.service.statistics.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.UpstreamRef;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.impl.ReconcilerService;
import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.statistics.rpc.AnalyzeTableRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StatsCaptureImplTest {
  private StatsCaptureImpl service;
  private TableRepository tableRepo;
  private NamespaceRepository namespaceRepo;
  private ReconcilerService reconcilerService;

  @BeforeEach
  void setUp() {
    service = new StatsCaptureImpl();
    tableRepo = mock(TableRepository.class);
    namespaceRepo = mock(NamespaceRepository.class);
    reconcilerService = mock(ReconcilerService.class);
    PrincipalProvider principalProvider = mock(PrincipalProvider.class);
    Authorizer authz = mock(Authorizer.class);

    service.tableRepo = tableRepo;
    service.namespaceRepo = namespaceRepo;
    service.reconcilerService = reconcilerService;
    service.principalProvider = principalProvider;
    service.authz = authz;

    PrincipalContext principalContext = mock(PrincipalContext.class);
    when(principalProvider.get()).thenReturn(principalContext);
    when(principalContext.getCorrelationId()).thenReturn("corr");
    when(principalContext.getAccountId()).thenReturn("acct");
    doNothing().when(authz).require(any(), anyString());
  }

  @Test
  void analyzeTableRunsMetadataThenStats() {
    ResourceId connectorId = rid("acct", "connector-1", ResourceKind.RK_CONNECTOR);
    ResourceId tableId = rid("acct", "table-1", ResourceKind.RK_TABLE);
    ResourceId namespaceId = rid("acct", "ns-1", ResourceKind.RK_NAMESPACE);

    when(tableRepo.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setNamespaceId(namespaceId)
                    .setDisplayName("orders")
                    .setUpstream(UpstreamRef.newBuilder().setConnectorId(connectorId))
                    .build()));
    when(namespaceRepo.getById(namespaceId))
        .thenReturn(
            Optional.of(
                Namespace.newBuilder()
                    .setResourceId(namespaceId)
                    .addParents("examples")
                    .setDisplayName("iceberg")
                    .build()));

    when(reconcilerService.reconcile(
            any(), any(), anyBoolean(), any(ReconcileScope.class), any(CaptureMode.class)))
        .thenReturn(new ReconcilerService.Result(1, 1, 0, null))
        .thenReturn(new ReconcilerService.Result(1, 0, 0, null));

    var response =
        service
            .analyzeTable(AnalyzeTableRequest.newBuilder().setTableId(tableId).build())
            .await()
            .indefinitely();

    assertEquals(1L, response.getMetadataTablesScanned());
    assertEquals(1L, response.getMetadataTablesChanged());
    assertEquals(0L, response.getMetadataErrors());
    assertEquals(1L, response.getStatsTablesScanned());
    assertEquals(0L, response.getStatsTablesChanged());
    assertEquals(0L, response.getStatsErrors());
  }

  @Test
  void analyzeTableRejectsTableWithoutUpstreamConnector() {
    ResourceId tableId = rid("acct", "table-1", ResourceKind.RK_TABLE);
    ResourceId namespaceId = rid("acct", "ns-1", ResourceKind.RK_NAMESPACE);

    when(tableRepo.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setNamespaceId(namespaceId)
                    .setDisplayName("orders")
                    .build()));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .analyzeTable(AnalyzeTableRequest.newBuilder().setTableId(tableId).build())
                    .await()
                    .indefinitely());
    assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
  }

  private static ResourceId rid(String accountId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(accountId).setId(id).setKind(kind).build();
  }
}
