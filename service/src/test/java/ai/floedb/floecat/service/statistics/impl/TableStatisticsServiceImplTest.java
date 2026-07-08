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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.PutTargetStatsRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Multi;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TableStatisticsServiceImplTest {

  @Test
  void putTargetStatsRejectsSystemTableBeforePersistence() {
    var svc = new TableStatisticsServiceImpl();
    svc.snapshots = mock(SnapshotRepository.class);
    svc.statsStore = mock(StatsStore.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.overlay = mock(CatalogOverlay.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_stats_table")
            .build();

    when(svc.overlay.resolve(tableId)).thenReturn(Optional.of(systemTableNode(tableId)));
    var pc = mock(PrincipalContext.class);
    when(svc.principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(svc.authz).require(any(), anyString());

    var request =
        PutTargetStatsRequest.newBuilder().setTableId(tableId).setSnapshotId(123L).build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () -> svc.putTargetStats(Multi.createFrom().item(request)).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(svc.snapshots, svc.statsStore, svc.idempotencyStore);
  }

  private static SystemTableNode systemTableNode(ResourceId tableId) {
    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_" + tableId.getId())
            .build();
    return new SystemTableNode.EngineSystemTableNode(
        tableId, 1L, Instant.EPOCH, "engine", "system_table", namespaceId, List.of(), null, null);
  }
}
