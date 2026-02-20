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
package ai.floedb.floecat.service.catalog.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.catalog.rpc.CreateSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.GetSnapshotRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotSpec;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.common.rpc.SnapshotRef;
import ai.floedb.floecat.common.rpc.SpecialSnapshot;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SnapshotServiceImplTest {

  @Test
  void ensureTableVisible_rejectsNonTableNode() {
    var svc = new SnapshotServiceImpl();

    svc.snapshotRepo = mock(SnapshotRepository.class);
    svc.statsRepo = mock(StatsRepository.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.overlay = mock(CatalogOverlay.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("t1")
            .build();

    when(svc.overlay.resolve(eq(tableId))).thenReturn(Optional.of(mock(NamespaceNode.class)));

    // Call a method that triggers ensureTableVisible
    var req =
        GetSnapshotRequest.newBuilder()
            .setTableId(tableId)
            .setSnapshot(SnapshotRef.newBuilder().setSpecial(SpecialSnapshot.SS_CURRENT))
            .build();

    // principal/authz plumbing
    var pc = mock(PrincipalContext.class);
    when(svc.principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    doNothing().when(svc.authz).require(any(), anyString());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.getSnapshot(req).await().indefinitely());
    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
  }

  @Test
  void createSnapshot_usesTableRepoSchemaWhenSpecSchemaMissing() {
    var svc = new SnapshotServiceImpl();

    svc.snapshotRepo = mock(SnapshotRepository.class);
    svc.tableRepo = mock(TableRepository.class);
    svc.statsRepo = mock(StatsRepository.class);
    svc.principal = mock(PrincipalProvider.class);
    svc.authz = mock(Authorizer.class);
    svc.idempotencyStore = mock(IdempotencyRepository.class);
    svc.overlay = mock(CatalogOverlay.class);

    var tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("t1")
            .build();

    // table is visible
    when(svc.overlay.resolve(eq(tableId))).thenReturn(Optional.of(mock(UserTableNode.class)));

    var tableRow =
        Table.newBuilder().setResourceId(tableId).setSchemaJson("{\"type\":\"struct\"}").build();
    when(svc.tableRepo.getById(eq(tableId))).thenReturn(Optional.of(tableRow));

    // principal/authz plumbing
    var pc = mock(PrincipalContext.class);
    when(svc.principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(svc.authz).require(any(), anyString());

    // snapshot repo behavior: no existing
    when(svc.snapshotRepo.getById(eq(tableId), anyLong())).thenReturn(Optional.empty());
    doNothing().when(svc.snapshotRepo).create(any(Snapshot.class));
    when(svc.snapshotRepo.metaForSafe(eq(tableId), anyLong()))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(1).build());

    var spec =
        SnapshotSpec.newBuilder()
            .setTableId(tableId)
            .setSnapshotId(123L)
            .build(); // no schema_json set

    var req = CreateSnapshotRequest.newBuilder().setSpec(spec).build();

    svc.createSnapshot(req).await().indefinitely();

    ArgumentCaptor<Snapshot> cap = ArgumentCaptor.forClass(Snapshot.class);
    verify(svc.snapshotRepo).create(cap.capture());

    assertEquals("{\"type\":\"struct\"}", cap.getValue().getSchemaJson());
  }
}
