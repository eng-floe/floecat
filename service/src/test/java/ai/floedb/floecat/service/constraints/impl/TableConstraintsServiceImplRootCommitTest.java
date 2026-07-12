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

package ai.floedb.floecat.service.constraints.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.AddTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.Snapshot;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestNodes;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Every constraints write must record the resulting bundle ref on the table root. */
class TableConstraintsServiceImplRootCommitTest {

  private TableConstraintsServiceImpl service;
  private ResourceId tableId;

  @BeforeEach
  void setUp() {
    service = new TableConstraintsServiceImpl();
    service.snapshots = mock(SnapshotRepository.class);
    service.constraints = mock(ConstraintRepository.class);
    service.principal = mock(PrincipalProvider.class);
    service.authz = mock(Authorizer.class);
    service.idempotencyStore = mock(IdempotencyRepository.class);
    service.overlay = mock(CatalogOverlay.class);
    service.rootWriter = mock(TableRootWriter.class);
    TestPrincipals.stubPrincipal(service.principal, service.authz);

    tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl")
            .build();
    when(service.overlay.resolve(tableId))
        .thenReturn(Optional.of(TestNodes.tableNode(tableId, "{}")));
    when(service.overlay.catalog(any())).thenReturn(Optional.empty());
    when(service.snapshots.getById(tableId, 5L))
        .thenReturn(
            Optional.of(Snapshot.newBuilder().setTableId(tableId).setSnapshotId(5L).build()));
    when(service.constraints.metaFor(any(), anyLong(), any()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(service.constraints.metaFor(any(), anyLong()))
        .thenReturn(MutationMeta.getDefaultInstance());
    when(service.constraints.metaForSafe(any(), anyLong()))
        .thenReturn(MutationMeta.getDefaultInstance());
  }

  @Test
  void putCommitsTheBundleOntoTheRoot() {
    when(service.constraints.putSnapshotConstraints(any(), anyLong(), any())).thenReturn(true);

    service
        .putTableConstraints(
            PutTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(5L)
                .setConstraints(
                    SnapshotConstraints.newBuilder()
                        .addConstraints(ConstraintDefinition.newBuilder().setName("pk")))
                .build())
        .await()
        .indefinitely();

    verify(service.rootWriter).commitConstraints(tableId, 5L);
  }

  @Test
  void addThroughTheCasFunnelCommitsOntoTheRoot() {
    when(service.constraints.getSnapshotConstraints(tableId, 5L)).thenReturn(Optional.empty());
    when(service.constraints.createSnapshotConstraintsIfAbsent(any(), anyLong(), any()))
        .thenReturn(true);

    service
        .addTableConstraint(
            AddTableConstraintRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(5L)
                .setConstraint(ConstraintDefinition.newBuilder().setName("pk"))
                .build())
        .await()
        .indefinitely();

    verify(service.rootWriter).commitConstraints(tableId, 5L);
  }

  @Test
  void deleteCommitsTheClearedBundleOntoTheRoot() {
    // deleteSnapshotConstraints removes only the (table, snapshot) pointer; the content-addressed
    // bundle blob is retained for pinned readers and reclaimed by CasBlobGc, so the pointer delete
    // is safe. The root ref is cleared so no new pin references the retired bundle.
    when(service.constraints.deleteSnapshotConstraints(tableId, 5L)).thenReturn(true);

    service
        .deleteTableConstraints(
            DeleteTableConstraintsRequest.newBuilder()
                .setTableId(tableId)
                .setSnapshotId(5L)
                .build())
        .await()
        .indefinitely();

    verify(service.rootWriter).commitConstraints(tableId, 5L);
  }

  @Test
  void deleteOnAnAlreadyGoneBundleStillClearsTheRootRef() {
    // A retry after a prior attempt deleted the pointer but failed its root commit: the pointer is
    // already gone, so deleteSnapshotConstraints returns false. The root ref must STILL be cleared
    // before reporting not-found — a stale ref would keep the removed bundle visible to new pins
    // and anchor its blob in root-chain GC.
    when(service.constraints.deleteSnapshotConstraints(tableId, 5L)).thenReturn(false);

    org.junit.jupiter.api.Assertions.assertThrows(
        io.grpc.StatusRuntimeException.class,
        () ->
            service
                .deleteTableConstraints(
                    DeleteTableConstraintsRequest.newBuilder()
                        .setTableId(tableId)
                        .setSnapshotId(5L)
                        .build())
                .await()
                .indefinitely());

    verify(service.rootWriter).commitConstraints(tableId, 5L);
  }
}
