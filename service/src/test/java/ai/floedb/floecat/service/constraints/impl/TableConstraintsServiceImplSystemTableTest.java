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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.AddTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.AppendTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ConstraintDefinition;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintRequest;
import ai.floedb.floecat.catalog.rpc.DeleteTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.GetTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.ListTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.MergeTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.PutTableConstraintsRequest;
import ai.floedb.floecat.catalog.rpc.SnapshotConstraints;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.TableNode;
import ai.floedb.floecat.service.repo.IdempotencyRepository;
import ai.floedb.floecat.service.repo.impl.ConstraintRepository;
import ai.floedb.floecat.service.repo.impl.SnapshotRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableConstraintsServiceImplSystemTableTest {

  private TableConstraintsServiceImpl service;
  private SnapshotRepository snapshots;
  private ConstraintRepository constraints;
  private PrincipalProvider principal;
  private Authorizer authz;
  private IdempotencyRepository idempotencyStore;
  private TestCatalogOverlay overlay;

  @BeforeEach
  void setup() {
    service = new TableConstraintsServiceImpl();

    snapshots = mock(SnapshotRepository.class);
    constraints = mock(ConstraintRepository.class);
    principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    idempotencyStore = mock(IdempotencyRepository.class);
    overlay = new TestCatalogOverlay();

    service.snapshots = snapshots;
    service.constraints = constraints;
    service.principal = principal;
    service.authz = authz;
    service.idempotencyStore = idempotencyStore;
    service.overlay = overlay;

    PrincipalContext pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(authz).require(any(), anyString());
  }

  @Test
  void putTableConstraints_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_put");
    overlay.addNode(systemTableNode(tableId, "system_constraints_put"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .putTableConstraints(
                        PutTableConstraintsRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(11L)
                            .setConstraints(SnapshotConstraints.getDefaultInstance())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(snapshots);
    verifyNoInteractions(constraints);
    verifyNoInteractions(idempotencyStore);
  }

  @Test
  void deleteTableConstraints_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_delete");
    overlay.addNode(systemTableNode(tableId, "system_constraints_delete"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteTableConstraints(
                        DeleteTableConstraintsRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(12L)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(constraints);
  }

  @Test
  void addTableConstraint_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_add_one");
    overlay.addNode(systemTableNode(tableId, "system_constraints_add_one"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .addTableConstraint(
                        AddTableConstraintRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(14L)
                            .setConstraint(
                                ConstraintDefinition.newBuilder().setName("pk_add_one").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(snapshots);
    verifyNoInteractions(constraints);
  }

  @Test
  void mergeTableConstraints_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_merge");
    overlay.addNode(systemTableNode(tableId, "system_constraints_merge"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .mergeTableConstraints(
                        MergeTableConstraintsRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(16L)
                            .setConstraints(SnapshotConstraints.getDefaultInstance())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(snapshots);
    verifyNoInteractions(constraints);
  }

  @Test
  void appendTableConstraints_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_append");
    overlay.addNode(systemTableNode(tableId, "system_constraints_append"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .appendTableConstraints(
                        AppendTableConstraintsRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(17L)
                            .setConstraints(SnapshotConstraints.getDefaultInstance())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(snapshots);
    verifyNoInteractions(constraints);
  }

  @Test
  void deleteTableConstraint_systemTable_isPermissionDenied() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_delete_one");
    overlay.addNode(systemTableNode(tableId, "system_constraints_delete_one"));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .deleteTableConstraint(
                        DeleteTableConstraintRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(15L)
                            .setConstraintName("pk_delete_one")
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(constraints);
  }

  @Test
  void getTableConstraints_systemTable_resolvesViaOverlay() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_get");
    overlay.addNode(systemTableNode(tableId, "system_constraints_get"));

    when(constraints.getSnapshotConstraints(tableId, 13L)).thenReturn(Optional.empty());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                service
                    .getTableConstraints(
                        GetTableConstraintsRequest.newBuilder()
                            .setTableId(tableId)
                            .setSnapshotId(13L)
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, ex.getStatus().getCode());
    verify(constraints).getSnapshotConstraints(tableId, 13L);
  }

  @Test
  void listTableConstraints_systemTable_resolvesViaOverlay() {
    ResourceId tableId = systemTableId("sys_tbl_constraints_list");
    overlay.addNode(systemTableNode(tableId, "system_constraints_list"));

    when(constraints.listSnapshotConstraints(
            any(), anyInt(), anyString(), any(StringBuilder.class)))
        .thenReturn(List.of());
    when(constraints.countSnapshotConstraints(tableId)).thenReturn(0);

    var response =
        service
            .listTableConstraints(
                ListTableConstraintsRequest.newBuilder().setTableId(tableId).build())
            .await()
            .indefinitely();

    assertEquals(0, response.getConstraintsCount());
    assertEquals(0, response.getPage().getTotalSize());
  }

  private static TableNode systemTableNode(ResourceId tableId, String displayName) {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(tableId.getAccountId())
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_constraints")
            .build();
    return new SystemTableNode.EngineSystemTableNode(
        tableId, 1L, Instant.now(), "engine-v", displayName, namespaceId, List.of(), null, null);
  }

  private static ResourceId systemTableId(String id) {
    return ResourceId.newBuilder()
        .setAccountId("acct")
        .setKind(ResourceKind.RK_TABLE)
        .setId(id)
        .build();
  }
}
