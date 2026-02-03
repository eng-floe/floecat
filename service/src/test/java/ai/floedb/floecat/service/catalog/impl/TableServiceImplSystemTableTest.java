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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.GetTableRequest;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableServiceImplSystemTableTest {

  private TableServiceImpl svc;

  private TableRepository tableRepo;
  private PrincipalProvider principal;
  private Authorizer authz;

  private TestCatalogOverlay overlay;

  @BeforeEach
  void setup() {
    svc = new TableServiceImpl();

    // Mockito deps
    tableRepo = mock(TableRepository.class);
    principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);

    overlay = new TestCatalogOverlay();

    // Wire required fields (package-private access: test in same package)
    svc.tableRepo = tableRepo;
    svc.principal = principal;
    svc.authz = authz;
    svc.overlay = overlay;

    // Minimal principal + authz behavior
    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(authz).require(any(), anyString());
  }

  @Test
  void getTable_systemTable_usesOverlay_notRepo() {
    ResourceId sysTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_tbl_1")
            .build();

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_1")
            .build();

    SystemTableNode node =
        new SystemTableNode.GenericSystemTableNode(
            sysTableId,
            1L,
            Instant.now(),
            "engine-v",
            "system_table",
            nsId,
            List.of(SchemaColumn.newBuilder().setName("c1").build()),
            null,
            null,
            TableBackendKind.TABLE_BACKEND_KIND_ENGINE);

    overlay.addNode(node);

    var resp =
        svc.getTable(GetTableRequest.newBuilder().setTableId(sysTableId).build())
            .await()
            .indefinitely();

    assertEquals(sysTableId, resp.getTable().getResourceId());
    assertEquals("system_table", resp.getTable().getDisplayName());
    assertEquals(nsId, resp.getTable().getNamespaceId());

    verifyNoInteractions(tableRepo);
  }

  @Test
  void deleteTable_systemTable_isPermissionDenied() {
    ResourceId sysTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_tbl_2")
            .build();

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_2")
            .build();

    SystemTableNode node =
        new SystemTableNode.EngineSystemTableNode(
            sysTableId, 1L, Instant.now(), "engine-v", "engine_sys", nsId, List.of(), null, null);

    overlay.addNode(node);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(sysTableId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());

    verifyNoInteractions(tableRepo);
  }

  @Test
  void deleteTable_systemTable_withPrecondition_isPermissionDenied() {
    ResourceId sysTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_tbl_pc_1")
            .build();

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_pc_1")
            .build();

    // Any SystemTableNode will do; origin() is SYSTEM.
    SystemTableNode node =
        new SystemTableNode.EngineSystemTableNode(
            sysTableId,
            1L,
            Instant.now(),
            "engine-v",
            "engine_sys_pc",
            nsId,
            List.of(),
            null,
            null);

    overlay.addNode(node);

    var req =
        DeleteTableRequest.newBuilder()
            .setTableId(sysTableId)
            .setPrecondition(
                ai.floedb.floecat.common.rpc.Precondition.newBuilder()
                    .setExpectedVersion(1L)
                    .setExpectedEtag("etag-1")
                    .build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.deleteTable(req).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());

    // System immutability must be enforced before any repo calls.
    verifyNoInteractions(tableRepo);
  }

  @Test
  void updateTable_systemTable_isPermissionDenied() {
    ResourceId sysTableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("sys_tbl_3")
            .build();

    ResourceId nsId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_ns_3")
            .build();

    SystemTableNode node =
        new SystemTableNode.EngineSystemTableNode(
            sysTableId, 1L, Instant.now(), "engine-v", "engine_sys", nsId, List.of(), null, null);

    overlay.addNode(node);

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.updateTable(UpdateTableRequest.newBuilder().setTableId(sysTableId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());

    verifyNoInteractions(tableRepo);
  }
}
