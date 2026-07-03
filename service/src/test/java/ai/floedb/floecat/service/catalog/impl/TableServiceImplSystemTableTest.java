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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.DeleteTableRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.catalog.rpc.TableSpec;
import ai.floedb.floecat.catalog.rpc.UpdateTableRequest;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.catalog.hint.EngineHintSchemaCleaner;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import ai.floedb.floecat.types.ManagedTableProperties;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableServiceImplSystemTableTest {

  private TableServiceImpl svc;

  private TableRepository tableRepo;
  private PrincipalProvider principal;
  private Authorizer authz;
  private EngineHintSchemaCleaner hintCleaner;
  private TopologyGraph topology;
  private UserGraph metadataGraph;

  private TestCatalogOverlay overlay;

  @BeforeEach
  void setup() {
    svc = new TableServiceImpl();

    // Mockito deps
    tableRepo = mock(TableRepository.class);
    principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    hintCleaner = mock(EngineHintSchemaCleaner.class);
    topology = mock(TopologyGraph.class);
    metadataGraph = mock(UserGraph.class);

    overlay = new TestCatalogOverlay();

    // Wire required fields (package-private access: test in same package)
    svc.tableRepo = tableRepo;
    svc.principal = principal;
    svc.authz = authz;
    svc.overlay = overlay;
    svc.hintCleaner = hintCleaner;
    svc.topology = topology;
    svc.metadataGraph = metadataGraph;

    // Minimal principal + authz behavior
    var pc = TestPrincipals.stubPrincipal(principal, authz);
    when(hintCleaner.shouldClearHints(any())).thenReturn(false);
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

  @Test
  void updateTable_catalogIdSetToSystemCatalog_isPermissionDenied() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_1")
            .build();
    ResourceId systemCatalogId = SystemNodeRegistry.systemCatalogContainerId("engine");
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_1")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl_user_1")
            .build();

    overlay.addNode(
        new CatalogNode(
            systemCatalogId,
            1L,
            Instant.now(),
            "engine",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            1L,
            Instant.now(),
            userCatalogId,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));
    overlay.addNode(userTableNode(tableId, userCatalogId, namespaceId));

    when(tableRepo.metaFor(tableId)).thenReturn(MutationMeta.getDefaultInstance());
    when(tableRepo.getById(tableId))
        .thenReturn(
            Optional.of(
                Table.newBuilder()
                    .setResourceId(tableId)
                    .setCatalogId(userCatalogId)
                    .setNamespaceId(namespaceId)
                    .setDisplayName("orders")
                    .setSchemaJson("{}")
                    .build()));

    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setCatalogId(systemCatalogId).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id").build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.updateTable(req).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verify(tableRepo, never()).update(any(), anyLong());
  }

  @Test
  void updateTable_propertiesPreservesManagedPropertiesWhenOmitted() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_props")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_props")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl_user_props")
            .build();

    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            1L,
            Instant.now(),
            userCatalogId,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));
    overlay.addNode(userTableNode(tableId, userCatalogId, namespaceId));

    Table current =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(userCatalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .setSchemaJson("{}")
            .putProperties(ManagedTableProperties.FORMAT_VERSION, "2")
            .putProperties("external", "old")
            .build();
    when(tableRepo.metaFor(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(tableRepo.getById(tableId)).thenReturn(Optional.of(current));
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(8L).build());
    when(tableRepo.update(any(Table.class), anyLong())).thenReturn(true);

    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(
                TableSpec.newBuilder()
                    .putProperties("external", "new")
                    .putProperties(ManagedTableProperties.FORMAT_VERSION, "1")
                    .build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("properties").build())
            .build();

    svc.updateTable(req).await().indefinitely();

    ArgumentCaptor<Table> tableCaptor = ArgumentCaptor.forClass(Table.class);
    verify(tableRepo).update(tableCaptor.capture(), anyLong());
    Table updated = tableCaptor.getValue();
    assertEquals("new", updated.getPropertiesMap().get("external"));
    assertEquals("2", updated.getPropertiesMap().get(ManagedTableProperties.FORMAT_VERSION));
  }

  private UserTableNode userTableNode(
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId) {
    return new UserTableNode(
        tableId,
        1L,
        Instant.now(),
        catalogId,
        namespaceId,
        "orders",
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        "{}",
        Map.of(),
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.<Long, Map<EngineHintKey, ai.floedb.floecat.metagraph.model.EngineHint>>of());
  }
}
