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
import static org.mockito.Mockito.times;
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
import ai.floedb.floecat.common.rpc.Precondition;
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
import ai.floedb.floecat.service.repo.impl.TableCleanupRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class TableServiceImplSystemTableTest {

  private TableServiceImpl svc;

  private TableRepository tableRepo;
  private TableCleanupRepository tableCleanupRepo;
  private RecursiveResourceDropper recursiveDropper;
  private MarkerStore markerStore;
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
    tableCleanupRepo = mock(TableCleanupRepository.class);
    recursiveDropper = mock(RecursiveResourceDropper.class);
    markerStore = mock(MarkerStore.class);
    principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    hintCleaner = mock(EngineHintSchemaCleaner.class);
    topology = mock(TopologyGraph.class);
    metadataGraph = mock(UserGraph.class);

    overlay = new TestCatalogOverlay();

    // Wire required fields (package-private access: test in same package)
    svc.tableRepo = tableRepo;
    svc.tableCleanupRepo = tableCleanupRepo;
    svc.recursiveDropper = recursiveDropper;
    svc.markerStore = markerStore;
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
  void deleteTableStagesCleanupAtomicallyWithRemovingThePointer() {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table")
            .build();
    overlay.addNode(userTableNode(tableId, catalogId, namespaceId));
    var table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId.toBuilder().clearAccountId())
            .setDisplayName("orders")
            .build();
    var cleanup =
        new TableCleanupRepository.Cleanup(
            namespaceId, tableId, Keys.namespaceTableCleanupPointer("acct", "ns", "table"), 1L);
    when(tableRepo.metaFor(tableId))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(7L).setBlobUri("blob://table").build());
    when(tableRepo.getByBlobUri("blob://table")).thenReturn(Optional.of(table));
    var namespacePin = mock(BatchGuard.class);
    var deleteGuard = mock(BatchGuard.class);
    var cleanupPlan = new TableCleanupRepository.DeletePlan(cleanup, deleteGuard);
    when(markerStore.namespacePinnedGuardIfPresent(namespaceId))
        .thenReturn(Optional.of(namespacePin));
    when(tableCleanupRepo.planDelete(namespaceId, tableId, namespacePin)).thenReturn(cleanupPlan);
    when(tableRepo.deleteWithPrecondition(tableId, 7L, deleteGuard)).thenReturn(true);
    when(tableCleanupRepo.pending(cleanup)).thenReturn(Optional.of(cleanup));
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(0L).build());

    svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build())
        .await()
        .indefinitely();

    var order = org.mockito.Mockito.inOrder(tableCleanupRepo, tableRepo, recursiveDropper);
    order.verify(tableCleanupRepo).planDelete(namespaceId, tableId, namespacePin);
    order.verify(tableRepo).deleteWithPrecondition(tableId, 7L, deleteGuard);
    order.verify(recursiveDropper).cleanupDeletedTable(cleanup);
  }

  @Test
  void deleteTableUsesTableScopedCleanupWhenNamespaceIsAlreadyAbsent() {
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("gone-ns")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table")
            .build();
    var table =
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_CATALOG)
                    .setId("cat"))
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .build();
    when(tableRepo.metaFor(tableId))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(7L).setBlobUri("blob://table").build());
    when(tableRepo.getByBlobUri("blob://table")).thenReturn(Optional.of(table));
    when(markerStore.namespacePinnedGuardIfPresent(namespaceId)).thenReturn(Optional.empty());
    when(tableRepo.deleteWithPrecondition(tableId, 7L)).thenReturn(true);
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(0L).build());

    svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build())
        .await()
        .indefinitely();

    verify(tableCleanupRepo, never()).planDelete(any(), any(), any());
    verify(tableRepo).deleteWithPrecondition(tableId, 7L);
    verify(recursiveDropper).cleanupDeletedTable(tableId);
  }

  @Test
  void conditionalDeleteReturnsNotFoundWhenAnotherDeleteWinsTheCas() {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("table-race")
            .build();
    overlay.addNode(userTableNode(tableId, catalogId, namespaceId));
    when(tableRepo.metaFor(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(tableRepo.deleteWithPrecondition(tableId, 7L)).thenReturn(false);
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(0L).build());

    var failure =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteTable(
                        DeleteTableRequest.newBuilder()
                            .setTableId(tableId)
                            .setPrecondition(
                                Precondition.newBuilder().setExpectedVersion(7L).build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.NOT_FOUND, failure.getStatus().getCode());
    verify(recursiveDropper, never()).cleanupDeletedTable(tableId);
  }

  @Test
  void deleteTableWithUnreadableBlobStillDeletesAndPurgesByTableId() {
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("corrupt")
            .build();
    overlay.addNode(userTableNode(tableId, catalogId, namespaceId));
    when(tableRepo.metaFor(tableId))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(7L).setBlobUri("blob://corrupt").build());
    when(tableRepo.getByBlobUri("blob://corrupt"))
        .thenThrow(new BaseResourceRepository.CorruptionException("parse failed"));
    when(tableRepo.deleteWithPrecondition(tableId, 7L)).thenReturn(true);
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(0L).build());

    svc.deleteTable(DeleteTableRequest.newBuilder().setTableId(tableId).build())
        .await()
        .indefinitely();

    verify(tableCleanupRepo, never()).prepare(any(), any());
    verify(tableRepo).deleteWithPrecondition(tableId, 7L);
    verify(recursiveDropper).cleanupDeletedTable(tableId);
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
            sysTableId, 1L, "engine-v", "engine_sys", nsId, List.of(), null, null);

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
            sysTableId, 1L, "engine-v", "engine_sys_pc", nsId, List.of(), null, null);

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
            sysTableId, 1L, "engine-v", "engine_sys", nsId, List.of(), null, null);

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
            "blob://test/v1",
            "engine",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            "blob://test/v1",
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
    verify(tableRepo, never()).update(any(), anyLong(), any());
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
            "blob://test/v1",
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
    when(tableRepo.update(any(Table.class), anyLong(), any())).thenReturn(true);

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
    verify(tableRepo).update(tableCaptor.capture(), anyLong(), any());
    Table updated = tableCaptor.getValue();
    assertEquals("new", updated.getPropertiesMap().get("external"));
    assertEquals("2", updated.getPropertiesMap().get(ManagedTableProperties.FORMAT_VERSION));
  }

  @Test
  void updateTable_withoutPrecondition_retriesConcurrentMutation() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_retry")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_retry")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl_user_retry")
            .build();

    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            "blob://test/v1",
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
            .build();
    when(tableRepo.metaFor(tableId))
        .thenReturn(
            MutationMeta.newBuilder().setPointerVersion(7L).build(),
            MutationMeta.newBuilder().setPointerVersion(8L).build());
    when(tableRepo.getById(tableId)).thenReturn(Optional.of(current));
    // The guarded overload: an update publishes into its namespace and so carries that namespace's
    // fence, whether or not this particular update reparents (see MarkerStore#namespaceChildGuard).
    when(tableRepo.update(any(Table.class), anyLong(), any())).thenReturn(false, true);
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(9L).build());

    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setDisplayName("renamed").build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
            .build();

    svc.updateTable(req).await().indefinitely();

    ArgumentCaptor<Long> versionCaptor = ArgumentCaptor.forClass(Long.class);
    verify(tableRepo, times(2)).update(any(Table.class), versionCaptor.capture(), any());
    assertEquals(List.of(7L, 8L), versionCaptor.getAllValues());
  }

  @Test
  void updateTable_withPrecondition_doesNotRetryConcurrentMutation() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_precondition")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_precondition")
            .build();
    ResourceId tableId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_TABLE)
            .setId("tbl_user_precondition")
            .build();

    overlay.addNode(
        new NamespaceNode(
            namespaceId,
            "blob://test/v1",
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
            .build();
    when(tableRepo.metaFor(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(7L).build());
    when(tableRepo.getById(tableId)).thenReturn(Optional.of(current));
    when(tableRepo.update(any(Table.class), anyLong(), any())).thenReturn(false);
    when(tableRepo.metaForSafe(tableId))
        .thenReturn(MutationMeta.newBuilder().setPointerVersion(8L).build());

    var req =
        UpdateTableRequest.newBuilder()
            .setTableId(tableId)
            .setSpec(TableSpec.newBuilder().setDisplayName("renamed").build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("display_name").build())
            .setPrecondition(Precondition.newBuilder().setExpectedVersion(7L).build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.updateTable(req).await().indefinitely());

    assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
    verify(tableRepo).update(any(Table.class), anyLong(), any());
  }

  private UserTableNode userTableNode(
      ResourceId tableId, ResourceId catalogId, ResourceId namespaceId) {
    return new UserTableNode(
        tableId,
        "blob://test/v1",
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
