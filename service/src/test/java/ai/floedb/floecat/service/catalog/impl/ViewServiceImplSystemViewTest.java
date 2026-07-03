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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.CreateViewRequest;
import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.catalog.rpc.ViewSpec;
import ai.floedb.floecat.catalog.rpc.ViewSqlDefinition;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
import ai.floedb.floecat.service.testsupport.TestPrincipals;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import com.google.protobuf.FieldMask;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ViewServiceImplSystemViewTest {

  private ViewServiceImpl svc;

  private ViewRepository viewRepo;
  private PrincipalProvider principal;
  private Authorizer authz;

  private TestCatalogOverlay overlay;

  @BeforeEach
  void setup() {
    svc = new ViewServiceImpl();

    viewRepo = mock(ViewRepository.class);
    principal = mock(PrincipalProvider.class);
    authz = mock(Authorizer.class);
    overlay = new TestCatalogOverlay();

    svc.viewRepo = viewRepo;
    svc.principal = principal;
    svc.authz = authz;
    svc.overlay = overlay;
    svc.metadataGraph = mock(UserGraph.class);

    var pc = TestPrincipals.stubPrincipal(principal, authz);
  }

  @Test
  void updateView_systemView_permissionDenied() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_VIEW)
            .setId("sys_view_update")
            .build();

    overlay.addNode(systemViewNode(viewId));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.updateView(
                        UpdateViewRequest.newBuilder()
                            .setViewId(viewId)
                            .setUpdateMask(FieldMask.newBuilder().addPaths("sql").build())
                            .build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(viewRepo);
  }

  @Test
  void deleteView_systemView_permissionDenied() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_VIEW)
            .setId("sys_view_delete")
            .build();

    overlay.addNode(systemViewNode(viewId));

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class,
            () ->
                svc.deleteView(DeleteViewRequest.newBuilder().setViewId(viewId).build())
                    .await()
                    .indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verifyNoInteractions(viewRepo);
  }

  @Test
  void updateView_catalogIdSetToSystemCatalog_permissionDenied() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_v")
            .build();
    ResourceId systemCatalogId = SystemNodeRegistry.systemCatalogContainerId("engine");
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_v")
            .build();
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_VIEW)
            .setId("view_user_v")
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
    overlay.addNode(userViewNode(viewId, userCatalogId, namespaceId));

    when(viewRepo.metaFor(viewId)).thenReturn(MutationMeta.getDefaultInstance());
    when(viewRepo.getById(viewId))
        .thenReturn(
            Optional.of(
                View.newBuilder()
                    .setResourceId(viewId)
                    .setCatalogId(userCatalogId)
                    .setNamespaceId(namespaceId)
                    .setDisplayName("v_orders")
                    .addSqlDefinitions(
                        ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                            .setSql("select 1")
                            .build())
                    .build()));

    var req =
        UpdateViewRequest.newBuilder()
            .setViewId(viewId)
            .setSpec(ViewSpec.newBuilder().setCatalogId(systemCatalogId).build())
            .setUpdateMask(FieldMask.newBuilder().addPaths("catalog_id").build())
            .build();

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.updateView(req).await().indefinitely());

    assertEquals(Status.Code.PERMISSION_DENIED, ex.getStatus().getCode());
    verify(viewRepo, never()).update(any(), anyLong());
  }

  @Test
  void createView_nameOwnedByOtherRelationKind_reportsRelationNameClaimed() {
    var ctx = writableViewContext();

    // No same-kind view exists (pre-check and re-check both empty), yet create() trips the shared
    // relation-name claim: the name is held by another relation kind (a table).
    when(viewRepo.getByName(any(), any(), any(), any())).thenReturn(Optional.empty());
    doThrow(new BaseResourceRepository.NameConflictException("claimed by a table"))
        .when(viewRepo)
        .create(any());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.createView(ctx).await().indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, ex.getStatus().getCode());
    // Kind-agnostic message, not the misleading "view already exists".
    org.junit.jupiter.api.Assertions.assertTrue(
        ex.getStatus().getDescription().contains("claimed by another relation"),
        () -> "unexpected message: " + ex.getStatus().getDescription());
  }

  @Test
  void createView_concurrentSameKindView_reportsViewAlreadyExists() {
    var ctx = writableViewContext();

    // Pre-check sees no view, but a concurrent view creation wins the claim before this create;
    // the re-check then finds the same-kind view, so it must surface VIEW_ALREADY_EXISTS.
    when(viewRepo.getByName(any(), any(), any(), any()))
        .thenReturn(Optional.empty())
        .thenReturn(Optional.of(View.newBuilder().setDisplayName("orders").build()));
    doThrow(new BaseResourceRepository.NameConflictException("concurrent view"))
        .when(viewRepo)
        .create(any());

    StatusRuntimeException ex =
        assertThrows(
            StatusRuntimeException.class, () -> svc.createView(ctx).await().indefinitely());

    assertEquals(Status.Code.ALREADY_EXISTS, ex.getStatus().getCode());
    org.junit.jupiter.api.Assertions.assertTrue(
        ex.getStatus().getDescription().contains("already exists"),
        () -> "unexpected message: " + ex.getStatus().getDescription());
  }

  /**
   * Builds a CreateViewRequest into a writable user catalog/namespace registered in the overlay.
   */
  private CreateViewRequest writableViewContext() {
    ResourceId userCatalogId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("cat_user_cv")
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("ns_user_cv")
            .build();

    overlay.addNode(
        new CatalogNode(
            userCatalogId,
            1L,
            Instant.now(),
            "user_catalog",
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

    return CreateViewRequest.newBuilder()
        .setSpec(
            ViewSpec.newBuilder()
                .setCatalogId(userCatalogId)
                .setNamespaceId(namespaceId)
                .setDisplayName("orders")
                .addOutputColumns(SchemaColumn.newBuilder().setName("c").build())
                .addSqlDefinitions(ViewSqlDefinition.newBuilder().setSql("select 1").build()))
        .build();
  }

  private ViewNode systemViewNode(ResourceId id) {
    var catalogId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId("sys_catalog")
            .build();

    var namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId("sys_namespace")
            .build();

    return new ViewNode(
        id,
        1L,
        Instant.now(),
        catalogId,
        namespaceId,
        "sys_view_" + id.getId(),
        "select 1",
        "sql",
        List.<SchemaColumn>of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.<EngineHintKey, EngineHint>of());
  }

  private ViewNode userViewNode(ResourceId id, ResourceId catalogId, ResourceId namespaceId) {
    return new ViewNode(
        id,
        1L,
        Instant.now(),
        catalogId,
        namespaceId,
        "user_view_" + id.getId(),
        "select 1",
        "sql",
        List.<SchemaColumn>of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.USER,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.<EngineHintKey, EngineHint>of());
  }
}
