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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.DeleteViewRequest;
import ai.floedb.floecat.catalog.rpc.GetViewRequest;
import ai.floedb.floecat.catalog.rpc.UpdateViewRequest;
import ai.floedb.floecat.common.rpc.PrincipalContext;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.security.impl.Authorizer;
import ai.floedb.floecat.service.security.impl.PrincipalProvider;
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

    var pc = mock(PrincipalContext.class);
    when(principal.get()).thenReturn(pc);
    when(pc.getCorrelationId()).thenReturn("corr");
    when(pc.getAccountId()).thenReturn("acct");
    doNothing().when(authz).require(any(), anyString());
  }

  @Test
  void getView_systemView_usesOverlay_notRepo() {
    ResourceId viewId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_VIEW)
            .setId("sys_view_get")
            .build();

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

    var node =
        new ViewNode(
            viewId,
            1L,
            Instant.now(),
            catalogId,
            namespaceId,
            "sys_view_get",
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

    overlay.addNode(node);

    var resp =
        svc.getView(GetViewRequest.newBuilder().setViewId(viewId).build()).await().indefinitely();

    assertEquals(viewId, resp.getView().getResourceId());
    assertEquals("sys_view_get", resp.getView().getDisplayName());
    verifyNoInteractions(viewRepo);
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
}
