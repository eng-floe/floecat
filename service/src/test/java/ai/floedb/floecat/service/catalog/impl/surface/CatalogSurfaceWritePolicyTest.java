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
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.scanner.spi.CatalogOverlay;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.TestCatalogOverlay;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceWritePolicyTest {

  private static final String ACCOUNT_ID = "acct";
  private static final String CORRELATION_ID = "corr";

  private final ResourceId catalogId = id(ResourceKind.RK_CATALOG, "cat");
  private final ResourceId namespaceId = id(ResourceKind.RK_NAMESPACE, "ns");
  private final ResourceId tableId = id(ResourceKind.RK_TABLE, "tbl");
  private final ResourceId viewId = id(ResourceKind.RK_VIEW, "view");

  private TestCatalogOverlay overlay;
  private CatalogSurfaceWritePolicy writePolicy;

  @BeforeEach
  void setup() {
    overlay = new TestCatalogOverlay();
    writePolicy = new CatalogSurfaceWritePolicy(overlay);
  }

  @Test
  void requireWritableUserResourcesReturnsResolvedNodes() {
    var catalog = catalogNode(catalogId, "examples");
    var namespace = namespaceNode(namespaceId, "public", GraphNodeOrigin.USER);
    var table = userTableNode(tableId);
    var view = viewNode(viewId, GraphNodeOrigin.USER);
    overlay.addNode(catalog);
    overlay.addNode(namespace);
    overlay.addRelation(namespaceId, table);
    overlay.addRelation(namespaceId, view);

    assertEquals(catalog, writePolicy.requireWritableCatalog(catalogId, CORRELATION_ID));
    assertEquals(namespace, writePolicy.requireWritableNamespace(namespaceId, CORRELATION_ID));
    assertEquals(table, writePolicy.requireWritableTable(tableId, CORRELATION_ID));
    assertEquals(view, writePolicy.requireWritableView(viewId, CORRELATION_ID));
  }

  @Test
  void requireWritableMissingObjectsReturnsNotFound() {
    assertStatus(
        Status.Code.NOT_FOUND, () -> writePolicy.requireWritableCatalog(catalogId, CORRELATION_ID));
    assertStatus(
        Status.Code.NOT_FOUND,
        () -> writePolicy.requireWritableNamespace(namespaceId, CORRELATION_ID));
    assertStatus(
        Status.Code.NOT_FOUND, () -> writePolicy.requireWritableTable(tableId, CORRELATION_ID));
    assertStatus(
        Status.Code.NOT_FOUND, () -> writePolicy.requireWritableView(viewId, CORRELATION_ID));
  }

  @Test
  void requireWritableNamespaceRejectsSystemOriginNode() {
    overlay.addNode(namespaceNode(namespaceId, "information_schema", GraphNodeOrigin.SYSTEM));

    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> writePolicy.requireWritableNamespace(namespaceId, CORRELATION_ID));
  }

  @Test
  void requireWritableSystemIdsRejectBeforeOverlayLookup() {
    CatalogOverlay mockedOverlay = mock(CatalogOverlay.class);
    var policy = new CatalogSurfaceWritePolicy(mockedOverlay);
    var systemCatalogId = SystemNodeRegistry.systemCatalogContainerId("engine");
    var systemNamespaceId =
        SystemNodeRegistry.resourceId("engine", ResourceKind.RK_NAMESPACE, "information_schema");

    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> policy.requireWritableCatalog(systemCatalogId, CORRELATION_ID));
    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> policy.requireWritableNamespace(systemNamespaceId, CORRELATION_ID));
    verifyNoInteractions(mockedOverlay);
  }

  @Test
  void requireWritableRelationsRejectSystemOriginNodes() {
    var systemTable = systemTableNode(tableId);
    var systemView = viewNode(viewId, GraphNodeOrigin.SYSTEM);
    overlay.addRelation(namespaceId, systemTable);
    overlay.addRelation(namespaceId, systemView);

    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> writePolicy.requireWritableTable(tableId, CORRELATION_ID));
    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> writePolicy.requireWritableView(viewId, CORRELATION_ID));
  }

  @Test
  void requireNamespacePathWriteEligibleRejectsSystemNamespacePaths() {
    overlay.addNode(namespaceNode(namespaceId, "information_schema", GraphNodeOrigin.SYSTEM));

    assertStatus(
        Status.Code.ALREADY_EXISTS,
        () ->
            writePolicy.requireNamespacePathWriteEligible(
                catalogId, List.of("information_schema"), CORRELATION_ID));
    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () ->
            writePolicy.requireNamespacePathWriteEligible(
                catalogId, List.of("information_schema", "tables"), CORRELATION_ID));
  }

  @Test
  void requireWritableDeleteChecksIgnoreMissingObjectsWhenCallerDoesNotCare() {
    assertDoesNotThrow(
        () -> writePolicy.requireWritableTableForDelete(tableId, CORRELATION_ID, false));
    assertDoesNotThrow(
        () -> writePolicy.requireWritableViewForDelete(viewId, CORRELATION_ID, false));
  }

  @Test
  void requireWritableDeleteChecksIgnoreOverlayResolutionFailuresWhenCallerDoesNotCare() {
    CatalogOverlay throwingOverlay = mock(CatalogOverlay.class);
    var throwingPolicy = new CatalogSurfaceWritePolicy(throwingOverlay);
    when(throwingOverlay.resolve(tableId))
        .thenThrow(new IllegalStateException("overlay unavailable"));
    when(throwingOverlay.resolve(viewId))
        .thenThrow(new IllegalStateException("overlay unavailable"));

    assertDoesNotThrow(
        () -> throwingPolicy.requireWritableTableForDelete(tableId, CORRELATION_ID, false));
    assertDoesNotThrow(
        () -> throwingPolicy.requireWritableViewForDelete(viewId, CORRELATION_ID, false));
  }

  @Test
  void requireWritableDeleteChecksStillRejectSystemObjectsWhenResolved() {
    var systemTable = systemTableNode(tableId);
    var systemView = viewNode(viewId, GraphNodeOrigin.SYSTEM);
    overlay.addRelation(namespaceId, systemTable);
    overlay.addRelation(namespaceId, systemView);

    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> writePolicy.requireWritableTableForDelete(tableId, CORRELATION_ID, false));
    assertStatus(
        Status.Code.PERMISSION_DENIED,
        () -> writePolicy.requireWritableViewForDelete(viewId, CORRELATION_ID, false));
  }

  private static void assertStatus(Status.Code code, ThrowingRunnable action) {
    StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, action::run);
    assertEquals(code, ex.getStatus().getCode());
  }

  private CatalogNode catalogNode(ResourceId id, String displayName) {
    return new CatalogNode(
        id,
        "blob://test/v1",
        displayName,
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private NamespaceNode namespaceNode(ResourceId id, String displayName, GraphNodeOrigin origin) {
    return new NamespaceNode(
        id, "blob://test/v1", catalogId, List.of(), displayName, origin, Map.of(), Map.of());
  }

  private UserTableNode userTableNode(ResourceId id) {
    return new UserTableNode(
        id,
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
        Map.<Long, Map<EngineHintKey, EngineHint>>of());
  }

  private SystemTableNode systemTableNode(ResourceId id) {
    return new SystemTableNode.GenericSystemTableNode(
        id,
        1L,
        "engine",
        "system_table",
        namespaceId,
        List.of(),
        null,
        null,
        TableBackendKind.TABLE_BACKEND_KIND_ENGINE);
  }

  private ViewNode viewNode(ResourceId id, GraphNodeOrigin origin) {
    return viewNode(id, origin, "orders_view");
  }

  private ViewNode viewNode(ResourceId id, GraphNodeOrigin origin, String displayName) {
    return new ViewNode(
        id,
        "blob://test/v1",
        catalogId,
        namespaceId,
        displayName,
        "select 1",
        "sql",
        List.<SchemaColumn>of(),
        List.of(),
        List.of(),
        origin,
        Map.of(),
        Optional.empty(),
        Map.<Long, Map<EngineHintKey, EngineHint>>of(),
        Map.<EngineHintKey, EngineHint>of());
  }

  private static ResourceId id(ResourceKind kind, String id) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT_ID).setKind(kind).setId(id).build();
  }

  private interface ThrowingRunnable {
    void run();
  }
}
