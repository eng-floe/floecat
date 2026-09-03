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

package ai.floedb.floecat.service.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.access.CatalogCapabilities;
import ai.floedb.floecat.catalog.access.CatalogCapability;
import ai.floedb.floecat.catalog.access.CatalogClient;
import ai.floedb.floecat.catalog.access.CatalogObjectName;
import ai.floedb.floecat.catalog.access.CatalogTable;
import ai.floedb.floecat.catalog.access.CatalogView;
import ai.floedb.floecat.catalog.access.CatalogViewDefinition;
import ai.floedb.floecat.catalog.access.ExternalObjectIdentity;
import ai.floedb.floecat.catalog.access.NamespacePath;
import ai.floedb.floecat.catalog.access.VendedStorageCredentials;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.integration.rpc.CatalogIntegration;
import ai.floedb.floecat.integration.rpc.CatalogIntegrationType;
import ai.floedb.floecat.integration.rpc.CatalogOverlay;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.repo.impl.CatalogIntegrationRepository;
import ai.floedb.floecat.service.repo.impl.CatalogOverlayRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogOverlayReconcilerTest {
  private static final String SCHEMA_JSON =
      "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":false,\"type\":\"long\"}]}";

  private CatalogOverlayReconciler reconciler;
  private CatalogIntegrationRepository integrations;
  private CatalogOverlayRepository overlays;
  private NamespaceRepository namespaces;
  private TableRepository tables;
  private ViewRepository views;
  private CatalogIntegration integration;
  private CatalogOverlay overlay;
  private FakeCatalogClient client;
  private InMemoryPointerStore pointers;

  @BeforeEach
  void setUp() {
    var overlayCatalogId = id("catalog", ResourceKind.RK_CATALOG);
    pointers = new InMemoryPointerStore();
    var blobs = new InMemoryBlobStore();
    integrations = new CatalogIntegrationRepository(pointers, blobs);
    overlays = new CatalogOverlayRepository(pointers, blobs);
    namespaces = spy(new NamespaceRepository(pointers, blobs));
    tables = new TableRepository(pointers, blobs);
    views = new ViewRepository(pointers, blobs);
    // The fences assert the catalog still exists, so the catalog is a real row here. Under the
    // mocked MarkerStore this test never needed one.
    new CatalogRepository(pointers, blobs)
        .create(
            Catalog.newBuilder().setResourceId(overlayCatalogId).setDisplayName("sales").build());

    integration =
        CatalogIntegration.newBuilder()
            .setResourceId(id("integration", ResourceKind.RK_CATALOG_INTEGRATION))
            .setDisplayName("upstream")
            .setType(CatalogIntegrationType.CIT_ICEBERG_REST)
            .setCatalogUri("https://catalog.example/v1")
            .build();
    overlay =
        CatalogOverlay.newBuilder()
            .setResourceId(id("overlay", ResourceKind.RK_CATALOG_OVERLAY))
            .setCatalogId(overlayCatalogId)
            .setIntegrationId(integration.getResourceId())
            .setDisplayName("sales")
            .addIncludeNamespaces(
                ai.floedb.floecat.integration.rpc.NamespacePath.newBuilder().addSegments("sales"))
            .build();
    integrations.create(integration);
    overlays.create(overlay);

    client = new FakeCatalogClient();
    var access = mock(CatalogIntegrationAccess.class);
    when(access.open(integration)).thenReturn(client);
    reconciler = new CatalogOverlayReconciler();
    reconciler.access = access;
    reconciler.integrations = integrations;
    reconciler.overlays = overlays;
    reconciler.namespaces = namespaces;
    reconciler.tables = tables;
    reconciler.views = views;
    reconciler.pointerStore = pointers;
    reconciler.tableRoots = mock(TableRootRepository.class);
    reconciler.rootWriter = mock(TableRootWriter.class);
    // A real MarkerStore over the same in-memory pointer store. A mock returning empty conditions
    // passes whether the reconciler folds a fence into its batch or drops it on the floor, which is
    // the failure this protocol calls silent and total -- so the fences here are real and the
    // assertions read the marker versions they move.
    reconciler.markerStore = markerStoreOver(pointers);
  }

  @Test
  void materializesSelectedInventoryAndRetiresWhatDisappears() {
    NamespacePath sales = NamespacePath.of("sales");
    NamespacePath europe = NamespacePath.of("sales", "eu");
    NamespacePath internal = NamespacePath.of("internal");
    client.children.put(NamespacePath.root(), List.of(sales, internal));
    client.children.put(sales, List.of(europe));
    client.children.put(europe, List.of());
    client.tables.put(
        new CatalogObjectName(sales, "orders"),
        new CatalogTable(
            new CatalogObjectName(sales, "orders"),
            ExternalObjectIdentity.stable("table-uuid"),
            "ICEBERG",
            SCHEMA_JSON,
            List.of(),
            Optional.of("s3://warehouse/orders/metadata.json"),
            Optional.of("s3://warehouse/orders"),
            Map.of()));
    client.views.put(
        new CatalogObjectName(europe, "summary"),
        new CatalogView(
            new CatalogObjectName(europe, "summary"),
            ExternalObjectIdentity.stable("view-uuid"),
            SCHEMA_JSON,
            List.of(new CatalogViewDefinition("select id from orders", "ansi")),
            europe,
            Map.of()));

    var first = reconcile();

    assertEquals(2, first.namespacesCreated());
    assertEquals(1, first.tablesCreated());
    assertEquals(1, first.viewsCreated());
    var salesNamespace = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    var europeNamespace =
        namespaces.getByPath("acct", "catalog", List.of("sales", "eu")).orElseThrow();
    var table =
        tables
            .getByName("acct", "catalog", salesNamespace.getResourceId().getId(), "orders")
            .orElseThrow();
    assertEquals(integration.getResourceId(), table.getUpstream().getCatalogIntegrationId());
    assertEquals(overlay.getResourceId(), table.getUpstream().getCatalogOverlayId());
    assertFalse(table.getUpstream().hasConnectorId());
    assertEquals(SCHEMA_JSON, table.getSchemaJson());
    assertTrue(
        views
            .getByName("acct", "catalog", europeNamespace.getResourceId().getId(), "summary")
            .isPresent());

    var markersBefore = markerVersions();
    var second = reconcile();
    assertEquals(new CatalogOverlayReconciler.Result(0, 0, 0, 0, 0, 0, 0, 0), second);
    // A pass that changes nothing writes nothing, markers included -- advancing one would cost a
    // concurrent writer its fence for no reason.
    assertEquals(markersBefore, markerVersions(), "a no-op pass moves no marker");

    client.children.put(sales, List.of());
    client.tables.clear();
    client.views.clear();
    var third = reconcile();
    assertEquals(1, third.namespacesDeleted());
    assertEquals(1, third.tablesDeleted());
    assertEquals(1, third.viewsDeleted());
    assertTrue(namespaces.getByPath("acct", "catalog", List.of("sales", "eu")).isEmpty());
    // Retiring a namespace takes its markers with it. Advancing them instead does not merely leave
    // a stale row: this namespace never had a children marker, so that marker samples as version
    // zero and the advance CREATES a pointer row for a namespace deleted in the same batch --
    // unreadable forever, since namespace ids never recur, and one per retired namespace per pass.
    var retiredId = europeNamespace.getResourceId().getId();
    assertTrue(
        markerVersions().keySet().stream().noneMatch(key -> key.contains(retiredId)),
        "the retired namespace leaves no marker row behind");

    reconciler.retireMaterializedResources(overlay);
    assertTrue(listLocalNamespaces().isEmpty());
  }

  @Test
  void retiresOldStableIdentityBeforeRecreatingTheSameTableName() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.tables.put(orders, catalogTable(orders, "old-table-uuid"));

    reconcile();
    var namespace = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    ResourceId oldId =
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "orders")
            .orElseThrow()
            .getResourceId();

    client.tables.put(orders, catalogTable(orders, "new-table-uuid"));
    var result = reconcile();

    ResourceId newId =
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "orders")
            .orElseThrow()
            .getResourceId();
    assertEquals(1, result.tablesDeleted());
    assertEquals(1, result.tablesCreated());
    assertNotEquals(oldId, newId);
  }

  @Test
  void retiresRenameDestinationBeforeMovingAStableTable() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    CatalogObjectName archived = new CatalogObjectName(sales, "archived_orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.tables.put(orders, catalogTable(orders, "orders-uuid"));
    client.tables.put(archived, catalogTable(archived, "archived-uuid"));

    reconcile();
    var namespace = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    ResourceId ordersId =
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "orders")
            .orElseThrow()
            .getResourceId();

    client.tables.clear();
    client.tables.put(archived, catalogTable(archived, "orders-uuid"));
    var result = reconcile();

    assertEquals(1, result.tablesDeleted());
    assertEquals(1, result.tablesUpdated());
    assertTrue(
        tables.getByName("acct", "catalog", namespace.getResourceId().getId(), "orders").isEmpty());
    assertEquals(
        ordersId,
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "archived_orders")
            .orElseThrow()
            .getResourceId());
  }

  /**
   * An update that moves a relation adds one to the destination namespace, so it has to pass that
   * namespace's relation fence -- otherwise a concurrent namespace delete reads the destination as
   * empty, commits, and the moved relation survives under a namespace id that is gone.
   *
   * <p>Asserted as participation rather than as a race: the fence is what a concurrent delete
   * contends with, so requesting it is the property, and it is deterministic.
   */
  @Test
  void anUpdateThatMovesARelationFencesOnlyTheDestinationNamespace() {
    NamespacePath sales = NamespacePath.of("sales");
    NamespacePath archive = NamespacePath.of("sales", "archive");
    CatalogObjectName inSales = new CatalogObjectName(sales, "orders");
    CatalogObjectName inArchive = new CatalogObjectName(archive, "orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of(archive));
    client.children.put(archive, List.of());
    client.tables.put(inSales, catalogTable(inSales, "orders-uuid"));

    reconcile();
    var from = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    var to = namespaces.getByPath("acct", "catalog", List.of("sales", "archive")).orElseThrow();

    // Same stable identity under a different namespace: resolved through existingByIdentity, so
    // the reconciler moves the existing row rather than creating a second one.
    client.tables.clear();
    client.tables.put(inArchive, catalogTable(inArchive, "orders-uuid"));
    long fromBefore = relationsMarkerVersion(from.getResourceId());
    long toBefore = relationsMarkerVersion(to.getResourceId());
    var result = reconcile();

    assertEquals(1, result.tablesUpdated());
    assertEquals(0, result.tablesCreated());
    // The destination marker moved, so it was in the batch that moved the table. Asking the
    // MarkerStore for the fence proves nothing -- the reconciler could ask and then not pass it to
    // the write. The source stays unchanged because removing a relation cannot orphan it there.
    assertEquals(
        fromBefore,
        relationsMarkerVersion(from.getResourceId()),
        "the namespace it leaves needs no fence");
    assertTrue(
        relationsMarkerVersion(to.getResourceId()) > toBefore,
        "the namespace it lands in is fenced against a concurrent delete");
  }

  /**
   * A materialized namespace joins its catalog's child set, not only its parent's.
   *
   * <p>Asserted on the marker version this pass actually moved, which is what a concurrent
   * DeleteCatalog loses its CAS to. A namespace at the root of a catalog has no parent to join, so
   * the catalog half is the only thing standing between it and that delete.
   */
  @Test
  void materializingANamespaceAssertsItsCatalogsChildSet() {
    NamespacePath sales = NamespacePath.of("sales");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());

    long before = catalogChildrenMarkerVersion();
    reconcile();

    assertTrue(
        catalogChildrenMarkerVersion() > before,
        "materializing a namespace joins its catalog's child set, or a concurrent catalog delete"
            + " passes its own fence and strands it");
  }

  /**
   * Retiring a namespace is the same check-then-delete that DeleteNamespace is fenced against, so
   * it carries the same guards: it must not delete a namespace that still has a child, and it must
   * assert both shape markers so a relation or child landing after the check costs it the commit.
   *
   * <p>The child here is deliberately not owned by the overlay, which is what makes the case
   * reachable: the ownership filter skips it, so nothing else would stop the parent going away
   * underneath it.
   */
  @Test
  void doesNotRetireANamespaceThatStillHasAChild() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.tables.put(orders, catalogTable(orders, "orders-uuid"));

    reconcile();
    var parent = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();

    // A child the overlay does not own, as a user would have created it.
    namespaces.create(
        Namespace.newBuilder()
            .setResourceId(
                ResourceId.newBuilder()
                    .setAccountId("acct")
                    .setKind(ResourceKind.RK_NAMESPACE)
                    .setId("ns-user-child")
                    .build())
            .setDisplayName("user_child")
            .addAllParents(List.of("sales"))
            .setCatalogId(parent.getCatalogId())
            .build());

    // Upstream drops the namespace entirely, so the reconciler wants to retire it.
    client.children.put(NamespacePath.root(), List.of());
    client.tables.clear();
    var result = reconcile();

    assertEquals(0, result.namespacesDeleted());
    assertTrue(
        namespaces.getByPath("acct", "catalog", List.of("sales")).isPresent(),
        "retiring the parent would leave the child under a path with no row");
  }

  @Test
  void retiresAViewBeforeCreatingATableWithTheSameRelationName() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName summary = new CatalogObjectName(sales, "summary");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.views.put(
        summary,
        new CatalogView(
            summary,
            ExternalObjectIdentity.stable("view-uuid"),
            SCHEMA_JSON,
            List.of(new CatalogViewDefinition("select 1", "ansi")),
            sales,
            Map.of()));

    reconcile();
    client.views.clear();
    client.tables.put(summary, catalogTable(summary, "table-uuid"));

    var result = reconcile();

    var namespace = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    assertEquals(1, result.viewsDeleted());
    assertEquals(1, result.tablesCreated());
    assertTrue(
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "summary")
            .isPresent());
  }

  @Test
  void staleOverlayGenerationCannotPublish() {
    NamespacePath sales = NamespacePath.of("sales");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    var observed = overlays.metaFor(overlay.getResourceId());
    assertTrue(
        overlays.update(
            overlay.toBuilder().setDisplayName("changed").build(), observed.getPointerVersion()));

    assertThrows(
        BaseResourceRepository.AbortRetryableException.class,
        () ->
            reconciler.reconcile(
                overlay, observed, integration, integrations.metaFor(integration.getResourceId())));
    assertTrue(listLocalNamespaces().isEmpty());
  }

  @Test
  void duplicateStableUpstreamIdentityIsRejectedBeforePublication() {
    NamespacePath sales = NamespacePath.of("sales");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    for (String name : List.of("orders", "returns")) {
      CatalogObjectName objectName = new CatalogObjectName(sales, name);
      client.tables.put(
          objectName,
          new CatalogTable(
              objectName,
              ExternalObjectIdentity.stable("duplicate-uuid"),
              "ICEBERG",
              SCHEMA_JSON,
              List.of(),
              Optional.empty(),
              Optional.empty(),
              Map.of()));
    }

    assertThrows(IllegalStateException.class, this::reconcile);
    assertTrue(listLocalNamespaces().isEmpty());
  }

  @Test
  void staleNamespaceContainingANonOverlayRelationIsLeftInPlace() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.tables.put(orders, catalogTable(orders, "table-uuid"));

    reconcile();
    var namespace = namespaces.getByPath("acct", "catalog", List.of("sales")).orElseThrow();
    var table =
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "orders")
            .orElseThrow();
    MutationMeta tableMeta = tables.metaFor(table.getResourceId());
    assertTrue(
        tables.update(table.toBuilder().clearProperties().build(), tableMeta.getPointerVersion()));

    client.children.clear();
    client.tables.clear();
    var result = reconcile();

    assertEquals(0, result.namespacesDeleted());
    assertTrue(namespaces.getByPath("acct", "catalog", List.of("sales")).isPresent());
    assertTrue(tables.getById(table.getResourceId()).isPresent());
  }

  @Test
  void normalizesSelectionAndMaterializedNamesButPreservesUpstreamIdentity() {
    String rawNamespace = "sales\u00a0\u00a0team";
    String rawChild = "private\u00a0\u00a0data";
    String rawTable = "daily\u00a0\u00a0orders";
    String rawView = "weekly  summary";
    NamespacePath sales = NamespacePath.of(rawNamespace);
    NamespacePath excluded = NamespacePath.of(rawNamespace, rawChild);
    CatalogObjectName tableName = new CatalogObjectName(sales, rawTable);
    CatalogObjectName viewName = new CatalogObjectName(sales, rawView);
    MutationMeta overlayMeta = overlays.metaFor(overlay.getResourceId());
    overlay =
        overlay.toBuilder()
            .clearIncludeNamespaces()
            .addIncludeNamespaces(
                ai.floedb.floecat.integration.rpc.NamespacePath.newBuilder()
                    .addSegments("sales team"))
            .addExcludeNamespaces(
                ai.floedb.floecat.integration.rpc.NamespacePath.newBuilder()
                    .addSegments("sales team")
                    .addSegments("private data"))
            .build();
    assertTrue(overlays.update(overlay, overlayMeta.getPointerVersion()));
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of(excluded));
    client.children.put(excluded, List.of());
    client.tables.put(tableName, catalogTable(tableName, "table-uuid"));
    client.tables.put(
        new CatalogObjectName(excluded, "hidden"),
        catalogTable(new CatalogObjectName(excluded, "hidden"), "hidden-uuid"));
    client.views.put(
        viewName,
        new CatalogView(
            viewName,
            ExternalObjectIdentity.stable("view-uuid"),
            SCHEMA_JSON,
            List.of(new CatalogViewDefinition("select 1", "ansi")),
            sales,
            Map.of()));

    reconcile();

    var namespace = namespaces.getByPath("acct", "catalog", List.of("sales team")).orElseThrow();
    var table =
        tables
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "daily orders")
            .orElseThrow();
    assertEquals(rawNamespace, table.getUpstream().getNamespacePath(0));
    assertEquals(rawTable, table.getUpstream().getTableDisplayName());
    assertTrue(
        views
            .getByName("acct", "catalog", namespace.getResourceId().getId(), "weekly summary")
            .isPresent());
    assertTrue(
        namespaces.getByPath("acct", "catalog", List.of("sales team", "private data")).isEmpty());
  }

  @Test
  void namespaceEnumerationUsesConsistentListing() {
    NamespacePath sales = NamespacePath.of("sales");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    reconcile();
    clearInvocations(namespaces);

    assertEquals(new CatalogOverlayReconciler.Result(0, 0, 0, 0, 0, 0, 0, 0), reconcile());

    verify(namespaces)
        .listConsistent(eq("acct"), eq("catalog"), eq(List.of()), eq(200), eq(""), any());
  }

  @Test
  void lostGenerationCannotLeaveAStaleTableRootPublication() {
    NamespacePath sales = NamespacePath.of("sales");
    CatalogObjectName orders = new CatalogObjectName(sales, "orders");
    client.children.put(NamespacePath.root(), List.of(sales));
    client.children.put(sales, List.of());
    client.tables.put(
        orders,
        new CatalogTable(
            orders,
            ExternalObjectIdentity.stable("table-uuid"),
            "ICEBERG",
            SCHEMA_JSON,
            List.of(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    doAnswer(
            invocation -> {
              ResourceId tableId = invocation.getArgument(0);
              tables.delete(tableId);
              MutationMeta current = overlays.metaFor(overlay.getResourceId());
              overlays.update(
                  overlay.toBuilder().setDisplayName("changed").build(),
                  current.getPointerVersion());
              return null;
            })
        .when(reconciler.rootWriter)
        .commitDefinition(any(), any());

    assertThrows(BaseResourceRepository.AbortRetryableException.class, this::reconcile);
    verify(reconciler.rootWriter).replaceDefinitionIfMatches(any(), any(), any());
    verify(reconciler.tableRoots, never()).deleteWithPrecondition(any(), anyLong());
  }

  /**
   * A real MarkerStore over the given pointer store.
   *
   * <p>Its {@code pointerStore} is package-private and this test is not in that package, so it is
   * set reflectively rather than by widening production API for a test.
   */
  private static MarkerStore markerStoreOver(InMemoryPointerStore pointers) {
    var markers = new MarkerStore();
    try {
      var field = MarkerStore.class.getDeclaredField("pointerStore");
      field.setAccessible(true);
      field.set(markers, pointers);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("could not wire MarkerStore for the test", e);
    }
    return markers;
  }

  /** The version of one marker row, zero when it has never been written. */
  private long markerVersion(String key) {
    return pointers.get(key).map(Pointer::getVersion).orElse(0L);
  }

  private long catalogChildrenMarkerVersion() {
    return markerVersion(
        Keys.catalogChildrenMarker(
            overlay.getCatalogId().getAccountId(), overlay.getCatalogId().getId()));
  }

  private long relationsMarkerVersion(ResourceId namespaceId) {
    return markerVersion(
        Keys.namespaceRelationsMarker(namespaceId.getAccountId(), namespaceId.getId()));
  }

  /** Every marker row the store holds, by key, so a pass can be asserted to move none of them. */
  private Map<String, Long> markerVersions() {
    var versions = new HashMap<String, Long>();
    for (var pointer : pointers.listPointersByPrefixConsistent("", Integer.MAX_VALUE, "", null)) {
      if (pointer.getKey().contains("markers/")) {
        versions.put(pointer.getKey(), pointer.getVersion());
      }
    }
    return versions;
  }

  private CatalogOverlayReconciler.Result reconcile() {
    return reconciler.reconcile(
        overlay,
        overlays.metaFor(overlay.getResourceId()),
        integration,
        integrations.metaFor(integration.getResourceId()));
  }

  private List<Namespace> listLocalNamespaces() {
    return namespaces.listConsistent("acct", "catalog", List.of(), 200, "", new StringBuilder());
  }

  private static CatalogTable catalogTable(CatalogObjectName name, String stableIdentity) {
    return new CatalogTable(
        name,
        ExternalObjectIdentity.stable(stableIdentity),
        "ICEBERG",
        SCHEMA_JSON,
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Map.of());
  }

  private static ResourceId id(String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId("acct").setId(value).setKind(kind).build();
  }

  private static final class FakeCatalogClient implements CatalogClient {
    private final Map<NamespacePath, List<NamespacePath>> children = new HashMap<>();
    private final Map<CatalogObjectName, CatalogTable> tables = new HashMap<>();
    private final Map<CatalogObjectName, CatalogView> views = new HashMap<>();

    @Override
    public CatalogCapabilities capabilities() {
      return CatalogCapabilities.of(
          CatalogCapability.LIST_NAMESPACES,
          CatalogCapability.LIST_TABLES,
          CatalogCapability.LOAD_TABLE,
          CatalogCapability.LIST_VIEWS,
          CatalogCapability.LOAD_VIEW);
    }

    @Override
    public void validate() {}

    @Override
    public List<NamespacePath> listNamespaces(NamespacePath parent) {
      return children.getOrDefault(parent, List.of());
    }

    @Override
    public List<CatalogObjectName> listTables(NamespacePath namespace) {
      return tables.keySet().stream().filter(name -> name.namespace().equals(namespace)).toList();
    }

    @Override
    public CatalogTable loadTable(CatalogObjectName table) {
      return tables.get(table);
    }

    @Override
    public List<CatalogObjectName> listViews(NamespacePath namespace) {
      return views.keySet().stream().filter(name -> name.namespace().equals(namespace)).toList();
    }

    @Override
    public CatalogView loadView(CatalogObjectName view) {
      return views.get(view);
    }

    @Override
    public Optional<VendedStorageCredentials> vendStorageCredentials(CatalogObjectName table) {
      return Optional.empty();
    }

    @Override
    public void validateStorageAccess(
        CatalogObjectName table, VendedStorageCredentials credentials) {}

    @Override
    public void close() {}
  }
}
