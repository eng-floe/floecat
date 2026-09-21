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
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.floedb.floecat.service.catalog.impl.surface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.GetRelationRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsRequest;
import ai.floedb.floecat.catalog.rpc.ListRelationsResponse;
import ai.floedb.floecat.catalog.rpc.Queryability;
import ai.floedb.floecat.catalog.rpc.RelationReference;
import ai.floedb.floecat.catalog.rpc.ResolveRelationsRequest;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ErrorCode;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.PageRequest;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.CatalogNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.metagraph.model.ViewNode;
import ai.floedb.floecat.query.rpc.Origin;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.scanner.spi.CatalogGraphView;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.scanner.utils.EnvironmentContext;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import ai.floedb.floecat.systemcatalog.graph.SystemResourceIdGenerator;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import ai.floedb.floecat.systemcatalog.util.TestCatalogGraphView;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogSurfaceRelationsTest {

  private static final String ACCOUNT_ID = "acct";
  private static final String CORRELATION_ID = "corr";
  private static final int MAX_NAMES = 1000;

  private final ResourceId catalogId = id(ResourceKind.RK_CATALOG, "cat");
  private final ResourceId namespaceId = id(ResourceKind.RK_NAMESPACE, "ns");
  private final CountingGraphView graphView = new CountingGraphView();
  private final FakeTableRepository tableRepo = new FakeTableRepository(graphView);
  private final FakeViewRepository viewRepo = new FakeViewRepository(graphView);
  private final Map<ResourceId, Long> currentSnapshots = new LinkedHashMap<>();

  @BeforeEach
  void setup() {
    graphView.addNode(
        new CatalogNode(
            catalogId,
            "blob://test/v1",
            "cat",
            Map.of(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Map.of()));
    graphView.addNode(
        new NamespaceNode(
            namespaceId,
            "blob://test/v1",
            catalogId,
            List.of(),
            "public",
            GraphNodeOrigin.USER,
            Map.of(),
            Map.of()));
  }

  @Test
  void rejectsKindsOutsideTableAndView() {
    assertThrows(
        StatusRuntimeException.class,
        () -> RelationScope.requestedKinds(List.of(ResourceKind.RK_FUNCTION), CORRELATION_ID));
  }

  @Test
  void emptyKindsSelectsTablesThenViews() {
    assertEquals(
        List.of(ResourceKind.RK_TABLE, ResourceKind.RK_VIEW),
        RelationScope.requestedKinds(List.of(), CORRELATION_ID));
  }

  @Test
  void listsTablesThenViewsAcrossBothRepositories() {
    tableRepo.add(userTable("orders"));
    viewRepo.add(userView("orders_view"));

    var response =
        list(
            ListRelationsRequest.newBuilder().setNamespaceId(namespaceId).setIncludeTotal(true),
            10);

    assertEquals(List.of("orders", "orders_view"), names(response));
    assertEquals(List.of(ResourceKind.RK_TABLE, ResourceKind.RK_VIEW), kinds(response));
    assertEquals(2, response.getPage().getTotalSize());
  }

  @Test
  void identityListingLeavesKindSpecificDetailsAbsent() {
    tableRepo.add(userTable("orders"));

    var response =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE),
            10);

    var relation = response.getResults(0).getRelation();
    assertEquals(ResourceKind.RK_TABLE, relation.getResourceId().getKind());
    assertFalse(relation.hasTable());
    assertTrue(relation.getPropertiesMap().isEmpty());
  }

  @Test
  void identityGetLeavesKindSpecificDetailsAbsent() {
    var table = userTable("orders");
    tableRepo.add(table);

    var relation =
        surface()
            .getRelation(
                GetRelationRequest.newBuilder().setRelationId(table.getResourceId()).build(),
                CORRELATION_ID)
            .getRelation();

    assertEquals(ResourceKind.RK_TABLE, relation.getResourceId().getKind());
    assertFalse(relation.hasTable());
    assertTrue(relation.getPropertiesMap().isEmpty());
  }

  @Test
  void listReportsAnUnhydratedRelationWithoutDroppingHealthyRows() {
    var broken = userTable("broken");
    graphView.addRelationRef(
        namespaceId,
        new CatalogGraphView.RelationRef(
            broken.getResourceId(), broken.getDisplayName(), ResourceKind.RK_TABLE));
    graphView.addUserTableNode(broken);
    tableRepo.add(userTable("healthy"));

    var response =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE)
                .setIncludeTotal(true)
                .setIncludeSchema(true),
            1);

    assertTrue(names(response).isEmpty());
    assertEquals(1, response.getResultsCount());
    assertTrue(response.getResults(0).hasError());
    var error = response.getResults(0).getError();
    assertEquals(broken.getResourceId(), error.getRelationId());
    assertEquals("broken", error.getName().getName());
    assertEquals(ErrorCode.MC_NOT_FOUND, error.getError().getCode());
    assertEquals(2, response.getPage().getTotalSize());

    var second =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE)
                .setIncludeTotal(true)
                .setIncludeSchema(true)
                .setPage(
                    PageRequest.newBuilder()
                        .setPageSize(1)
                        .setPageToken(response.getPage().getNextPageToken())),
            1);
    assertEquals(List.of("healthy"), names(second));
    assertTrue(second.getResults(0).hasRelation());
    assertTrue(second.getPage().getNextPageToken().isBlank());
  }

  @Test
  void qualifiesRowErrorsInRecursiveListings() {
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    var broken = tableIn(child.id(), "broken");
    tableRepo.add(broken);
    graphView.bind(name("public", "nested", "broken"), broken.getResourceId());
    graphView.failTableSchemaWith(
        broken.getResourceId(),
        new StatusRuntimeException(
            io.grpc.Status.FAILED_PRECONDITION.withDescription("schema unavailable")));

    var response =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE)
                .setIncludeSchema(true),
            1);

    assertTrue(response.getResults(0).hasError());
    assertEquals(
        List.of("public", "nested"), response.getResults(0).getError().getName().getPathList());
    assertEquals("broken", response.getResults(0).getError().getName().getName());
  }

  @Test
  void doesNotTurnUnexpectedHydrationFailuresIntoRowErrors() {
    var broken = userTable("broken");
    tableRepo.add(broken);
    graphView.failTableSchemaWith(
        broken.getResourceId(), new IllegalStateException("mapper invariant broken"));

    assertThrows(
        IllegalStateException.class,
        () ->
            list(
                ListRelationsRequest.newBuilder()
                    .setNamespaceId(namespaceId)
                    .addKinds(ResourceKind.RK_TABLE)
                    .setIncludeSchema(true),
                1));
  }

  @Test
  void doesNotAdvertiseAnEmptyPageAfterTheLastSystemRelation() {
    graphView.addRelation(namespaceId, systemTable("only_system"));

    var response =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE),
            1);

    assertEquals(List.of("only_system"), names(response));
    assertTrue(response.getPage().getNextPageToken().isBlank());
  }

  @Test
  void pagesAcrossTheKindBoundaryWithAnOpaqueToken() {
    tableRepo.add(userTable("orders"));
    viewRepo.add(userView("orders_view"));

    var first = list(ListRelationsRequest.newBuilder().setNamespaceId(namespaceId), 1);
    assertEquals(List.of("orders"), names(first));

    String token = first.getPage().getNextPageToken();
    assertFalse(token.isBlank());
    assertTrue(token.startsWith("rel:"));
    assertFalse(token.contains("tbl:"));
    assertFalse(token.contains("view:"));

    var second =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token)),
            1);
    assertEquals(List.of("orders_view"), names(second));
    assertTrue(second.getPage().getNextPageToken().isBlank());
  }

  @Test
  void rejectsATokenMintedForADifferentKindFilter() {
    tableRepo.add(userTable("orders"));
    viewRepo.add(userView("orders_view"));

    String token =
        list(ListRelationsRequest.newBuilder().setNamespaceId(namespaceId), 1)
            .getPage()
            .getNextPageToken();

    var narrowed =
        ListRelationsRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .addKinds(ResourceKind.RK_TABLE)
            .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token))
            .build();

    assertThrows(
        StatusRuntimeException.class,
        () -> surface().listRelations(narrowed, ACCOUNT_ID, CORRELATION_ID));
  }

  @Test
  void rejectsATokenMintedForADifferentScope() {
    tableRepo.add(userTable("orders"));
    viewRepo.add(userView("orders_view"));

    String token =
        list(ListRelationsRequest.newBuilder().setNamespaceId(namespaceId), 1)
            .getPage()
            .getNextPageToken();

    var recursive =
        ListRelationsRequest.newBuilder()
            .setNamespaceId(namespaceId)
            .setRecursive(true)
            .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token))
            .build();

    assertThrows(
        StatusRuntimeException.class,
        () -> surface().listRelations(recursive, ACCOUNT_ID, CORRELATION_ID));
  }

  @Test
  void reportsOriginForUserAndSystemRelations() {
    tableRepo.add(userTable("orders"));
    graphView.addRelation(namespaceId, systemTable("pg_class"));

    var response = list(ListRelationsRequest.newBuilder().setNamespaceId(namespaceId), 10);

    Map<String, Origin> origins = new LinkedHashMap<>();
    response.getResultsList().stream()
        .filter(result -> result.hasRelation())
        .map(result -> result.getRelation())
        .forEach(r -> origins.put(r.getDisplayName(), r.getOrigin()));
    assertEquals(Origin.ORIGIN_USER, origins.get("orders"));
    assertEquals(Origin.ORIGIN_BUILTIN, origins.get("pg_class"));
  }

  @Test
  void statusReportsQueryabilityFromTheCommittedCurrentSnapshot() {
    tableRepo.add(userTable("orders"));
    viewRepo.add(userView("orders_view"));
    graphView.addRelation(namespaceId, systemTable("pg_class"));

    var response =
        list(
            ListRelationsRequest.newBuilder().setNamespaceId(namespaceId).setIncludeStatus(true),
            10);

    Map<String, Queryability> status = new LinkedHashMap<>();
    response.getResultsList().stream()
        .filter(result -> result.hasRelation())
        .map(result -> result.getRelation())
        .forEach(r -> status.put(r.getDisplayName(), r.getStatus().getQueryability()));
    // No committed pointer for the user table yet.
    assertEquals(Queryability.Q_NOT_QUERYABLE_NO_SNAPSHOT, status.get("orders"));
    assertEquals(Queryability.Q_QUERYABLE, status.get("pg_class"));
    assertEquals(Queryability.Q_QUERYABLE, status.get("orders_view"));
  }

  @Test
  void statusCarriesTheCurrentSnapshotIdOnceCommitted() {
    var table = userTable("orders");
    tableRepo.add(table);
    currentSnapshots.put(table.getResourceId(), 42L);

    var response =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE)
                .setIncludeStatus(true),
            10);

    var status = response.getResults(0).getRelation().getStatus();
    assertEquals(Queryability.Q_QUERYABLE, status.getQueryability());
    assertEquals(42L, status.getCurrentSnapshotId());
  }

  @Test
  void recursiveListingReachesNestedNamespaces() {
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    tableRepo.add(tableIn(namespaceId, "orders"));
    tableRepo.add(tableIn(child.id(), "nested_orders"));

    var flat =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders"), names(flat));

    var deep =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders", "nested_orders"), names(deep));
  }

  @Test
  void catalogScopeStartsAtTopLevelAndRecursesOnRequest() {
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    tableRepo.add(tableIn(namespaceId, "orders"));
    tableRepo.add(tableIn(child.id(), "nested_orders"));

    var topLevel =
        list(
            ListRelationsRequest.newBuilder()
                .setCatalogId(catalogId)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders"), names(topLevel));

    var everything =
        list(
            ListRelationsRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders", "nested_orders"), names(everything));
  }

  @Test
  void catalogScopeNormalizesPointerNamespacePaths() {
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    graphView.setPointerNamespaceRefs(
        List.of(
            new CatalogGraphView.NamespaceRef(namespaceId, "public", catalogId, List.of()),
            new CatalogGraphView.NamespaceRef(child.id(), "nested", catalogId, List.of("public"))));
    tableRepo.add(tableIn(namespaceId, "orders"));
    tableRepo.add(tableIn(child.id(), "nested_orders"));

    var topLevel =
        list(
            ListRelationsRequest.newBuilder()
                .setCatalogId(catalogId)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders"), names(topLevel));

    var recursive =
        list(
            ListRelationsRequest.newBuilder()
                .setCatalogId(catalogId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE),
            10);
    assertEquals(List.of("orders", "nested_orders"), names(recursive));
  }

  @Test
  void listedPointerNameResolvesBackByCatalogDisplayName() {
    graphView.setPointerNamespaceRefs(
        List.of(new CatalogGraphView.NamespaceRef(namespaceId, "public", catalogId, List.of())));
    var table = tableIn(namespaceId, "orders");
    tableRepo.add(table);

    var listed =
        list(
                ListRelationsRequest.newBuilder()
                    .setNamespaceId(namespaceId)
                    .addKinds(ResourceKind.RK_TABLE),
                1)
            .getResults(0)
            .getRelation();
    assertEquals("cat", listed.getName().getCatalog());
    graphView.bind(listed.getName(), table.getResourceId());

    var resolved =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder().addReferences(ref(listed.getName())).build(),
                MAX_NAMES,
                CORRELATION_ID);
    assertTrue(resolved.getResults(0).hasRelation());
    assertEquals(table.getResourceId(), resolved.getResults(0).getRelation().getResourceId());
  }

  @Test
  void pagesAcrossANamespaceBoundary() {
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    tableRepo.add(tableIn(namespaceId, "orders"));
    tableRepo.add(tableIn(child.id(), "nested_orders"));

    var first =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE),
            1);
    assertEquals(List.of("orders"), names(first));

    String token = first.getPage().getNextPageToken();
    assertFalse(token.isBlank());

    var second =
        list(
            ListRelationsRequest.newBuilder()
                .setNamespaceId(namespaceId)
                .setRecursive(true)
                .addKinds(ResourceKind.RK_TABLE)
                .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token)),
            1);
    assertEquals(List.of("nested_orders"), names(second));
    assertTrue(second.getPage().getNextPageToken().isBlank());
  }

  @Test
  void resolveRelationsReturnsPerItemErrorsInsteadOfFailingTheBatch() {
    var table = systemTable("orders");
    graphView.addRelation(namespaceId, table);
    graphView.bind(name("public", "orders"), table.id());

    var response =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("public", "orders")))
                    .addReferences(ref(name("public", "missing")))
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);

    assertEquals(2, response.getResultsCount());
    assertTrue(response.getResults(0).hasRelation());
    assertEquals("orders", response.getResults(0).getRelation().getDisplayName());
    assertTrue(response.getResults(1).hasError());
    assertEquals(ErrorCode.MC_NOT_FOUND, response.getResults(1).getError().getCode());
  }

  @Test
  void pagesAcrossNamespacesWhoseNamesSharePrefixes() {
    // "public" is a prefix of "public.nested", which a printable key separator would mis-order.
    var child = namespace(List.of("public"), "nested");
    graphView.addNode(child);
    tableRepo.add(tableIn(namespaceId, "a"));
    tableRepo.add(tableIn(child.id(), "b"));
    viewRepo.add(viewIn(namespaceId, "c"));
    viewRepo.add(viewIn(child.id(), "d"));

    var seen = new ArrayList<String>();
    String token = "";
    do {
      var response =
          surface()
              .listRelations(
                  ListRelationsRequest.newBuilder()
                      .setNamespaceId(namespaceId)
                      .setRecursive(true)
                      .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token))
                      .build(),
                  ACCOUNT_ID,
                  CORRELATION_ID);
      seen.addAll(names(response));
      token = response.getPage().getNextPageToken();
    } while (!token.isEmpty());

    // Every relation exactly once: tables then views within each namespace, namespaces in order.
    assertEquals(List.of("a", "c", "b", "d"), seen);
  }

  @Test
  void pagesAcrossLiteralDottedAndNestedNamespaceNamesWithoutCollision() {
    var literal = namespace(List.of(), "a.b");
    var nested = namespace(List.of("a"), "b");
    graphView.addNode(literal);
    graphView.addNode(nested);
    tableRepo.add(tableIn(literal.id(), "literal_table"));
    tableRepo.add(tableIn(nested.id(), "nested_table"));

    var request =
        ListRelationsRequest.newBuilder()
            .setCatalogId(catalogId)
            .setRecursive(true)
            .addKinds(ResourceKind.RK_TABLE);
    var seen = new ArrayList<String>();
    String token = "";
    for (int page = 0; page < 10; page++) {
      var response =
          surface()
              .listRelations(
                  request
                      .setPage(PageRequest.newBuilder().setPageSize(1).setPageToken(token))
                      .build(),
                  ACCOUNT_ID,
                  CORRELATION_ID);
      seen.addAll(names(response));
      token = response.getPage().getNextPageToken();
      if (token.isEmpty()) {
        break;
      }
    }

    assertEquals(2, seen.size());
    assertTrue(seen.contains("literal_table"));
    assertTrue(seen.contains("nested_table"));
    assertTrue(token.isEmpty(), "dotted namespace pagination should terminate");
  }

  @Test
  void relationPageTokenIsBoundToCatalogContext() {
    tableRepo.add(userTable("first"));
    tableRepo.add(userTable("second"));
    var firstPage =
        surface()
            .listRelations(
                ListRelationsRequest.newBuilder()
                    .setNamespaceId(namespaceId)
                    .addKinds(ResourceKind.RK_TABLE)
                    .setPage(PageRequest.newBuilder().setPageSize(1))
                    .build(),
                ACCOUNT_ID,
                CORRELATION_ID);

    var engineContext =
        CatalogContext.of(
            EnvironmentContext.of("lakehouse", "1"), EngineContext.of("trino", "470"));
    assertThrows(
        StatusRuntimeException.class,
        () ->
            surface(engineContext)
                .listRelations(
                    ListRelationsRequest.newBuilder()
                        .setNamespaceId(namespaceId)
                        .addKinds(ResourceKind.RK_TABLE)
                        .setPage(
                            PageRequest.newBuilder()
                                .setPageSize(1)
                                .setPageToken(firstPage.getPage().getNextPageToken()))
                        .build(),
                    ACCOUNT_ID,
                    CORRELATION_ID));
  }

  @Test
  void resolveTakesTheFirstCandidateThatResolves() {
    var table = systemTable("orders");
    graphView.addRelation(namespaceId, table);
    // Only the second candidate exists, as when a search path misses its first namespace.
    graphView.bind(name("public", "orders"), table.id());

    var response =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("sales", "orders"), name("public", "orders")))
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);

    assertEquals(1, response.getResultsCount());
    assertTrue(response.getResults(0).hasRelation());
    assertEquals(name("public", "orders"), response.getResults(0).getResolvedName());
  }

  @Test
  void resolveReportsOneErrorWhenNoCandidateResolves() {
    var response =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("sales", "orders"), name("public", "orders")))
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);

    assertEquals(1, response.getResultsCount());
    assertEquals(ErrorCode.MC_NOT_FOUND, response.getResults(0).getError().getCode());
  }

  @Test
  void resolveRejectsABatchOverTheCandidateLimit() {
    var request =
        ResolveRelationsRequest.newBuilder()
            .addReferences(ref(name("a", "t"), name("b", "t"), name("c", "t")))
            .build();

    assertThrows(
        StatusRuntimeException.class, () -> surface().resolveRelations(request, 2, CORRELATION_ID));
  }

  @Test
  void resolveRelationsEnvelopesAReadFailureForOneName() {
    var table = systemTable("orders");
    graphView.addRelation(namespaceId, table);
    graphView.bind(name("public", "orders"), table.id());
    // Bound but absent from the graph: reading it fails for this name only.
    graphView.bind(name("public", "ghost"), id(ResourceKind.RK_TABLE, "ghost"));

    var response =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("public", "ghost")))
                    .addReferences(ref(name("public", "orders")))
                    .setIncludeSchema(true)
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);

    assertTrue(response.getResults(0).hasError());
    assertTrue(response.getResults(1).hasRelation());
  }

  @Test
  void perItemErrorKeepsTheStructuredDetailOfTheFailure() {
    // Bound but absent from the graph, so reading it fails for this reference only.
    graphView.bind(name("public", "ghost"), id(ResourceKind.RK_TABLE, "ghost"));

    var response =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("public", "ghost")))
                    .setIncludeSchema(true)
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);

    var error = response.getResults(0).getError();
    assertEquals(ErrorCode.MC_NOT_FOUND, error.getCode());
    // The message key and params come from the failure itself, not from a re-derived summary.
    assertFalse(error.getMessageKey().isBlank(), "per-item error should keep the message key");
    assertEquals("ghost", error.getParamsMap().get("id"));
    assertEquals(CORRELATION_ID, error.getCorrelationId());
  }

  @Test
  void aBackendFailureFailsTheCallInsteadOfReportingEveryRelationMissing() {
    var id = id(ResourceKind.RK_TABLE, "orders");
    graphView.bind(name("public", "orders"), id);
    graphView.failResolveWith(
        id, new StatusRuntimeException(io.grpc.Status.UNAVAILABLE.withDescription("store down")));

    var request =
        ResolveRelationsRequest.newBuilder()
            .addReferences(ref(name("public", "orders")))
            .setIncludeSchema(true)
            .build();

    var thrown =
        assertThrows(
            StatusRuntimeException.class,
            () -> surface().resolveRelations(request, MAX_NAMES, CORRELATION_ID));
    assertEquals(io.grpc.Status.Code.UNAVAILABLE, thrown.getStatus().getCode());
  }

  @Test
  void schemaIsPopulatedOnlyWhenRequested() {
    var table = systemTable("orders");
    graphView.addRelation(namespaceId, table);
    graphView.bind(name("public", "orders"), table.id());
    graphView.setTableSchema(table.id(), List.of(SchemaColumn.newBuilder().setName("id").build()));

    var withSchema =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("public", "orders")))
                    .setIncludeSchema(true)
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);
    assertEquals(1, withSchema.getResults(0).getRelation().getSchema().getColumnsCount());

    var withoutSchema =
        surface()
            .resolveRelations(
                ResolveRelationsRequest.newBuilder()
                    .addReferences(ref(name("public", "orders")))
                    .build(),
                MAX_NAMES,
                CORRELATION_ID);
    assertFalse(withoutSchema.getResults(0).getRelation().hasSchema());
  }

  private CatalogSurfaceRelations surface() {
    return surface(CatalogContext.empty());
  }

  private CatalogSurfaceRelations surface(CatalogContext context) {
    return new CatalogSurfaceRelations(
        tableRepo,
        viewRepo,
        tableId -> Optional.ofNullable(currentSnapshots.get(tableId)),
        graphView,
        context);
  }

  private ListRelationsResponse list(ListRelationsRequest.Builder request, int pageSize) {
    if (!request.hasPage()) {
      request.setPage(PageRequest.newBuilder().setPageSize(pageSize));
    }
    return surface().listRelations(request.build(), ACCOUNT_ID, CORRELATION_ID);
  }

  private static List<String> names(ListRelationsResponse response) {
    return response.getResultsList().stream()
        .filter(result -> result.hasRelation())
        .map(result -> result.getRelation().getDisplayName())
        .toList();
  }

  private static List<ResourceKind> kinds(ListRelationsResponse response) {
    return response.getResultsList().stream()
        .filter(result -> result.hasRelation())
        .map(result -> result.getRelation().getResourceId().getKind())
        .toList();
  }

  private NamespaceNode namespace(List<String> parents, String displayName) {
    return new NamespaceNode(
        id(ResourceKind.RK_NAMESPACE, String.join(".", parents) + "." + displayName),
        "blob://test/v1",
        catalogId,
        parents,
        displayName,
        GraphNodeOrigin.USER,
        Map.of(),
        Map.of());
  }

  private ai.floedb.floecat.catalog.rpc.View viewIn(ResourceId namespace, String displayName) {
    return ai.floedb.floecat.catalog.rpc.View.newBuilder()
        .setResourceId(id(ResourceKind.RK_VIEW, displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespace)
        .setDisplayName(displayName)
        .build();
  }

  private Table tableIn(ResourceId namespace, String displayName) {
    return Table.newBuilder()
        .setResourceId(id(ResourceKind.RK_TABLE, displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespace)
        .setDisplayName(displayName)
        .build();
  }

  private Table userTable(String displayName) {
    return Table.newBuilder()
        .setResourceId(id(ResourceKind.RK_TABLE, displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(displayName)
        .build();
  }

  private ai.floedb.floecat.catalog.rpc.View userView(String displayName) {
    return ai.floedb.floecat.catalog.rpc.View.newBuilder()
        .setResourceId(id(ResourceKind.RK_VIEW, displayName))
        .setCatalogId(catalogId)
        .setNamespaceId(namespaceId)
        .setDisplayName(displayName)
        .build();
  }

  private SystemTableNode systemTable(String displayName) {
    return new SystemTableNode.GenericSystemTableNode(
        systemId(displayName),
        1L,
        "engine",
        displayName,
        namespaceId,
        List.of(),
        null,
        null,
        TableBackendKind.TABLE_BACKEND_KIND_ENGINE);
  }

  @SuppressWarnings("unused")
  private ViewNode viewNode(String displayName) {
    return new ViewNode(
        id(ResourceKind.RK_VIEW, "sys_" + displayName),
        "blob://test/v1",
        catalogId,
        namespaceId,
        displayName,
        "select 1",
        "sql",
        List.of(),
        List.of(),
        List.of(),
        GraphNodeOrigin.SYSTEM,
        Map.of(),
        Optional.empty(),
        Map.of(),
        Map.of());
  }

  /** A real marker-bearing system id, so RelationOrigin resolves the way production does. */
  private static ResourceId systemId(String displayName) {
    var uuid =
        SystemResourceIdGenerator.uuidFromBytes(
            SystemResourceIdGenerator.base(ResourceKind.RK_TABLE, "pg_catalog." + displayName));
    return ResourceId.newBuilder()
        .setAccountId("_system")
        .setKind(ResourceKind.RK_TABLE)
        .setId(uuid.toString())
        .build();
  }

  private static RelationReference ref(NameRef... candidates) {
    return RelationReference.newBuilder().addAllCandidates(List.of(candidates)).build();
  }

  private static NameRef name(String namespace, String name) {
    return NameRef.newBuilder().addPath(namespace).setName(name).build();
  }

  private static NameRef name(String first, String second, String name) {
    return NameRef.newBuilder().addPath(first).addPath(second).setName(name).build();
  }

  private static ResourceId id(ResourceKind kind, String id) {
    return ResourceId.newBuilder().setAccountId(ACCOUNT_ID).setKind(kind).setId(id).build();
  }

  /** Keyset-paged over an in-memory list, matching the repository contract the pager relies on. */
  private static final class FakeTableRepository extends TableRepository {
    private final CountingGraphView graphView;
    private final RelationRows<Table> rows =
        new RelationRows<>(table -> table.getNamespaceId().getId());

    FakeTableRepository(CountingGraphView graphView) {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
      this.graphView = graphView;
    }

    void add(Table table) {
      rows.add(table);
      graphView.addUserTableNode(table);
      graphView.addRelationRef(
          table.getNamespaceId(),
          new CatalogGraphView.RelationRef(
              table.getResourceId(), table.getDisplayName(), ResourceKind.RK_TABLE));
    }

    @Override
    public Optional<Table> getById(ResourceId tableResourceId) {
      return rows.rows().stream()
          .filter(table -> table.getResourceId().equals(tableResourceId))
          .findFirst();
    }

    @Override
    public List<Table> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String cursor,
        StringBuilder next) {
      return page(rows.inNamespace(namespaceId), Table::getDisplayName, limit, cursor, next);
    }

    @Override
    public List<CatalogGraphView.RelationRef> listRefs(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String cursor,
        StringBuilder next) {
      return page(
          graphView.userRelationRefs(namespaceId, ResourceKind.RK_TABLE),
          CatalogGraphView.RelationRef::name,
          limit,
          cursor,
          next);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return graphView.userRelationRefs(namespaceId, ResourceKind.RK_TABLE).size();
    }
  }

  private static final class FakeViewRepository extends ViewRepository {
    private final CountingGraphView graphView;
    private final RelationRows<ai.floedb.floecat.catalog.rpc.View> rows =
        new RelationRows<>(view -> view.getNamespaceId().getId());

    FakeViewRepository(CountingGraphView graphView) {
      super(new InMemoryPointerStore(), new InMemoryBlobStore());
      this.graphView = graphView;
    }

    void add(ai.floedb.floecat.catalog.rpc.View view) {
      rows.add(view);
      graphView.addRelationRef(
          view.getNamespaceId(),
          new CatalogGraphView.RelationRef(
              view.getResourceId(), view.getDisplayName(), ResourceKind.RK_VIEW));
    }

    @Override
    public Optional<ai.floedb.floecat.catalog.rpc.View> getById(ResourceId viewResourceId) {
      return rows.rows().stream()
          .filter(view -> view.getResourceId().equals(viewResourceId))
          .findFirst();
    }

    @Override
    public List<ai.floedb.floecat.catalog.rpc.View> list(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String cursor,
        StringBuilder next) {
      return page(
          rows.inNamespace(namespaceId),
          ai.floedb.floecat.catalog.rpc.View::getDisplayName,
          limit,
          cursor,
          next);
    }

    @Override
    public List<CatalogGraphView.RelationRef> listRefs(
        String accountId,
        String catalogId,
        String namespaceId,
        int limit,
        String cursor,
        StringBuilder next) {
      return page(
          graphView.userRelationRefs(namespaceId, ResourceKind.RK_VIEW),
          CatalogGraphView.RelationRef::name,
          limit,
          cursor,
          next);
    }

    @Override
    public int count(String accountId, String catalogId, String namespaceId) {
      return graphView.userRelationRefs(namespaceId, ResourceKind.RK_VIEW).size();
    }
  }

  private static final class RelationRows<T> {
    private final List<T> rows = new ArrayList<>();
    private final Function<T, String> namespaceId;

    private RelationRows(Function<T, String> namespaceId) {
      this.namespaceId = namespaceId;
    }

    private void add(T row) {
      rows.add(row);
    }

    private List<T> rows() {
      return List.copyOf(rows);
    }

    private List<T> inNamespace(String id) {
      return rows.stream().filter(row -> namespaceId.apply(row).equals(id)).toList();
    }
  }

  private static <T> List<T> page(
      List<T> rows,
      java.util.function.Function<T, String> key,
      int limit,
      String cursor,
      StringBuilder next) {
    var out = new ArrayList<T>(limit);
    for (T row : rows) {
      if (cursor != null && !cursor.isBlank() && key.apply(row).compareTo(cursor) <= 0) {
        continue;
      }
      if (out.size() == limit) {
        next.append(key.apply(out.get(out.size() - 1)));
        return out;
      }
      out.add(row);
    }
    return out;
  }

  private static final class CountingGraphView extends TestCatalogGraphView {
    private final Map<String, ResourceId> byName = new LinkedHashMap<>();
    private final Map<ResourceId, List<CatalogGraphView.RelationRef>> relationRefs =
        new LinkedHashMap<>();
    private List<CatalogGraphView.NamespaceRef> pointerNamespaceRefs = List.of();

    private final Map<ResourceId, RuntimeException> resolveFailures = new LinkedHashMap<>();
    private final Map<ResourceId, RuntimeException> tableSchemaFailures = new LinkedHashMap<>();

    void bind(NameRef name, ResourceId id) {
      byName.put(NameRefUtil.lookupKey(name), id);
    }

    void addRelationRef(ResourceId namespaceId, CatalogGraphView.RelationRef relationRef) {
      relationRefs.computeIfAbsent(namespaceId, ignored -> new ArrayList<>()).add(relationRef);
    }

    void setPointerNamespaceRefs(List<CatalogGraphView.NamespaceRef> refs) {
      pointerNamespaceRefs = List.copyOf(refs);
    }

    List<CatalogGraphView.RelationRef> userRelationRefs(String namespaceId, ResourceKind kind) {
      return relationRefs.entrySet().stream()
          .filter(entry -> entry.getKey().getId().equals(namespaceId))
          .flatMap(entry -> entry.getValue().stream())
          .filter(ref -> ref.kind() == kind)
          .toList();
    }

    void addUserTableNode(Table table) {
      addNode(
          new UserTableNode(
              table.getResourceId(),
              "blob://test/v1/" + table.getResourceId().getId(),
              table.getCatalogId(),
              table.getNamespaceId(),
              table.getDisplayName(),
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
              Map.of()));
    }

    @Override
    public List<CatalogGraphView.RelationRef> listRelationRefs(
        ResourceId catalogId, ResourceId namespaceId, CatalogContext catalogContext) {
      var refs = new ArrayList<>(relationRefs.getOrDefault(namespaceId, List.of()));
      refs.addAll(super.listRelationRefs(catalogId, namespaceId, catalogContext));
      return refs;
    }

    @Override
    public List<CatalogGraphView.NamespaceRef> listNamespaceRefs(
        ResourceId catalogId, CatalogContext catalogContext) {
      return pointerNamespaceRefs.isEmpty()
          ? super.listNamespaceRefs(catalogId, catalogContext)
          : pointerNamespaceRefs;
    }

    @Override
    public Optional<CatalogGraphView.NamespaceRef> namespaceRef(
        ResourceId namespaceId, CatalogContext catalogContext) {
      return pointerNamespaceRefs.isEmpty()
          ? super.namespaceRef(namespaceId, catalogContext)
          : pointerNamespaceRefs.stream().filter(ref -> ref.id().equals(namespaceId)).findFirst();
    }

    @Override
    public Optional<String> catalogName(ResourceId id, CatalogContext catalogContext) {
      return Optional.of("cat");
    }

    void failResolveWith(ResourceId id, RuntimeException failure) {
      resolveFailures.put(id, failure);
    }

    void failTableSchemaWith(ResourceId id, RuntimeException failure) {
      tableSchemaFailures.put(id, failure);
    }

    @Override
    public Optional<ai.floedb.floecat.metagraph.model.GraphNode> resolve(
        ResourceId id, CatalogContext catalogContext) {
      RuntimeException failure = resolveFailures.get(id);
      if (failure != null) {
        throw failure;
      }
      return super.resolve(id, catalogContext);
    }

    @Override
    public Optional<ResourceId> resolveName(
        String correlationId, NameRef ref, CatalogContext catalogContext) {
      return Optional.ofNullable(byName.get(NameRefUtil.lookupKey(ref)));
    }

    @Override
    public Optional<NameRef> tableName(ResourceId id, CatalogContext catalogContext) {
      return byName.entrySet().stream()
          .filter(entry -> entry.getValue().equals(id))
          .map(entry -> nameFromCanonical(entry.getKey()))
          .findFirst();
    }

    @Override
    public List<SchemaColumn> tableSchema(ResourceId id, CatalogContext catalogContext) {
      RuntimeException failure = tableSchemaFailures.get(id);
      if (failure != null) {
        throw failure;
      }
      return super.tableSchema(id, catalogContext);
    }

    @Override
    public Optional<NameRef> viewName(ResourceId id, CatalogContext catalogContext) {
      return tableName(id, catalogContext);
    }

    private static NameRef nameFromCanonical(String canonical) {
      String[] parts = canonical.split("\\.");
      NameRef.Builder builder = NameRef.newBuilder().setName(parts[parts.length - 1]);
      for (int i = 0; i < parts.length - 1; i++) {
        builder.addPath(parts[i]);
      }
      return builder.build();
    }
  }
}
