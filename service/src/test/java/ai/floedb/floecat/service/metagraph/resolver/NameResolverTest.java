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

package ai.floedb.floecat.service.metagraph.resolver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.cache.CacheEvents;
import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.context.PropagatedContext;
import ai.floedb.floecat.service.repo.cache.AuthoritativePointerStore;
import ai.floedb.floecat.service.repo.cache.CachingPointerStore;
import ai.floedb.floecat.service.repo.cache.PointerCache;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.testsupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.FakeViewRepository;
import ai.floedb.floecat.storage.memory.InMemoryBlobStore;
import ai.floedb.floecat.storage.memory.InMemoryPointerStore;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NameResolverTest {

  private FakeCatalogRepository catalogRepository;
  private FakeNamespaceRepository namespaceRepository;
  private FakeTableRepository tableRepository;
  private FakeViewRepository viewRepository;
  private NameResolver resolver;

  @BeforeEach
  void setUp() {
    catalogRepository = new FakeCatalogRepository();
    namespaceRepository = new FakeNamespaceRepository();
    tableRepository = new FakeTableRepository();
    viewRepository = new FakeViewRepository();
    resolver =
        new NameResolver(catalogRepository, namespaceRepository, tableRepository, viewRepository);

    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat").build());

    ResourceId namespaceId = rid("account", "ns", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns")
            .build());

    ResourceId tableId = rid("account", "tbl", ResourceKind.RK_TABLE);
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(tableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders")
            .setSchemaJson("{}")
            .build());

    ResourceId viewId = rid("account", "view", ResourceKind.RK_VIEW);
    viewRepository.put(
        View.newBuilder()
            .setResourceId(viewId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders_v")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                    .setSql("select 1")
                    .build())
            .build());
  }

  @Test
  void resolveCatalogIdReturnsResource() {
    assertThat(resolver.resolveCatalogId("corr", "account", "cat"))
        .hasValueSatisfying(r -> assertThat(r.getId()).isEqualTo("cat"));
  }

  @Test
  void resolveNamespaceIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").setName("ns").build();
    assertThat(resolver.resolveNamespaceId("corr", "account", ref))
        .hasValueSatisfying(r -> assertThat(r.getId()).isEqualTo("ns"));
  }

  @Test
  void resolveTableIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders").build();
    assertThat(resolver.resolveTableId("corr", "account", ref))
        .hasValueSatisfying(r -> assertThat(r.getId()).isEqualTo("tbl"));
  }

  @Test
  void resolveViewIdReturnsResource() {
    NameRef ref = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders_v").build();
    assertThat(resolver.resolveViewId("corr", "account", ref))
        .hasValueSatisfying(r -> assertThat(r.getId()).isEqualTo("view"));
  }

  @Test
  void resolveRelationIdsBlankNameDoesNotSuppressValidSibling() {
    NameRef blank = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("").build();
    NameRef valid = NameRef.newBuilder().setCatalog("cat").addPath("ns").setName("orders").build();

    var resolved = resolver.resolveRelationIds("account", List.of(blank, valid));

    assertThat(resolved.get(blank)).isEmpty();
    assertThat(resolved.get(valid)).hasValueSatisfying(r -> assertThat(r.getId()).isEqualTo("tbl"));
  }

  @Test
  void listTableIdsCollectsAcrossNamespaces() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);

    ResourceId ns2Id = rid("account", "ns2", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(ns2Id)
            .setCatalogId(catalogId)
            .setDisplayName("ns2")
            .build());

    ResourceId tbl2Id = rid("account", "tbl2", ResourceKind.RK_TABLE);
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(tbl2Id)
            .setCatalogId(catalogId)
            .setNamespaceId(ns2Id)
            .setDisplayName("products")
            .setSchemaJson("{}")
            .build());

    List<ResourceId> ids = resolver.listTableIds("account", "cat");
    Set<String> idSet = ids.stream().map(ResourceId::getId).collect(Collectors.toSet());
    assertThat(idSet).containsExactlyInAnyOrder("tbl", "tbl2");
  }

  @Test
  void listTableIdsPassesTheRequestCancellationToTheNamespaceFanout() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    ResourceId ns2Id = rid("account", "ns2", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(ns2Id)
            .setCatalogId(catalogId)
            .setDisplayName("ns2")
            .build());
    AtomicBoolean cancelled = new AtomicBoolean(true);

    try (var ignored = PropagatedContext.bindCancellation(cancelled::get)) {
      assertThatThrownBy(() -> resolver.listTableIds("account", "cat"))
          .isInstanceOf(CancellationException.class);
    }
  }

  @Test
  void listTableIdsCancellationStopsQueuedNamespaceScans() throws Exception {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);
    for (int i = 2; i <= 9; i++) {
      ResourceId namespaceId = rid("account", "ns" + i, ResourceKind.RK_NAMESPACE);
      namespaceRepository.put(
          Namespace.newBuilder()
              .setResourceId(namespaceId)
              .setCatalogId(catalogId)
              .setDisplayName("ns" + i)
              .build());
    }
    CountDownLatch started = new CountDownLatch(8);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch stopped = new CountDownLatch(8);
    AtomicInteger scans = new AtomicInteger();
    TableRepository blockingTableRepository =
        new TableRepository(new InMemoryPointerStore(), new InMemoryBlobStore()) {
          @Override
          public List<RelationRef> listRefs(
              String accountId, String catalogId, String namespaceId) {
            scans.incrementAndGet();
            started.countDown();
            try {
              awaitUninterruptibly(release);
              return List.of();
            } finally {
              stopped.countDown();
            }
          }
        };
    NameResolver blockingResolver =
        new NameResolver(
            catalogRepository, namespaceRepository, blockingTableRepository, viewRepository);
    AtomicBoolean cancelled = new AtomicBoolean();
    CompletableFuture<List<ResourceId>> result =
        CompletableFuture.supplyAsync(
            () -> {
              try (var ignored = PropagatedContext.bindCancellation(cancelled::get)) {
                return blockingResolver.listTableIds("account", "cat");
              }
            });

    assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
    cancelled.set(true);
    try {
      assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS))
          .hasCauseInstanceOf(CancellationException.class);
      assertThat(scans.get()).isEqualTo(8);
    } finally {
      release.countDown();
    }
    assertThat(stopped.await(5, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void listViewIdsCollectsAcrossNamespaces() {
    ResourceId catalogId = rid("account", "cat", ResourceKind.RK_CATALOG);

    ResourceId ns2Id = rid("account", "ns2", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(ns2Id)
            .setCatalogId(catalogId)
            .setDisplayName("ns2")
            .build());

    ResourceId view2Id = rid("account", "view2", ResourceKind.RK_VIEW);
    viewRepository.put(
        View.newBuilder()
            .setResourceId(view2Id)
            .setCatalogId(catalogId)
            .setNamespaceId(ns2Id)
            .setDisplayName("orders_v2")
            .addSqlDefinitions(
                ai.floedb.floecat.catalog.rpc.ViewSqlDefinition.newBuilder()
                    .setSql("select 2")
                    .build())
            .build());

    List<ResourceId> ids = resolver.listViewIds("account", "cat");
    Set<String> idSet = ids.stream().map(ResourceId::getId).collect(Collectors.toSet());
    assertThat(idSet).containsExactlyInAnyOrder("view", "view2");
  }

  @Test
  void selectedNamespacesUseExactPointerLookups() {
    NamespaceRepository namespaces = mock(NamespaceRepository.class);
    var names =
        new NameResolver(
            mock(CatalogRepository.class),
            namespaces,
            mock(TableRepository.class),
            mock(ViewRepository.class));
    ResourceId catalog = rid("acct", "cat", ResourceKind.RK_CATALOG);
    Set<String> selected = Set.of("sales");
    NamespaceRef sales = new NamespaceRef(rid("acct", "ns", ResourceKind.RK_NAMESPACE), "sales");
    when(namespaces.listRefsByName("acct", "cat", selected)).thenReturn(List.of(sales));

    assertThat(names.listNamespaceRefsByName(catalog, selected)).containsExactly(sales);

    verify(namespaces).listRefsByName("acct", "cat", selected);
    verify(namespaces, never()).listRefs("acct", "cat");
  }

  @Test
  void relationRefsCombinePointerOnlyTableAndViewListings() {
    TableRepository tables = mock(TableRepository.class);
    ViewRepository views = mock(ViewRepository.class);
    var names =
        new NameResolver(
            mock(CatalogRepository.class), mock(NamespaceRepository.class), tables, views);
    ResourceId catalog = rid("acct", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespace = rid("acct", "ns", ResourceKind.RK_NAMESPACE);
    RelationRef table =
        new RelationRef(
            rid("acct", "table", ResourceKind.RK_TABLE), "orders", ResourceKind.RK_TABLE);
    RelationRef view =
        new RelationRef(
            rid("acct", "view", ResourceKind.RK_VIEW), "orders_v", ResourceKind.RK_VIEW);
    when(tables.listRefs("acct", "cat", "ns")).thenReturn(List.of(table));
    when(views.listRefs("acct", "cat", "ns")).thenReturn(List.of(view));

    assertThat(names.listRelationRefs(catalog, namespace)).containsExactly(table, view);
  }

  @Test
  void aRelationResolveMissDoesNotReachKvOnceTheAccountIndexIsComplete() {
    CountingPointerStore raw = new CountingPointerStore();
    PointerCache pointers =
        new PointerCache(AuthoritativePointerStore.of(raw), 1024L * 1024L, CacheEvents.none());
    var cached = new CachingPointerStore(raw, pointers);
    var blobs = new InMemoryBlobStore();
    var catalogs = new CatalogRepository(cached, blobs);
    var namespaces = new NamespaceRepository(cached, blobs);
    var tables = new TableRepository(cached, blobs);
    var views = new ViewRepository(cached, blobs);
    var names = new NameResolver(catalogs, namespaces, tables, views);

    ResourceId catalogId = rid("account", "catalog", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("account", "namespace", ResourceKind.RK_NAMESPACE);
    catalogs.create(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("catalog").build());
    namespaces.create(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("namespace")
            .build());

    // Loading any complete addressing key blocks until the whole account index is authoritative.
    assertThat(cached.get(Keys.relationPointerByName("account", "catalog", "namespace", "warmup")))
        .isEmpty();
    assertThat(pointers.completeAccountCount()).isEqualTo(1L);
    raw.resetReads();

    NameRef missing =
        NameRef.newBuilder().setCatalog("catalog").addPath("namespace").setName("missing").build();
    assertThat(names.resolveRelationId("account", missing)).isEmpty();
    assertThat(raw.reads)
        .as("the relation claim and legacy table/view fallbacks are authoritative memory misses")
        .hasValue(0);
  }

  @Test
  void listTableIdsEmptyForUnknownCatalog() {
    assertThat(resolver.listTableIds("account", "no-such-catalog")).isEmpty();
  }

  @Test
  void listTableIdsSingleNamespaceSkipsParallelPath() {
    // Verifies the single-namespace fast path returns the same result.
    List<ResourceId> ids = resolver.listTableIds("account", "cat");
    assertThat(ids).hasSize(1);
    assertThat(ids.get(0).getId()).isEqualTo("tbl");
  }

  @Test
  void resolveTableIdRejectsUnspecifiedKind() {
    ResourceId catalogId = rid("account", "cat2", ResourceKind.RK_CATALOG);
    catalogRepository.put(
        Catalog.newBuilder().setResourceId(catalogId).setDisplayName("cat2").build());

    ResourceId namespaceId = rid("account", "ns2", ResourceKind.RK_NAMESPACE);
    namespaceRepository.put(
        Namespace.newBuilder()
            .setResourceId(namespaceId)
            .setCatalogId(catalogId)
            .setDisplayName("ns2")
            .build());

    ResourceId rawTableId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("tbl2")
            .setKind(ResourceKind.RK_UNSPECIFIED)
            .build();
    tableRepository.put(
        Table.newBuilder()
            .setResourceId(rawTableId)
            .setCatalogId(catalogId)
            .setNamespaceId(namespaceId)
            .setDisplayName("orders2")
            .setSchemaJson("{}")
            .build());

    NameRef ref = NameRef.newBuilder().setCatalog("cat2").addPath("ns2").setName("orders2").build();
    assertThatThrownBy(() -> resolver.resolveTableId("corr", "account", ref))
        .isInstanceOf(IllegalStateException.class);
  }

  private static ResourceId rid(String account, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(id).setKind(kind).build();
  }

  private static final class CountingPointerStore extends InMemoryPointerStore {
    private final AtomicInteger reads = new AtomicInteger();

    @Override
    public Optional<Pointer> get(String key) {
      reads.incrementAndGet();
      return super.get(key);
    }

    @Override
    public Optional<Pointer> getConsistent(String key) {
      reads.incrementAndGet();
      return super.getConsistent(key);
    }

    @Override
    public Map<String, Pointer> getBatch(List<String> keys) {
      reads.incrementAndGet();
      return super.getBatch(keys);
    }

    @Override
    public Map<String, Pointer> getBatchConsistent(List<String> keys) {
      reads.incrementAndGet();
      return super.getBatchConsistent(keys);
    }

    @Override
    public List<Pointer> listPointersByPrefix(
        String prefix, int limit, String pageToken, StringBuilder nextTokenOut) {
      reads.incrementAndGet();
      return super.listPointersByPrefix(prefix, limit, pageToken, nextTokenOut);
    }

    private void resetReads() {
      reads.set(0);
    }
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }
}
