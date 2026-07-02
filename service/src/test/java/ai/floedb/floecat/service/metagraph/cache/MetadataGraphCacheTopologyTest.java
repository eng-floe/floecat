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

package ai.floedb.floecat.service.metagraph.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.CatalogRepository.CatalogRef;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MetadataGraphCacheTopologyTest {

  private CatalogRepository catalogRepository;
  private NamespaceRepository namespaceRepository;
  private TableRepository tableRepository;
  private ViewRepository viewRepository;
  private MetadataGraphCache cache;

  @BeforeEach
  void setUp() {
    catalogRepository = mock(CatalogRepository.class);
    namespaceRepository = mock(NamespaceRepository.class);
    tableRepository = mock(TableRepository.class);
    viewRepository = mock(ViewRepository.class);
    cache =
        new MetadataGraphCache(
            catalogRepository,
            namespaceRepository,
            tableRepository,
            viewRepository,
            new TestObservability(),
            100,
            2,
            100,
            100,
            15);
  }

  @Test
  void resolveCatalogRefByName_cachesExactRepositoryLookup() {
    CatalogRef ref = new CatalogRef(rid("acct", "cat", ResourceKind.RK_CATALOG), "prod");
    when(catalogRepository.refByName("acct", "prod")).thenReturn(Optional.of(ref));

    assertThat(cache.resolveCatalogRefByName("acct", "prod")).contains(ref);
    assertThat(cache.resolveCatalogRefByName("acct", "prod")).contains(ref);

    verify(catalogRepository, times(1)).refByName("acct", "prod");
  }

  @Test
  void listNamespaceRefsByName_usesExactRepositoryLookupOnColdCache() {
    ResourceId catalogId = rid("acct", "cat", ResourceKind.RK_CATALOG);
    Set<String> names = Set.of("sales");
    NamespaceRef ref = new NamespaceRef(rid("acct", "ns", ResourceKind.RK_NAMESPACE), "sales");
    when(namespaceRepository.listRefsByName("acct", "cat", names)).thenReturn(List.of(ref));

    List<NamespaceRef> refs = cache.listNamespaceRefsByName(catalogId, names);

    assertThat(refs).containsExactly(ref);
    verify(namespaceRepository).listRefsByName("acct", "cat", names);
    verify(namespaceRepository, never()).listRefs("acct", "cat");
  }

  @Test
  void resolveNamespaceRefByPath_cachesExactRepositoryLookup() {
    ResourceId catalogId = rid("acct", "cat", ResourceKind.RK_CATALOG);
    NamespaceRef ref =
        new NamespaceRef(
            rid("acct", "ns", ResourceKind.RK_NAMESPACE), "sales", catalogId, List.of("sales"));
    when(namespaceRepository.listRefsByName("acct", "cat", Set.of("sales")))
        .thenReturn(List.of(ref));

    assertThat(cache.resolveNamespaceRefByPath(catalogId, List.of("sales"))).contains(ref);
    assertThat(cache.resolveNamespaceRefByPath(catalogId, List.of("sales"))).contains(ref);

    verify(namespaceRepository, times(1)).listRefsByName("acct", "cat", Set.of("sales"));
  }

  @Test
  void listRelationRefsByName_usesExactRepositoryLookupOnColdCache() {
    ResourceId catalogId = rid("acct", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("acct", "ns", ResourceKind.RK_NAMESPACE);
    Set<String> names = Set.of("orders");
    RelationRef ref =
        new RelationRef(rid("acct", "tbl", ResourceKind.RK_TABLE), "orders", ResourceKind.RK_TABLE);
    when(tableRepository.listRefsByName("acct", "cat", "ns", names)).thenReturn(List.of(ref));
    when(viewRepository.listRefsByName("acct", "cat", "ns", names)).thenReturn(List.of());

    List<RelationRef> refs = cache.listRelationRefsByName(catalogId, namespaceId, names);

    assertThat(refs).containsExactly(ref);
    verify(tableRepository).listRefsByName("acct", "cat", "ns", names);
    verify(viewRepository).listRefsByName("acct", "cat", "ns", names);
    verify(tableRepository, never()).listRefs("acct", "cat", "ns");
    verify(viewRepository, never()).listRefs("acct", "cat", "ns");
  }

  @Test
  void resolveRelationRefsByName_cachesAmbiguousExactLookup() {
    ResourceId catalogId = rid("acct", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespaceId = rid("acct", "ns", ResourceKind.RK_NAMESPACE);
    RelationRef table =
        new RelationRef(rid("acct", "tbl", ResourceKind.RK_TABLE), "orders", ResourceKind.RK_TABLE);
    RelationRef view =
        new RelationRef(rid("acct", "view", ResourceKind.RK_VIEW), "orders", ResourceKind.RK_VIEW);
    when(tableRepository.listRefsByName("acct", "cat", "ns", Set.of("orders")))
        .thenReturn(List.of(table));
    when(viewRepository.listRefsByName("acct", "cat", "ns", Set.of("orders")))
        .thenReturn(List.of(view));

    var resolved = cache.resolveRelationRefsByName(catalogId, namespaceId, "orders");
    var cached = cache.resolveRelationRefsByName(catalogId, namespaceId, "orders");

    assertThat(resolved.tableId()).isEqualTo(table.id());
    assertThat(resolved.viewId()).isEqualTo(view.id());
    assertThat(resolved.isAmbiguous()).isTrue();
    assertThat(cached).isEqualTo(resolved);
    verify(tableRepository, times(1)).listRefsByName("acct", "cat", "ns", Set.of("orders"));
    verify(viewRepository, times(1)).listRefsByName("acct", "cat", "ns", Set.of("orders"));
  }

  private static ResourceId rid(String accountId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(accountId).setId(id).setKind(kind).build();
  }
}
