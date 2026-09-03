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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph.NamespaceRef;
import ai.floedb.floecat.scanner.spi.TopologyGraph.RelationRef;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NameResolverLightweightRefsTest {

  private NamespaceRepository namespaces;
  private TableRepository tables;
  private ViewRepository views;
  private NameResolver names;

  @BeforeEach
  void setUp() {
    namespaces = mock(NamespaceRepository.class);
    tables = mock(TableRepository.class);
    views = mock(ViewRepository.class);
    names = new NameResolver(mock(CatalogRepository.class), namespaces, tables, views);
  }

  @Test
  void selectedNamespacesUseExactPointerLookups() {
    ResourceId catalog = id("acct", "cat", ResourceKind.RK_CATALOG);
    Set<String> selected = Set.of("sales");
    NamespaceRef sales = new NamespaceRef(id("acct", "ns", ResourceKind.RK_NAMESPACE), "sales");
    when(namespaces.listRefsByName("acct", "cat", selected)).thenReturn(List.of(sales));

    assertThat(names.listNamespaceRefsByName(catalog, selected)).containsExactly(sales);

    verify(namespaces).listRefsByName("acct", "cat", selected);
    verify(namespaces, never()).listRefs("acct", "cat");
  }

  @Test
  void relationRefsCombinePointerOnlyTableAndViewListings() {
    ResourceId catalog = id("acct", "cat", ResourceKind.RK_CATALOG);
    ResourceId namespace = id("acct", "ns", ResourceKind.RK_NAMESPACE);
    RelationRef table =
        new RelationRef(
            id("acct", "table", ResourceKind.RK_TABLE), "orders", ResourceKind.RK_TABLE);
    RelationRef view =
        new RelationRef(id("acct", "view", ResourceKind.RK_VIEW), "orders_v", ResourceKind.RK_VIEW);
    when(tables.listRefs("acct", "cat", "ns")).thenReturn(List.of(table));
    when(views.listRefs("acct", "cat", "ns")).thenReturn(List.of(view));

    assertThat(names.listRelationRefs(catalog, namespace)).containsExactly(table, view);
  }

  private static ResourceId id(String account, String value, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(account).setId(value).setKind(kind).build();
  }
}
