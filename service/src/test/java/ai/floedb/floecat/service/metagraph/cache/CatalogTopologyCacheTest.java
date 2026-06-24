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
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.telemetry.TestObservability;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CatalogTopologyCacheTest {

  private NamespaceRepository namespaceRepository;
  private TableRepository tableRepository;
  private ViewRepository viewRepository;
  private CatalogTopologyCache cache;

  @BeforeEach
  void setUp() {
    namespaceRepository = mock(NamespaceRepository.class);
    tableRepository = mock(TableRepository.class);
    viewRepository = mock(ViewRepository.class);
    cache =
        new CatalogTopologyCache(
            namespaceRepository,
            tableRepository,
            viewRepository,
            new TestObservability(),
            100,
            100,
            15);
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

  private static ResourceId rid(String accountId, String id, ResourceKind kind) {
    return ResourceId.newBuilder().setAccountId(accountId).setId(id).setKind(kind).build();
  }
}
