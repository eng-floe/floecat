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

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeCatalogRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeNamespaceRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeTableRepository;
import ai.floedb.floecat.service.testsupport.UserRepositoryTestSupport.FakeViewRepository;
import java.util.List;
import java.util.Set;
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
  void resolveRelationIdsBlankNameDoesNotPoisonValidSibling() {
    // Regression: a blank-name ref processed first must not cache an empty scope under the
    // name-independent (catalog + path) memo key and starve a valid sibling in the same batch.
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
}
