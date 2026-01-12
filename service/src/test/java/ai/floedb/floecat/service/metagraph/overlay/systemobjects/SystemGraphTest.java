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

package ai.floedb.floecat.service.metagraph.overlay.systemobjects;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.service.testsupport.FakeSystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for SystemGraph. */
class SystemGraphTest {

  private static final String ENGINE = "floedb";
  private static final String VERSION = "1.0";

  private SystemGraph systemGraph;
  private ResourceId systemCatalogId;
  private ResourceId wrongCatalogId;
  private ResourceId namespaceId;
  private ResourceId tableId;
  private ResourceId defaultTableId;

  @BeforeEach
  void setup() {
    FakeSystemNodeRegistry registry = new FakeSystemNodeRegistry();

    NameRef nsName = NameRefUtil.name("pg_catalog");
    NameRef tableName = NameRefUtil.name("pg_catalog", "pg_class");

    SystemCatalogData catalogData =
        new SystemCatalogData(
            List.of(), // functions
            List.of(), // operators
            List.of(), // types
            List.of(), // casts
            List.of(), // collations
            List.of(), // aggregates
            List.of(new SystemNamespaceDef(nsName, "pg_catalog", List.of())),
            List.of(
                new SystemTableDef(
                    tableName,
                    "pg_class",
                    List.of(),
                    TableBackendKind.FLOECAT,
                    "scanner",
                    List.of())),
            List.of() // views
            );

    registry.register(ENGINE, catalogData);
    registry.register(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, catalogData);

    systemGraph = new SystemGraph(registry, 16);

    systemCatalogId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId(ENGINE)
            .build();

    wrongCatalogId =
        ResourceId.newBuilder()
            .setAccountId("user")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("other")
            .build();

    namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(ENGINE + ":pg_catalog")
            .build();

    tableId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId(ENGINE + ":pg_catalog.pg_class")
            .build();
    defaultTableId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_TABLE)
            .setId(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG + ":pg_catalog.pg_class")
            .build();
  }

  @Test
  void listRelations_visibleFromAnyCatalog() {
    List<GraphNode> nodes = systemGraph.listRelations(wrongCatalogId, ENGINE, VERSION);
    assertThat(nodes).extracting(node -> node.displayName()).contains("pg_class");
  }

  @Test
  void listRelations_returnsRelationsForCorrectCatalog() {
    List<GraphNode> nodes = systemGraph.listRelations(systemCatalogId, ENGINE, VERSION);
    assertThat(nodes).hasSize(1);
    assertThat(nodes.get(0).id()).isEqualTo(tableId);
  }

  @Test
  void listRelationsInNamespace_returnsRelations() {
    List<GraphNode> nodes =
        systemGraph.listRelationsInNamespace(systemCatalogId, namespaceId, ENGINE, VERSION);
    assertThat(nodes).hasSize(1);
    assertThat(nodes.get(0).id()).isEqualTo(tableId);
  }

  @Test
  void listNamespaces_visibleFromAnyCatalog() {
    List<NamespaceNode> nodes = systemGraph.listNamespaces(wrongCatalogId, ENGINE, VERSION);
    assertThat(nodes).extracting(NamespaceNode::displayName).contains("pg_catalog");
  }

  @Test
  void listNamespaces_returnsNamespaces() {
    assertThat(systemGraph.listNamespaces(systemCatalogId, ENGINE, VERSION)).hasSize(1);
  }

  @Test
  void resolveTable_findsSystemTable() {
    NameRef ref = NameRefUtil.name("pg_catalog", "pg_class");
    assertThat(systemGraph.resolveTable(ref, ENGINE, VERSION)).contains(tableId);
  }

  @Test
  void resolveTable_returnsEmptyForUnknown() {
    NameRef ref = NameRefUtil.name("pg_catalog", "does_not_exist");
    assertThat(systemGraph.resolveTable(ref, ENGINE, VERSION)).isEmpty();
  }

  @Test
  void tableName_reverseLookupWorks() {
    assertThat(systemGraph.tableName(tableId, ENGINE, VERSION))
        .isPresent()
        .get()
        .satisfies(
            ref -> {
              assertThat(ref.getCatalog()).isEqualTo(ENGINE);
              assertThat(ref.getPathList()).containsExactly("pg_catalog");
              assertThat(ref.getName()).isEqualTo("pg_class");
            });
  }

  @Test
  void tableName_withoutEngineUsesFloecatDefaultCatalog() {
    assertThat(systemGraph.tableName(defaultTableId, "", ""))
        .isPresent()
        .get()
        .satisfies(
            ref ->
                assertThat(ref.getCatalog()).isEqualTo(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG));
  }

  @Test
  void tableName_withUppercaseEngineNormalizesCatalog() {
    assertThat(systemGraph.tableName(tableId, "FLOEDB", VERSION))
        .isPresent()
        .get()
        .satisfies(ref -> assertThat(ref.getCatalog()).isEqualTo("floedb"));
  }

  @Test
  void listCatalogs_returnsEngineCatalogs() {
    ResourceId defaultCatalogId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG)
            .build();

    assertThat(systemGraph.listCatalogs())
        .containsExactlyInAnyOrder(defaultCatalogId, systemCatalogId);
  }

  @Test
  void catalog_returnsCatalogNode() {
    assertThat(systemGraph.catalog(systemCatalogId, ENGINE, VERSION))
        .isPresent()
        .hasValueSatisfying(node -> assertThat(node.displayName()).isEqualTo(ENGINE));
  }
}
