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
import ai.floedb.floecat.metagraph.model.FunctionNode;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeOrigin;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.service.testsupport.FakeSystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry.BuiltinNodes;
import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
                    List.<SystemColumnDef>of(),
                    TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
                    "scanner",
                    List.of())),
            List.of() // views
            ,
            List.of());

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
    assertThat(nodes).extracting(GraphNode::id).contains(tableId);
    assertThat(nodes).hasSizeGreaterThanOrEqualTo(1);
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
    assertThat(systemGraph.listNamespaces(systemCatalogId, ENGINE, VERSION))
        .extracting(NamespaceNode::displayName)
        .contains("information_schema");
  }

  @Test
  void pluginOverridesInformationSchemaDefinitions() {
    FakeSystemNodeRegistry registry =
        new FakeSystemNodeRegistry(new PluginInformationSchemaProvider());

    SystemCatalogData overrideCatalog =
        new SystemCatalogData(
            List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(), List.of());

    registry.register(ENGINE, overrideCatalog);

    SystemGraph customGraph = new SystemGraph(registry, 16);

    ResourceId overrideCatalogId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId(ENGINE)
            .build();

    assertThat(customGraph.listRelations(overrideCatalogId, ENGINE, VERSION))
        .extracting(GraphNode::displayName)
        .contains("tables_override", "plugin_table")
        .doesNotContain("tables");

    assertThat(
            customGraph.resolveTable(
                NameRefUtil.name("information_schema", "tables"), ENGINE, VERSION))
        .isPresent();

    assertThat(
            customGraph.resolveTable(
                NameRefUtil.name("information_schema", "plugin_table"), ENGINE, VERSION))
        .isPresent();
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

  @Test
  void functionsWithUnqualifiedDisplayNameStillLandInNamespace() {
    String engineKind = "stub-engine";
    String engineVersion = "1.0";
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_CATALOG)
            .setId(engineKind)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
            .setKind(ResourceKind.RK_NAMESPACE)
            .setId(engineKind + ":pg_catalog")
            .build();

    NamespaceNode namespaceNode =
        new NamespaceNode(
            namespaceId,
            1,
            Instant.EPOCH,
            catalogId,
            List.of("pg_catalog"),
            "pg_catalog",
            GraphNodeOrigin.SYSTEM,
            Map.of(),
            Map.of());

    FunctionNode functionNode =
        new FunctionNode(
            ResourceId.newBuilder()
                .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
                .setKind(ResourceKind.RK_FUNCTION)
                .setId(engineKind + ":pg_catalog.short_name")
                .build(),
            1,
            Instant.EPOCH,
            engineVersion,
            namespaceId,
            "short_name",
            List.of(),
            ResourceId.newBuilder()
                .setAccountId(SystemNodeRegistry.SYSTEM_ACCOUNT)
                .setKind(ResourceKind.RK_TYPE)
                .setId(engineKind + ":pg_catalog.int4")
                .build(),
            false,
            false,
            Map.of());

    SystemFunctionDef functionDef =
        new SystemFunctionDef(
            NameRefUtil.name("pg_catalog", "short_name"),
            List.of(),
            NameRefUtil.name("pg_catalog", "int4"),
            false,
            false,
            List.of());

    SystemCatalogData catalogData =
        new SystemCatalogData(
            List.of(functionDef),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemNamespaceDef(NameRefUtil.name("pg_catalog"), "pg_catalog", List.of())),
            List.of(),
            List.of(),
            List.of());

    SystemNodeRegistry.BuiltinNodes nodes =
        new SystemNodeRegistry.BuiltinNodes(
            engineKind,
            engineVersion,
            "fp",
            List.of(functionNode),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespaceNode),
            List.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of(NameRefUtil.canonical(NameRefUtil.name("pg_catalog")), namespaceId),
            catalogData);

    SystemGraph graph = new SystemGraph(new StubSystemNodeRegistry(nodes), 16);

    List<FunctionNode> functions = graph.listFunctions(namespaceId, engineKind, engineVersion);
    assertThat(functions).hasSize(1);
    assertThat(functions.get(0).displayName()).isEqualTo("short_name");
  }

  private static final class PluginInformationSchemaProvider
      implements SystemObjectScannerProvider {

    private final SystemNamespaceDef namespace =
        new SystemNamespaceDef(
            NameRefUtil.name("information_schema"), "information_schema", List.of());

    private final SystemTableDef tablesOverride =
        new SystemTableDef(
            NameRefUtil.name("information_schema", "tables"),
            "tables_override",
            List.of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());

    private final SystemTableDef pluginTable =
        new SystemTableDef(
            NameRefUtil.name("information_schema", "plugin_table"),
            "plugin_table",
            List.of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "plugin-scanner",
            List.of());

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of(namespace, tablesOverride, pluginTable);
    }

    @Override
    public List<SystemObjectDef> definitions(String engineKind, String engineVersion) {
      return definitions();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return ENGINE.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return supportsEngine(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind, String engineVersion) {
      return supports(name, engineKind);
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.empty();
    }
  }

  private static final class StubSystemNodeRegistry extends SystemNodeRegistry {

    private final BuiltinNodes nodes;

    private StubSystemNodeRegistry(BuiltinNodes nodes) {
      super(
          new SystemDefinitionRegistry(new StubSystemCatalogProvider()),
          new StubSystemObjectScannerProvider(),
          List.of());
      this.nodes = nodes;
    }

    @Override
    public BuiltinNodes nodesFor(EngineContext ctx) {
      return nodes;
    }
  }

  private static final class StubSystemCatalogProvider implements SystemCatalogProvider {

    @Override
    public SystemEngineCatalog load(EngineContext ctx) {
      return SystemEngineCatalog.from(
          EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, SystemCatalogData.empty());
    }

    @Override
    public List<String> engineKinds() {
      return List.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG);
    }
  }

  private static final class StubSystemObjectScannerProvider
      implements SystemObjectScannerProvider {

    @Override
    public List<SystemObjectDef> definitions() {
      return List.of();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return true;
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return true;
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return Optional.empty();
    }

    @Override
    public List<SystemObjectDef> definitions(String engineKind, String engineVersion) {
      return definitions();
    }
  }
}
