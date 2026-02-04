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

package ai.floedb.floecat.systemcatalog.graph;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.NamespaceNode;
import ai.floedb.floecat.query.rpc.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastDef;
import ai.floedb.floecat.systemcatalog.def.SystemCastMethod;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.def.SystemColumnDef;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemNamespaceDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.def.SystemViewDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.graph.model.SystemTableNode;
import ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider;
import ai.floedb.floecat.systemcatalog.provider.ServiceLoaderSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.testsupport.SystemCatalogTestProviders;
import ai.floedb.floecat.systemcatalog.util.EngineContext;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SystemNodeRegistryTest {

  private static final String FLOE_KIND = "floe-demo"; // main engine under test
  private static final String PG_KIND = "pg"; // alternate engine
  private static final String TEST_PAYLOAD_TYPE = "test.payload";
  private static final String HINT_ENGINE = "hint-engine";
  private static final String FUNCTION_HINT = "hint.function";
  private static final String OPERATOR_HINT = "hint.operator";
  private static final String TYPE_HINT = "hint.type";
  private static final String CAST_HINT = "hint.cast";
  private static final String COLLATION_HINT = "hint.collation";
  private static final String AGGREGATE_HINT = "hint.aggregate";

  // helper to build NameRef the same way pbtxt does
  private static NameRef nr(String full) {
    int idx = full.indexOf('.');
    if (idx < 0) {
      return NameRef.newBuilder().setName(full).build();
    }
    return NameRef.newBuilder()
        .setName(full.substring(idx + 1))
        .addPath(full.substring(0, idx))
        .build();
  }

  @Test
  void filtersByEngineKindAndVersion() {
    var registry = registryWithCatalogs();
    var nodeRegistry = registryWith(registry);

    // ---------------------------------
    // floe-demo / version 16.0
    // ---------------------------------
    var floe16 = nodeRegistry.nodesFor(FLOE_KIND, "16.0");

    assertThat(floe16.functions())
        .extracting(fn -> fn.displayName())
        .contains("pg_catalog.pg_only");

    assertThat(floe16.functions())
        .extracting(fn -> fn.displayName())
        .contains("pg_catalog.shared_fn")
        .doesNotContain("pg_catalog.pg_fn", "pg_catalog.pg_legacy");

    // ---------------------------------
    // floe-demo / version 17.0
    // ---------------------------------
    var floe17 = nodeRegistry.nodesFor(FLOE_KIND, "17.0");

    assertThat(floe17.functions())
        .extracting(fn -> fn.displayName())
        .contains("pg_catalog.pg_only");

    assertThat(floe17.functions())
        .extracting(fn -> fn.displayName())
        .doesNotContain("pg_catalog.pg_legacy");

    // ---------------------------------
    // pg / version 16.0   (alternate engine)
    // ---------------------------------
    var pg16 = nodeRegistry.nodesFor(PG_KIND, "16.0");

    assertThat(pg16.functions()).extracting(fn -> fn.displayName()).contains("pg_catalog.pg_fn");

    assertThat(pg16.functions())
        .extracting(fn -> fn.displayName())
        .doesNotContain("pg_catalog.pg_only");

    // ---------------------------------
    // Extract persisted rules in catalog
    // ---------------------------------
    var catalog = floe16.toCatalogData();

    assertThat(
            catalog.functions().stream()
                .filter(def -> def.name().equals(nr("pg_catalog.pg_only")))
                .findFirst()
                .orElseThrow()
                .engineSpecific())
        .singleElement()
        .extracting(EngineSpecificRule::engineKind)
        .isEqualTo(FLOE_KIND);

    assertThat(
            catalog.functions().stream()
                .filter(def -> def.name().equals(nr("pg_catalog.shared_fn")))
                .findFirst()
                .orElseThrow()
                .engineSpecific())
        .isEmpty();
  }

  @Test
  void missingEngineKindFails() {
    var registry = registryWithCatalogs();
    var nodeRegistry = registryWith(registry);

    assertThat(nodeRegistry.nodesFor("", "16.0").functions()).isEmpty();
  }

  @Test
  void missingEngineVersionMeansAnyVersion() {
    var registry = registryWithCatalogs();
    var nodeRegistry = registryWith(registry);

    assertThat(nodeRegistry.nodesFor(FLOE_KIND, "").functions())
        .extracting(fn -> fn.displayName())
        .contains(
            "pg_catalog.shared_fn",
            "pg_catalog.pg_legacy" // maxVersion applies only when version is known
            );
  }

  @Test
  void tablesAndViewsRespectVersionFilters() {
    SystemCatalogData catalog = catalogWithVersionedObjects();
    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(PG_KIND, catalog)));
    var registry = registryWith(defs);

    var nodesV1 = registry.nodesFor(PG_KIND, "1.0");
    assertThat(nodesV1.tableNames()).doesNotContainKey("custom.legacy_table");
    assertThat(nodesV1.viewNames()).containsKey("custom.preview_view");

    var nodesV2 = registry.nodesFor(PG_KIND, "2.0");
    assertThat(nodesV2.tableNames()).containsKey("custom.legacy_table");
    assertThat(nodesV2.viewNames()).doesNotContainKey("custom.preview_view");
  }

  @Test
  void blankVersionTreatsAsLegacyView() {
    SystemCatalogData catalog = catalogWithVersionedObjects();
    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(PG_KIND, catalog)));
    var registry = registryWith(defs);

    var nodes = registry.nodesFor(PG_KIND, "");
    assertThat(nodes.tableNames()).doesNotContainKey("custom.legacy_table");
    assertThat(nodes.viewNames()).containsKey("custom.preview_view");
  }

  @Test
  void unqualifiedTablesAreSkipped() {
    SystemNamespaceDef namespace =
        new SystemNamespaceDef(NameRefUtil.name("custom"), "custom", List.of());
    SystemTableDef qualified =
        new SystemTableDef(
            NameRefUtil.name("custom", "ok"),
            "ok",
            List.of(column("id")),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());
    SystemTableDef unqualified =
        new SystemTableDef(
            NameRefUtil.name("orphan"),
            "orphan",
            List.of(column("id")),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(namespace),
            List.of(qualified, unqualified),
            List.of(),
            List.of());

    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of("kind", catalog)));
    var nodes = registryWith(defs).nodesFor("kind", "");

    assertThat(nodes.tableNames()).containsKey("custom.ok");
    assertThat(nodes.tableNames()).doesNotContainKey("orphan");
  }

  @Test
  void missingNamespaceSkipsTable() {
    SystemTableDef missingNamespace =
        new SystemTableDef(
            NameRefUtil.name("missing", "table"),
            "table",
            List.of(column("id")),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(missingNamespace),
            List.of(),
            List.of());

    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of("kind", catalog)));
    var nodes = registryWith(defs).nodesFor("kind", "");

    assertThat(nodes.tableNames()).doesNotContainKey("missing.table");
  }

  @Test
  void engineKindIsLowercasedForCaching() {
    var registry = registryWithCatalogs();
    var nodeRegistry = registryWith(registry);

    var nodesLower = nodeRegistry.nodesFor("floe-demo", "16.0");
    var nodesUpper = nodeRegistry.nodesFor("FLOE-DEMO", "16.0");

    // Same logical catalog, same instance from cache
    assertThat(nodesLower).isSameAs(nodesUpper);
  }

  @Test
  void resourceIdBuildsStableIds() {
    var id = SystemNodeRegistry.resourceId("floe", ResourceKind.RK_FUNCTION, nr("pg_catalog.abs"));

    assertThat(id.getAccountId()).isEqualTo("_system");
    assertThat(id.getKind()).isEqualTo(ResourceKind.RK_FUNCTION);
    assertThat(id.getId()).isEqualTo("floe:pg_catalog.abs");
  }

  @Test
  void floecatInternalIncludesInformationSchemaTables() {
    var registry = registryWithCatalogs();
    var nodeRegistry = registryWith(registry);

    var tables = canonicalTableNames(nodeRegistry.nodesFor("", ""));

    assertThat(tables)
        .contains(
            "information_schema.tables",
            "information_schema.columns",
            "information_schema.schemata");
  }

  @Test
  void buildsFunctionNodeWithCorrectResourceIds() {
    var rule =
        new EngineSpecificRule(FLOE_KIND, "1.0", "", TEST_PAYLOAD_TYPE, new byte[0], Map.of());
    var fn =
        new SystemFunctionDef(
            nr("pg.abs"), List.of(nr("pg.int4")), nr("pg.int4"), false, false, List.of(rule));

    var catalog =
        new SystemCatalogData(
            List.of(fn),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    var registry =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes =
        registryWith(registry, new SystemCatalogTestProviders.VersionedTableProvider(FLOE_KIND))
            .nodesFor(FLOE_KIND, "1.0");

    var built = nodes.functions().get(0);

    assertThat(built.displayName()).isEqualTo("pg.abs");
    assertThat(built.argumentTypes().get(0).getId()).isEqualTo(FLOE_KIND + ":pg.int4");
    assertThat(built.returnType().getId()).isEqualTo(FLOE_KIND + ":pg.int4");
  }

  @Test
  void overloadedFunctions_haveDistinctResourceIds() {
    var int4 = new SystemTypeDef(nr("pg_catalog.int4"), "N", false, null, List.of());
    var text = new SystemTypeDef(nr("pg_catalog.text"), "S", false, null, List.of());

    // same function name, different arg types => overloads
    var fInt =
        new SystemFunctionDef(
            nr("pg_catalog.overloaded"),
            List.of(nr("pg_catalog.int4")),
            nr("pg_catalog.int4"),
            false,
            false,
            List.of());

    var fText =
        new SystemFunctionDef(
            nr("pg_catalog.overloaded"),
            List.of(nr("pg_catalog.text")),
            nr("pg_catalog.text"),
            false,
            false,
            List.of());

    var catalog =
        new SystemCatalogData(
            List.of(fInt, fText),
            List.of(),
            List.of(int4, text),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes = registryWith(defs).nodesFor(FLOE_KIND, "1.0");

    assertThat(nodes.functions()).hasSize(2);

    var ids = nodes.functions().stream().map(fn -> fn.id().getId()).toList();
    assertThat(ids)
        .containsExactlyInAnyOrder(
            FLOE_KIND + ":pg_catalog.overloaded(pg_catalog.int4)",
            FLOE_KIND + ":pg_catalog.overloaded(pg_catalog.text)");

    // sanity: both are for the same display name (safeName) but distinct IDs
    assertThat(nodes.functions())
        .extracting(fn -> fn.displayName())
        .containsOnly("pg_catalog.overloaded");
  }

  @Test
  void overloadedOperatorsProduceDistinctIds() {
    SystemOperatorDef opInt =
        new SystemOperatorDef(
            NameRefUtil.name("add"),
            NameRefUtil.name("pg_catalog.int4"),
            NameRefUtil.name("pg_catalog.int4"),
            NameRefUtil.name("pg_catalog.int4"),
            true,
            true,
            List.of());
    SystemOperatorDef opText =
        new SystemOperatorDef(
            NameRefUtil.name("add"),
            NameRefUtil.name("pg_catalog.text"),
            NameRefUtil.name("pg_catalog.text"),
            NameRefUtil.name("pg_catalog.text"),
            true,
            true,
            List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(opInt, opText),
            List.of(
                new SystemTypeDef(NameRefUtil.name("pg_catalog.int4"), "I", false, null, List.of()),
                new SystemTypeDef(
                    NameRefUtil.name("pg_catalog.text"), "T", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes = registryWith(defs).nodesFor(FLOE_KIND, "1.0");

    var ids = nodes.operators().stream().map(node -> node.id().getId()).toList();
    assertThat(ids)
        .containsExactlyInAnyOrder(
            FLOE_KIND + ":add(pg_catalog.int4,pg_catalog.int4)->pg_catalog.int4",
            FLOE_KIND + ":add(pg_catalog.text,pg_catalog.text)->pg_catalog.text");
  }

  @Test
  void aggregatesWithDifferentSignaturesProduceDistinctIds() {
    SystemAggregateDef aggSum =
        new SystemAggregateDef(
            NameRefUtil.name("sum"),
            List.of(NameRefUtil.name("pg_catalog.int4")),
            NameRefUtil.name("pg_catalog.int4"),
            NameRefUtil.name("pg_catalog.int4"),
            List.of());
    SystemAggregateDef aggCount =
        new SystemAggregateDef(
            NameRefUtil.name("sum"),
            List.of(NameRefUtil.name("pg_catalog.text")),
            NameRefUtil.name("pg_catalog.int4"),
            NameRefUtil.name("pg_catalog.int4"),
            List.of());
    SystemAggregateDef aggStateDifferent =
        new SystemAggregateDef(
            NameRefUtil.name("sum"),
            List.of(NameRefUtil.name("pg_catalog.int4")),
            NameRefUtil.name("pg_catalog.text"),
            NameRefUtil.name("pg_catalog.int4"),
            List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(
                new SystemTypeDef(NameRefUtil.name("pg_catalog.int4"), "I", false, null, List.of()),
                new SystemTypeDef(
                    NameRefUtil.name("pg_catalog.text"), "T", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(aggSum, aggCount, aggStateDifferent),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes = registryWith(defs).nodesFor(FLOE_KIND, "1.0");

    assertThat(nodes.aggregates())
        .extracting(node -> node.id().getId())
        .containsExactlyInAnyOrder(
            FLOE_KIND + ":sum(pg_catalog.int4)->pg_catalog.int4[pg_catalog.int4]",
            FLOE_KIND + ":sum(pg_catalog.text)->pg_catalog.int4[pg_catalog.int4]",
            FLOE_KIND + ":sum(pg_catalog.int4)->pg_catalog.int4[pg_catalog.text]");
  }

  @Test
  void collationsWithDifferentLocalesKeepDistinctIdentities() {
    SystemCollationDef enColl =
        new SystemCollationDef(NameRefUtil.name("locale"), "en_US", List.of());
    SystemCollationDef frColl =
        new SystemCollationDef(NameRefUtil.name("locale"), "fr_FR", List.of());

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(enColl, frColl),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes = registryWith(defs).nodesFor(FLOE_KIND, "1.0");

    assertThat(nodes.collations())
        .extracting(node -> node.id().getId())
        .containsExactlyInAnyOrder(FLOE_KIND + ":locale.en_US", FLOE_KIND + ":locale.fr_FR");
  }

  @Test
  void providerDefinitionsCanVaryByVersion() {
    var nodeRegistry =
        registryWith(
            registryWithCatalogs(),
            new SystemCatalogTestProviders.VersionedTableProvider(FLOE_KIND));

    var tables16 = canonicalTableNames(nodeRegistry.nodesFor(FLOE_KIND, "16.0"));
    var tables17 = canonicalTableNames(nodeRegistry.nodesFor(FLOE_KIND, "17.0"));
    var table16 = FLOE_KIND + ".versioned_16.0";
    var table17 = FLOE_KIND + ".versioned_17.0";

    assertThat(tables16).contains(table16);
    assertThat(tables17).contains(table17);
    assertThat(tables16).doesNotContain(table17);
    assertThat(tables17).doesNotContain(table16);
  }

  @Test
  void providerDefinitionsSkippedWhenHeadersMissing() {
    var provider = new SystemCatalogTestProviders.VersionedTableProvider(FLOE_KIND);
    var nodeRegistry = registryWith(registryWithCatalogs(), provider);

    var tables = canonicalTableNames(nodeRegistry.nodesFor("", "16.0"));
    assertThat(tables).doesNotContain(FLOE_KIND + ".versioned_16.0");
    assertThat(provider.invocationCount()).isEqualTo(0);
  }

  @Test
  void providerDefinitionsCachedPerVersion() {
    var provider = new SystemCatalogTestProviders.VersionedTableProvider(FLOE_KIND);
    var nodeRegistry = registryWith(registryWithCatalogs(), provider);

    nodeRegistry.nodesFor(FLOE_KIND, "16.0");
    nodeRegistry.nodesFor(FLOE_KIND, "16.0");

    assertThat(provider.invocationCount()).isEqualTo(1);
  }

  @Test
  void systemNodesRetainEngineHints() {
    NameRef int4 = NameRefUtil.name("pg_catalog", "int4");
    NameRef text = NameRefUtil.name("pg_catalog", "text");

    EngineSpecificRule functionRule = hintRule(FUNCTION_HINT);
    EngineSpecificRule operatorRule = hintRule(OPERATOR_HINT);
    EngineSpecificRule typeRule = hintRule(TYPE_HINT);
    EngineSpecificRule castRule = hintRule(CAST_HINT);
    EngineSpecificRule collationRule = hintRule(COLLATION_HINT);
    EngineSpecificRule aggregateRule = hintRule(AGGREGATE_HINT);

    SystemFunctionDef function =
        new SystemFunctionDef(
            NameRefUtil.name("pg_catalog", "hint_fn"),
            List.of(int4),
            int4,
            false,
            false,
            List.of(functionRule));
    SystemOperatorDef operator =
        new SystemOperatorDef(
            NameRefUtil.name("pg_catalog", "hint_op"),
            int4,
            int4,
            int4,
            false,
            false,
            List.of(operatorRule));
    SystemTypeDef type =
        new SystemTypeDef(
            NameRefUtil.name("pg_catalog", "hint_type"), "N", false, null, List.of(typeRule));
    SystemTypeDef textType =
        new SystemTypeDef(
            NameRefUtil.name("pg_catalog", "text"), "T", false, null, List.of(typeRule));
    SystemCastDef cast =
        new SystemCastDef(
            NameRefUtil.name("pg_catalog", "text_cast"),
            int4,
            text,
            SystemCastMethod.EXPLICIT,
            List.of(castRule));
    SystemCollationDef collation =
        new SystemCollationDef(
            NameRefUtil.name("pg_catalog", "hint_collation"), "en_US", List.of(collationRule));
    SystemAggregateDef aggregate =
        new SystemAggregateDef(
            NameRefUtil.name("pg_catalog", "hint_agg"),
            List.of(int4),
            int4,
            int4,
            List.of(aggregateRule));

    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(function),
            List.of(operator),
            List.of(type, textType),
            List.of(cast),
            List.of(collation),
            List.of(aggregate),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(HINT_ENGINE, catalog)));
    var nodes = registryWith(defs).nodesFor(HINT_ENGINE, "1.0");

    assertHasHint(nodes.functions(), FUNCTION_HINT);
    assertHasHint(nodes.operators(), OPERATOR_HINT);
    assertHasHint(nodes.types(), TYPE_HINT);
    assertHasHint(nodes.casts(), CAST_HINT);
    assertHasHint(nodes.collations(), COLLATION_HINT);
    assertHasHint(nodes.aggregates(), AGGREGATE_HINT);
  }

  @Test
  void registryEngineSpecific_hintsLayeredAndOverridden() {
    var internalRule =
        new EngineSpecificRule(
            "", "", "", "dict.shared", new byte[] {1}, Map.of("dict_name", "shared"));
    var pluginRule =
        new EngineSpecificRule(
            FLOE_KIND, "", "", "dict.shared", new byte[] {2}, Map.of("dict_name", "shared"));
    var overlayRule =
        new EngineSpecificRule(
            FLOE_KIND, "", "", "dict.shared", new byte[] {3}, Map.of("dict_name", "shared"));

    var catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(pluginRule));

    var registry =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var overlayProvider =
        new SystemCatalogTestProviders.RegistryHintProvider(FLOE_KIND, List.of(overlayRule));
    var nodeRegistry =
        new SystemNodeRegistry(
            registry, new TestInternalProvider(List.of(internalRule)), List.of(overlayProvider));

    var merged = nodeRegistry.nodesFor(FLOE_KIND, "16.0").catalogData().registryEngineSpecific();

    assertThat(merged).hasSize(1);
    assertThat(merged.get(0)).isEqualTo(overlayRule);
  }

  @Test
  void registryEngineSpecific_versionWindowsCoexist() {
    var baseRule =
        new EngineSpecificRule(
            FLOE_KIND, "", "15.999", "dict.shared", new byte[] {1}, Map.of("dict_name", "shared"));
    var nextRule =
        new EngineSpecificRule(
            FLOE_KIND, "16.0", "", "dict.shared", new byte[] {2}, Map.of("dict_name", "shared"));

    var registry = registryWithCatalogs();
    var provider =
        new SystemCatalogTestProviders.RegistryHintProvider(FLOE_KIND, List.of(baseRule, nextRule));
    var nodeRegistry = registryWith(registry, provider);

    var merged15 = nodeRegistry.nodesFor(FLOE_KIND, "15.0").catalogData().registryEngineSpecific();
    assertThat(merged15).hasSize(1);
    assertThat(merged15.get(0)).isEqualTo(baseRule);

    var merged16 = nodeRegistry.nodesFor(FLOE_KIND, "16.0").catalogData().registryEngineSpecific();
    assertThat(merged16).hasSize(1);
    assertThat(merged16.get(0)).isEqualTo(nextRule);
  }

  // -----------------------------------------------------
  // Test Catalog Setup
  // -----------------------------------------------------
  private static SystemDefinitionRegistry registryWithCatalogs() {

    var sharedRule = new EngineSpecificRule("", "", "", TEST_PAYLOAD_TYPE, new byte[0], Map.of());

    var floeRule =
        new EngineSpecificRule(FLOE_KIND, "16.0", "", TEST_PAYLOAD_TYPE, new byte[0], Map.of());

    var floeLegacyRule =
        new EngineSpecificRule(FLOE_KIND, "", "15.0", TEST_PAYLOAD_TYPE, new byte[0], Map.of());

    var pgRule = new EngineSpecificRule(PG_KIND, "", "", TEST_PAYLOAD_TYPE, new byte[0], Map.of());

    // TYPE
    var int4Name = nr("pg_catalog.int4");
    var int4 = new SystemTypeDef(int4Name, "N", false, null, List.of(sharedRule));

    // FUNCTIONS
    var sharedFnName = nr("pg_catalog.shared_fn");
    var pgOnlyName = nr("pg_catalog.pg_only");
    var pgLegacyName = nr("pg_catalog.pg_legacy");
    var pgFnName = nr("pg_catalog.pg_fn");

    var functions =
        List.of(
            new SystemFunctionDef(
                sharedFnName, List.of(int4Name), int4Name, false, false, List.of()),
            new SystemFunctionDef(
                pgOnlyName, List.of(int4Name), int4Name, false, false, List.of(floeRule)),
            new SystemFunctionDef(
                pgLegacyName, List.of(int4Name), int4Name, false, false, List.of(floeLegacyRule)),
            new SystemFunctionDef(
                pgFnName, List.of(int4Name), int4Name, false, false, List.of(pgRule)));

    var catalog =
        new SystemCatalogData(
            functions,
            List.of(),
            List.of(int4),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    return new SystemDefinitionRegistry(
        new StaticSystemCatalogProvider(
            Map.of(
                FLOE_KIND, catalog,
                PG_KIND, catalog)));
  }

  private static List<SystemObjectScannerProvider> extensionProviders(
      SystemObjectScannerProvider... extras) {
    return Stream.of(extras).toList();
  }

  private static List<String> canonicalTableNames(SystemNodeRegistry.BuiltinNodes nodes) {
    return nodes.catalogData().tables().stream().map(t -> NameRefUtil.canonical(t.name())).toList();
  }

  private static SystemCatalogData catalogWithVersionedObjects() {
    EngineSpecificRule tableRule =
        new EngineSpecificRule(PG_KIND, "2.0", "", "table.rule", null, Map.of());
    EngineSpecificRule viewRule =
        new EngineSpecificRule(PG_KIND, "", "1.5", "view.rule", null, Map.of());
    SystemNamespaceDef namespace =
        new SystemNamespaceDef(NameRefUtil.name("custom"), "custom", List.of());
    SystemTableDef table =
        new SystemTableDef(
            NameRefUtil.name("custom", "legacy_table"),
            "legacy_table",
            List.of(column("value")),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "legacy_scanner",
            List.of(tableRule));
    SystemViewDef view =
        new SystemViewDef(
            NameRefUtil.name("custom", "preview_view"),
            "preview_view",
            "select 1",
            "",
            List.of(column("value")),
            List.of(viewRule));

    return new SystemCatalogData(
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(namespace),
        List.of(table),
        List.of(view),
        List.of());
  }

  private static SystemColumnDef column(String name) {
    return new SystemColumnDef(
        name, NameRef.newBuilder().setName("INT").build(), false, 1, null, List.of());
  }

  private static void assertHasHint(List<? extends GraphNode> nodes, String payloadType) {
    boolean found =
        nodes.stream()
            .map(GraphNode::engineHints)
            .flatMap(map -> map.keySet().stream())
            .map(EngineHintKey::payloadType)
            .anyMatch(payloadType::equals);
    assertThat(found)
        .describedAs("engine hint %s must exist on at least one node", payloadType)
        .isTrue();
  }

  private static EngineSpecificRule hintRule(String payloadType) {
    return new EngineSpecificRule(
        HINT_ENGINE, "", "", payloadType, new byte[] {1}, Map.of("payload_type", payloadType));
  }

  private static FloecatInternalProvider internalProvider() {
    return new FloecatInternalProvider();
  }

  private static SystemNodeRegistry registryWith(
      SystemDefinitionRegistry defs, SystemObjectScannerProvider... extras) {
    return new SystemNodeRegistry(defs, internalProvider(), extensionProviders(extras));
  }

  private static final class TestInternalProvider implements SystemObjectScannerProvider {

    private final FloecatInternalProvider delegate = new FloecatInternalProvider();
    private final List<EngineSpecificRule> hints;

    private TestInternalProvider(List<EngineSpecificRule> hints) {
      this.hints = List.copyOf(hints);
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return delegate.definitions();
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return delegate.supportsEngine(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return delegate.supports(name, engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind, String engineVersion) {
      return supports(name, engineKind);
    }

    @Override
    public Optional<SystemObjectScanner> provide(
        String scannerId, String engineKind, String engineVersion) {
      return delegate.provide(scannerId, engineKind, engineVersion);
    }

    @Override
    public List<EngineSpecificRule> registryEngineSpecific(
        String engineKind, String engineVersion) {
      return hints;
    }
  }

  @Test
  void floecatInternalTablesExistForEngineWithoutPlugin() {
    ServiceLoaderSystemCatalogProvider loader = new ServiceLoaderSystemCatalogProvider();
    SystemDefinitionRegistry defs = new SystemDefinitionRegistry(loader);
    SystemNodeRegistry registry =
        new SystemNodeRegistry(defs, loader.internalProvider(), loader.providers());

    EngineContext ctx = EngineContext.of("pg", "");
    SystemEngineCatalog engineCatalog = defs.catalog(ctx);
    assertThat(engineCatalog.tables()).isNotEmpty();
    assertThat(engineCatalog.namespaces()).isNotEmpty();
    SystemNodeRegistry.BuiltinNodes nodes = registry.nodesFor(ctx);

    assertThat(nodes.tableNames())
        .containsKey("information_schema.tables")
        .containsKey("information_schema.columns")
        .containsKey("information_schema.schemata");
    assertThat(
            nodes.tableNodes().stream()
                .map(node -> node.id().getId())
                .filter(id -> id.startsWith("pg:information_schema"))
                .toList())
        .isNotEmpty();
  }

  @Test
  void namespaceBucketsAlwaysHaveEntries() {
    var registry = registryWith(registryWithCatalogs());
    SystemNodeRegistry.BuiltinNodes nodes = registry.nodesFor(FLOE_KIND, "16.0");

    for (NamespaceNode ns : nodes.namespaceNodes()) {
      assertThat(nodes.tablesByNamespace()).containsKey(ns.id());
      assertThat(nodes.viewsByNamespace()).containsKey(ns.id());
      assertThat(nodes.tablesByNamespace().get(ns.id())).isNotNull();
      assertThat(nodes.viewsByNamespace().get(ns.id())).isNotNull();
    }
  }

  @Test
  void tableWithoutNamespaceIsSkipped() {
    SystemTableDef orphan =
        new SystemTableDef(
            NameRefUtil.name("orphan"),
            "orphan",
            List.<SystemColumnDef>of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(orphan),
            List.of(),
            List.of());
    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of("kind", catalog)));
    var nodes = registryWith(defs).nodesFor("kind", "");

    assertThat(nodes.tableNames()).doesNotContainKey("orphan");
  }

  @Test
  void tableWithMissingNamespaceIsSkipped() {
    SystemTableDef missingNamespace =
        new SystemTableDef(
            NameRefUtil.name("missing", "table"),
            "table",
            List.<SystemColumnDef>of(),
            TableBackendKind.TABLE_BACKEND_KIND_FLOECAT,
            "scanner",
            List.of());
    SystemCatalogData catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(), // no namespaces defined
            List.of(missingNamespace),
            List.of(),
            List.of());
    SystemDefinitionRegistry defs =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of("kind", catalog)));
    var nodes = registryWith(defs).nodesFor("kind", "");

    assertThat(nodes.tableNames()).doesNotContainKey("missing.table");
  }

  @Test
  void pluginTableOverridesInternalDefinition() {
    SystemDefinitionRegistry defs = registryWithCatalogs();
    SystemObjectScannerProvider provider =
        new SystemCatalogTestProviders.OverridingTableProvider(
            PG_KIND, NameRefUtil.name("information_schema", "tables"), "overridden_scanner");

    SystemNodeRegistry registry = registryWith(defs, provider);
    var nodes = registry.nodesFor(PG_KIND, "16.0");

    SystemTableDef overridden =
        nodes.catalogData().tables().stream()
            .filter(def -> NameRefUtil.canonical(def.name()).equals("information_schema.tables"))
            .findFirst()
            .orElseThrow();
    assertThat(overridden.scannerId()).isEqualTo("overridden_scanner");
    String scannerId = null;
    for (SystemTableNode node : nodes.tableNodes()) {
      if (!node.id().getId().endsWith("information_schema.tables")) {
        continue;
      }
      scannerId = ((SystemTableNode.FloeCatSystemTableNode) node).scannerId();
      break;
    }
    assertThat(scannerId).isEqualTo("overridden_scanner");
  }
}
