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
import ai.floedb.floecat.metagraph.model.TableBackendKind;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTableDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SystemNodeRegistryTest {

  private static final String FLOE_KIND = "floe-demo"; // main engine under test
  private static final String PG_KIND = "pg"; // alternate engine

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
    var rule = new EngineSpecificRule(FLOE_KIND, "1.0", "", "", new byte[0], Map.of());
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
            List.of());
    var registry =
        new SystemDefinitionRegistry(new StaticSystemCatalogProvider(Map.of(FLOE_KIND, catalog)));
    var nodes =
        registryWith(registry, new VersionedTableProvider(FLOE_KIND)).nodesFor(FLOE_KIND, "1.0");

    var built = nodes.functions().get(0);

    assertThat(built.displayName()).isEqualTo("pg.abs");
    assertThat(built.argumentTypes().get(0).getId()).isEqualTo(FLOE_KIND + ":pg.int4");
    assertThat(built.returnType().getId()).isEqualTo(FLOE_KIND + ":pg.int4");
  }

  @Test
  void providerDefinitionsCanVaryByVersion() {
    var nodeRegistry = registryWith(registryWithCatalogs(), new VersionedTableProvider(FLOE_KIND));

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
    var provider = new VersionedTableProvider(FLOE_KIND);
    var nodeRegistry = registryWith(registryWithCatalogs(), provider);

    var tables = canonicalTableNames(nodeRegistry.nodesFor("", "16.0"));
    assertThat(tables).doesNotContain(FLOE_KIND + ".versioned_16.0");
    assertThat(provider.invocationCount()).isEqualTo(0);
  }

  @Test
  void providerDefinitionsCachedPerVersion() {
    var provider = new VersionedTableProvider(FLOE_KIND);
    var nodeRegistry = registryWith(registryWithCatalogs(), provider);

    nodeRegistry.nodesFor(FLOE_KIND, "16.0");
    nodeRegistry.nodesFor(FLOE_KIND, "16.0");

    assertThat(provider.invocationCount()).isEqualTo(1);
  }

  // -----------------------------------------------------
  // Test Catalog Setup
  // -----------------------------------------------------
  private static SystemDefinitionRegistry registryWithCatalogs() {

    var sharedRule = new EngineSpecificRule("", "", "", "", new byte[0], Map.of());

    var floeRule = new EngineSpecificRule(FLOE_KIND, "16.0", "", "", new byte[0], Map.of());

    var floeLegacyRule = new EngineSpecificRule(FLOE_KIND, "", "15.0", "", new byte[0], Map.of());

    var pgRule = new EngineSpecificRule(PG_KIND, "", "", "", new byte[0], Map.of());

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

  private static final class VersionedTableProvider implements SystemObjectScannerProvider {

    private final String engineKind;
    private final AtomicInteger definitionsCalled = new AtomicInteger();

    private VersionedTableProvider(String engineKind) {
      this.engineKind = engineKind;
    }

    @Override
    public List<SystemObjectDef> definitions() {
      return definitions(engineKind, "");
    }

    @Override
    public List<SystemObjectDef> definitions(String engineKind, String engineVersion) {
      definitionsCalled.incrementAndGet();
      return List.of(tableFor(engineKind, engineVersion));
    }

    @Override
    public boolean supportsEngine(String engineKind) {
      return this.engineKind.equals(engineKind);
    }

    @Override
    public boolean supports(NameRef name, String engineKind) {
      return this.engineKind.equals(engineKind);
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

    int invocationCount() {
      return definitionsCalled.get();
    }

    private SystemTableDef tableFor(String engineKind, String engineVersion) {
      String suffix = engineVersion == null || engineVersion.isEmpty() ? "default" : engineVersion;
      return new SystemTableDef(
          NameRefUtil.name(engineKind, "versioned_" + suffix),
          "versioned_" + suffix,
          List.of(),
          TableBackendKind.FLOECAT,
          "version-scanner",
          List.of());
    }
  }

  private static FloecatInternalProvider internalProvider() {
    return new FloecatInternalProvider();
  }

  private static SystemNodeRegistry registryWith(
      SystemDefinitionRegistry defs, SystemObjectScannerProvider... extras) {
    return new SystemNodeRegistry(defs, internalProvider(), extensionProviders(extras));
  }
}
