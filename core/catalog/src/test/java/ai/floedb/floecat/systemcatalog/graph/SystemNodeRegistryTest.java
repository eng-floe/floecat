package ai.floedb.floecat.systemcatalog.graph;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.provider.StaticSystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import java.util.List;
import java.util.Map;
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
    var nodeRegistry = new SystemNodeRegistry(registry);

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
    var nodeRegistry = new SystemNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor("", "16.0").functions()).isEmpty();
  }

  @Test
  void missingEngineVersionReturnsEmpty() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new SystemNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor(FLOE_KIND, "").functions()).isEmpty();
  }

  @Test
  void engineKindIsLowercasedForCaching() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new SystemNodeRegistry(registry);

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
    var nodes = new SystemNodeRegistry(registry).nodesFor(FLOE_KIND, "1.0");

    var built = nodes.functions().get(0);

    assertThat(built.displayName()).isEqualTo("pg.abs");
    assertThat(built.argumentTypes().get(0).getId()).isEqualTo(FLOE_KIND + ":pg.int4");
    assertThat(built.returnType().getId()).isEqualTo(FLOE_KIND + ":pg.int4");
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
}
