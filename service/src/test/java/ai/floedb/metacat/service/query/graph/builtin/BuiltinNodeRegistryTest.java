package ai.floedb.metacat.service.query.graph.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinNodeRegistryTest {

  private static final String FLOE_KIND = "floe-demo"; // main engine under test
  private static final String PG_KIND = "pg"; // alternate engine

  @Test
  void filtersByEngineKindAndVersion() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    // ---------------------------------
    // floe-demo / version 16.0
    // ---------------------------------
    var floe16 = nodeRegistry.nodesFor(FLOE_KIND, "16.0");

    assertThat(floe16.functions()).extracting(fn -> fn.name()).contains("pg_catalog.pg_only");

    assertThat(floe16.functions())
        .extracting(fn -> fn.name())
        .contains("pg_catalog.shared_fn")
        .doesNotContain("pg_catalog.pg_fn", "pg_catalog.pg_legacy");

    // ---------------------------------
    // floe-demo / version 17.0
    // ---------------------------------
    var floe17 = nodeRegistry.nodesFor(FLOE_KIND, "17.0");

    assertThat(floe17.functions()).extracting(fn -> fn.name()).contains("pg_catalog.pg_only");

    assertThat(floe17.functions())
        .extracting(fn -> fn.name())
        .doesNotContain("pg_catalog.pg_legacy");

    // ---------------------------------
    // pg / version 16.0   (alternate engine)
    // ---------------------------------
    var pg16 = nodeRegistry.nodesFor(PG_KIND, "16.0");

    assertThat(pg16.functions()).extracting(fn -> fn.name()).contains("pg_catalog.pg_fn");

    assertThat(pg16.functions()).extracting(fn -> fn.name()).doesNotContain("pg_catalog.pg_only");

    // ---------------------------------
    // Extract persisted rules in catalog
    // ---------------------------------
    var catalog = floe16.toCatalogData();

    assertThat(
            catalog.functions().stream()
                .filter(def -> def.name().equals("pg_catalog.pg_only"))
                .findFirst()
                .orElseThrow()
                .engineSpecific())
        .singleElement()
        .extracting(EngineSpecificRule::engineKind)
        .isEqualTo(FLOE_KIND);

    assertThat(
            catalog.functions().stream()
                .filter(def -> def.name().equals("pg_catalog.shared_fn"))
                .findFirst()
                .orElseThrow()
                .engineSpecific())
        .isEmpty();
  }

  @Test
  void missingEngineKindFails() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor("", "16.0").functions()).isEmpty();
  }

  @Test
  void missingEngineVersionReturnsEmpty() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor(FLOE_KIND, "").functions()).isEmpty();
  }

  // -----------------------------------------------------
  // Test Catalog Setup
  // -----------------------------------------------------
  private static BuiltinDefinitionRegistry registryWithCatalogs() {

    var sharedRule = new EngineSpecificRule("", "", "", null, null, null, null, null, Map.of());

    var floeRule =
        new EngineSpecificRule(FLOE_KIND, "16.0", "", null, null, null, null, null, Map.of());

    var floeLegacyRule =
        new EngineSpecificRule(FLOE_KIND, "", "15.0", null, null, null, null, null, Map.of());

    var pgRule = new EngineSpecificRule(PG_KIND, "", "", null, null, null, null, null, Map.of());

    // TYPE: (name, category, isArray, elementType, rules)
    var int4 = new BuiltinTypeDef("pg_catalog.int4", "N", false, null, List.of(sharedRule));

    // FUNCTION: (name, args, returnType, isAggregate, isWindow, rules)
    var functions =
        List.of(
            new BuiltinFunctionDef(
                "pg_catalog.shared_fn",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                List.of()),
            new BuiltinFunctionDef(
                "pg_catalog.pg_only",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                List.of(floeRule)),
            new BuiltinFunctionDef(
                "pg_catalog.pg_legacy",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                List.of(floeLegacyRule)),
            new BuiltinFunctionDef(
                "pg_catalog.pg_fn",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                List.of(pgRule)));

    var catalog =
        new BuiltinCatalogData(
            functions, List.of(), List.of(int4), List.of(), List.of(), List.of());

    var loader =
        new StaticBuiltinCatalogLoader(
            Map.of(
                FLOE_KIND, catalog,
                PG_KIND, catalog));

    return new BuiltinDefinitionRegistry(loader);
  }

  private static final class StaticBuiltinCatalogLoader extends BuiltinCatalogLoader {
    private final Map<String, BuiltinCatalogData> catalogs;

    private StaticBuiltinCatalogLoader(Map<String, BuiltinCatalogData> catalogs) {
      this.catalogs = catalogs;
    }

    @Override
    public BuiltinCatalogData getCatalog(String engineKind) {
      BuiltinCatalogData data = catalogs.get(engineKind);
      if (data == null) {
        throw new BuiltinCatalogNotFoundException(engineKind);
      }
      return data;
    }
  }
}
