package ai.floedb.metacat.service.query.graph.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceKind;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinNodeRegistryTest {

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
    var nodeRegistry = new BuiltinNodeRegistry(registry);

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
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor("", "16.0").functions()).isEmpty();
  }

  @Test
  void missingEngineVersionReturnsEmpty() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    assertThat(nodeRegistry.nodesFor(FLOE_KIND, "").functions()).isEmpty();
  }

  @Test
  void resourceIdBuildsStableIds() {
    var id = BuiltinNodeRegistry.resourceId("floe", ResourceKind.RK_FUNCTION, nr("pg_catalog.abs"));

    assertThat(id.getTenantId()).isEqualTo("_builtin");
    assertThat(id.getKind()).isEqualTo(ResourceKind.RK_FUNCTION);
    assertThat(id.getId()).isEqualTo("floe:pg_catalog.abs");
  }

  @Test
  void buildsFunctionNodeWithCorrectResourceIds() {
    var rule =
        new EngineSpecificRule(FLOE_KIND, "1.0", "", null, null, null, null, null, null, Map.of());
    var fn =
        new BuiltinFunctionDef(
            nr("pg.abs"), List.of(nr("pg.int4")), nr("pg.int4"), false, false, List.of(rule));

    var catalog =
        new BuiltinCatalogData(List.of(fn), List.of(), List.of(), List.of(), List.of(), List.of());
    var registry = new BuiltinDefinitionRegistry(new StaticLoader(Map.of(FLOE_KIND, catalog)));
    var nodes = new BuiltinNodeRegistry(registry).nodesFor(FLOE_KIND, "1.0");

    var built = nodes.functions().get(0);

    assertThat(built.displayName()).isEqualTo("pg.abs");
    assertThat(built.argumentTypes().get(0).getId()).isEqualTo(FLOE_KIND + ":pg.int4");
    assertThat(built.returnType().getId()).isEqualTo(FLOE_KIND + ":pg.int4");
  }

  // -----------------------------------------------------
  // Test Catalog Setup
  // -----------------------------------------------------
  private static BuiltinDefinitionRegistry registryWithCatalogs() {

    var sharedRule =
        new EngineSpecificRule("", "", "", null, null, null, null, null, null, Map.of());

    var floeRule =
        new EngineSpecificRule(FLOE_KIND, "16.0", "", null, null, null, null, null, null, Map.of());

    var floeLegacyRule =
        new EngineSpecificRule(FLOE_KIND, "", "15.0", null, null, null, null, null, null, Map.of());

    var pgRule =
        new EngineSpecificRule(PG_KIND, "", "", null, null, null, null, null, null, Map.of());

    // TYPE
    var int4Name = nr("pg_catalog.int4");
    var int4 = new BuiltinTypeDef(int4Name, "N", false, null, List.of(sharedRule));

    // FUNCTIONS
    var sharedFnName = nr("pg_catalog.shared_fn");
    var pgOnlyName = nr("pg_catalog.pg_only");
    var pgLegacyName = nr("pg_catalog.pg_legacy");
    var pgFnName = nr("pg_catalog.pg_fn");

    var functions =
        List.of(
            new BuiltinFunctionDef(
                sharedFnName, List.of(int4Name), int4Name, false, false, List.of()),
            new BuiltinFunctionDef(
                pgOnlyName, List.of(int4Name), int4Name, false, false, List.of(floeRule)),
            new BuiltinFunctionDef(
                pgLegacyName, List.of(int4Name), int4Name, false, false, List.of(floeLegacyRule)),
            new BuiltinFunctionDef(
                pgFnName, List.of(int4Name), int4Name, false, false, List.of(pgRule)));

    var catalog =
        new BuiltinCatalogData(
            functions, List.of(), List.of(int4), List.of(), List.of(), List.of());

    var loader =
        new StaticLoader(
            Map.of(
                FLOE_KIND, catalog,
                PG_KIND, catalog));

    return new BuiltinDefinitionRegistry(loader);
  }

  private static final class StaticLoader extends BuiltinCatalogLoader {
    private final Map<String, BuiltinCatalogData> catalogs;

    private StaticLoader(Map<String, BuiltinCatalogData> catalogs) {
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
