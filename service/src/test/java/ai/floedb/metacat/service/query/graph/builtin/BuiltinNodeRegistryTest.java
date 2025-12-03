package ai.floedb.metacat.service.query.graph.builtin;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogLoader;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogNotFoundException;
import ai.floedb.metacat.catalog.builtin.BuiltinDefinitionRegistry;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.BuiltinTypeDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinNodeRegistryTest {

  @Test
  void filtersByEngineKindAndVersion() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);

    var pg16 = nodeRegistry.nodesFor("postgres", "16.0");
    assertThat(pg16.functions()).extracting(fn -> fn.name()).contains("pg_catalog.pg_only");
    assertThat(pg16.functions())
        .extracting(fn -> fn.name())
        .contains("pg_catalog.shared_fn")
        .doesNotContain("pg_catalog.floedb_fn", "pg_catalog.pg_legacy");

    var pg17 = nodeRegistry.nodesFor("postgres", "17.0");
    assertThat(pg17.functions()).extracting(fn -> fn.name()).contains("pg_catalog.pg_only");
    assertThat(pg17.functions()).extracting(fn -> fn.name()).doesNotContain("pg_catalog.pg_legacy");

    var floe16 = nodeRegistry.nodesFor("floedb", "16.0");
    assertThat(floe16.functions()).extracting(fn -> fn.name()).contains("pg_catalog.floedb_fn");
    assertThat(floe16.functions()).extracting(fn -> fn.name()).doesNotContain("pg_catalog.pg_only");
  }

  @Test
  void missingEngineKindFails() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);
    var nodes = nodeRegistry.nodesFor("", "16.0");
    assertThat(nodes.functions()).isEmpty();
  }

  @Test
  void missingEngineVersionReturnsEmpty() {
    var registry = registryWithCatalogs();
    var nodeRegistry = new BuiltinNodeRegistry(registry);
    var nodes = nodeRegistry.nodesFor("postgres", "");
    assertThat(nodes.functions()).isEmpty();
  }

  private static BuiltinDefinitionRegistry registryWithCatalogs() {
    var sharedRule = new EngineSpecificRule("", "", "", Map.of());
    var pgRule = new EngineSpecificRule("postgres", "16.0", "", Map.of());
    var pgLegacyRule = new EngineSpecificRule("postgres", "", "15.0", Map.of());
    var floeRule = new EngineSpecificRule("floedb", "", "", Map.of());
    var int4 = new BuiltinTypeDef("pg_catalog.int4", 23, "N", false, null, List.of(sharedRule));
    var functions =
        List.of(
            new BuiltinFunctionDef(
                "pg_catalog.shared_fn",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                true,
                true,
                List.of()),
            new BuiltinFunctionDef(
                "pg_catalog.pg_only",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                true,
                true,
                List.of(pgRule)),
            new BuiltinFunctionDef(
                "pg_catalog.pg_legacy",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                true,
                true,
                List.of(pgLegacyRule)),
            new BuiltinFunctionDef(
                "pg_catalog.floedb_fn",
                List.of("pg_catalog.int4"),
                "pg_catalog.int4",
                false,
                false,
                true,
                true,
                List.of(floeRule)));
    var catalog =
        new BuiltinCatalogData(
            "demo", functions, List.of(), List.of(int4), List.of(), List.of(), List.of());
    var loader =
        new StaticBuiltinCatalogLoader(Map.of("15.0", catalog, "16.0", catalog, "17.0", catalog));
    return new BuiltinDefinitionRegistry(loader);
  }

  private static final class StaticBuiltinCatalogLoader extends BuiltinCatalogLoader {
    private final Map<String, BuiltinCatalogData> catalogs;

    private StaticBuiltinCatalogLoader(Map<String, BuiltinCatalogData> catalogs) {
      this.catalogs = catalogs;
    }

    @Override
    public BuiltinCatalogData getCatalog(String engineVersion) {
      BuiltinCatalogData data = catalogs.get(engineVersion);
      if (data == null) {
        throw new BuiltinCatalogNotFoundException(engineVersion);
      }
      return data;
    }
  }
}
