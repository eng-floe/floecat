package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogLoader;
import ai.floedb.metacat.catalog.builtin.BuiltinDefinitionRegistry;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.model.BuiltinFunctionNode;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinCatalogHintProviderTest {

  @Test
  void emitsJsonPayloadForMatchingRule() {
    var int4Rule = new EngineSpecificRule("postgres", "16.0", "", Map.of("oid", "1250"));
    var int8Rule = new EngineSpecificRule("postgres", "16.0", "", Map.of("oid", "1251"));
    var catalog =
        new BuiltinCatalogData(
            "demo",
            List.of(
                new BuiltinFunctionDef(
                    "pg_catalog.int4_abs",
                    List.of("pg_catalog.int4"),
                    "pg_catalog.int4",
                    false,
                    false,
                    true,
                    true,
                    List.of(int4Rule)),
                new BuiltinFunctionDef(
                    "pg_catalog.int4_abs",
                    List.of("pg_catalog.int8"),
                    "pg_catalog.int8",
                    false,
                    false,
                    true,
                    true,
                    List.of(int8Rule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    var registry = new BuiltinDefinitionRegistry(new StaticLoader(Map.of("demo", catalog)));
    var provider = new BuiltinCatalogHintProvider(registry);

    var engineKey = new EngineKey("postgres", "16.0");
    var nodeInt4 = functionNode(List.of("pg_catalog.int4"), "pg_catalog.int4");
    var hint =
        provider.compute(nodeInt4, engineKey, BuiltinCatalogHintProvider.HINT_TYPE, "correlation");
    assertThat(new String(hint.payload(), StandardCharsets.UTF_8)).contains("\"oid\":\"1250\"");

    var nodeInt8 = functionNode(List.of("pg_catalog.int8"), "pg_catalog.int8");
    var hintInt8 =
        provider.compute(nodeInt8, engineKey, BuiltinCatalogHintProvider.HINT_TYPE, "correlation");
    assertThat(new String(hintInt8.payload(), StandardCharsets.UTF_8)).contains("\"oid\":\"1251\"");
  }

  @Test
  void missingPropertiesReturnEmptyJson() {
    var catalog =
        new BuiltinCatalogData(
            "demo",
            List.of(
                new BuiltinFunctionDef(
                    "pg_catalog.int4_abs",
                    List.of("pg_catalog.int4"),
                    "pg_catalog.int4",
                    false,
                    false,
                    true,
                    true,
                    List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());
    var registry = new BuiltinDefinitionRegistry(new StaticLoader(Map.of("demo", catalog)));
    var provider = new BuiltinCatalogHintProvider(registry);
    var engineKey = new EngineKey("postgres", "16.0");
    var hint =
        provider.compute(
            functionNode(List.of("pg_catalog.int4"), "pg_catalog.int4"),
            engineKey,
            BuiltinCatalogHintProvider.HINT_TYPE,
            "correlation");
    assertThat(new String(hint.payload(), StandardCharsets.UTF_8)).isEqualTo("{}");
  }

  private static BuiltinFunctionNode functionNode(List<String> args, String returnType) {
    return new BuiltinFunctionNode(
        ResourceId.newBuilder()
            .setTenantId("_builtin")
            .setKindValue(ResourceKind.RK_FUNCTION_VALUE)
            .setId("demo:pg_catalog.int4_abs")
            .build(),
        1L,
        Instant.EPOCH,
        "demo",
        "pg_catalog.int4_abs",
        args,
        returnType,
        false,
        false,
        true,
        true,
        Map.of());
  }

  private static final class StaticLoader extends BuiltinCatalogLoader {
    private final Map<String, BuiltinCatalogData> catalogs;

    private StaticLoader(Map<String, BuiltinCatalogData> catalogs) {
      this.catalogs = catalogs;
    }

    @Override
    public BuiltinCatalogData getCatalog(String engineVersion) {
      BuiltinCatalogData data = catalogs.get(engineVersion);
      if (data == null) {
        throw new IllegalArgumentException("missing catalog");
      }
      return data;
    }
  }
}
