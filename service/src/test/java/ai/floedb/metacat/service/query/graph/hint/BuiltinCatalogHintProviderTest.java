package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.BuiltinCatalogData;
import ai.floedb.metacat.catalog.builtin.BuiltinCatalogLoader;
import ai.floedb.metacat.catalog.builtin.BuiltinDefinitionRegistry;
import ai.floedb.metacat.catalog.builtin.BuiltinFunctionDef;
import ai.floedb.metacat.catalog.builtin.EngineSpecificRule;
import ai.floedb.metacat.common.rpc.NameRef;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.query.rpc.FloeFunctionSpecific;
import ai.floedb.metacat.service.query.graph.builtin.BuiltinNodeRegistry;
import ai.floedb.metacat.service.query.graph.model.BuiltinFunctionNode;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BuiltinCatalogHintProviderTest {

  private static final String ENGINE_KIND = "floe-demo";

  private static NameRef n(String name) {
    return NameRef.newBuilder().setName(name).build();
  }

  @Test
  void emitsJsonPayloadForMatchingRule() {

    var int4Rule =
        new EngineSpecificRule(
            ENGINE_KIND,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(11).setProisstrict(true).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "1250"));

    var int8Rule =
        new EngineSpecificRule(
            ENGINE_KIND,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(11).setProisstrict(true).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "1251"));

    var catalog =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    n("pg_catalog.int4_abs"),
                    List.of(n("pg_catalog.int4")),
                    n("pg_catalog.int4"),
                    false,
                    false,
                    List.of(int4Rule)),
                new BuiltinFunctionDef(
                    n("pg_catalog.int4_abs"),
                    List.of(n("pg_catalog.int8")),
                    n("pg_catalog.int8"),
                    false,
                    false,
                    List.of(int8Rule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var registry = new BuiltinDefinitionRegistry(new StaticLoader(Map.of(ENGINE_KIND, catalog)));
    var provider = new BuiltinCatalogHintProvider(registry);

    var engineKey = new EngineKey(ENGINE_KIND, "16.0");

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
            List.of(
                new BuiltinFunctionDef(
                    n("pg_catalog.int4_abs"),
                    List.of(n("pg_catalog.int4")),
                    n("pg_catalog.int4"),
                    false,
                    false,
                    List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var registry = new BuiltinDefinitionRegistry(new StaticLoader(Map.of(ENGINE_KIND, catalog)));
    var provider = new BuiltinCatalogHintProvider(registry);

    var engineKey = new EngineKey(ENGINE_KIND, "16.0");

    var hint =
        provider.compute(
            functionNode(List.of("pg_catalog.int4"), "pg_catalog.int4"),
            engineKey,
            BuiltinCatalogHintProvider.HINT_TYPE,
            "correlation");

    assertThat(new String(hint.payload(), StandardCharsets.UTF_8)).isEqualTo("{}");
  }

  private static BuiltinFunctionNode functionNode(List<String> args, String returnType) {
    List<ResourceId> argIds =
        args.stream()
            .map(
                a ->
                    BuiltinNodeRegistry.resourceId(
                        ENGINE_KIND, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(a).build()))
            .toList();

    ResourceId retId =
        BuiltinNodeRegistry.resourceId(
            ENGINE_KIND, ResourceKind.RK_TYPE, NameRef.newBuilder().setName(returnType).build());

    ResourceId fnId =
        BuiltinNodeRegistry.resourceId(
            ENGINE_KIND,
            ResourceKind.RK_FUNCTION,
            NameRef.newBuilder().setName("int4_abs").addPath("pg_catalog").build());

    return new BuiltinFunctionNode(
        fnId,
        1L,
        Instant.EPOCH,
        "16.0",
        "pg_catalog.int4_abs",
        argIds,
        retId,
        false,
        false,
        Map.of());
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
        throw new IllegalArgumentException("missing catalog");
      }
      return data;
    }
  }
}
