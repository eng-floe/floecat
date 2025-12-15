package ai.floedb.floecat.systemcatalog.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FunctionHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void selectsCorrectRulePerOverload() {

    var rule4 =
        new EngineSpecificRule(
            ENGINE, "16.0", "", "", null, Map.of("oid", "4444", "pronamespace", "100"));

    var rule8 =
        new EngineSpecificRule(
            ENGINE, "16.0", "", "", null, Map.of("oid", "8888", "pronamespace", "200"));

    var catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.abs"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(rule4)),
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.abs"),
                    List.of(BuiltinTestSupport.nr("pg.int8")),
                    BuiltinTestSupport.nr("pg.int8"),
                    false,
                    false,
                    List.of(rule8))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var n4 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.abs");
    var n8 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int8"), "pg.int8", "pg.abs");

    var result = provider.compute(n4, key, SystemCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "4444");

    var result2 = provider.compute(n8, key, SystemCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result2.contentType()).contains("");
    assertThat(result2.payload()).isEmpty();
    assertThat(result2.metadata()).containsEntry("oid", "8888");
  }
}
