package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.query.rpc.FloeFunctionSpecific;
import ai.floedb.floecat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FunctionHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void selectsCorrectRulePerOverload() {

    var rule4 =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(100).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "4444"));

    var rule8 =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(200).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "8888"));

    var catalog =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    BuiltinTestSupport.nr("pg.abs"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(rule4)),
                new BuiltinFunctionDef(
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
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var n4 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.abs");
    var n8 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int8"), "pg.int8", "pg.abs");

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(n4, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"4444\"");

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(n8, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"8888\"");
  }
}
