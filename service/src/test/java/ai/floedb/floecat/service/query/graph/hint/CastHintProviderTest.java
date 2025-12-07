package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CastHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void matchesCastSignatureAndMethod() {

    var rule =
        new EngineSpecificRule(
            ENGINE, "16.0", "", null, null, null, null, null, null, Map.of("oid", "7712"));

    var catalog =
        new BuiltinCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new BuiltinCastDef(
                    BuiltinTestSupport.nr("pg.cast"),
                    BuiltinTestSupport.nr("pg.int4"),
                    BuiltinTestSupport.nr("pg.text"),
                    BuiltinCastMethod.EXPLICIT,
                    List.of(rule))),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.castNode(ENGINE, "pg.cast", "pg.int4", "pg.text", "explicit");

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"7712\"");
  }
}
