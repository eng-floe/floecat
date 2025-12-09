package ai.floedb.floecat.catalog.builtin.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.def.BuiltinCastDef;
import ai.floedb.floecat.catalog.builtin.def.BuiltinCastMethod;
import ai.floedb.floecat.catalog.builtin.hint.BuiltinCatalogHintProvider;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.catalog.utils.BuiltinTestSupport;
import ai.floedb.floecat.metagraph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CastHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void matchesCastSignatureAndMethod() {

    var rule = new EngineSpecificRule(ENGINE, "16.0", "", "", null, Map.of("oid", "7712"));

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

    var result = provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "7712");
  }
}
