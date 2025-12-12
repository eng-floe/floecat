package ai.floedb.floecat.systemcatalog.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypeHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void emitsTypeProperties() {

    var rule =
        new EngineSpecificRule(
            ENGINE, "16.0", "", "", null, Map.of("oid", "1616", "typmodin", "12"));

    var catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(
                new SystemTypeDef(
                    BuiltinTestSupport.nr("pg.int4"), "N", false, null, List.of(rule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.typeNode(ENGINE, "pg.int4");

    var result = provider.compute(node, key, SystemCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "1616");
  }
}
