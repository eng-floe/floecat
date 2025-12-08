package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.service.query.graph.model.EngineKey;
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
        new BuiltinCatalogData(
            List.of(),
            List.of(),
            List.of(
                new BuiltinTypeDef(
                    BuiltinTestSupport.nr("pg.int4"), "N", false, null, List.of(rule))),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.typeNode(ENGINE, "pg.int4");

    var result = provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "1616");
  }
}
