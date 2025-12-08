package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.catalog.builtin.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CollationHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void emitsCollationProperties() {

    var rule =
        new EngineSpecificRule(
            ENGINE, "16.0", "", "", null, Map.of("collcollate", "blah", "oid", "4141"));

    var catalog =
        new BuiltinCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new BuiltinCollationDef(BuiltinTestSupport.nr("pg.en_US"), "en-US", List.of(rule))),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.collationNode(ENGINE, "pg.en_US");

    var result = provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "4141");
  }
}
