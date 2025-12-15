package ai.floedb.floecat.systemcatalog.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
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
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemCollationDef(BuiltinTestSupport.nr("pg.en_US"), "en-US", List.of(rule))),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.collationNode(ENGINE, "pg.en_US");

    var result = provider.compute(node, key, SystemCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "4141");
  }
}
