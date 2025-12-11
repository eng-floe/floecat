package ai.floedb.floecat.catalog.builtin.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.def.BuiltinAggregateDef;
import ai.floedb.floecat.catalog.builtin.hint.BuiltinCatalogHintProvider;
import ai.floedb.floecat.catalog.builtin.registry.BuiltinCatalogData;
import ai.floedb.floecat.catalog.common.engine.EngineSpecificRule;
import ai.floedb.floecat.catalog.utils.BuiltinTestSupport;
import ai.floedb.floecat.metagraph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregateHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void matchesAggregateSignature() {

    var rule =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            "json",
            "{\"aggfinalextra\":true}".getBytes(),
            Map.of("oid", "6006"));

    var catalog =
        new BuiltinCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new BuiltinAggregateDef(
                    BuiltinTestSupport.nr("pg.sum"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.state"),
                    BuiltinTestSupport.nr("pg.int4"),
                    List.of(rule))));

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.aggregateNode(ENGINE, "pg.sum", List.of("pg.int4"), "pg.int4");

    var result = provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid");
    var payload = result.payload();
    assertThat(result.contentType()).contains("json");
    assertThat(new String(payload)).contains("aggfinalextra");
    assertThat(result.metadata()).containsEntry("oid", "6006");
  }
}
