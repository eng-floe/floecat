package ai.floedb.floecat.systemcatalog.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemAggregateDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
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
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemAggregateDef(
                    BuiltinTestSupport.nr("pg.sum"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.state"),
                    BuiltinTestSupport.nr("pg.int4"),
                    List.of(rule))),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.aggregateNode(ENGINE, "pg.sum", List.of("pg.int4"), "pg.int4");

    var result = provider.compute(node, key, SystemCatalogHintProvider.HINT_TYPE, "cid");
    var payload = result.payload();
    assertThat(result.contentType()).contains("json");
    assertThat(new String(payload)).contains("aggfinalextra");
    assertThat(result.metadata()).containsEntry("oid", "6006");
  }
}
