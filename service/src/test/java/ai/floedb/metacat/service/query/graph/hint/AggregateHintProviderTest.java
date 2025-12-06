package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.query.rpc.FloeAggregateSpecific;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
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
            null,
            null,
            null,
            null,
            FloeAggregateSpecific.newBuilder().setAggfinalextra(true).build(),
            null,
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

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"6006\"")
        .contains("\"aggfinalextra\":\"true\"");
  }
}
