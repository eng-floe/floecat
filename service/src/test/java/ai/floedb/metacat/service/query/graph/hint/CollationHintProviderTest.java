package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.query.rpc.FloeCollationSpecific;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CollationHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void emitsCollationProperties() {

    var rule =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            null,
            null,
            null,
            null,
            null,
            FloeCollationSpecific.newBuilder().setCollcollate("blah").build(),
            Map.of("oid", "4141"));

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

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"4141\"")
        .contains("\"collcollate\":\"blah\"");
  }
}
