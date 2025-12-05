package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.query.rpc.FloeTypeSpecific;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TypeHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void emitsTypeProperties() {

    var rule =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            null,
            null,
            null,
            FloeTypeSpecific.newBuilder().setTypmodin("12").build(),
            null,
            null,
            Map.of("oid", "1616"));

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

    assertThat(
            BuiltinTestSupport.json(
                provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid")))
        .contains("\"oid\":\"1616\"")
        .contains("\"typmodin\":\"12\"");
  }
}
