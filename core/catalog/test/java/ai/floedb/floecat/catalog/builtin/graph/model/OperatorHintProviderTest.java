package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.catalog.builtin.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OperatorHintProviderTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void matchesOperatorSignature() {

    var ruleLt = new EngineSpecificRule(ENGINE, "16.0", "", "", null, Map.of("oid", "9001"));

    var catalog =
        new BuiltinCatalogData(
            List.of(),
            List.of(
                new BuiltinOperatorDef(
                    BuiltinTestSupport.nr("pg.<"),
                    BuiltinTestSupport.nr("pg.int4"),
                    BuiltinTestSupport.nr("pg.int4"),
                    BuiltinTestSupport.nr("pg.bool"),
                    false,
                    false,
                    List.of(ruleLt))),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.operatorNode(ENGINE, "pg.<", "pg.int4", "pg.int4", "pg.bool");

    var result = provider.compute(node, key, BuiltinCatalogHintProvider.HINT_TYPE, "cid");
    assertThat(result.contentType()).contains("");
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "9001");
  }
}
