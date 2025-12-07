package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.builtin.*;
import ai.floedb.floecat.query.rpc.FloeFunctionSpecific;
import ai.floedb.floecat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HintFingerprintTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void fingerprintChangesWhenRuleChanges() {

    var ruleA =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(1).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "1000"));

    var ruleB =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FloeFunctionSpecific.newBuilder().setPronamespace(1).build(),
            null,
            null,
            null,
            null,
            null,
            Map.of("oid", "2000"));

    var catalogA =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    BuiltinTestSupport.nr("pg.test"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(ruleA))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var catalogB =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    BuiltinTestSupport.nr("pg.test"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(ruleB))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var pA = BuiltinTestSupport.providerFrom(ENGINE, catalogA);
    var pB = BuiltinTestSupport.providerFrom(ENGINE, catalogB);

    var node = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.test");

    var fpA =
        pA.fingerprint(node, new EngineKey(ENGINE, "16.0"), BuiltinCatalogHintProvider.HINT_TYPE);
    var fpB =
        pB.fingerprint(node, new EngineKey(ENGINE, "16.0"), BuiltinCatalogHintProvider.HINT_TYPE);

    assertThat(fpA).isNotEqualTo(fpB);
  }
}
