package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.builtin.*;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MultiEngineIsolationTest {

  @Test
  void builtinIdsDoNotCollideAcrossEngines() {

    var pgRule =
        new EngineSpecificRule(
            "pg", "16.0", "", null, null, null, null, null, null, Map.of("oid", "111"));

    var floeRule =
        new EngineSpecificRule(
            "floe", "1.0", "", null, null, null, null, null, null, Map.of("oid", "222"));

    var catalogPG =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    BuiltinTestSupport.nr("pg.id"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(pgRule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var catalogFloe =
        new BuiltinCatalogData(
            List.of(
                new BuiltinFunctionDef(
                    BuiltinTestSupport.nr("pg.id"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(floeRule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var p1 = BuiltinTestSupport.providerFrom("pg", catalogPG);
    var p2 = BuiltinTestSupport.providerFrom("floe", catalogFloe);

    var nPG = BuiltinTestSupport.functionNode("pg", List.of("pg.int4"), "pg.int4", "pg.id");
    var nFloe = BuiltinTestSupport.functionNode("floe", List.of("pg.int4"), "pg.int4", "pg.id");

    var jsonPG =
        BuiltinTestSupport.json(
            p1.compute(
                nPG, new EngineKey("pg", "16.0"), BuiltinCatalogHintProvider.HINT_TYPE, "cid"));
    var jsonFloe =
        BuiltinTestSupport.json(
            p2.compute(
                nFloe, new EngineKey("floe", "1.0"), BuiltinCatalogHintProvider.HINT_TYPE, "cid"));

    assertThat(jsonPG).contains("\"oid\":\"111\"");
    assertThat(jsonFloe).contains("\"oid\":\"222\"");
  }
}
