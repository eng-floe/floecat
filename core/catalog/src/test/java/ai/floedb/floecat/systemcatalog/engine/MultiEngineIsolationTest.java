package ai.floedb.floecat.systemcatalog.engine;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.hint.SystemCatalogHintProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MultiEngineIsolationTest {

  @Test
  void builtinIdsDoNotCollideAcrossEngines() {

    var pgRule = new EngineSpecificRule("pg", "16.0", "", "", null, Map.of("oid", "111"));

    var floeRule = new EngineSpecificRule("floe", "1.0", "", "", null, Map.of("oid", "222"));
    var catalogPG =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
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
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var catalogFloe =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
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
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var p1 = BuiltinTestSupport.providerFrom("pg", catalogPG);
    var p2 = BuiltinTestSupport.providerFrom("floe", catalogFloe);

    var nPG = BuiltinTestSupport.functionNode("pg", List.of("pg.int4"), "pg.int4", "pg.id");
    var nFloe = BuiltinTestSupport.functionNode("floe", List.of("pg.int4"), "pg.int4", "pg.id");

    assertThat(
            p1.compute(nPG, new EngineKey("pg", "16.0"), SystemCatalogHintProvider.HINT_TYPE, "cid")
                .metadata()
                .get("oid"))
        .isEqualTo("111");
    assertThat(
            p2.compute(
                    nFloe, new EngineKey("floe", "1.0"), SystemCatalogHintProvider.HINT_TYPE, "cid")
                .metadata()
                .get("oid"))
        .isEqualTo("222");
  }
}
