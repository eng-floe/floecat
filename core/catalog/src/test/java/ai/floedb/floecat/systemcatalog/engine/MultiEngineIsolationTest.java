/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.floedb.floecat.systemcatalog.engine;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MultiEngineIsolationTest {

  private static final String FUNCTION_PAYLOAD_TYPE = "builtin.systemcatalog.function.properties";

  @Test
  void builtinIdsDoNotCollideAcrossEngines() {

    var pgRule =
        new EngineSpecificRule("pg", "16.0", "", FUNCTION_PAYLOAD_TYPE, null, Map.of("oid", "111"));

    var floeRule =
        new EngineSpecificRule(
            "floe", "1.0", "", FUNCTION_PAYLOAD_TYPE, null, Map.of("oid", "222"));
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
            p1.compute(nPG, new EngineKey("pg", "16.0"), FUNCTION_PAYLOAD_TYPE, "cid")
                .orElseThrow()
                .metadata()
                .get("oid"))
        .isEqualTo("111");
    assertThat(
            p2.compute(nFloe, new EngineKey("floe", "1.0"), FUNCTION_PAYLOAD_TYPE, "cid")
                .orElseThrow()
                .metadata()
                .get("oid"))
        .isEqualTo("222");
  }
}
