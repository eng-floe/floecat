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

class HintFingerprintTest {

  private static final String ENGINE = "floe-demo";

  @Test
  void fingerprintChangesWhenRuleChanges() {

    var ruleA =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            "builtin.systemcatalog.function.payload+json",
            null,
            Map.of("oid", "1000", "pronamespace", "1"));

    var ruleB =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            "builtin.systemcatalog.function.payload+json",
            null,
            Map.of("oid", "2000", "pronamespace", "2"));

    var catalogA =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
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
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var catalogB =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
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
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var pA = BuiltinTestSupport.providerFrom(ENGINE, catalogA);
    var pB = BuiltinTestSupport.providerFrom(ENGINE, catalogB);

    var node = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.test");

    var fpA =
        pA.fingerprint(
            node, new EngineKey(ENGINE, "16.0"), "builtin.systemcatalog.function.payload+json");
    var fpB =
        pB.fingerprint(
            node, new EngineKey(ENGINE, "16.0"), "builtin.systemcatalog.function.payload+json");

    assertThat(fpA).isNotEqualTo(fpB);
  }
}
