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

package ai.floedb.floecat.systemcatalog.graph.model;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FunctionHintProviderTest {

  private static final String ENGINE = "floe-demo";
  private static final String FUNCTION_PAYLOAD_TYPE = "builtin.systemcatalog.function.properties";

  @Test
  void selectsCorrectRulePerOverload() {

    var rule4 =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FUNCTION_PAYLOAD_TYPE,
            null,
            Map.of("oid", "4444", "pronamespace", "100"));

    var rule8 =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            FUNCTION_PAYLOAD_TYPE,
            null,
            Map.of("oid", "8888", "pronamespace", "200"));

    var catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.abs"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(rule4)),
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.abs"),
                    List.of(BuiltinTestSupport.nr("pg.int8")),
                    BuiltinTestSupport.nr("pg.int8"),
                    false,
                    false,
                    List.of(rule8))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var n4 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.abs");
    var n8 = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int8"), "pg.int8", "pg.abs");

    var result = provider.compute(n4, key, FUNCTION_PAYLOAD_TYPE, "cid").orElseThrow();
    assertThat(result.payloadType()).isEqualTo(FUNCTION_PAYLOAD_TYPE);
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "4444");

    var result2 = provider.compute(n8, key, FUNCTION_PAYLOAD_TYPE, "cid").orElseThrow();
    assertThat(result2.payloadType()).isEqualTo(FUNCTION_PAYLOAD_TYPE);
    assertThat(result2.payload()).isEmpty();
    assertThat(result2.metadata()).containsEntry("oid", "8888");
  }
}
