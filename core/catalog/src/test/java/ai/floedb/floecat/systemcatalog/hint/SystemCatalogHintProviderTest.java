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

package ai.floedb.floecat.systemcatalog.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.systemcatalog.def.SystemFunctionDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class SystemCatalogHintProviderTest {

  private static final String ENGINE = "floe-demo";
  private static final String HINT_ONE = "function.one";
  private static final String HINT_TWO = "function.two";

  @Test
  void computeReturnsHintsPerPayloadType() {
    var ruleOne =
        new EngineSpecificRule(ENGINE, "16.0", "", HINT_ONE, new byte[] {1}, Map.of("hint", "one"));
    var ruleTwo =
        new EngineSpecificRule(ENGINE, "16.0", "", HINT_TWO, new byte[] {2}, Map.of("hint", "two"));

    var catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.test"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(ruleOne, ruleTwo))),
            List.of(),
            List.of(
                new SystemTypeDef(BuiltinTestSupport.nr("pg.int4"), "N", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var node = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.test");
    var key = new EngineKey(ENGINE, "16.0");

    var hintOne = provider.compute(node, key, HINT_ONE, "cid").orElseThrow();
    assertThat(hintOne.payloadType()).isEqualTo(HINT_ONE);
    assertThat(hintOne.metadata()).containsEntry("hint", "one");

    var hintTwo = provider.compute(node, key, HINT_TWO, "cid").orElseThrow();
    assertThat(hintTwo.payloadType()).isEqualTo(HINT_TWO);
    assertThat(hintTwo.metadata()).containsEntry("hint", "two");

    var fallback = provider.compute(node, key, "missing", "cid");
    assertThat(fallback).isEmpty();

    assertThat(provider.supports(GraphNodeKind.FUNCTION, HINT_ONE)).isTrue();
    assertThat(provider.supports(GraphNodeKind.FUNCTION, "missing")).isFalse();
  }

  @Test
  void lastEngineSpecificRuleWinsForPayloadType() {
    var ruleA =
        new EngineSpecificRule(ENGINE, "16.0", "", HINT_ONE, new byte[] {1}, Map.of("oid", "100"));
    var ruleB =
        new EngineSpecificRule(ENGINE, "16.0", "", HINT_ONE, new byte[] {2}, Map.of("oid", "200"));

    var catalog =
        new SystemCatalogData(
            List.of(
                new SystemFunctionDef(
                    BuiltinTestSupport.nr("pg.test"),
                    List.of(BuiltinTestSupport.nr("pg.int4")),
                    BuiltinTestSupport.nr("pg.int4"),
                    false,
                    false,
                    List.of(ruleA, ruleB))),
            List.of(),
            List.of(
                new SystemTypeDef(BuiltinTestSupport.nr("pg.int4"), "N", false, null, List.of())),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var node = BuiltinTestSupport.functionNode(ENGINE, List.of("pg.int4"), "pg.int4", "pg.test");
    var hint = provider.compute(node, new EngineKey(ENGINE, "16.0"), HINT_ONE, "cid").orElseThrow();
    assertThat(hint.metadata()).containsEntry("oid", "200");
  }
}
