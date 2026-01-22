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
import ai.floedb.floecat.systemcatalog.def.SystemOperatorDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OperatorHintProviderTest {

  private static final String ENGINE = "floe-demo";
  private static final String OPERATOR_PAYLOAD_TYPE = "builtin.systemcatalog.operator.properties";

  @Test
  void matchesOperatorSignature() {

    var ruleLt =
        new EngineSpecificRule(
            ENGINE, "16.0", "", OPERATOR_PAYLOAD_TYPE, null, Map.of("oid", "9001"));

    var catalog =
        new SystemCatalogData(
            List.of(),
            List.of(
                new SystemOperatorDef(
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
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.operatorNode(ENGINE, "pg.<", "pg.int4", "pg.int4", "pg.bool");

    var result = provider.compute(node, key, OPERATOR_PAYLOAD_TYPE, "cid").orElseThrow();
    assertThat(result.payloadType()).isEqualTo(OPERATOR_PAYLOAD_TYPE);
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "9001");
  }
}
