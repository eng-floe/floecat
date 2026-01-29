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
import ai.floedb.floecat.systemcatalog.def.SystemCollationDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.utils.BuiltinTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CollationHintProviderTest {

  private static final String ENGINE = "floe-demo";
  private static final String COLLATION_PAYLOAD_TYPE = "builtin.systemcatalog.collation.properties";

  @Test
  void emitsCollationProperties() {

    var rule =
        new EngineSpecificRule(
            ENGINE,
            "16.0",
            "",
            COLLATION_PAYLOAD_TYPE,
            null,
            Map.of("collcollate", "blah", "oid", "4141"));

    var catalog =
        new SystemCatalogData(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(
                new SystemCollationDef(BuiltinTestSupport.nr("pg.en_US"), "en_US", List.of(rule))),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of());

    var provider = BuiltinTestSupport.providerFrom(ENGINE, catalog);
    var key = new EngineKey(ENGINE, "16.0");

    var node = BuiltinTestSupport.collationNode(ENGINE, "pg.en_US", "en_US");

    var result = provider.compute(node, key, COLLATION_PAYLOAD_TYPE, "cid").orElseThrow();
    assertThat(result.payloadType()).isEqualTo(COLLATION_PAYLOAD_TYPE);
    assertThat(result.payload()).isEmpty();
    assertThat(result.metadata()).containsEntry("oid", "4141");
  }
}
