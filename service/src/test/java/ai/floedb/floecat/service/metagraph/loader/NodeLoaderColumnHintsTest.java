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

package ai.floedb.floecat.service.metagraph.loader;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.metagraph.hint.EngineHintMetadata;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NodeLoaderColumnHintsTest {

  private static final String ENGINE_KIND = "floedb";
  private static final String ENGINE_VERSION = "1.0";

  @Test
  void loadColumnHintsConvertsIdsStrings() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put(
        EngineHintMetadata.columnHintKey("floe.column+proto", 1L),
        EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, new byte[] {1}));
    props.put(
        EngineHintMetadata.columnHintKey("floe.column+proto", 2L),
        EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, new byte[] {2}));
    props.put("engine.hint.column.floe.column+proto.invalid", "junk"); // ignored

    Map<Long, Map<EngineHintKey, EngineHint>> hints = NodeLoader.relationHints(props).columnHints();
    assertThat(hints).containsKeys(1L, 2L).doesNotContainKey(0L);
    EngineHintKey key = new EngineHintKey(ENGINE_KIND, ENGINE_VERSION, "floe.column+proto");
    assertThat(hints.get(1L)).containsKey(key);
    assertThat(hints.get(2L)).containsKey(key);
    assertThat(hints.get(2L).get(key).payload()).containsExactly((byte) 2);
  }
}
