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

package ai.floedb.floecat.metagraph.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineHintMetadataTest {

  private static final String ENGINE_KIND = "floedb";
  private static final String ENGINE_VERSION = "1.0";
  private static final String PAYLOAD_TYPE = "floe.relation+proto";

  @Test
  void encodeValueRoundtrips() {
    byte[] payload = new byte[] {1, 2, 3};
    String encoded = EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, payload);

    EngineHintMetadata.DecodedValue decoded = EngineHintMetadata.decodeValue(encoded).orElseThrow();
    assertThat(decoded.engineKind()).isEqualTo(ENGINE_KIND);
    assertThat(decoded.engineVersion()).isEqualTo(ENGINE_VERSION);
    assertThat(decoded.payload()).containsExactly(payload);
  }

  @Test
  void hintsFromPropertiesIncludesPayload() {
    byte[] payload = new byte[] {5, 6, 7};
    String key = EngineHintMetadata.tableHintKey(PAYLOAD_TYPE);
    String value = EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, payload);

    Map<String, String> props = new LinkedHashMap<>();
    props.put(key, value);

    Map<EngineHintKey, EngineHint> hints = EngineHintMetadata.hintsFromProperties(props);
    EngineHintKey hintKey = new EngineHintKey(ENGINE_KIND, ENGINE_VERSION, PAYLOAD_TYPE);
    assertThat(hints).containsKey(hintKey);
    EngineHint hint = hints.get(hintKey);
    assertThat(hint.payloadType()).isEqualTo(PAYLOAD_TYPE);
    assertThat(hint.payload()).containsExactly(payload);
  }

  @Test
  void columnHintsGroupById() {
    byte[] payload = new byte[] {9, 10};
    String key = EngineHintMetadata.columnHintKey("floe.column+proto", 3L);
    String value = EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, payload);

    Map<String, String> props = new LinkedHashMap<>();
    props.put(key, value);

    Map<Long, Map<EngineHintKey, EngineHint>> columnHints = EngineHintMetadata.columnHints(props);
    assertThat(columnHints).containsKey(3L);
    Map<EngineHintKey, EngineHint> hintsForColumn = columnHints.get(3L);
    EngineHintKey hintKey = new EngineHintKey(ENGINE_KIND, ENGINE_VERSION, "floe.column+proto");
    assertThat(hintsForColumn).containsKey(hintKey);
    assertThat(hintsForColumn.get(hintKey).payload()).containsExactly(payload);
  }

  @Test
  void columnHintsKeepAllIdsWithMetadata() {
    byte[] payload1 = new byte[] {9, 10};
    byte[] payload2 = new byte[] {11, 12};
    String key1 = EngineHintMetadata.columnHintKey("floe.column+proto", 4L);
    String key2 = EngineHintMetadata.columnHintKey("floe.column+proto", 5L);
    Map<String, String> props = new LinkedHashMap<>();
    props.put(key1, EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, payload1));
    props.put(key2, EngineHintMetadata.encodeValue(ENGINE_KIND, ENGINE_VERSION, payload2));

    Map<Long, Map<EngineHintKey, EngineHint>> columnHints = EngineHintMetadata.columnHints(props);
    assertThat(columnHints).containsKeys(4L, 5L);
    EngineHintKey hintKey = new EngineHintKey(ENGINE_KIND, ENGINE_VERSION, "floe.column+proto");
    assertThat(columnHints.get(4L).get(hintKey).payload()).containsExactly(payload1);
    assertThat(columnHints.get(5L).get(hintKey).payload()).containsExactly(payload2);
    assertThat(columnHints.get(5L).get(hintKey).metadata())
        .containsEntry(EngineHintMetadata.COLUMN_HINT_COLUMN_KEY, "5");
  }

  @Test
  void parseHintKey_defensiveForMalformedColumnSuffix() {
    assertThat(EngineHintMetadata.parseHintKey("engine.hint.column.floe.column+proto.NOT_AN_INT"))
        .isEmpty();
    EngineHintMetadata.HintKeyInfo info =
        EngineHintMetadata.parseHintKey("engine.hint.column.floe.column+proto.3").orElseThrow();
    assertThat(info.relationHint()).isFalse();
    assertThat(info.columnId()).isEqualTo(3L);
  }

  @Test
  void payloadTypeNormalizationLowercasesAndAllowsDots() {
    String key = EngineHintMetadata.tableHintKey("FLOE.RELATION+PROTO");
    assertThat(key).contains("floe.relation+proto");
    assertThat(key).doesNotContain("FLOE");
  }

  @Test
  void payloadTypePatternRejectsIllegalCharacters() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> EngineHintMetadata.tableHintKey("bad;payload"));
  }
}
