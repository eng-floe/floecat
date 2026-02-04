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

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineHintsMapperTest {

  @Test
  void duplicateHintKeyOverwritesPreviousEntry() {
    var rule1 =
        new EngineSpecificRule("pg", "1.0", "", "hint", new byte[] {1, 2}, Map.of("x", "1"));
    var rule2 =
        new EngineSpecificRule("pg", "1.0", "", "hint", new byte[] {3, 4}, Map.of("x", "2"));
    var hints = EngineHintsMapper.toHints("pg", "1.0", List.of(rule1, rule2));
    assertThat(hints).hasSize(1);
    var entry = hints.values().iterator().next();
    assertThat(entry.payload()).containsExactly(3, 4);
    assertThat(entry.metadata()).containsEntry("x", "2");
  }

  @Test
  void nullPayloadIsTreatedAsEmpty() {
    var rule = new EngineSpecificRule("pg", "1.0", "", "hint", null, Map.of());
    var hints = EngineHintsMapper.toHints("pg", "1.0", List.of(rule));
    assertThat(hints).hasSize(1);
    var intent = hints.values().iterator().next();
    assertThat(intent.payload()).hasSize(0);
  }
}
