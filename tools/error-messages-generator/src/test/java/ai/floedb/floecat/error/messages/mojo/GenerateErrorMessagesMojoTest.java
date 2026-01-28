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

package ai.floedb.floecat.error.messages.mojo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import org.junit.jupiter.api.Test;

class GenerateErrorMessagesMojoTest {
  @Test
  void normalizeConstantNameCollapsesSeparators() {
    assertEquals(
        "QUERY_INPUT_UNRESOLVED",
        GenerateErrorMessagesMojo.normalizeConstantName("query.input.unresolved"));
    assertEquals("TABLE_NAME", GenerateErrorMessagesMojo.normalizeConstantName("table.name"));
    assertEquals("NUMBER_1", GenerateErrorMessagesMojo.normalizeConstantName("number.1"));
  }

  @Test
  void renderPlaceholderLiteralSortsAndQuotesValues() {
    assertEquals("Set.of()", GenerateErrorMessagesMojo.renderPlaceholderLiteral(Set.of()));
    assertEquals(
        "Set.of(\"a\", \"z\")",
        GenerateErrorMessagesMojo.renderPlaceholderLiteral(Set.of("z", "a")));
  }
}
