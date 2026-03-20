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

package ai.floedb.floecat.client.cli;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.client.cli.util.CliUtils;
import java.util.List;
import org.junit.jupiter.api.Test;

final class CsvListTest {

  private static List<String> csv(String s) {
    return CliUtils.csvList(s);
  }

  @Test
  void basicSplit() {
    assertEquals(List.of("a", "b", "c"), csv("a,b,c"));
  }

  @Test
  void quotedPreservesInternalCommas() {
    assertEquals(List.of("a,b", "c"), csv("\"a,b\",c"));
  }

  @Test
  void escapedCommaOutsideQuotes() {
    assertEquals(List.of("a,b", "c"), csv("a\\,b,c"));
  }

  @Test
  void unclosedQuoteThrows() {
    assertThrows(IllegalArgumentException.class, () -> csv("\"a,b"));
  }
}
