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
package ai.floedb.floecat.connector.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DeltaStringBoundsTest {
  @Test
  void aMaximumEndingInTheReplacementCharacterIsWidened() {
    // Delta before 3.3.0: "aaa" then U+FFFD, below "aaa" then U+1F600 in code point order.
    assertThat(DeltaStringBounds.max("aaa\uFFFD")).isEqualTo("aab");
    // A cut through U+1F600 after "a", written "a?" then U+FFFD.
    assertThat(DeltaStringBounds.max("a?\uFFFD")).isEqualTo("b");
    assertThat(DeltaStringBounds.max("a\uD7FF\uFFFD")).isEqualTo("a\uE000");
    assertThat(DeltaStringBounds.max("a\uDBFF\uFFFD")).isEqualTo("b");
    assertThat(DeltaStringBounds.max("\uDBFF\uDFFF\uFFFD")).isNull();
    assertThat(DeltaStringBounds.max("aaa\uD83D\uDE00")).isEqualTo("aaa\uD83D\uDE00");
  }
}
