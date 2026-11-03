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

package ai.floedb.floecat.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class MergedCountTest {

  @Test
  void aCountIsKnownOnlyWhenEveryContributorReportsIt() {
    MergedCount reported = new MergedCount();
    MergedCount unreported = new MergedCount();

    reported.add(2L);
    reported.add(3L);
    unreported.add(2L);
    unreported.add(null);

    assertEquals(5L, reported.value());
    assertNull(unreported.value(), "a contributor without a count may hold any number");

    MergedCount negative = new MergedCount();
    negative.add(-1L);
    MergedCount overflowing = new MergedCount();
    overflowing.add(Long.MAX_VALUE);
    overflowing.add(1L);
    assertNull(negative.value());
    assertNull(overflowing.value());
  }
}
