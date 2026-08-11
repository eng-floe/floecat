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

package ai.floedb.floecat.service.gc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class BloomReferenceIndexTest {

  @Test
  void memoryIsFixedAndDoesNotRetainMillionLongKeys() {
    BloomReferenceIndex index = new BloomReferenceIndex(1_000_000L, 0.000001d, 17L);
    long fixedBytes = index.memoryBytes();

    for (int i = 0; i < 1_000_000; i++) {
      index.add(
          "/accounts/account-with-a-long-name/tables/table-with-a-long-name/index-sidecars/"
              + i
              + "/a-content-addressed-object-name-that-is-not-retained.parquet");
    }

    assertTrue(fixedBytes < 4_000_000L);
    assertTrue(index.memoryBytes() == fixedBytes);
    assertTrue(index.insertions() <= 1_000_000L);
  }

  @Test
  void capacityFailureThrowsInsteadOfCreatingAFalseNegative() {
    BloomReferenceIndex index = new BloomReferenceIndex(1L, 0.000001d, 23L);
    index.add("first");

    assertThrows(
        ReferenceIndex.CapacityExceededException.class,
        () -> {
          for (int i = 0; i < 100; i++) {
            index.add("overflow-" + i);
          }
        });
    assertTrue(index.mightContain("first"));
    assertFalse(index.mightContain("definitely-absent"));
  }

  @Test
  void seedsRotateFalsePositivePatternWithoutChangingInsertedMembership() {
    BloomReferenceIndex first = new BloomReferenceIndex(10L, 0.2d, 1L);
    BloomReferenceIndex second = new BloomReferenceIndex(10L, 0.2d, 2L);
    for (int i = 0; i < 10; i++) {
      first.add("live-" + i);
      second.add("live-" + i);
    }

    for (int i = 0; i < 10; i++) {
      assertTrue(first.mightContain("live-" + i));
      assertTrue(second.mightContain("live-" + i));
    }
    boolean patternDiffers = false;
    for (int i = 0; i < 1_000; i++) {
      String garbage = "garbage-" + i;
      patternDiffers |= first.mightContain(garbage) != second.mightContain(garbage);
    }
    assertTrue(patternDiffers);
  }
}
