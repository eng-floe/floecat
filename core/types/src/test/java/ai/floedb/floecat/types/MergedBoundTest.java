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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class MergedBoundTest {
  private static final LogicalType INTS = LogicalType.of(LogicalKind.INT);

  @Test
  void aBoundHoldsOnlyWhenEveryContributorWithValuesGivesOne() {
    MergedBound withNullContributor = MergedBound.lower();
    MergedBound withUnboundedContributor = MergedBound.lower();

    withNullContributor.add(INTS, 3L, true);
    withNullContributor.add(INTS, null, false);
    withUnboundedContributor.add(INTS, 3L, true);
    withUnboundedContributor.add(INTS, null, true);

    assertEquals(
        3L, withNullContributor.value(), "a contributor of only NULLs has no bound to give");
    // A partial merge's row count covers only the files that reported the column.
    assertTrue(MergedBound.mayHoldValues(0L, null, null));
    assertNull(
        withUnboundedContributor.value(), "the unbounded contributor's values may lie below");
  }

  @Test
  void onlyValidCountsOfNothingButNullsAndNansShowAContributorHoldsNoValues() {
    assertFalse(MergedBound.mayHoldValues(5L, 3L, 2L));
    assertFalse(MergedBound.mayHoldValues(5L, 5L, null));
    // Negative, inconsistent or overflowing counts show nothing.
    assertTrue(MergedBound.mayHoldValues(-1L, 0L, null));
    assertTrue(MergedBound.mayHoldValues(5L, -1L, null));
    assertTrue(MergedBound.mayHoldValues(5L, 3L, -1L));
    assertTrue(MergedBound.mayHoldValues(5L, 6L, null));
    assertTrue(MergedBound.mayHoldValues(5L, Long.MAX_VALUE, 1L));
  }

  @Test
  void aBoundThatCannotBeOrderedLeavesTheMergeUnbounded() {
    MergedBound nan = MergedBound.upper();
    MergedBound untyped = MergedBound.upper();

    nan.add(LogicalType.of(LogicalKind.DOUBLE), 1.0, true);
    nan.add(LogicalType.of(LogicalKind.DOUBLE), Double.NaN, true);
    untyped.add(null, 1L, true);

    assertNull(nan.value(), "NaN bounds nothing");
    assertNull(untyped.value(), "a bound without a type has no order");

    // Rounding the latest timestamp up to the column's precision leaves the type's range.
    MergedBound overflowing = MergedBound.upper();
    LogicalType micros = LogicalType.temporal(LogicalKind.TIMESTAMP, 6);
    overflowing.add(micros, java.time.LocalDateTime.of(2026, 1, 1, 0, 0), true);
    overflowing.add(micros, java.time.LocalDateTime.MAX, true);
    assertNull(overflowing.value());
  }

  @Test
  void aBoundTheTypeCannotHoldIsRoundedOutward() {
    LogicalType floats = LogicalType.of(LogicalKind.FLOAT);
    MergedBound lower = MergedBound.lower();
    MergedBound upper = MergedBound.upper();
    MergedBound zeroLower = MergedBound.lower();
    MergedBound zeroUpper = MergedBound.upper();

    lower.add(floats, 0.1d, true);
    upper.add(floats, Math.nextUp((double) 0.1f), true);
    zeroLower.add(floats, 0.0f, true);
    zeroUpper.add(floats, -0.0f, true);

    assertEquals(Math.nextDown(0.1f), lower.value(), "0.1f is above the double 0.1");
    assertEquals(Math.nextUp(0.1f), upper.value(), "0.1f is below a double just above it");
    assertEquals(-0.0f, zeroLower.value(), "+0.0 may stand for -0.0 values");
    assertEquals(0.0f, zeroUpper.value(), "-0.0 may stand for +0.0 values");
  }

  @Test
  void aBinaryBoundSurvivesMoreThanOneContributor() {
    LogicalType binary = LogicalType.of(LogicalKind.BINARY);
    MergedBound lower = MergedBound.lower();
    MergedBound upper = MergedBound.upper();

    for (byte[] bound : new byte[][] {{2}, {1}, {3}}) {
      lower.add(binary, bound, true);
      upper.add(binary, bound, true);
    }

    assertArrayEquals(
        new byte[] {1}, (byte[]) LogicalCoercions.coerceStatValue(binary, lower.value()));
    assertArrayEquals(
        new byte[] {3}, (byte[]) LogicalCoercions.coerceStatValue(binary, upper.value()));
  }
}
