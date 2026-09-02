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

package ai.floedb.floecat.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CacheWeightsTest {

  /** A value that knows its own retained size, which is how domain types declare their weight. */
  private record Declared(long bytes) implements WeightedValue {
    @Override
    public long estimatedWeightBytes() {
      return bytes;
    }
  }

  @Test
  void aValueThatDeclaresItsWeightIsTakenAtItsWord() {
    // This is the seam that keeps this weigher free of domain types: anything with private
    // aggregate structure reports its own size rather than being introspected here.
    assertThat(CacheWeights.entry(new Declared(500_000), 0L)).isGreaterThanOrEqualTo(500_000);
  }

  @Test
  void aNegativeDeclaredWeightIsRefusedRatherThanFlooredToNothing() {
    // Flooring would charge entry machinery alone -- the flat default this class exists not to
    // apply, reached through the sanctioned door instead of the refused one.
    assertThatThrownBy(() -> CacheWeights.entry(new Declared(-1_000), 0L))
        .isInstanceOf(UnweighableValueException.class)
        .hasMessageContaining("negative weight");
  }

  @Test
  void aShapeThisCannotWalkIsRefusedRatherThanGivenAFlatDefault() {
    // A flat default would charge a record retaining megabytes the same as an empty one, and the
    // byte budget would be wrong in the direction that exhausts the heap. Refusing turns that
    // into a failure at the first put, in the change that introduced the type.
    record Unweighable(long a, long b) {}

    assertThatThrownBy(() -> CacheWeights.entry(new Unweighable(1, 2), 0L))
        .isInstanceOf(UnweighableValueException.class)
        .hasMessageContaining("WeightedValue");
    assertThatThrownBy(() -> CacheWeights.entry(new long[] {1L}, 0L))
        .isInstanceOf(UnweighableValueException.class);
  }

  @Test
  void textAndBytesAreWeighedByTheirLength() {
    assertThat(CacheWeights.entry("x".repeat(1_000), 0L))
        .isGreaterThan(CacheWeights.entry("x".repeat(10), 0L));
    assertThat(CacheWeights.entry(new byte[4_096], 0L))
        .isGreaterThan(CacheWeights.entry(new byte[16], 0L));
  }

  @Test
  void containersAccumulateTheirElements() {
    var small = List.of(new Declared(100), new Declared(100));
    var large = List.of(new Declared(10_000), new Declared(10_000));

    assertThat(CacheWeights.entry(large, 0L)).isGreaterThan(CacheWeights.entry(small, 0L));
    assertThat(CacheWeights.entry(Map.of("a", new Declared(10_000)), 0L))
        .isGreaterThan(CacheWeights.entry(Map.of("a", new Declared(1)), 0L));
  }

  @Test
  void theKeyCountsTowardTheEntry() {
    assertThat(CacheWeights.entry(new Declared(0), 1_000L))
        .isGreaterThan(CacheWeights.entry(new Declared(0), 0L));
  }

  @Test
  void aNegativeKeyWeightIsRefusedForTheSameReason() {
    assertThatThrownBy(() -> CacheWeights.entry(new Declared(100), -1_000L))
        .isInstanceOf(UnweighableValueException.class)
        .hasMessageContaining("key weight is negative");
  }

  @Test
  void aSelfReferencingValueIsWeighedOnceRatherThanRecursingForever() {
    var cyclic = new ArrayList<Object>();
    cyclic.add(new Declared(10));
    cyclic.add(cyclic);

    assertThat(CacheWeights.entry(cyclic, 0L)).isPositive();
  }

  @Test
  void anOversizedValueSaturatesInsteadOfOverflowing() {
    // The sum must not wrap: a wrapped total reads as a tiny entry and escapes eviction entirely.
    assertThat(CacheWeights.entry(new Declared(Long.MAX_VALUE), 4_096L)).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void anArbitraryPrecisionNumberIsWeighedByItsMagnitudeNotAsABoxedScalar() {
    // Both are Numbers, so the boxed-scalar branch would charge a megabyte of magnitude 32 bytes.
    var big = new BigInteger("2").pow(8_000_000);

    assertThat(CacheWeights.entry(big, 0L)).isGreaterThan(1_000_000L);
    assertThat(CacheWeights.entry(new BigDecimal(big), 0L)).isGreaterThan(1_000_000L);
    assertThat(CacheWeights.entry(BigInteger.ONE, 0L)).isLessThan(CacheWeights.entry(big, 0L));
  }
}
