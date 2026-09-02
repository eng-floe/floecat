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

import ai.floedb.floecat.cache.CacheBudget.Claim;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CacheBudgetTest {

  private static final long GB = 1024L * 1024L * 1024L;

  @Test
  void aMissingFamilyClaimHoldsNothing() {
    CacheBudget budget = new CacheBudget(Map.of());

    assertThat(budget.bytesFor(CacheFamily.POINTER)).isZero();
  }

  @Test
  void aShareResolvesAgainstTheTotal() {
    CacheBudget budget =
        CacheBudget.split(100L * GB, Map.of(CacheFamily.POINTER, new Claim.Share(0.1)));

    assertThat(budget.bytesFor(CacheFamily.POINTER)).isEqualTo(10L * GB);
  }

  @Test
  void anAbsoluteFigureIgnoresTheTotalSize() {
    CacheBudget budget =
        CacheBudget.split(100L * GB, Map.of(CacheFamily.POINTER, new Claim.Bytes(4L * GB)));

    assertThat(budget.bytesByFamily()).containsOnlyKeys(CacheFamily.POINTER);
    assertThat(budget.bytesFor(CacheFamily.POINTER)).isEqualTo(4L * GB);
  }

  @Test
  void everyRouteToAZeroBudgetIsRefused() {
    // Three different arrivals at the same zero; each is refused rather than accepted quietly.
    assertThatThrownBy(
            () -> CacheBudget.split(0L, Map.of(CacheFamily.POINTER, new Claim.Share(0.1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("total must be positive");

    assertThatThrownBy(
            () -> CacheBudget.split(GB, Map.of(CacheFamily.POINTER, new Claim.Bytes(0L))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("resolves to 0");

    // A share small enough to round away against a small total: valid share, valid total, no cache.
    assertThatThrownBy(
            () -> CacheBudget.split(100L, Map.of(CacheFamily.POINTER, new Claim.Share(0.001))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("resolves to 0");
  }

  @Test
  void aZeroIsRefusedByTheTypeAndNotOnlyBySplit() {
    // split's guarantee is only worth what the type enforces, and the constructor is public.
    assertThatThrownBy(() -> new CacheBudget(Map.of(CacheFamily.POINTER, 0L)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be positive");
    assertThatThrownBy(() -> new CacheBudget(Map.of(CacheFamily.POINTER, -1L)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void anOutOfRangeShareCannotBeConstructedAtAll() {
    // On the type, so it holds for callers that never go through split.
    assertThatThrownBy(() -> new Claim.Share(1.5))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be in (0, 1]");
    assertThatThrownBy(() -> new Claim.Share(0.0)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aPinnedFigureMayNotExceedTheTotal() {
    assertThatThrownBy(
            () ->
                CacheBudget.split(GB, Map.of(CacheFamily.POINTER, new Claim.Bytes(Long.MAX_VALUE))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than");
  }
}
