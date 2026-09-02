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

import java.util.EnumMap;
import java.util.Map;

/**
 * How one total splits across the memory caches. A pure value: it reads no configuration and knows
 * no container, so the same arithmetic serves the service, its tests, and any harness that needs
 * the same figures.
 *
 * <p>A family with no configured claim takes nothing. What the figures should be set to is a
 * deployment question, documented in {@code docs/caching.md} -- nothing about a sizing scenario
 * belongs in arithmetic.
 */
public record CacheBudget(Map<CacheFamily, Long> bytesByFamily) {

  public CacheBudget {
    bytesByFamily = Map.copyOf(bytesByFamily);
    bytesByFamily.forEach(
        (family, bytes) -> {
          // Not just negative: absence is the one spelling of "takes nothing" (see split).
          if (bytes <= 0) {
            throw new IllegalArgumentException(
                "cache budget for " + family + " must be positive, but was " + bytes);
          }
        });
  }

  /** How one family claims its part of the total: a fraction of it, or an absolute figure. */
  public sealed interface Claim {

    /** {@code fraction} of the total, so the figure follows the container it was derived from. */
    record Share(double fraction) implements Claim {

      /**
       * Checked here, so an out-of-range share cannot be constructed at all. Outside {@code (0, 1]}
       * it is a typo that resolves either to a cache holding nothing or to one sized past its
       * total.
       */
      public Share {
        if (!(fraction > 0.0) || fraction > 1.0) {
          throw new IllegalArgumentException("share must be in (0, 1], but was " + fraction);
        }
      }
    }

    /** An absolute figure, pinned against the total moving. */
    record Bytes(long bytes) implements Claim {}
  }

  /**
   * Resolves each family's claim against one total.
   *
   * <p>Pure, and the only place the arithmetic lives, so the rules below hold for the service, its
   * tests and any harness without any of them repeating them. A family with no claim is absent from
   * the result and takes nothing.
   *
   * <p>Every claim resolving to zero is rejected, including a share small enough to round away: a
   * cache sized zero reports a 0% hit rate that reads as ineffective rather than misconfigured.
   * Claiming NOTHING is different and allowed -- the family is absent from the result. A missing
   * configuration resolves the same way, and the cache built for it is what refuses a zero budget.
   *
   * @throws IllegalArgumentException if the total is not positive, a claim resolves to zero, or the
   *     claims together exceed the total. A claim that is itself out of range is refused when it is
   *     constructed, not here.
   */
  public static CacheBudget split(long totalBytes, Map<CacheFamily, Claim> claims) {
    if (totalBytes <= 0) {
      throw new IllegalArgumentException("cache total must be positive, but was " + totalBytes);
    }
    Map<CacheFamily, Long> resolved = new EnumMap<>(CacheFamily.class);
    long allocated = 0L;
    for (Map.Entry<CacheFamily, Claim> claim : claims.entrySet()) {
      long bytes = resolve(claim.getKey(), claim.getValue(), totalBytes);
      resolved.put(claim.getKey(), bytes);
      // Saturating, not wrapping. Two absurd max-bytes figures sum negative, and a negative total
      // passes the very check below that exists to catch them.
      allocated = allocated > Long.MAX_VALUE - bytes ? Long.MAX_VALUE : allocated + bytes;
    }
    if (allocated > totalBytes) {
      throw new IllegalArgumentException(
          "cache budgets total "
              + allocated
              + " bytes, which is more than the "
              + totalBytes
              + " they are split from: "
              + resolved);
    }
    return new CacheBudget(resolved);
  }

  private static long resolve(CacheFamily family, Claim claim, long totalBytes) {
    long bytes =
        switch (claim) {
          case Claim.Share share -> (long) (totalBytes * share.fraction());
          case Claim.Bytes pinned -> pinned.bytes();
        };
    if (bytes <= 0) {
      throw new IllegalArgumentException(
          "cache budget for " + family + " resolves to " + bytes + " bytes from " + claim);
    }
    return bytes;
  }

  /** Bytes this family may hold, or zero for one that takes no memory share. */
  public long bytesFor(CacheFamily family) {
    return bytesByFamily.getOrDefault(family, 0L);
  }
}
