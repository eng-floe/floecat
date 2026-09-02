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

package ai.floedb.floecat.service.cache;

import ai.floedb.floecat.cache.CacheBudget;
import ai.floedb.floecat.cache.CacheFamily;
import io.quarkus.arc.Unremovable;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.Config;
import org.jboss.logging.Logger;

/**
 * What each cache may hold on this node, resolved once at startup.
 *
 * <p>The total is derived from the container rather than compiled in: the JVM already sizes its
 * heap from the container memory limit, so a share of the maximum heap follows the container
 * without this having to read cgroups. A deployment that wants a different number sets {@code
 * floecat.cache.total-bytes} and the derivation is skipped entirely.
 *
 * <p>A cache's figure is a share of that total, or an absolute {@code max-bytes} pinned instead.
 * The pointer share comes from the reference sizing scenario -- a 100,000-table account at 100
 * columns, where addressing needs 0.32 GB of the 3.34 GB the memory caches hold between them.
 * Shares resolve against that total, not the heap. Nothing about the scenario is compiled in; it
 * says what to set, not what the code assumes.
 *
 * <p>Every family resolves the same way, by tag, over {@link CacheFamily#values()}: adding a family
 * adds its two properties and nothing else, with no per-family injection point to forget.
 *
 * <p>{@link Startup} because the validation is the point of this class. Resolved lazily it would
 * report a misconfiguration on a request rather than at boot, and being unused it would be removed
 * from the container and never run at all.
 */
@Startup
@Unremovable
@ApplicationScoped
public class CacheBudgetResolver {

  private static final Logger LOG = Logger.getLogger(CacheBudgetResolver.class);

  private static final String TOTAL_BYTES = "floecat.cache.total-bytes";
  private static final String HEAP_SHARE = "floecat.cache.heap-share";

  private final CacheBudget budget;

  @Inject
  public CacheBudgetResolver(Config config) {
    long total =
        config
            .getOptionalValue(TOTAL_BYTES, Long.class)
            .orElseGet(() -> derivedFromHeap(share(config, HEAP_SHARE)));

    Map<CacheFamily, CacheBudget.Claim> claims = new EnumMap<>(CacheFamily.class);
    for (CacheFamily family : CacheFamily.values()) {
      claim(config, family).ifPresent(claim -> claims.put(family, claim));
    }
    this.budget = CacheBudget.split(total, claims);

    LOG.infof("cache budgets resolved: total=%d bytes, split=%s", total, budget.bytesByFamily());
  }

  /**
   * A family's claim on the total: {@code max-bytes} if it pins one, otherwise its {@code share}. A
   * family that configures neither takes nothing.
   */
  private static Optional<CacheBudget.Claim> claim(Config config, CacheFamily family) {
    String prefix = "floecat.cache." + family.tag() + ".";
    Optional<Long> pinned = config.getOptionalValue(prefix + "max-bytes", Long.class);
    if (pinned.isPresent()) {
      return pinned.map(CacheBudget.Claim.Bytes::new);
    }
    return config
        .getOptionalValue(prefix + "share", Double.class)
        .map(fraction -> asShare(prefix + "share", fraction));
  }

  /**
   * Required, with no default here: {@code application.properties} ships one, and a default in code
   * would be a second source of truth -- silently the one in force if the property were dropped.
   */
  private static CacheBudget.Claim.Share share(Config config, String property) {
    return asShare(property, config.getValue(property, Double.class));
  }

  /**
   * Adds the property name to {@link CacheBudget.Claim.Share}'s range rule, so a bad share names
   * the configuration line it came from.
   */
  private static CacheBudget.Claim.Share asShare(String property, double fraction) {
    try {
      return new CacheBudget.Claim.Share(fraction);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(property + ": " + e.getMessage(), e);
    }
  }

  private static long derivedFromHeap(CacheBudget.Claim.Share heapShare) {
    return (long) (Runtime.getRuntime().maxMemory() * heapShare.fraction());
  }

  /**
   * Bytes this cache may hold. Zero for a family that claims none.
   *
   * <p>The only door onto the resolved split: handing out the {@link CacheBudget} as well would be
   * two ways to reach the same number, and this is the one a cache needs when it is built.
   */
  public long bytesFor(CacheFamily family) {
    return budget.bytesFor(family);
  }
}
