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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.cache.CacheFamily;
import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;

class CacheBudgetResolverTest {

  private static final long GB = 1024L * 1024L * 1024L;

  private static Config config(Map<String, String> properties) {
    return new SmallRyeConfigBuilder()
        // Expression expansion, so ${FLOECAT_...:default} in the shipped file resolves here the
        // way it does at boot rather than arriving as a literal.
        .addDefaultInterceptors()
        .withSources(new PropertiesConfigSource(properties, "test", 500))
        .build();
  }

  @Test
  void takesTheShareOfAnExplicitTotal() {
    var budgets =
        new CacheBudgetResolver(
            config(
                Map.of(
                    "floecat.cache.total-bytes", Long.toString(10 * GB),
                    "floecat.cache.heap-share", "0.5",
                    "floecat.cache.pointer.share", "0.096")));

    assertThat(budgets.bytesFor(CacheFamily.POINTER)).isEqualTo((long) (10 * GB * 0.096d));
  }

  @Test
  void derivesTheTotalFromTheHeapWhenNoneIsSet() {
    // The JVM sizes its heap from the container memory limit, so following the heap follows the
    // container without this having to read cgroups.
    var budgets =
        new CacheBudgetResolver(
            config(
                Map.of("floecat.cache.heap-share", "0.5", "floecat.cache.pointer.share", "0.096")));

    assertThat(budgets.bytesFor(CacheFamily.POINTER))
        .isEqualTo((long) ((long) (Runtime.getRuntime().maxMemory() * 0.5d) * 0.096d));
  }

  @Test
  void anAbsoluteMaxBytesWinsOverTheShare() {
    var budgets =
        new CacheBudgetResolver(
            config(
                Map.of(
                    "floecat.cache.total-bytes",
                    Long.toString(10 * GB),
                    "floecat.cache.heap-share",
                    "0.5",
                    "floecat.cache.pointer.share",
                    "0.096",
                    "floecat.cache.pointer.max-bytes",
                    Long.toString(64L * 1024 * 1024))));

    assertThat(budgets.bytesFor(CacheFamily.POINTER)).isEqualTo(64L * 1024 * 1024);
  }

  @Test
  void everyFamilyIsResolvedByTagRatherThanBeingWiredOneAtATime() {
    // The property names are derived from CacheFamily.values(), so a family added to the enum is
    // configurable without an injection point being added for it.
    var budgets =
        new CacheBudgetResolver(
            config(
                Map.of(
                    "floecat.cache.total-bytes", Long.toString(10 * GB),
                    "floecat.cache.heap-share", "0.5",
                    "floecat.cache.pointer.max-bytes", Long.toString(GB))));

    assertThat(budgets.bytesFor(CacheFamily.POINTER)).isEqualTo(GB);
  }

  @Test
  void aCacheWithNoBudgetGetsNothingRatherThanEverything() {
    // A family with no configured claim gets nothing rather than everything, and the rest of the
    // total stays unclaimed -- under-using the heap rather than over-committing it.
    var budgets =
        new CacheBudgetResolver(
            config(
                Map.of(
                    "floecat.cache.total-bytes",
                    Long.toString(10 * GB),
                    "floecat.cache.heap-share",
                    "0.5")));

    assertThat(budgets.bytesFor(CacheFamily.POINTER)).isZero();
  }

  @Test
  void aClaimMayNotExceedTheTotalItIsSplitFrom() {
    // A max-bytes pinned above the total is the operator mistake that OOMs the node, and no share
    // being out of range says anything about it. Multi-family overcommit is pinned on the pure
    // arithmetic in CacheBudgetTest, since only one family is on the contract today.
    assertThatThrownBy(
            () ->
                new CacheBudgetResolver(
                    config(
                        Map.of(
                            "floecat.cache.total-bytes", Long.toString(10 * GB),
                            "floecat.cache.heap-share", "0.5",
                            "floecat.cache.pointer.max-bytes", Long.toString(20 * GB)))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("more than");
  }

  @Test
  void aShareOutsideItsRangeFailsAtStartup() {
    // A typo here resolves to a cache that holds nothing, or one sized past the heap it was split
    // from -- both silent until they are behaviour. The failure names the property, because the
    // value on its own does not tell an operator which line of their configuration to fix.
    for (String bad : new String[] {"0.0", "-0.1", "1.5"}) {
      assertThatThrownBy(
              () ->
                  new CacheBudgetResolver(
                      config(
                          Map.of(
                              "floecat.cache.heap-share",
                              "0.5",
                              "floecat.cache.pointer.share",
                              bad))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("floecat.cache.pointer.share");
      assertThatThrownBy(
              () ->
                  new CacheBudgetResolver(
                      config(
                          Map.of(
                              "floecat.cache.heap-share",
                              bad,
                              "floecat.cache.pointer.share",
                              "0.096"))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("floecat.cache.heap-share");
    }
  }

  @Test
  void theShippedPropertiesResolveToAUsableSplit() throws Exception {
    // The file the service actually boots on. Nothing here carries a default of its own, so a
    // property dropped from that file is a startup failure -- and this is what says so before the
    // deployment does.
    // The file itself, not the classpath: test resources overlay an application.properties of
    // their own, and a classpath lookup would read that one and prove nothing about what ships.
    var properties = new Properties();
    try (var in = Files.newInputStream(Path.of("src/main/resources/application.properties"))) {
      properties.load(in);
    }
    var shipped = new HashMap<String, String>();
    properties.forEach((key, value) -> shipped.put((String) key, (String) value));

    var budgets = new CacheBudgetResolver(config(shipped));

    assertThat(budgets.bytesFor(CacheFamily.POINTER)).isPositive();
  }
}
