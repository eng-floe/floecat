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
package ai.floedb.floecat.scanner.spi;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class StatsProviderBatchTest {

  private static ResourceId id(String s) {
    return ResourceId.newBuilder().setId(s).build();
  }

  /**
   * The default tableStatsBatch resolves each id once, via tableStats, and returns every result.
   */
  @Test
  void defaultBatchResolvesEachIdOnce() {
    Map<ResourceId, Integer> calls = new ConcurrentHashMap<>();
    StatsProvider provider =
        new StatsProvider() {
          @Override
          public Optional<TableStatsView> tableStats(ResourceId tableId) {
            calls.merge(tableId, 1, Integer::sum);
            return Optional.of(
                new TableStatsView() {
                  @Override
                  public ResourceId tableId() {
                    return tableId;
                  }

                  @Override
                  public long snapshotId() {
                    return 1L;
                  }

                  @Override
                  public OptionalLong rowCountValue() {
                    return OptionalLong.of(7L);
                  }

                  @Override
                  public OptionalLong totalSizeBytesValue() {
                    return OptionalLong.empty();
                  }
                });
          }
        };

    Map<ResourceId, Optional<StatsProvider.TableStatsView>> out =
        provider.tableStatsBatch(List.of(id("a"), id("b"), id("a")));

    assertThat(out.keySet()).containsExactlyInAnyOrder(id("a"), id("b"));
    assertThat(out.get(id("a"))).isPresent();
    assertThat(out.get(id("b"))).isPresent();
    // Duplicate id collapsed to a single resolve.
    assertThat(calls.get(id("a"))).isEqualTo(1);
    assertThat(calls.get(id("b"))).isEqualTo(1);
  }

  /** NONE (and any provider that returns empty) yields an empty Optional per id, never null. */
  @Test
  void noneProviderReturnsEmptyPerId() {
    Map<ResourceId, Optional<StatsProvider.TableStatsView>> out =
        StatsProvider.NONE.tableStatsBatch(List.of(id("x"), id("y")));
    assertThat(out).containsOnlyKeys(id("x"), id("y"));
    assertThat(out.get(id("x"))).isEmpty();
    assertThat(out.get(id("y"))).isEmpty();
  }
}
