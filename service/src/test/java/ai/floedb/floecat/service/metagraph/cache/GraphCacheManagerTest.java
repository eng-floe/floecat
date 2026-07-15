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

package ai.floedb.floecat.service.metagraph.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.telemetry.Telemetry;
import ai.floedb.floecat.telemetry.TestObservability;
import com.google.protobuf.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class GraphCacheManagerTest {

  @Test
  void metaCacheStoresAndInvalidatesEntries() {
    GraphCacheManager manager = new GraphCacheManager(true, 10, 2L, new TestObservability());
    ResourceId tableId = rid("account", "tbl");
    MutationMeta meta = mutationMeta(7L);

    assertThat(manager.getMeta(tableId)).isNull();
    manager.putMeta(tableId, meta);
    assertThat(manager.getMeta(tableId)).isEqualTo(meta);

    manager.invalidate(tableId);
    assertThat(manager.getMeta(tableId)).isNull();
  }

  @Test
  void absenceMetasAreNeverCached() {
    GraphCacheManager manager = new GraphCacheManager(true, 10, 2L, new TestObservability());
    ResourceId tableId = rid("account", "tbl");

    // A missing pointer arrives as a blank-URI meta (pointerMetaForSafe does not throw). Caching
    // it would let a storm of negative lookups evict real entries from the size-bounded cache
    // without the entry ever serving anything.
    manager.putMeta(tableId, MutationMeta.newBuilder().setPointerVersion(0L).build());

    assertThat(manager.getMeta(tableId)).isNull();
  }

  @Test
  void disabledMetaCacheIgnoresWrites() {
    GraphCacheManager manager = new GraphCacheManager(true, 10, 0L, new TestObservability());
    ResourceId tableId = rid("account", "tbl");
    manager.putMeta(tableId, mutationMeta(1L));

    assertThat(manager.getMeta(tableId)).isNull();
  }

  @Test
  void metaCacheRecordsHitAndMissCounters() {
    TestObservability metrics = new TestObservability();
    GraphCacheManager manager = new GraphCacheManager(true, 10, 2L, metrics);
    ResourceId tableId = rid("account", "tbl");

    assertThat(manager.getMeta(tableId)).isNull();
    manager.putMeta(tableId, mutationMeta(2L));
    assertThat(manager.getMeta(tableId)).isNotNull();

    assertThat(metrics.counterValue(Telemetry.Metrics.CACHE_MISSES)).isGreaterThan(0);
    assertThat(metrics.counterValue(Telemetry.Metrics.CACHE_HITS)).isGreaterThan(0);
    assertThat(metrics.counterTagHistory(Telemetry.Metrics.CACHE_HITS))
        .anySatisfy(
            tags ->
                assertThat(tags)
                    .anyMatch(
                        tag ->
                            Telemetry.TagKey.CACHE_NAME.equals(tag.key())
                                && "graph-meta-cache".equals(tag.value())));
  }

  private static ResourceId rid(String accountId, String id) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }

  private static MutationMeta mutationMeta(long pointerVersion) {
    // Prod pointer metas always name a blob; a blank URI means absence and is filtered by putMeta.
    return MutationMeta.newBuilder()
        .setPointerVersion(pointerVersion)
        .setBlobUri("blob/v" + pointerVersion)
        .setUpdatedAt(
            Timestamp.newBuilder()
                .setSeconds(Instant.parse("2024-01-01T00:00:00Z").getEpochSecond())
                .build())
        .build();
  }
}
