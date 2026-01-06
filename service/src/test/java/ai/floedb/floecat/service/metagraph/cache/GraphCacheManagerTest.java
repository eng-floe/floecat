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

import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.cache.GraphCacheKey;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.service.testsupport.TestNodes;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphCacheManagerTest {

  private SimpleMeterRegistry registry;
  private GraphCacheManager cacheManager;

  @BeforeEach
  void setUp() {
    registry = new SimpleMeterRegistry();
    cacheManager = new GraphCacheManager(true, 10, registry);
  }

  @Test
  void perAccountCachesDoNotLeak() {
    ResourceId tableA = rid("account-a", "A");
    ResourceId tableB = rid("account-b", "B");
    GraphCacheKey keyA = new GraphCacheKey(tableA, 1L);
    GraphCacheKey keyB = new GraphCacheKey(tableB, 1L);

    cacheManager.put(tableA, keyA, tableNode(tableA));

    assertThat(cacheManager.get(tableA, keyA)).isNotNull();
    assertThat(cacheManager.get(tableB, keyA)).isNull();

    cacheManager.put(tableB, keyB, tableNode(tableB));

    assertThat(cacheManager.get(tableB, keyB)).isNotNull();
    assertThat(cacheManager.get(tableA, keyB)).isNull();
  }

  @Test
  void invalidateRemovesAllVersions() {
    ResourceId tableId = rid("account", "tbl");
    GraphCacheKey v1 = new GraphCacheKey(tableId, 1L);
    GraphCacheKey v2 = new GraphCacheKey(tableId, 2L);
    cacheManager.put(tableId, v1, tableNode(tableId));
    cacheManager.put(tableId, v2, tableNode(tableId));

    assertThat(cacheManager.get(tableId, v1)).isNotNull();
    assertThat(cacheManager.get(tableId, v2)).isNotNull();

    cacheManager.invalidate(tableId);

    assertThat(cacheManager.get(tableId, v1)).isNull();
    assertThat(cacheManager.get(tableId, v2)).isNull();
  }

  @Test
  void disabledCacheReturnsNull() {
    GraphCacheManager disabled = new GraphCacheManager(false, 10, registry);
    ResourceId tableId = rid("account", "tbl");
    GraphCacheKey key = new GraphCacheKey(tableId, 1L);

    disabled.put(tableId, key, tableNode(tableId));

    assertThat(disabled.get(tableId, key)).isNull();
  }

  private UserTableNode tableNode(ResourceId tableId) {
    return TestNodes.tableNode(tableId, "{}");
  }

  private static ResourceId rid(String accountId, String id) {
    return ResourceId.newBuilder()
        .setAccountId(accountId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }
}
