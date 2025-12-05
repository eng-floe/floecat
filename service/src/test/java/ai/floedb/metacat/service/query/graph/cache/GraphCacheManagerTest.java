package ai.floedb.metacat.service.query.graph.cache;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.TestNodes;
import ai.floedb.metacat.service.query.graph.model.TableNode;
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
  void perTenantCachesDoNotLeak() {
    ResourceId tableA = rid("tenant-a", "A");
    ResourceId tableB = rid("tenant-b", "B");
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
    ResourceId tableId = rid("tenant", "tbl");
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
    ResourceId tableId = rid("tenant", "tbl");
    GraphCacheKey key = new GraphCacheKey(tableId, 1L);

    disabled.put(tableId, key, tableNode(tableId));

    assertThat(disabled.get(tableId, key)).isNull();
  }

  private TableNode tableNode(ResourceId tableId) {
    return TestNodes.tableNode(tableId, "{}");
  }

  private static ResourceId rid(String tenantId, String id) {
    return ResourceId.newBuilder()
        .setTenantId(tenantId)
        .setId(id)
        .setKind(ResourceKind.RK_TABLE)
        .build();
  }
}
