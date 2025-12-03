package ai.floedb.metacat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.metacat.catalog.rpc.TableFormat;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.common.rpc.ResourceKind;
import ai.floedb.metacat.service.query.graph.model.EngineHint;
import ai.floedb.metacat.service.query.graph.model.EngineKey;
import ai.floedb.metacat.service.query.graph.model.RelationNode;
import ai.floedb.metacat.service.query.graph.model.RelationNodeKind;
import ai.floedb.metacat.service.query.graph.model.TableNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class EngineHintManagerTest {

  @Test
  void cachesAndInvalidatesHints() {
    TestProvider provider = new TestProvider();
    EngineHintManager manager =
        new EngineHintManager(List.of(provider), new SimpleMeterRegistry(), 1024);

    TableNode node = tableNode(1L);
    EngineKey key = new EngineKey("floedb", "1.0.0");

    EngineHint hint1 = manager.get(node, key, "test", "cid").orElseThrow();
    EngineHint hint2 = manager.get(node, key, "test", "cid").orElseThrow();
    assertThat(provider.computeCount.get()).isEqualTo(1);
    assertThat(hint2.payload()).isEqualTo(hint1.payload());

    manager.invalidate(node.id());
    TableNode nodeUpdated = tableNode(2L);
    manager.get(nodeUpdated, key, "test", "cid").orElseThrow();
    assertThat(provider.computeCount.get()).isEqualTo(2);
  }

  @Test
  void returnsEmptyWhenNoProvider() {
    EngineHintManager manager = new EngineHintManager(List.of(), new SimpleMeterRegistry(), 1024);
    assertThat(manager.get(tableNode(1L), new EngineKey("floedb", "1"), "missing", "cid"))
        .isEmpty();
  }

  private static TableNode tableNode(long version) {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setTenantId("tenant")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setTenantId("tenant")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setTenantId("tenant")
            .setId("namespace")
            .setKind(ResourceKind.RK_NAMESPACE)
            .build();
    return new TableNode(
        tableId,
        version,
        Instant.now(),
        catalogId,
        namespaceId,
        "tbl",
        TableFormat.TF_ICEBERG,
        "{}",
        Map.of(),
        List.of(),
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of());
  }

  private static final class TestProvider implements EngineHintProvider {

    private final AtomicInteger computeCount = new AtomicInteger();

    @Override
    public boolean supports(RelationNodeKind kind, String hintType) {
      return kind == RelationNodeKind.TABLE && hintType.equals("test");
    }

    @Override
    public boolean isAvailable(EngineKey engineKey) {
      return true;
    }

    @Override
    public String fingerprint(RelationNode node, EngineKey engineKey, String hintType) {
      return node.version() + "-fp";
    }

    @Override
    public EngineHint compute(
        RelationNode node, EngineKey engineKey, String hintType, String correlationId) {
      computeCount.incrementAndGet();
      return new EngineHint("application/test", new byte[] {1, 2, 3});
    }
  }
}
