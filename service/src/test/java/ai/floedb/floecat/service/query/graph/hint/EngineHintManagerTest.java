package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.query.graph.model.EngineHint;
import ai.floedb.floecat.service.query.graph.model.EngineKey;
import ai.floedb.floecat.service.query.graph.model.RelationNode;
import ai.floedb.floecat.service.query.graph.model.RelationNodeKind;
import ai.floedb.floecat.service.query.graph.model.TableNode;
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
            .setAccountId("account")
            .setId("table")
            .setKind(ResourceKind.RK_TABLE)
            .build();
    ResourceId catalogId =
        ResourceId.newBuilder()
            .setAccountId("account")
            .setId("catalog")
            .setKind(ResourceKind.RK_CATALOG)
            .build();
    ResourceId namespaceId =
        ResourceId.newBuilder()
            .setAccountId("account")
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

  @Test
  void differentFingerprintsProduceDifferentCacheEntries() {
    var provider =
        new TestProvider() {
          @Override
          public String fingerprint(RelationNode node, EngineKey key, String hintType) {
            return computeCount.get() == 0 ? "fp1" : "fp2";
          }
        };

    var manager = new EngineHintManager(List.of(provider), new SimpleMeterRegistry(), 1024);
    var key = new EngineKey("floedb", "1");

    // same node id and version
    var n = tableNode(1);

    manager.get(n, key, "test", "cid").orElseThrow(); // compute #1, fp1

    // flip fingerprint
    manager.get(n, key, "test", "cid").orElseThrow(); // compute #2, fp2
    manager.get(n, key, "test", "cid").orElseThrow(); // cached

    assertThat(provider.computeCount.get()).isEqualTo(2);
  }

  @Test
  void selectsFirstMatchingProvider() {
    var p1 = new TestProvider();
    var p2 = new TestProvider();

    EngineHintManager manager =
        new EngineHintManager(List.of(p1, p2), new SimpleMeterRegistry(), 1024);

    var node = tableNode(1);
    var key = new EngineKey("floedb", "1");

    manager.get(node, key, "test", "cid");

    assertThat(p1.computeCount.get()).isEqualTo(1);
    assertThat(p2.computeCount.get()).isEqualTo(0);
  }

  @Test
  void evictionOccursWhenWeightExceeded() {
    var provider =
        new TestProvider() {
          @Override
          public EngineHint compute(RelationNode node, EngineKey k, String h, String c) {
            computeCount.incrementAndGet();
            return new EngineHint("t", new byte[512]); // heavy
          }
        };

    // max 600 bytes -> only one entry can fit
    var manager = new EngineHintManager(List.of(provider), new SimpleMeterRegistry(), 600, true);
    var key = new EngineKey("f", "1");

    var n1 = tableNode(1);
    var n2 = tableNode(2);

    // Insert entry #1
    manager.get(n1, key, "test", "cid");

    // Insert entry #2 -> eviction of #1 must happen here
    manager.get(n2, key, "test", "cid");
    manager.cache().cleanUp();

    // Verify cache weight never exceeds limit
    long weight =
        manager.cache().policy().eviction().map(ev -> ev.weightedSize().orElse(0L)).orElse(0L);

    assertThat(weight).isLessThanOrEqualTo(600);

    // Ensure eviction happened: only 1 entry remains
    long entryCount = manager.cache().asMap().size();
    assertThat(entryCount).isEqualTo(1);

    // Access whichever entry was evicted: it must recompute.
    // But since we cannot know which entry was evicted, we simply check
    // computeCount >= 2 (N1 or N2) + 1 (one recomputation)
    manager.get(tableNode(1), key, "test", "cid");
    manager.get(tableNode(2), key, "test", "cid");

    assertThat(provider.computeCount.get()).isGreaterThanOrEqualTo(3);
  }

  @Test
  void providerExceptionIsPropagated() {
    EngineHintProvider bad =
        new EngineHintProvider() {
          @Override
          public boolean supports(RelationNodeKind k, String h) {
            return true;
          }

          @Override
          public boolean isAvailable(EngineKey k) {
            return true;
          }

          @Override
          public String fingerprint(RelationNode n, EngineKey k, String h) {
            return "x";
          }

          @Override
          public EngineHint compute(RelationNode n, EngineKey k, String h, String c) {
            throw new RuntimeException("boom");
          }
        };

    EngineHintManager manager =
        new EngineHintManager(List.of(bad), new SimpleMeterRegistry(), 1024);

    assertThatThrownBy(() -> manager.get(tableNode(1), new EngineKey("floedb", "1"), "test", "cid"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("boom");
  }

  private static class TestProvider implements EngineHintProvider {

    protected final AtomicInteger computeCount = new AtomicInteger();

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
