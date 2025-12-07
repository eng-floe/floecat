package ai.floedb.floecat.service.query.graph.hint;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.*;
import ai.floedb.floecat.service.query.graph.model.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HintCacheTest {

  @Test
  void cachesAndInvalidates() {
    var provider = new TestProvider();
    var mgr = new EngineHintManager(java.util.List.of(provider), new SimpleMeterRegistry(), 1024);

    var table = tableNode(1L);
    var key = new EngineKey("demo", "1");

    var h1 = mgr.get(table, key, "test", "cid").orElseThrow();
    var h2 = mgr.get(table, key, "test", "cid").orElseThrow();

    assertThat(provider.calls.get()).isEqualTo(1);
    assertThat(h1.payload()).isEqualTo(h2.payload());

    mgr.invalidate(table.id());

    mgr.get(tableNode(2L), key, "test", "cid").orElseThrow();
    assertThat(provider.calls.get()).isEqualTo(2);
  }

  private static TableNode tableNode(long version) {
    ResourceId tableId =
        ResourceId.newBuilder()
            .setTenantId("t")
            .setId("tbl")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    return new TableNode(
        tableId,
        version,
        Instant.EPOCH,
        tableId,
        tableId,
        "tbl",
        ai.floedb.floecat.catalog.rpc.TableFormat.TF_ICEBERG,
        "{}",
        Map.of(),
        java.util.List.of(),
        Map.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        java.util.List.of(),
        Map.of());
  }

  static class TestProvider implements EngineHintProvider {
    AtomicInteger calls = new AtomicInteger();

    @Override
    public boolean supports(RelationNodeKind k, String h) {
      return k == RelationNodeKind.TABLE && h.equals("test");
    }

    @Override
    public boolean isAvailable(EngineKey k) {
      return true;
    }

    @Override
    public String fingerprint(RelationNode n, EngineKey k, String h) {
      return n.version() + "";
    }

    @Override
    public EngineHint compute(RelationNode n, EngineKey k, String h, String c) {
      calls.incrementAndGet();
      return new EngineHint("application/test", new byte[] {1, 2, 3});
    }
  }
}
