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

package ai.floedb.floecat.service.metagraph.hint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.TableFormat;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.hint.EngineHintProvider;
import ai.floedb.floecat.metagraph.model.EngineHint;
import ai.floedb.floecat.metagraph.model.EngineHintKey;
import ai.floedb.floecat.metagraph.model.EngineKey;
import ai.floedb.floecat.metagraph.model.GraphNode;
import ai.floedb.floecat.metagraph.model.GraphNodeKind;
import ai.floedb.floecat.metagraph.model.UserTableNode;
import ai.floedb.floecat.telemetry.TestObservability;
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
        new EngineHintManager(List.of(provider), new TestObservability(), 1024);

    UserTableNode node = tableNode(1L);
    var engineKey = new EngineKey("floedb", "1.0.0");
    var hintKey = new EngineHintKey(engineKey.engineKind(), engineKey.engineVersion(), "test");
    EngineHint hint1 = manager.get(node, engineKey, "test", "cid").orElseThrow();
    EngineHint hint2 = manager.get(node, hintKey, "cid").orElseThrow();
    assertThat(provider.computeCount.get()).isEqualTo(1);
    assertThat(hint2.payload()).isEqualTo(hint1.payload());

    manager.invalidate(node.id());
    UserTableNode nodeUpdated = tableNode(2L);
    manager.get(nodeUpdated, new EngineKey("floedb", "1.0.0"), "test", "cid").orElseThrow();
    assertThat(provider.computeCount.get()).isEqualTo(2);
  }

  @Test
  void returnsEmptyWhenNoProvider() {
    EngineHintManager manager = new EngineHintManager(List.of(), new TestObservability(), 1024);
    assertThat(manager.get(tableNode(1L), new EngineKey("floedb", "1"), "missing", "cid"))
        .isEmpty();
  }

  private static UserTableNode tableNode(long version) {
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
    return new UserTableNode(
        tableId,
        version,
        Instant.now(),
        catalogId,
        namespaceId,
        "tbl",
        TableFormat.TF_ICEBERG,
        ColumnIdAlgorithm.CID_FIELD_ID,
        "{}",
        Map.of(),
        List.of(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(),
        Map.of(),
        Map.of());
  }

  @Test
  void differentFingerprintsProduceDifferentCacheEntries() {
    var provider =
        new TestProvider() {
          @Override
          public String fingerprint(GraphNode node, EngineKey key, String payloadType) {
            return computeCount.get() == 0 ? "fp1" : "fp2";
          }
        };

    var manager = new EngineHintManager(List.of(provider), new TestObservability(), 1024);
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
        new EngineHintManager(List.of(p1, p2), new TestObservability(), 1024);

    var node = tableNode(1);
    var key = new EngineHintKey("floedb", "1", "test");

    manager.get(node, key, "cid");

    assertThat(p1.computeCount.get()).isEqualTo(1);
    assertThat(p2.computeCount.get()).isEqualTo(0);
  }

  @Test
  void evictionOccursWhenWeightExceeded() {
    var provider =
        new TestProvider() {
          @Override
          public Optional<EngineHint> compute(
              GraphNode node, EngineKey k, String payloadType, String c) {
            computeCount.incrementAndGet();
            return Optional.of(new EngineHint("t", new byte[512])); // heavy
          }
        };

    // max 600 bytes -> only one entry can fit
    var manager = new EngineHintManager(List.of(provider), new TestObservability(), 600, true);
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
          public boolean supports(GraphNodeKind k, String h) {
            return true;
          }

          @Override
          public boolean isAvailable(EngineKey k) {
            return true;
          }

          @Override
          public String fingerprint(GraphNode n, EngineKey k, String h) {
            return "x";
          }

          @Override
          public Optional<EngineHint> compute(GraphNode n, EngineKey k, String h, String c) {
            throw new RuntimeException("boom");
          }
        };

    EngineHintManager manager = new EngineHintManager(List.of(bad), new TestObservability(), 1024);

    assertThatThrownBy(() -> manager.get(tableNode(1), new EngineKey("floedb", "1"), "test", "cid"))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("boom");
  }

  @Test
  void cacheIsolatedByPayloadType() {
    TestProvider.GLOBAL_CALLS.set(0);
    var manager = new EngineHintManager(List.of(new TestProvider()), new TestObservability(), 1024);
    var node = tableNode(1);
    var key = new EngineKey("floedb", "1");
    manager.get(node, key, "test", "cid").orElseThrow();
    manager.get(node, key, "other", "cid").orElseThrow();

    assertThat(TestProvider.GLOBAL_CALLS.get()).isEqualTo(2);
    manager.get(node, key, "test", "cid");
    manager.get(node, key, "other", "cid");
    assertThat(TestProvider.GLOBAL_CALLS.get()).isEqualTo(2);
  }

  @Test
  void unsupportedPayloadTypeNeverComputes() {
    var provider = new TestProvider();
    var manager = new EngineHintManager(List.of(provider), new TestObservability(), 1024);
    var node = tableNode(1);

    assertThat(manager.get(node, new EngineKey("floedb", "1"), "missing", "cid")).isEmpty();
    assertThat(provider.computeCount.get()).isEqualTo(0);
  }

  private static class TestProvider implements EngineHintProvider {

    protected final AtomicInteger computeCount = new AtomicInteger();
    static final AtomicInteger GLOBAL_CALLS = new AtomicInteger();
    static final AtomicInteger callsStatic = new AtomicInteger();

    @Override
    public boolean supports(GraphNodeKind kind, String payloadType) {
      return kind == GraphNodeKind.TABLE
          && (payloadType.equals("test") || payloadType.equals("other"));
    }

    @Override
    public boolean isAvailable(EngineKey engineKey) {
      return true;
    }

    @Override
    public String fingerprint(GraphNode node, EngineKey engineKey, String payloadType) {
      return node.version() + "-fp";
    }

    @Override
    public Optional<EngineHint> compute(
        GraphNode node, EngineKey engineKey, String payloadType, String correlationId) {
      computeCount.incrementAndGet();
      GLOBAL_CALLS.incrementAndGet();
      callsStatic.incrementAndGet();
      return Optional.of(new EngineHint("test.payload+bytes", new byte[] {1, 2, 3}));
    }
  }
}
