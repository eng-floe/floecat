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

package ai.floedb.floecat.service.query.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.metagraph.model.RelationNode;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.SummaryContext;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.TimingAccumulator;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport.FakeCatalogOverlay;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Direct tests of {@link RelationResolutionCache}: the per-request name/node memo the {@link
 * UserObjectBundleService} driver resolves through. Verifies single-flight memoization (present and
 * empty-negative), name normalization collapsing to one key, and that every hit/miss and
 * resolve-nanos lands in the shared {@link TimingAccumulator}. A counting {@link
 * FakeCatalogOverlay} proves the resolve ran at most once per key.
 */
class RelationResolutionCacheTest {

  private static final String CID = "corr-1";
  private static final EngineContext ENGINE = EngineContext.of("pg", "16.0");

  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acct")
          .setId("TABLE_X")
          .setKind(ResourceKind.RK_TABLE)
          .build();

  private static final NameRef NAME = NameRef.newBuilder().setCatalog("cat").setName("x").build();

  private final FakeCatalogOverlay overlay = new FakeCatalogOverlay();
  private TimingAccumulator timings;
  private RelationResolutionCache cache;

  @BeforeEach
  void setUp() {
    overlay.clear();
    overlay.registerTable(TABLE, UserObjectBundleTestSupport.schemaFor("id_x"), NAME);
    timings = new TimingAccumulator();
    cache = new RelationResolutionCache(overlay, CID, ENGINE, timings);
  }

  @Test
  void resolveNameMemoizesPresentResult() {
    Optional<ResourceId> first = cache.resolveName(NAME);
    Optional<ResourceId> second = cache.resolveName(NAME);

    assertThat(first).contains(TABLE);
    assertThat(second).contains(TABLE);
    // Resolved once, then served from the memo.
    assertThat(overlay.resolveNameCount(NAME)).isEqualTo(1);
    assertThat(cache.nameEntries()).isEqualTo(1);

    Recording rec = flush();
    assertThat(rec.get("name_cache_misses")).isEqualTo(1L);
    assertThat(rec.get("name_cache_hits")).isEqualTo(1L);
    assertThat((long) rec.get("name_resolve")).isGreaterThanOrEqualTo(0L);
  }

  @Test
  void resolveNameMemoizesEmptyNegative() {
    NameRef unknown = NameRef.newBuilder().setCatalog("cat").setName("missing").build();

    Optional<ResourceId> first = cache.resolveName(unknown);
    Optional<ResourceId> second = cache.resolveName(unknown);

    assertThat(first).isEmpty();
    assertThat(second).isEmpty();
    // A negative result is cached too: the overlay is not re-queried on the second miss-key lookup.
    assertThat(overlay.resolveNameCount(unknown)).isEqualTo(1);
    assertThat(cache.nameEntries()).isEqualTo(1);

    Recording rec = flush();
    assertThat(rec.get("name_cache_misses")).isEqualTo(1L);
    assertThat(rec.get("name_cache_hits")).isEqualTo(1L);
  }

  @Test
  void resolveNodeMemoizesPresentAndEmpty() {
    ResourceId unknown =
        ResourceId.newBuilder()
            .setAccountId("acct")
            .setId("nope")
            .setKind(ResourceKind.RK_TABLE)
            .build();

    assertThat(cache.resolveNode(TABLE)).isPresent();
    assertThat(cache.resolveNode(TABLE)).isPresent();
    assertThat(cache.resolveNode(unknown)).isEmpty();
    assertThat(cache.resolveNode(unknown)).isEmpty();

    // Each distinct id resolves through the overlay exactly once.
    assertThat(overlay.resolveCount(TABLE)).isEqualTo(1);
    assertThat(overlay.resolveCount(unknown)).isEqualTo(1);
    assertThat(cache.nodeEntries()).isEqualTo(2);

    Recording rec = flush();
    assertThat(rec.get("node_cache_misses")).isEqualTo(2L);
    assertThat(rec.get("node_cache_hits")).isEqualTo(2L);
    assertThat((long) rec.get("node_resolve")).isGreaterThanOrEqualTo(0L);
  }

  @Test
  void canonicalNameUsesAndMemoizesTheOverlayNamingBoundary() {
    AtomicInteger tableNameCalls = new AtomicInteger();
    FakeCatalogOverlay namingOverlay =
        new FakeCatalogOverlay() {
          @Override
          public Optional<NameRef> tableName(ResourceId id) {
            tableNameCalls.incrementAndGet();
            return super.tableName(id);
          }
        };
    namingOverlay.registerTable(TABLE, UserObjectBundleTestSupport.schemaFor("id_x"), NAME);
    RelationResolutionCache namingCache =
        new RelationResolutionCache(namingOverlay, CID, ENGINE, new TimingAccumulator());
    RelationNode relation = (RelationNode) namingOverlay.resolve(TABLE).orElseThrow();

    assertThat(namingCache.canonicalName(relation)).isEqualTo(NAME);
    assertThat(namingCache.canonicalName(relation)).isEqualTo(NAME);
    assertThat(tableNameCalls).hasValue(1);
  }

  @Test
  void normalizationCollapsesWhitespaceVariantsToOneKey() {
    NameRef padded = NameRef.newBuilder().setCatalog("  cat  ").setName("  x  ").build();

    Optional<ResourceId> exact = cache.resolveName(NAME); // miss: hits the overlay
    Optional<ResourceId> whitespace = cache.resolveName(padded); // hit: same normalized key

    assertThat(exact).contains(TABLE);
    assertThat(whitespace).contains(TABLE);
    // Only the first (exact) ref reached the overlay; the padded variant collapsed onto its key.
    assertThat(overlay.resolveNameCount(NAME)).isEqualTo(1);
    assertThat(overlay.resolveNameCount(padded)).isEqualTo(0);
    assertThat(cache.nameEntries()).isEqualTo(1);

    Recording rec = flush();
    assertThat(rec.get("name_cache_misses")).isEqualTo(1L);
    assertThat(rec.get("name_cache_hits")).isEqualTo(1L);
  }

  private Recording flush() {
    Recording rec = new Recording();
    timings.flushInto(rec, new SummaryContext("q", CID, 0, 0, 0.0, 0.0, 0.0, 0, 0, 0, "completed"));
    return rec;
  }

  /** Captures every key/value the flush writes; all timer paths are no-ops. */
  private static final class Recording implements PhaseDiagnostics {
    private final Map<String, Object> values = new LinkedHashMap<>();

    @Override
    public Timer timer(String key) {
      return Timer.NOOP;
    }

    @Override
    public void nanos(String key, long nanos) {
      values.put(key, nanos);
    }

    @Override
    public void count(String key) {}

    @Override
    public void add(String key, long amount) {}

    @Override
    public void put(String key, String value) {
      values.put(key, value);
    }

    @Override
    public void put(String key, long value) {
      values.put(key, value);
    }

    @Override
    public void put(String key, double value) {
      values.put(key, value);
    }

    @Override
    public void put(String key, boolean value) {
      values.put(key, value);
    }

    @Override
    public void emit(String eventName) {}

    Object get(String key) {
      return values.get(key);
    }
  }
}
