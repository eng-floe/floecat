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

import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.SummaryContext;
import ai.floedb.floecat.service.query.catalog.UserObjectBundleService.TimingAccumulator;
import ai.floedb.floecat.telemetry.PhaseDiagnostics;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Direct unit tests for the single per-request telemetry tally: {@link TimingAccumulator#mergeFrom}
 * sums every field across two instances, and {@link TimingAccumulator#flushInto} writes exactly the
 * documented summary key set (docs/telemetry/diagnostics.md) with the expected write verbs.
 */
class TimingAccumulatorTest {

  /** The complete, ordered summary key set the GetUserObjects contract emits. */
  private static final Set<String> EXPECTED_KEYS =
      Set.of(
          "query_id",
          "correlation_id",
          "candidates",
          "chunks",
          "found",
          "not_found",
          "total_ms",
          "resolve",
          "normalize",
          "select_relation",
          "default_catalog",
          "name_resolve",
          "node_resolve",
          "base_inject",
          "pin_collect",
          "pin_commit",
          "pin_ms",
          "relation_build",
          "decoration",
          "stats_lookup",
          "stats_warm",
          "decorate_relation",
          "decorate_view",
          "decorate_columns",
          "decorate_column_invoke",
          "decorate_complete",
          "scheduling_ms",
          "decorator_warm_hits",
          "hint_persist",
          "default_catalog_lookups",
          "name_cache_hits",
          "name_cache_misses",
          "node_cache_hits",
          "node_cache_misses",
          "name_cache_entries",
          "node_cache_entries",
          "relation_cache_entries",
          "outcome");

  @Test
  void mergeFromSumsEveryFieldAcrossTwoInstances() {
    TimingAccumulator a = new TimingAccumulator();
    a.addStatsLookupNanos(1);
    a.addDecorateRelationNanos(1);
    a.addDecorateViewNanos(1);
    a.addDecorateColumnsNanos(1);
    a.addDecorateColumnInvokeNanos(1);
    a.addDecorateCompleteNanos(1);
    a.addDecoratePersistRelationNanos(1);
    a.addDecoratePersistColumnsNanos(1);
    a.addDecorateColumnWarmHits(1);
    a.addResolveNanos(1);
    a.addNormalizeNanos(1);
    a.addDefaultCatalogNanos(1);
    a.addBaseInjectNanos(1);
    a.addPinCollectNanos(1);
    a.addPinCommitNanos(1);
    a.addRelationBuildNanos(1);
    a.addDecorationNanos(1);
    a.addSelectRelationNanos(1);
    a.addNameResolveNanos(1);
    a.addNodeResolveNanos(1);
    a.recordFound();
    a.recordNotFound();
    a.recordDefaultCatalogLookup();
    a.recordNameCacheHit();
    a.recordNameCacheMiss();
    a.recordNodeCacheHit();
    a.recordNodeCacheMiss();

    TimingAccumulator b = new TimingAccumulator();
    b.addStatsLookupNanos(10);
    b.addDecorateRelationNanos(10);
    b.addDecorateViewNanos(10);
    b.addDecorateColumnsNanos(10);
    b.addDecorateColumnInvokeNanos(10);
    b.addDecorateCompleteNanos(10);
    b.addDecoratePersistRelationNanos(10);
    b.addDecoratePersistColumnsNanos(10);
    b.addDecorateColumnWarmHits(10);
    b.addResolveNanos(10);
    b.addNormalizeNanos(10);
    b.addDefaultCatalogNanos(10);
    b.addBaseInjectNanos(10);
    b.addPinCollectNanos(10);
    b.addPinCommitNanos(10);
    b.addRelationBuildNanos(10);
    b.addDecorationNanos(10);
    b.addSelectRelationNanos(10);
    b.addNameResolveNanos(10);
    b.addNodeResolveNanos(10);
    b.recordFound();
    b.recordNotFound();
    b.recordDefaultCatalogLookup();
    b.recordNameCacheHit();
    b.recordNameCacheMiss();
    b.recordNodeCacheHit();
    b.recordNodeCacheMiss();

    a.mergeFrom(b);

    RecordingDiagnostics rec = new RecordingDiagnostics();
    a.flushInto(rec, context("merge"));

    // Timers: each folded field is the sum of the two instances (1 + 10 = 11).
    assertThat(rec.get("stats_lookup")).isEqualTo(11L);
    assertThat(rec.get("decorate_relation")).isEqualTo(11L);
    assertThat(rec.get("decorate_view")).isEqualTo(11L);
    assertThat(rec.get("decorate_columns")).isEqualTo(11L);
    assertThat(rec.get("decorate_column_invoke")).isEqualTo(11L);
    assertThat(rec.get("decorate_complete")).isEqualTo(11L);
    assertThat(rec.get("resolve")).isEqualTo(11L);
    assertThat(rec.get("normalize")).isEqualTo(11L);
    assertThat(rec.get("default_catalog")).isEqualTo(11L);
    assertThat(rec.get("base_inject")).isEqualTo(11L);
    assertThat(rec.get("pin_collect")).isEqualTo(11L);
    assertThat(rec.get("pin_commit")).isEqualTo(11L);
    assertThat(rec.get("relation_build")).isEqualTo(11L);
    assertThat(rec.get("decoration")).isEqualTo(11L);
    assertThat(rec.get("select_relation")).isEqualTo(11L);
    assertThat(rec.get("name_resolve")).isEqualTo(11L);
    assertThat(rec.get("node_resolve")).isEqualTo(11L);
    // hint_persist is the sum of both persist sub-phases across both instances: (1+1) + (10+10).
    assertThat(rec.get("hint_persist")).isEqualTo(22L);
    // Counters: each records once per instance, so the merge yields 2.
    assertThat(rec.get("found")).isEqualTo(2L);
    assertThat(rec.get("not_found")).isEqualTo(2L);
    assertThat(rec.get("decorator_warm_hits")).isEqualTo(11L);
    assertThat(rec.get("default_catalog_lookups")).isEqualTo(2L);
    assertThat(rec.get("name_cache_hits")).isEqualTo(2L);
    assertThat(rec.get("name_cache_misses")).isEqualTo(2L);
    assertThat(rec.get("node_cache_hits")).isEqualTo(2L);
    assertThat(rec.get("node_cache_misses")).isEqualTo(2L);
  }

  @Test
  void flushIntoWritesEveryContractKeyAndEmitsSummary() {
    TimingAccumulator tally = new TimingAccumulator();
    // Distinct values so a mis-wired key would surface as a wrong number, not a coincidental match.
    tally.addResolveNanos(100);
    tally.addNormalizeNanos(200);
    tally.addSelectRelationNanos(300);
    tally.addDefaultCatalogNanos(400);
    tally.addNameResolveNanos(500);
    tally.addNodeResolveNanos(600);
    tally.addBaseInjectNanos(700);
    tally.addPinCollectNanos(800);
    tally.addPinCommitNanos(900);
    tally.addRelationBuildNanos(1000);
    tally.addDecorationNanos(1100);
    tally.addStatsLookupNanos(1200);
    tally.addDecorateRelationNanos(1300);
    tally.addDecorateViewNanos(1400);
    tally.addDecorateColumnsNanos(1500);
    tally.addDecorateColumnInvokeNanos(1600);
    tally.addDecorateCompleteNanos(1700);
    tally.addDecoratePersistRelationNanos(1800);
    tally.addDecoratePersistColumnsNanos(1900);
    tally.addDecorateColumnWarmHits(7);
    tally.recordFound();
    tally.recordFound();
    tally.recordNotFound();
    tally.recordDefaultCatalogLookup();
    tally.recordNameCacheHit();
    tally.recordNameCacheMiss();
    tally.recordNameCacheMiss();
    tally.recordNodeCacheHit();
    tally.recordNodeCacheMiss();

    SummaryContext ctx =
        new SummaryContext("q-1", "corr-1", 5, 3, 12.5, 1.7, 0.4, 11, 13, 17, "completed");

    RecordingDiagnostics rec = new RecordingDiagnostics();
    tally.flushInto(rec, ctx);

    assertThat(rec.keys()).isEqualTo(EXPECTED_KEYS);
    assertThat(rec.emittedEvent()).isEqualTo("floecat.get_user_objects.summary");

    // Context-carried scalars and derived durations.
    assertThat(rec.get("query_id")).isEqualTo("q-1");
    assertThat(rec.get("correlation_id")).isEqualTo("corr-1");
    assertThat(rec.get("candidates")).isEqualTo(5L);
    assertThat(rec.get("chunks")).isEqualTo(3L);
    assertThat(rec.get("total_ms")).isEqualTo(12.5);
    assertThat(rec.get("pin_ms")).isEqualTo(1.7);
    assertThat(rec.get("scheduling_ms")).isEqualTo(0.4);
    assertThat(rec.get("name_cache_entries")).isEqualTo(11L);
    assertThat(rec.get("node_cache_entries")).isEqualTo(13L);
    assertThat(rec.get("relation_cache_entries")).isEqualTo(17L);
    assertThat(rec.get("outcome")).isEqualTo("completed");

    // Tally-owned timers and counters.
    assertThat(rec.get("resolve")).isEqualTo(100L);
    assertThat(rec.get("normalize")).isEqualTo(200L);
    assertThat(rec.get("select_relation")).isEqualTo(300L);
    assertThat(rec.get("default_catalog")).isEqualTo(400L);
    assertThat(rec.get("name_resolve")).isEqualTo(500L);
    assertThat(rec.get("node_resolve")).isEqualTo(600L);
    assertThat(rec.get("base_inject")).isEqualTo(700L);
    assertThat(rec.get("pin_collect")).isEqualTo(800L);
    assertThat(rec.get("pin_commit")).isEqualTo(900L);
    assertThat(rec.get("relation_build")).isEqualTo(1000L);
    assertThat(rec.get("decoration")).isEqualTo(1100L);
    assertThat(rec.get("stats_lookup")).isEqualTo(1200L);
    assertThat(rec.get("decorate_relation")).isEqualTo(1300L);
    assertThat(rec.get("decorate_view")).isEqualTo(1400L);
    assertThat(rec.get("decorate_columns")).isEqualTo(1500L);
    assertThat(rec.get("decorate_column_invoke")).isEqualTo(1600L);
    assertThat(rec.get("decorate_complete")).isEqualTo(1700L);
    assertThat(rec.get("hint_persist")).isEqualTo(3700L); // 1800 + 1900
    assertThat(rec.get("decorator_warm_hits")).isEqualTo(7L);
    assertThat(rec.get("found")).isEqualTo(2L);
    assertThat(rec.get("not_found")).isEqualTo(1L);
    assertThat(rec.get("default_catalog_lookups")).isEqualTo(1L);
    assertThat(rec.get("name_cache_hits")).isEqualTo(1L);
    assertThat(rec.get("name_cache_misses")).isEqualTo(2L);
    assertThat(rec.get("node_cache_hits")).isEqualTo(1L);
    assertThat(rec.get("node_cache_misses")).isEqualTo(1L);
  }

  /**
   * The stats warm pass is its own wall-clock interval emitted as stats_warm; the residual
   * scheduling time must exclude it, or the same interval is counted twice (once as stats_warm,
   * again in scheduling). Regression guard for the split of stats_warm out of stats_lookup.
   */
  @Test
  void schedulingExcludesTheStatsWarmInterval() {
    TimingAccumulator tally = new TimingAccumulator();
    tally.addStatsWarmNanos(1_000_000L); // 1 ms warming
    tally.addStatsLookupNanos(500_000L); // 0.5 ms of per-relation reads
    // total = warm + lookup + a 0.4 ms residual; only the residual is scheduling.
    long total = 1_000_000L + 500_000L + 400_000L;
    assertThat(tally.schedulingNanos(total)).isEqualTo(400_000L);
  }

  private static SummaryContext context(String outcome) {
    return new SummaryContext("q", "c", 0, 0, 0.0, 0.0, 0.0, 0, 0, 0, outcome);
  }

  /**
   * Records every key/value written and the emitted event name; all timer/count paths are no-ops.
   */
  private static final class RecordingDiagnostics implements PhaseDiagnostics {
    private final Map<String, Object> values = new LinkedHashMap<>();
    private String emittedEvent;

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
    public void emit(String eventName) {
      this.emittedEvent = eventName;
    }

    Object get(String key) {
      return values.get(key);
    }

    Set<String> keys() {
      return values.keySet();
    }

    String emittedEvent() {
      return emittedEvent;
    }
  }
}
