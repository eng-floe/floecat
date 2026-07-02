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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.SketchPayload;
import ai.floedb.floecat.catalog.rpc.SketchRole;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.query.rpc.FetchTargetStatsRequest;
import ai.floedb.floecat.query.rpc.RequestedStat;
import ai.floedb.floecat.query.rpc.StatRole;
import ai.floedb.floecat.query.rpc.StatsResultStatus;
import ai.floedb.floecat.query.rpc.TableStatsRequest;
import ai.floedb.floecat.query.rpc.TargetStatsBundleChunk;
import ai.floedb.floecat.query.rpc.TargetStatsNeed;
import ai.floedb.floecat.service.query.catalog.testsupport.UserObjectBundleTestSupport;
import ai.floedb.floecat.service.query.impl.QueryContext;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Latency and scenario benchmark for {@link PlannerStatsBundleService}.
 *
 * <p>Uses an in-memory store — measures pure serving overhead (proto materialization, iteration,
 * serialization) without I/O. Real-stack latency will be higher by the round-trip cost of the
 * underlying store (see the integration perf test in core).
 *
 * <p><b>Run with defaults (recommended first run):</b>
 *
 * <pre>
 *   mvn test -pl service -Dtest=StatsPerfTest
 * </pre>
 *
 * <p><b>Tunable parameters:</b>
 *
 * <pre>
 *   # Comma-separated target counts to benchmark
 *   -Dstats.perf.targets=10,100,500,1000
 *
 *   # Scenarios: SCALAR and/or THETA
 *   -Dstats.perf.scenario=SCALAR,THETA
 *
 *   # Theta sketch k parameter (affects sketch size in THETA mode)
 *   -Dstats.perf.theta_k=4096
 *
 *   # Warmup iterations (JIT compiles hot paths before measurement)
 *   -Dstats.perf.warmup=5
 *
 *   # Measurement iterations per cell
 *   -Dstats.perf.runs=30
 *
 *   # p95 thresholds: warn is printed by default; fail is opt-in for dedicated perf gates
 *   -Dstats.perf.warn_ms=20
 *   -Dstats.perf.fail_ms=50
 * </pre>
 *
 * <p><b>Examples:</b>
 *
 * <pre>
 *   # Quick sweep with sketches enabled
 *   mvn test -pl service -Dtest=StatsPerfTest -Dstats.perf.scenario=SCALAR,THETA
 *
 *   # Tune theta k: compare 256 vs 4096
 *   mvn test -pl service -Dtest=StatsPerfTest \
 *       -Dstats.perf.scenario=THETA -Dstats.perf.theta_k=256
 *   mvn test -pl service -Dtest=StatsPerfTest \
 *       -Dstats.perf.scenario=THETA -Dstats.perf.theta_k=4096
 *
 *   # High target count stress test
 *   mvn test -pl service -Dtest=StatsPerfTest \
 *       -Dstats.perf.targets=100,500,1000,2000 -Dstats.perf.runs=50
 * </pre>
 */
@Tag("perf")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StatsPerfTest extends PlannerStatsBundleServiceTestSupport {

  // ── Tunable via -D system properties ──────────────────────────────────────

  private static final List<Integer> TARGETS =
      parseInts(System.getProperty("stats.perf.targets", "10,100,500"));

  private static final List<PerfScenario> SCENARIOS =
      parseLevels(System.getProperty("stats.perf.scenario", "SCALAR"));

  private static final int THETA_K = intProp("stats.perf.theta_k", 4096);
  private static final int WARMUP = intProp("stats.perf.warmup", 5);
  private static final int RUNS = intProp("stats.perf.runs", 30);
  private static final double WARN_P95_MS = doubleProp("stats.perf.warn_ms", 20.0);
  private static final double FAIL_P95_MS =
      doubleProp("stats.perf.fail_ms", Double.POSITIVE_INFINITY);

  // ── Fixed per test run ─────────────────────────────────────────────────────

  private static final long SNAPSHOT_ID = 100L;
  private static final String QUERY_ID = "perf-q";

  /** Compact theta sketch bytes at THETA_K, built once and reused for all THETA records. */
  private static byte[] THETA_BYTES;

  private static final String THETA_SKETCH_TYPE = "apache-datasketches-theta-v4";

  private enum PerfScenario {
    SCALAR,
    THETA
  }

  @BeforeAll
  static void buildSketchBytes() {
    var sketch = UpdateSketch.builder().setNominalEntries(THETA_K).build();
    // Fill past k so the sketch enters estimation mode and compacts to ~k*8 bytes.
    for (int i = 0; i < THETA_K * 2; i++) sketch.update(i);
    THETA_BYTES = sketch.compact(true, null).toByteArray();
  }

  // ── Benchmark ─────────────────────────────────────────────────────────────

  /**
   * Measures serving latency (p50/p95/max) and scenario size for each (target_count, scenario)
   * combination. Always prints the table regardless of assertion result.
   */
  @Test
  @Order(1)
  void latencyTable() {
    var rows = new ArrayList<Row>();
    var warnings = new ArrayList<String>();

    for (int n : TARGETS) {
      for (var level : SCENARIOS) {
        var store = new UserObjectBundleTestSupport.TestQueryContextStore();
        var ctx = queryContextWithPin(QUERY_ID, SNAPSHOT_ID);
        store.seed(ctx);

        var repo = createRepository();
        seedStats(repo, n, level);

        // Leave headroom above n so no OMITTED_BY_BUDGET during measurement.
        var svc = createService(repo, store, /* chunkSize= */ 1000, /* maxTables= */ 5, n + 50);
        var req = buildRequest(n, level);

        // Validate that stats were actually seeded and served before measuring.
        // Fails fast if results are NOT_FOUND (pin mismatch, missing records, etc.)
        assertAllHitComplete(invoke(svc, ctx, req), n, level);

        // Warmup — let the JIT compile hot paths before taking measurements.
        for (int i = 0; i < WARMUP; i++) invoke(svc, ctx, req);

        // Measure.
        long[] ns = new long[RUNS];
        int bytes = 0;
        for (int i = 0; i < RUNS; i++) {
          long t0 = System.nanoTime();
          var chunks = invoke(svc, ctx, req);
          ns[i] = System.nanoTime() - t0;
          if (i == 0) bytes = scenarioBytes(chunks);
        }

        double p50 = percentileMs(ns, 50);
        double p95 = percentileMs(ns, 95);
        double max = Arrays.stream(ns).max().orElse(0) / 1_000_000.0;
        rows.add(new Row(n, level, p50, p95, max, bytes));

        if (failThresholdEnabled() && p95 > FAIL_P95_MS) {
          warnings.add(
              "FAIL  %3d targets %-12s p95=%.1fms > fail threshold (%.0fms)"
                  .formatted(n, levelName(level), p95, FAIL_P95_MS));
        } else if (p95 > WARN_P95_MS) {
          warnings.add(
              "WARN  %3d targets %-12s p95=%.1fms > warn threshold (%.0fms)"
                  .formatted(n, levelName(level), p95, WARN_P95_MS));
        }
      }
    }

    printTable(rows, warnings);

    var failures =
        failThresholdEnabled()
            ? rows.stream().filter(r -> r.p95 > FAIL_P95_MS).toList()
            : List.<Row>of();
    if (!failures.isEmpty()) {
      fail(
          "Stats perf failures (p95 > %.0fms): %s"
              .formatted(
                  FAIL_P95_MS,
                  failures.stream()
                      .map(r -> "%d targets %s=%.1fms".formatted(r.n, levelName(r.level), r.p95))
                      .reduce((a, b) -> a + ", " + b)
                      .orElse("")));
    }
  }

  // ── Seeding ───────────────────────────────────────────────────────────────

  private static void seedStats(StatsRepository repo, int n, PerfScenario level) {
    for (long col = 1; col <= n; col++) {
      ScalarStats stats =
          level == PerfScenario.THETA
              ? scalarWithSketch(col)
              : sampleStats(TABLE, SNAPSHOT_ID, col);
      repo.putTargetStats(TargetStatsRecords.columnRecord(TABLE, SNAPSHOT_ID, col, stats, null));
    }
  }

  private static ScalarStats scalarWithSketch(long columnId) {
    return ScalarStats.newBuilder()
        .setDisplayName("col" + columnId)
        .setRowCount(columnId * 100)
        .setNullCount(columnId)
        .addSketches(
            SketchPayload.newBuilder()
                .setRole(SketchRole.SKETCH_ROLE_NDV)
                .setSketchType(THETA_SKETCH_TYPE)
                .setData(ByteString.copyFrom(THETA_BYTES))
                .putParams("k", String.valueOf(THETA_K))
                .build())
        .build();
  }

  // ── Request building ──────────────────────────────────────────────────────

  private static FetchTargetStatsRequest buildRequest(int n, PerfScenario level) {
    List<TargetStatsNeed> needs = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      needs.add(
          targetNeedBuilder(level)
              .setTarget(
                  StatsTarget.newBuilder()
                      .setColumn(ColumnStatsTarget.newBuilder().setColumnId(i + 1L)))
              .setPriority(i + 1)
              .build());
    }
    return FetchTargetStatsRequest.newBuilder()
        .setQueryId(QUERY_ID)
        .addTables(TableStatsRequest.newBuilder().setTableId(TABLE).addAllTargets(needs))
        .build();
  }

  // ── Validation ────────────────────────────────────────────────────────────

  private static void assertAllHitComplete(
      List<TargetStatsBundleChunk> chunks, int expectedN, PerfScenario level) {
    var results = flatten(chunks);
    assertEquals(
        expectedN,
        results.size(),
        "Expected %d results but got %d — seeding or request mismatch?"
            .formatted(expectedN, results.size()));

    var notHit =
        results.stream()
            .filter(r -> r.getStatus() != StatsResultStatus.STATS_RESULT_HIT_COMPLETE)
            .toList();
    if (!notHit.isEmpty()) {
      var summary =
          notHit.stream()
              .map(
                  r ->
                      "col %d → %s%s"
                          .formatted(
                              r.getTarget().getColumn().getColumnId(),
                              r.getStatus(),
                              r.hasFailure() ? " (" + r.getFailure().getCode() + ")" : ""))
              .reduce((a, b) -> a + ", " + b)
              .orElse("?");
      fail("%d/%d results are not HIT_COMPLETE: %s".formatted(notHit.size(), expectedN, summary));
    }

    // Spot-check the first result has actual stats data.
    var first = results.getFirst();
    assertTrue(first.hasStats(), "HIT_COMPLETE result must carry a stats record");
    assertTrue(first.getStats().hasScalar(), "Column stats result must have a scalar scenario");
    assertTrue(first.getStats().getScalar().getRowCount() > 0, "Seeded row_count must be > 0");
    if (level == PerfScenario.THETA) {
      assertFalse(
          first.getStats().getScalar().getSketchesList().isEmpty(),
          "THETA scenario level must carry at least one sketch");
    }
  }

  // ── Invocation + measurement helpers ─────────────────────────────────────

  private static List<TargetStatsBundleChunk> invoke(
      PlannerStatsBundleService svc, QueryContext ctx, FetchTargetStatsRequest req) {
    return svc.streamTargets("perf", ctx, req).collect().asList().await().indefinitely();
  }

  private static int scenarioBytes(List<TargetStatsBundleChunk> chunks) {
    return chunks.stream().mapToInt(com.google.protobuf.AbstractMessage::getSerializedSize).sum();
  }

  private static double percentileMs(long[] ns, int pct) {
    long[] sorted = Arrays.copyOf(ns, ns.length);
    Arrays.sort(sorted);
    int idx = (int) Math.ceil(pct / 100.0 * sorted.length) - 1;
    return sorted[Math.max(0, idx)] / 1_000_000.0;
  }

  // ── Output ────────────────────────────────────────────────────────────────

  private static void printTable(List<Row> rows, List<String> warnings) {
    String sep = "─".repeat(72);
    String header =
        "  Stats Serving Benchmark  (InMemoryStore · %d runs after %d warmup · theta k=%d)"
            .formatted(RUNS, WARMUP, THETA_K);

    System.out.println();
    System.out.println("┌" + sep + "┐");
    System.out.printf("│  %-70s│%n", header);
    System.out.println("├──────────┬──────────────┬──────────┬──────────┬──────────┬────────────┤");
    System.out.println(
        "│ targets  │ scenario      │  p50 ms  │  p95 ms  │  max ms  │  bytes     │");
    System.out.println("├──────────┼──────────────┼──────────┼──────────┼──────────┼────────────┤");

    for (var r : rows) {
      String warn = r.p95 > FAIL_P95_MS ? " ✗" : r.p95 > WARN_P95_MS ? " ⚠" : "";
      System.out.printf(
          "│ %8d │ %-12s │ %8.2f │ %7.2f%s │ %8.2f │ %10s │%n",
          r.n, levelName(r.level), r.p50, r.p95, warn, r.max, formatBytes(r.bytes));
    }

    System.out.println("└──────────┴──────────────┴──────────┴──────────┴──────────┴────────────┘");

    if (!warnings.isEmpty()) {
      System.out.println();
      warnings.forEach(w -> System.out.println("  " + w));
    }
    System.out.println();
  }

  private static String formatBytes(int bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return "%.1f KB".formatted(bytes / 1024.0);
    return "%.1f MB".formatted(bytes / (1024.0 * 1024));
  }

  private static String levelName(PerfScenario level) {
    return switch (level) {
      case SCALAR -> "SCALAR";
      case THETA -> "THETA";
    };
  }

  // ── Config parsing ────────────────────────────────────────────────────────

  private static List<Integer> parseInts(String csv) {
    return Arrays.stream(csv.split(",")).map(String::trim).map(Integer::parseInt).toList();
  }

  private static List<PerfScenario> parseLevels(String csv) {
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .map(
            s ->
                switch (s) {
                  case "SCALAR" -> PerfScenario.SCALAR;
                  case "THETA" -> PerfScenario.THETA;
                  default -> throw new IllegalArgumentException("Unknown scenario level: " + s);
                })
        .toList();
  }

  private static TargetStatsNeed.Builder targetNeedBuilder(PerfScenario scenario) {
    TargetStatsNeed.Builder builder =
        TargetStatsNeed.newBuilder()
            .addRequestedStats(RequestedStat.newBuilder().setRole(StatRole.STAT_ROLE_SCALAR));
    if (scenario == PerfScenario.THETA) {
      builder.addRequestedStats(
          RequestedStat.newBuilder()
              .setRole(StatRole.STAT_ROLE_NDV)
              .setSketchType(THETA_SKETCH_TYPE));
    }
    return builder;
  }

  private static int intProp(String key, int def) {
    return Integer.parseInt(System.getProperty(key, String.valueOf(def)));
  }

  private static double doubleProp(String key, double def) {
    return Double.parseDouble(System.getProperty(key, String.valueOf(def)));
  }

  private static boolean failThresholdEnabled() {
    return Double.isFinite(FAIL_P95_MS);
  }

  private record Row(int n, PerfScenario level, double p50, double p95, double max, int bytes) {}
}
