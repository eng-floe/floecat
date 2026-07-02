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

package ai.floedb.floecat.connector.common;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.connector.common.ndv.ColumnNdv;
import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.connector.common.ndv.ParquetAvgWidthProvider;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.Test;

class GenericStatsEngineTest {

  // Minimal Planner stub backed by a fixed list of PlannedFiles
  private static Planner<Integer> planner(
      Set<Integer> columns,
      Map<Integer, String> names,
      Map<Integer, LogicalType> types,
      List<PlannedFile<Integer>> files) {

    return new Planner<>() {
      @Override
      public Map<Integer, String> columnNamesByKey() {
        return names;
      }

      @Override
      public Map<Integer, LogicalType> logicalTypesByKey() {
        return types;
      }

      @Override
      public NdvProvider ndvProvider() {
        return null;
      }

      @Override
      public Set<Integer> columns() {
        return columns;
      }

      @Override
      public Iterator<PlannedFile<Integer>> iterator() {
        return files.iterator();
      }
    };
  }

  @Test
  void perFileStats_includesAllPlannerColumns_evenWhenMetricsAreSparse() {
    // Planner knows 3 columns: int (1), date (2), timestamp (3)
    var cols = Set.of(1, 2, 3);
    var names = Map.of(1, "int_col", 2, "date_col", 3, "ts_col");
    var types =
        Map.of(
            1, LogicalType.of(LogicalKind.INT),
            2, LogicalType.of(LogicalKind.DATE),
            3, LogicalType.of(LogicalKind.TIMESTAMP));

    // File metrics only cover the int column — simulating Iceberg not emitting
    // metrics for date/timestamp types
    var file =
        new PlannedFile<>(
            "s3://bucket/file.parquet",
            "PARQUET",
            100L,
            1024L,
            Map.of(1, 100L), // rowCounts: int only
            Map.of(1, 5L), // nullCounts: int only
            null, // nanCounts
            Map.of(1, (Object) 0), // lowerBounds: int only
            Map.of(1, (Object) 99), // upperBounds: int only
            null,
            0,
            null);

    var engine =
        new GenericStatsEngine<>(
            planner(cols, names, types, List.of(file)), null, null, names, types);

    var result = engine.compute();

    assertEquals(1, result.files().size());
    var fileAgg = result.files().get(0);

    // All 3 columns must be present — including date and timestamp
    assertEquals(3, fileAgg.columns().size(), "per-file stats must include all schema columns");
    assertTrue(fileAgg.columns().containsKey(1), "int column must be present");
    assertTrue(fileAgg.columns().containsKey(2), "date column must be present");
    assertTrue(fileAgg.columns().containsKey(3), "timestamp column must be present");

    // int column has real stats
    var intAgg = fileAgg.columns().get(1);
    assertEquals(100L, intAgg.rowCount());
    assertEquals(5L, intAgg.nullCount());
    assertEquals(0, intAgg.min());
    assertEquals(99, intAgg.max());

    // date and timestamp remain sparse inside GenericStatsEngine; required row-count fallback
    // happens later during view/proto building.
    var dateAgg = fileAgg.columns().get(2);
    assertNull(dateAgg.rowCount(), "date rowCount should be null when no metrics");
    assertNull(dateAgg.nullCount(), "date nullCount should be null when no metrics");
    assertNull(dateAgg.min(), "date min should be null when no metrics");
    assertNull(dateAgg.max(), "date max should be null when no metrics");

    var tsAgg = fileAgg.columns().get(3);
    assertNull(tsAgg.rowCount(), "timestamp rowCount should be null when no metrics");
    assertNull(tsAgg.nullCount(), "timestamp nullCount should be null when no metrics");
    assertNull(tsAgg.min(), "timestamp min should be null when no metrics");
    assertNull(tsAgg.max(), "timestamp max should be null when no metrics");
  }

  @Test
  void perFileStats_preservesMetricsColumns_whenAllColumnsHaveMetrics() {
    // Regression: dense input should still work correctly
    var cols = Set.of(1, 2);
    var names = Map.of(1, "a", 2, "b");
    var types = Map.of(1, LogicalType.of(LogicalKind.INT), 2, LogicalType.of(LogicalKind.INT));

    var file =
        new PlannedFile<>(
            "s3://bucket/full.parquet",
            "PARQUET",
            50L,
            512L,
            Map.of(1, 40L, 2, 50L),
            Map.of(1, 10L, 2, 0L),
            null,
            Map.of(1, (Object) 1, 2, (Object) 100L),
            Map.of(1, (Object) 99, 2, (Object) 999L),
            null,
            0,
            null);

    var engine =
        new GenericStatsEngine<>(
            planner(cols, names, types, List.of(file)), null, null, names, types);
    var result = engine.compute();

    var fileAgg = result.files().get(0);
    assertEquals(2, fileAgg.columns().size());
    assertEquals(40L, fileAgg.columns().get(1).rowCount());
    assertEquals(50L, fileAgg.columns().get(2).rowCount());
  }

  private static byte[] oneValueTheta() {
    UpdateSketch sketch = UpdateSketch.builder().build();
    sketch.update("test-value");
    return sketch.compact().toByteArray();
  }

  @Test
  void perFileNdv_propagatedToFileAggs_whenNdvProviderPresent() {
    var cols = Set.of(1);
    var names = Map.of(1, "col_a");
    var types = Map.of(1, LogicalType.of(LogicalKind.STRING));
    byte[] sketchBytes = oneValueTheta();

    NdvProvider provider =
        (filePath, sinks) -> {
          ColumnNdv ndv = sinks.get("col_a");
          if (ndv != null) ndv.mergeTheta(sketchBytes);
        };
    var file =
        new PlannedFile<>(
            "s3://b/f.parquet",
            "PARQUET",
            10L,
            100L,
            Map.of(1, 10L),
            null,
            null,
            null,
            null,
            null,
            0,
            null);

    var result =
        new GenericStatsEngine<>(
                planner(cols, names, types, List.of(file)), provider, null, names, types)
            .compute();

    assertEquals(1, result.files().size());
    ColumnNdv ndv = result.files().get(0).columns().get(1).ndv();
    assertNotNull(ndv, "per-file ColumnNdv must be non-null when NdvProvider contributed");
    assertFalse(ndv.sketches.isEmpty(), "finalized ColumnNdv must contain at least one sketch");
    assertEquals("apache-datasketches-theta-v1", ndv.sketches.get(0).type);
  }

  @Test
  void ambiguousLeafNames_areNotCrossAttributedForNdv() {
    // Two distinct columns share a leaf name (nested schema: a.name and b.name collide once
    // Iceberg exposes leaf names). A name-keyed provider cannot tell them apart, so rather than
    // attribute one column's sketch to both, neither column gets an NDV sketch.
    var cols = Set.of(1, 2);
    var names = Map.of(1, "name", 2, "name");
    var types =
        Map.of(1, LogicalType.of(LogicalKind.STRING), 2, LogicalType.of(LogicalKind.STRING));
    byte[] sketchBytes = oneValueTheta();

    NdvProvider provider =
        (filePath, sinks) -> {
          ColumnNdv ndv = sinks.get("name");
          if (ndv != null) ndv.mergeTheta(sketchBytes);
        };
    var file =
        new PlannedFile<>(
            "s3://b/nested.parquet",
            "PARQUET",
            10L,
            100L,
            Map.of(1, 10L, 2, 10L),
            null,
            null,
            null,
            null,
            null,
            0,
            null);

    var result =
        new GenericStatsEngine<>(
                planner(cols, names, types, List.of(file)), provider, null, names, types)
            .compute();

    assertNull(
        result.files().get(0).columns().get(1).ndv(),
        "ambiguous-name column must not be cross-attributed a sketch");
    assertNull(result.files().get(0).columns().get(2).ndv());
    assertNull(result.columns().get(1).ndv(), "table-level ambiguous column must have no NDV");
    assertNull(result.columns().get(2).ndv());
  }

  @Test
  void bootstrapNdv_includedInFileAggs_whenNoPerFileProvider() {
    // When ndvProvider=null but bootstrapNdv has Puffin-sourced table-level NDV,
    // the bootstrap sketch must appear in every file record so FileGroupTargetStatsRollup
    // can merge it.  Theta-union of N identical sketches is idempotent (S ∪ S = S).
    var cols = Set.of(1);
    var names = Map.of(1, "col_b");
    var types = Map.of(1, LogicalType.of(LogicalKind.STRING));
    byte[] sketchBytes = oneValueTheta();

    NdvProvider bootstrap =
        (filePath, sinks) -> {
          ColumnNdv ndv = sinks.get("col_b");
          if (ndv != null) ndv.mergeTheta(sketchBytes);
        };
    var file =
        new PlannedFile<>(
            "s3://b/puffin.parquet",
            "PARQUET",
            20L,
            200L,
            Map.of(1, 20L),
            null,
            null,
            null,
            null,
            null,
            0,
            null);

    var result =
        new GenericStatsEngine<>(
                planner(cols, names, types, List.of(file)), null, bootstrap, names, types)
            .compute();

    assertEquals(1, result.files().size());
    ColumnNdv ndv = result.files().get(0).columns().get(1).ndv();
    assertNotNull(ndv, "bootstrap ColumnNdv must be propagated to file record");
    assertFalse(ndv.sketches.isEmpty(), "bootstrap sketch must be serialized into file record");
  }

  @Test
  void avgWidth_skippedForNonParquetFiles() {
    // The avg-width provider reads a Parquet footer; it must not be invoked on ORC/Avro files
    // (which would throw and warn per file). The lookup counter must stay at zero.
    var cols = Set.of(1);
    var names = Map.of(1, "c");
    var types = Map.of(1, LogicalType.of(LogicalKind.STRING));
    var opened = new AtomicInteger();
    ParquetAvgWidthProvider avgWidth =
        new ParquetAvgWidthProvider(
            path -> {
              opened.incrementAndGet();
              throw new IllegalStateException("must not open a non-Parquet footer: " + path);
            });
    var orcFile =
        new PlannedFile<>(
            "s3://b/data.orc",
            "ORC",
            10L,
            100L,
            Map.of(1, 10L),
            null,
            null,
            null,
            null,
            null,
            0,
            null);

    new GenericStatsEngine<>(
            planner(cols, names, types, List.of(orcFile)), null, null, avgWidth, names, types)
        .compute();

    assertEquals(0, opened.get(), "avg-width footer must not be opened for non-Parquet files");
  }

  @Test
  void avgWidth_ceilingDivisionProducesAtLeastOneForSubByteColumns() {
    // 500 uncompressed bytes for 1000 rows = 0.5 bytes/row.
    // Without ceiling + Math.max(1), integer division produces 0 — the planner then
    // discards the stat (it filters stawidth <= 0).
    var acc = new ParquetAvgWidthProvider.AvgWidthAcc();
    acc.addBytes(500L /* uncompressed bytes */);
    acc.addRows(1000L /* rows */);

    Long result = acc.avgWidthBytes();
    assertNotNull(result, "avgWidthBytes() must not return null when rows > 0");
    assertTrue(
        result >= 1L,
        "ceiling division must produce >= 1 for sub-byte-per-row encodings, got " + result);
    assertEquals(1L, result, "ceiling(500/1000) = 1");
  }

  @Test
  void avgWidth_rollupCeilingPreservesMinimumOne() {
    // Verify that FileGroupTargetStatsRollup's ceiling division also produces >= 1.
    // Direct test of AvgWidthAcc.merge + ceiling.
    var acc1 = new ParquetAvgWidthProvider.AvgWidthAcc();
    acc1.addBytes(300L); /* 0.3 bytes/row → ceiling = 1 */
    acc1.addRows(1000L);
    var acc2 = new ParquetAvgWidthProvider.AvgWidthAcc();
    acc2.addBytes(400L); /* 0.4 bytes/row */
    acc2.addRows(1000L);
    acc1.merge(acc2);

    Long result = acc1.avgWidthBytes();
    assertNotNull(result);
    assertTrue(
        result >= 1L, "merged sub-byte-per-row accumulators must produce >= 1, got " + result);
    assertEquals(1L, result, "ceiling(700/2000) = 1");
  }
}
