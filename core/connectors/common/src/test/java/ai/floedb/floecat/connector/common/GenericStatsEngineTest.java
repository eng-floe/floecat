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

import ai.floedb.floecat.connector.common.ndv.NdvProvider;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
            Map.of(1, 100L), // valueCounts: int only
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
    assertEquals(100L, intAgg.valueCount());
    assertEquals(5L, intAgg.nullCount());
    assertEquals(0, intAgg.min());
    assertEquals(99, intAgg.max());

    // date and timestamp have null stats (no metrics in source)
    var dateAgg = fileAgg.columns().get(2);
    assertNull(dateAgg.valueCount(), "date valueCount should be null when no metrics");
    assertNull(dateAgg.nullCount(), "date nullCount should be null when no metrics");
    assertNull(dateAgg.min(), "date min should be null when no metrics");
    assertNull(dateAgg.max(), "date max should be null when no metrics");

    var tsAgg = fileAgg.columns().get(3);
    assertNull(tsAgg.valueCount(), "timestamp valueCount should be null when no metrics");
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
    assertEquals(40L, fileAgg.columns().get(1).valueCount());
    assertEquals(50L, fileAgg.columns().get(2).valueCount());
  }
}
