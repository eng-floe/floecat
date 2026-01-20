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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * ConnectorStatsViewBuilderEqualityFieldIdsTest: Verify that equality field IDs are properly
 * preserved through the stats building pipeline.
 *
 * <p>RISK CONTEXT: Iceberg delete files use equality field IDs to specify which fields are used for
 * row-level equality comparisons. If these IDs are lost during stats ingestion, downstream
 * reconciliation and delete processing may fail silently.
 *
 * <p>IMPLEMENTATION: FileAgg interface now exposes equalityFieldIds() with a default empty list.
 * Connectors that handle equality deletes should populate this field so that the generic builder
 * can preserve the IDs without requiring connector-specific code.
 */
@DisplayName("Equality field IDs preservation in stats building")
class ConnectorStatsViewBuilderEqualityFieldIdsTest {

  /**
   * Test: Generic builder preserves equality field IDs from FileAgg when present.
   *
   * <p>This ensures that connectors using toFileColumnStatsView() will not silently drop equality
   * field IDs.
   */
  @Test
  void toFileColumnStatsView_preservesEqualityFieldIds() {
    // Mock FileAgg with equality field IDs
    class MockFileAgg implements StatsEngine.FileAgg<String> {
      private final List<Integer> eqIds = List.of(1, 2, 5);

      @Override
      public String path() {
        return "s3://bucket/equality_deletes.parquet";
      }

      @Override
      public String format() {
        return "iceberg";
      }

      @Override
      public long rowCount() {
        return 100;
      }

      @Override
      public long sizeBytes() {
        return 5000;
      }

      @Override
      public Map<String, StatsEngine.ColumnAgg> columns() {
        return new HashMap<>();
      }

      @Override
      public boolean isDelete() {
        return true;
      }

      @Override
      public boolean isEqualityDelete() {
        return true;
      }

      @Override
      public List<Integer> equalityFieldIds() {
        return eqIds;
      }
    }

    List<StatsEngine.FileAgg<String>> files = List.of(new MockFileAgg());

    var results =
        ConnectorStatsViewBuilder.toFileColumnStatsView(
            files, k -> k, // nameOf: use key as name
            null, null, null, k -> null); // typeOf

    assertEquals(1, results.size(), "Should have one file view");
    var fileView = results.get(0);

    assertNotNull(fileView.equalityFieldIds(), "Equality field IDs should be present");
    assertEquals(
        List.of(1, 2, 5),
        fileView.equalityFieldIds(),
        "Equality field IDs should match the FileAgg source");
  }

  /** Test: Generic builder handles null equalityFieldIds() gracefully (defaults to empty). */
  @Test
  void toFileColumnStatsView_handlesNullEqualityFieldIds() {
    class MockFileAggNoEq implements StatsEngine.FileAgg<String> {
      @Override
      public String path() {
        return "data.parquet";
      }

      @Override
      public String format() {
        return "iceberg";
      }

      @Override
      public long rowCount() {
        return 1000;
      }

      @Override
      public long sizeBytes() {
        return 50000;
      }

      @Override
      public Map<String, StatsEngine.ColumnAgg> columns() {
        return new HashMap<>();
      }

      // Intentionally return null (shouldn't happen, but be defensive)
      @Override
      public List<Integer> equalityFieldIds() {
        return null;
      }
    }

    List<StatsEngine.FileAgg<String>> files = List.of(new MockFileAggNoEq());

    var results =
        ConnectorStatsViewBuilder.toFileColumnStatsView(files, k -> k, null, null, null, k -> null);

    assertEquals(1, results.size(), "Should have one file view");
    var fileView = results.get(0);

    assertNotNull(fileView.equalityFieldIds(), "Should never be null (defensive handling)");
    assertTrue(fileView.equalityFieldIds().isEmpty(), "Should be empty when source is null");
  }

  /**
   * Test: Equality field IDs are preserved through full stats building pipeline (data + delete
   * files).
   */
  @Test
  void fullPipeline_preservesEqualityFieldIds_forDeleteFiles() {
    class MockDataFileAgg implements StatsEngine.FileAgg<String> {
      @Override
      public String path() {
        return "data.parquet";
      }

      @Override
      public String format() {
        return "iceberg";
      }

      @Override
      public long rowCount() {
        return 1000;
      }

      @Override
      public long sizeBytes() {
        return 50000;
      }

      @Override
      public Map<String, StatsEngine.ColumnAgg> columns() {
        return new HashMap<>();
      }

      // Data files don't have equality field IDs
      @Override
      public List<Integer> equalityFieldIds() {
        return List.of();
      }
    }

    class MockEqualityDeleteFileAgg implements StatsEngine.FileAgg<String> {
      @Override
      public String path() {
        return "eq_deletes.parquet";
      }

      @Override
      public String format() {
        return "iceberg";
      }

      @Override
      public long rowCount() {
        return 50;
      }

      @Override
      public long sizeBytes() {
        return 2500;
      }

      @Override
      public Map<String, StatsEngine.ColumnAgg> columns() {
        return new HashMap<>();
      }

      @Override
      public boolean isDelete() {
        return true;
      }

      @Override
      public boolean isEqualityDelete() {
        return true;
      }

      // Equality delete files specify which fields participate in equality
      @Override
      public List<Integer> equalityFieldIds() {
        return List.of(3, 4); // Fields 3 and 4 used for equality
      }
    }

    List<StatsEngine.FileAgg<String>> files =
        List.of(new MockDataFileAgg(), new MockEqualityDeleteFileAgg());

    var results =
        ConnectorStatsViewBuilder.toFileColumnStatsView(files, k -> k, null, null, null, k -> null);

    assertEquals(2, results.size(), "Should have two file views");

    var dataFileView = results.get(0);
    var deleteFileView = results.get(1);

    // Data file: no equality IDs
    assertEquals(
        List.of(), dataFileView.equalityFieldIds(), "Data file should have no equality field IDs");

    // Delete file: equality IDs preserved
    assertEquals(
        List.of(3, 4),
        deleteFileView.equalityFieldIds(),
        "Equality delete file should preserve equality field IDs");
  }
}
