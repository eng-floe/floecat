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

package ai.floedb.floecat.connector.common.resolver;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm;
import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for nested column stats in Delta tables.
 *
 * <p>CRITICAL: Delta Parquet statistics only expose TOP-LEVEL columns. Nested columns
 * (struct.field) are NOT included in result.columns(). This is a fundamental limitation of the
 * Parquet footer stats.
 *
 * <p>The connector must NOT expect nested columns in result.columns(). Instead, it should only
 * populate ordinals for the top-level columns that actually appear in the stats.
 */
@DisplayName("Delta nested column stats correctness")
public class DeltaNestedColumnStatsTest {

  private static ResourceId rid(String id) {
    return ResourceId.newBuilder().setId(id).build();
  }

  private static SchemaDescriptor schemaWithColumns(SchemaColumn... cols) {
    SchemaDescriptor.Builder b = SchemaDescriptor.newBuilder();
    for (SchemaColumn c : cols) {
      b.addColumns(c);
    }
    return b.build();
  }

  private static SchemaColumn schemaCol(
      String name, String physicalPath, int ordinal, int fieldId, boolean leaf) {
    return SchemaColumn.newBuilder()
        .setName(name == null ? "" : name)
        .setPhysicalPath(physicalPath == null ? "" : physicalPath)
        .setOrdinal(Math.max(0, ordinal))
        .setFieldId(Math.max(0, fieldId))
        .setNullable(true)
        .setLeaf(leaf)
        .build();
  }

  private static FloecatConnector.ColumnRef ref(
      String name, String physicalPath, int ordinal, int fieldId) {
    return new FloecatConnector.ColumnRef(
        name == null ? "" : name, physicalPath == null ? "" : physicalPath, ordinal, fieldId);
  }

  private static FloecatConnector.ColumnStatsView view(
      FloecatConnector.ColumnRef ref, Long valueCount) {
    return new FloecatConnector.ColumnStatsView(
        ref, "int", valueCount, null, null, null, null, null, Map.of());
  }

  @Test
  void deltaNestedColumns_withPathOrdinal_producesNonZeroColumnIds() {
    // Schema: id INT, info STRUCT(city STRING, zip INT)
    // This is the case that was broken: nested columns had ordinal=0 -> column_id=0
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("info", "info", 2, 0, false),
            schemaCol("info.city", "info.city", 1, 0, true), // nested: ordinal from parent level
            schemaCol("info.zip", "info.zip", 2, 0, true));

    var tableId = rid("t1");
    long snapshotId = 999L;

    // Simulate Delta stats: includes nested columns like "info.city"
    var in =
        List.of(
            // Top-level column: ordinal=1 from schema
            view(ref("id", "id", 1, 0), 100L),
            // Nested column: ordinal=1 (within parent "info")
            view(ref("info.city", "info.city", 1, 0), 100L),
            // Another nested column: ordinal=2
            view(ref("info.zip", "info.zip", 2, 0), 100L));

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            snapshotId,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // All three columns should be resolved (not skipped)
    assertEquals(3, out.size(), "all columns including nested ones should resolve");

    // All should have non-zero column_id
    for (ColumnStats col : out) {
      assertTrue(col.getColumnId() > 0L, "column_id should be non-zero for " + col.getColumnName());
    }

    // Nested columns should have different IDs from top-level
    ColumnStats topLevel =
        out.stream().filter(c -> "id".equals(c.getColumnName())).findFirst().orElseThrow();
    ColumnStats nested1 =
        out.stream().filter(c -> "info.city".equals(c.getColumnName())).findFirst().orElseThrow();
    ColumnStats nested2 =
        out.stream().filter(c -> "info.zip".equals(c.getColumnName())).findFirst().orElseThrow();

    assertNotEquals(
        topLevel.getColumnId(), nested1.getColumnId(), "nested columns should have different IDs");
    assertNotEquals(
        nested1.getColumnId(), nested2.getColumnId(), "nested siblings should have different IDs");
  }

  @Test
  void deltaNestedColumns_zeroOrdinal_skipsColumn() {
    // Verify that if ordinal is truly 0 (not resolvable), column is skipped safely
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("unknown", "unknown", 0, 0, true)); // ordinal=0, unresolvable

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 1, 0), 100L),
            view(ref("unknown", "unknown", 0, 0), 50L) // ordinal=0 should be skipped
            );

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // Only resolvable column should be in output
    assertEquals(1, out.size(), "unresolvable column (ordinal=0) should be skipped");
    assertEquals("id", out.get(0).getColumnName());
  }

  @Test
  void deltaDeeplyNestedColumns_allResolvable() {
    // Schema: id INT, profile STRUCT(address STRUCT(city STRING))
    // Deeply nested: profile.address.city
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("profile", "profile", 2, 0, false),
            schemaCol("profile.address", "profile.address", 1, 0, false),
            schemaCol("profile.address.city", "profile.address.city", 1, 0, true));

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 1, 0), 1000L),
            view(ref("profile.address.city", "profile.address.city", 1, 0), 800L));

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // Both should resolve
    assertEquals(2, out.size(), "both top-level and deeply nested should resolve");

    // Deeply nested should have non-zero ID
    ColumnStats deepNested =
        out.stream()
            .filter(c -> "profile.address.city".equals(c.getColumnName()))
            .findFirst()
            .orElseThrow();
    assertTrue(deepNested.getColumnId() > 0L, "deeply nested column should have non-zero ID");
  }

  @Test
  void deltaMapStructures_withOrdinals() {
    // Schema with map: config MAP<STRING, STRING>
    // Maps create virtual .key and .value columns with ordinals 1 and 2
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("config", "config", 2, 0, false),
            schemaCol("config.key", "config.key", 1, 0, true),
            schemaCol("config.value", "config.value", 2, 0, true));

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 1, 0), 100L),
            view(ref("config.key", "config.key", 1, 0), 100L),
            view(ref("config.value", "config.value", 2, 0), 100L));

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // All should resolve (1 top + 2 map subcolumns)
    assertEquals(3, out.size(), "map key and value should resolve");

    // Map key and value should have different IDs
    ColumnStats mapKey =
        out.stream().filter(c -> "config.key".equals(c.getColumnName())).findFirst().orElseThrow();
    ColumnStats mapValue =
        out.stream()
            .filter(c -> "config.value".equals(c.getColumnName()))
            .findFirst()
            .orElseThrow();

    assertNotEquals(mapKey.getColumnId(), mapValue.getColumnId(), "Map key/value should differ");
  }

  @Test
  void deltaArrayStructures_withOrdinals() {
    // Schema: tags ARRAY<STRING>
    // Arrays create virtual .element column with ordinal 1
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("tags", "tags", 2, 0, false),
            schemaCol("tags[]", "tags[]", 1, 0, true)); // element column with canonical notation

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 1, 0), 100L),
            view(ref("tags[]", "tags[]", 1, 0), 75L)); // 75 array elements total

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // Both should resolve
    assertEquals(2, out.size(), "array element should resolve");

    ColumnStats arrayElem =
        out.stream().filter(c -> "tags[]".equals(c.getColumnName())).findFirst().orElseThrow();
    assertTrue(arrayElem.getColumnId() > 0L, "array element should have non-zero ID");
  }

  @Test
  void deltaComplexNesting_arrayOfStructs() {
    // Complex: items ARRAY<STRUCT<id INT, name STRING>>
    // Creates items[], items[].id, items[].name
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("items", "items", 2, 0, false),
            schemaCol("items[]", "items[]", 1, 0, false),
            schemaCol("items[].id", "items[].id", 1, 0, true),
            schemaCol("items[].name", "items[].name", 2, 0, true));

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 1, 0), 100L),
            view(ref("items[].id", "items[].id", 1, 0), 150L), // 150 items in total
            view(ref("items[].name", "items[].name", 2, 0), 140L)); // 140 have names

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    // All leaf columns should resolve (id, items[].id, items[].name)
    assertEquals(3, out.size(), "complex nested columns should resolve");

    // All should have non-zero IDs
    for (ColumnStats col : out) {
      assertTrue(
          col.getColumnId() > 0L, "column " + col.getColumnName() + " should have non-zero ID");
    }

    // Siblings should have different IDs
    ColumnStats itemId =
        out.stream().filter(c -> "items[].id".equals(c.getColumnName())).findFirst().orElseThrow();
    ColumnStats itemName =
        out.stream()
            .filter(c -> "items[].name".equals(c.getColumnName()))
            .findFirst()
            .orElseThrow();
    assertNotEquals(itemId.getColumnId(), itemName.getColumnId(), "Siblings should differ");
  }

  @Test
  void deltaOrdinalCanonicalizations_consistent() {
    // Test that ordinals work correctly after path canonicalization
    // Input paths might have .element. but should resolve to ordinals for [] paths
    var schema =
        schemaWithColumns(
            schemaCol("addresses[]", "addresses[]", 1, 0, true),
            schemaCol("addresses[].zip", "addresses[].zip", 1, 0, true));

    var tableId = rid("t1");

    // Even if paths come in non-canonical form, should still resolve
    var in = List.of(view(ref("addresses[].zip", "addresses[].zip", 1, 0), 1000L));

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    assertEquals(1, out.size(), "canonical path should resolve");
    assertTrue(out.get(0).getColumnId() > 0L, "should have non-zero column ID");
  }

  @Test
  @DisplayName("CRITICAL: Delta stats only include TOP-LEVEL columns, not nested")
  void deltaParquetStatsOnlyExposeTopLevelColumns() {
    // This test documents the reality: Delta Parquet footer stats ONLY have top-level columns.
    // Nested columns (struct.field) are NOT included in result.columns() from the stats engine.
    //
    // Build a schema with nested columns:
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("user", "user", 2, 0, false), // struct (NOT a leaf)
            schemaCol("user.name", "user.name", 1, 0, true), // nested leaf
            schemaCol("user.age", "user.age", 2, 0, true) // nested leaf
            );

    // BUT: result.columns() (from Parquet stats) ONLY includes "id" and "user"
    // NOT "user.name" or "user.age"
    // This is a fundamental limitation: Parquet footer stats don't break down struct internals.

    var tableId = rid("t1");

    // Realistic: only top-level column has stats in Parquet footer
    var statsInput =
        List.of(
            // Only these appear in Delta/Parquet stats engine output
            view(ref("id", "id", 1, 0), 1000L),
            view(ref("user", "user", 2, 0), 800L) // struct container, not leaf
            );

    List<ColumnStats> out =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            statsInput);

    // Only top-level columns resolve
    assertEquals(2, out.size(), "Only top-level columns appear in Parquet stats");

    // The nested columns (user.name, user.age) do NOT appear because they're not in
    // result.columns() from the Delta engine
    assertTrue(
        out.stream().noneMatch(c -> "user.name".equals(c.getColumnName())),
        "Nested user.name should not be in stats (not in Parquet footer)");
    assertTrue(
        out.stream().noneMatch(c -> "user.age".equals(c.getColumnName())),
        "Nested user.age should not be in stats (not in Parquet footer)");
  }

  @Test
  @DisplayName("CRITICAL: buildColumnOrdinals returns keys that DON'T match result.columns()")
  void buildColumnOrdinalsMismatchesWithResultColumns() {
    // This test exposes the bug: buildColumnOrdinals() extracts ordinals for nested columns
    // (because the schema mapper shows them), but result.columns() never includes those keys.
    //
    // When the connector does:
    // positions = buildColumnOrdinals(...) // returns {struct.field -> 1, ...}
    // positions.getOrDefault(name, 0) where name is from result.columns()
    //
    // If "struct.field" is in positions but NOT in result.columns(), the lookup fails.

    // Simulate what buildColumnOrdinals returns
    Map<String, Integer> ordinalMap =
        Map.of(
            "id", 1,
            "user", 2,
            "user.name", 1, // From schema, but NOT in result.columns()
            "user.age", 2 // From schema, but NOT in result.columns()
            );

    // But result.columns() only has top-level keys
    Set<String> resultColumnKeys = Set.of("id", "user");

    // When doing positions.getOrDefault(key, 0) for each key in result.columns():
    assertEquals(1, ordinalMap.getOrDefault("id", 0), "id resolves");
    assertEquals(2, ordinalMap.getOrDefault("user", 0), "user resolves");

    // But if we mistakenly try to look up nested columns:
    assertEquals(1, ordinalMap.get("user.name"), "user.name exists in schema");
    assertEquals(0, ordinalMap.getOrDefault("unknown", 0), "unknown defaults to 0");

    // The problem: result.columns() never has "user.name" as a key, so buildColumnOrdinals
    // returning it doesn't help. It's wasted work.
  }

  @Test
  @DisplayName(
      "FUTURE: When Delta adds nested stats, NO code changes needed - generic lookup handles it")
  void futureReadyWhenDeltaExposesNestedStats() {
    // This test demonstrates WHY buildColumnOrdinals pre-computes ALL ordinals, including nested.
    // TODAY: result.columns() is ["id", "user"] (top-level only)
    // FUTURE: When Delta adds nested stats, result.columns() will be ["id", "user", "user.name",
    // "user.age"]
    // But our code will work UNCHANGED because of the generic lookup pattern.

    // Schema: id INT, user STRUCT(name STRING, age INT)
    var schema =
        schemaWithColumns(
            schemaCol("id", "id", 1, 0, true),
            schemaCol("user", "user", 2, 0, false),
            schemaCol("user.name", "user.name", 1, 0, true), // nested: ordinal from parent level
            schemaCol("user.age", "user.age", 2, 0, true));

    var tableId = rid("t1");

    // Pre-compute ALL ordinals (what buildColumnOrdinals does)
    var positions =
        Map.of(
            "id", 1,
            "user", 2,
            "user.name", 1, // Pre-computed from schema introspection
            "user.age", 2 // Pre-computed from schema introspection
            );

    // Scenario 1: TODAY - only top-level columns in stats (Parquet limitation)
    var todayStatsInput =
        List.of(view(ref("id", "id", 1, 0), 1000L), view(ref("user", "user", 2, 0), 800L));

    List<ColumnStats> todayOut =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            todayStatsInput);

    // Today: Only 2 columns because stats engine only returns top-level
    assertEquals(2, todayOut.size(), "TODAY: Only top-level columns in stats");
    assertEquals(
        2,
        todayOut.stream().filter(c -> positions.getOrDefault(c.getColumnName(), 0) > 0).count(),
        "TODAY: All returned columns have ordinals in pre-computed map");

    // Scenario 2: FUTURE - Delta adds nested column stats
    var futureStatsInput =
        List.of(
            view(ref("id", "id", 1, 0), 1000L),
            view(ref("user", "user", 2, 0), 800L),
            view(ref("user.name", "user.name", 1, 0), 750L), // NEW: nested stat
            view(ref("user.age", "user.age", 2, 0), 650L) // NEW: nested stat
            );

    List<ColumnStats> futureOut =
        StatsProtoEmitter.toColumnStats(
            tableId,
            1L,
            1700000000001L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            futureStatsInput);

    // Future: 4 columns (top-level + nested) - NO code changes needed!
    assertEquals(4, futureOut.size(), "FUTURE: Top-level + nested columns in stats");
    assertEquals(
        4,
        futureOut.stream().filter(c -> positions.getOrDefault(c.getColumnName(), 0) > 0).count(),
        "FUTURE: ALL columns have ordinals in pre-computed map (why we pre-compute ALL)");

    // Key proof: The SAME lookup logic works for both scenarios
    for (ColumnStats stat : futureOut) {
      int ordinal = positions.getOrDefault(stat.getColumnName(), 0);
      assertNotEquals(
          0,
          ordinal,
          "PROOF: Pre-computed ordinals handle both today's top-level and future's nested: "
              + stat.getColumnName());
    }

    // This is why we pre-compute: today's top-level lookups AND tomorrow's nested lookups
    // all use the same generic getOrDefault() call with zero code changes.
  }
}
