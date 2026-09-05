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

package ai.floedb.floecat.connector.iceberg.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.common.GenericStatsEngine;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class IcebergPlannerTest {

  @Test
  void contentIdentitiesUseCommittedSequenceAndRecordCount() {
    assertThat(IcebergPlanner.dataContentIdentity(7L, 10L)).isEqualTo("iceberg-data-v1:7:10");
    assertThat(IcebergPlanner.deleteContentIdentity(8L, 2L)).isEqualTo("iceberg-delete-v1:8:2");
  }

  @Test
  void contentIdentitiesCanonicalizeUnassignedSequenceNumbers() {
    assertThat(IcebergPlanner.dataContentIdentity(null, 10L)).isEqualTo("iceberg-data-v1::10");
    assertThat(IcebergPlanner.deleteContentIdentity(0L, 2L)).isEqualTo("iceberg-delete-v1::2");
  }

  @Test
  void plannerIndexesNestedSnapshotFieldIds() {
    Schema schema =
        new Schema(
            10,
            Types.NestedField.optional(
                1,
                "user",
                Types.StructType.of(
                    Types.NestedField.optional(2, "name", Types.StringType.get()),
                    Types.NestedField.optional(3, "age", Types.IntegerType.get()))));
    Snapshot snapshot =
        (Snapshot)
            Proxy.newProxyInstance(
                Snapshot.class.getClassLoader(),
                new Class<?>[] {Snapshot.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "schemaId" -> 10;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    Table table =
        (Table)
            Proxy.newProxyInstance(
                Table.class.getClassLoader(),
                new Class<?>[] {Table.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "snapshot" -> snapshot;
                      case "schema" -> schema;
                      case "schemas" -> Map.of(10, schema);
                      case "specs" -> Map.of();
                      case "spec" -> null;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });

    try (IcebergPlanner planner =
        new IcebergPlanner(table, 1L, Set.of(2, 3), Set.of(), null, false)) {
      assertThat(planner.columnNamesByKey())
          .containsEntry(2, "user.name")
          .containsEntry(3, "user.age");
      assertThat(planner.logicalTypesByKey()).containsKeys(2, 3);
    }
  }

  @Test
  void plannerMaterializesSameNamedNestedLeafMetricsByStableId() {
    Schema schema =
        new Schema(
            10,
            Types.NestedField.optional(
                1,
                "left",
                Types.StructType.of(
                    Types.NestedField.optional(2, "code", Types.IntegerType.get()))),
            Types.NestedField.optional(
                3,
                "right",
                Types.StructType.of(
                    Types.NestedField.optional(4, "code", Types.IntegerType.get()))));
    DataFile dataFile =
        dataFile(
            Map.of(2, 100L, 4, 100L),
            Map.of(2, 7L, 4, 20L),
            Map.of(
                2, Conversions.toByteBuffer(Types.IntegerType.get(), 10),
                4, Conversions.toByteBuffer(Types.IntegerType.get(), 1_000)),
            Map.of(
                2, Conversions.toByteBuffer(Types.IntegerType.get(), 20),
                4, Conversions.toByteBuffer(Types.IntegerType.get(), 2_000)));
    AtomicBoolean includedColumnStats = new AtomicBoolean();
    Table table = tableWithPlannedFile(schema, dataFile, includedColumnStats);

    try (IcebergPlanner planner =
        new IcebergPlanner(table, 1L, Set.of(2, 4), Set.of(), null, true)) {
      var result =
          new GenericStatsEngine<>(
                  planner, null, null, planner.columnNamesByKey(), planner.logicalTypesByKey())
              .compute();

      assertThat(includedColumnStats.get()).isTrue();
      assertThat(planner.columnNamesByKey())
          .containsExactlyInAnyOrderEntriesOf(Map.of(2, "left.code", 4, "right.code"));
      assertThat(result.columns().get(2).rowCount()).isEqualTo(100L);
      assertThat(result.columns().get(2).nullCount()).isEqualTo(7L);
      assertThat(result.columns().get(2).min()).isEqualTo(10L);
      assertThat(result.columns().get(2).max()).isEqualTo(20L);
      assertThat(result.columns().get(4).rowCount()).isEqualTo(100L);
      assertThat(result.columns().get(4).nullCount()).isEqualTo(20L);
      assertThat(result.columns().get(4).min()).isEqualTo(1_000L);
      assertThat(result.columns().get(4).max()).isEqualTo(2_000L);
    }
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimeMicrosToLocalTime() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.TimeType.get(), 45_296_123_456L);

    assertThat(canonical).isEqualTo(LocalTime.of(12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeDecodedBoundDropsOutOfRangeTimeMicros() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.TimeType.get(), 86_400_000_000L);

    assertThat(canonical).isNull();
  }

  @Test
  void canonicalizeDecodedBoundLeavesNonTemporalValuesUntouched() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(Types.StringType.get(), "already-canonical");

    assertThat(canonical).isEqualTo("already-canonical");
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestampMicrosToLocalDateTime() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampType.withoutZone(), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56, 123_456_000));
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestampMicrosToInstantWhenAdjustedToUtc() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampType.withZone(), 1_735_734_896_123_456L);

    assertThat(canonical).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456Z"));
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestampNanosToLocalDateTime() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampNanoType.withoutZone(), 1_735_734_896_123_456_789L);

    assertThat(canonical).isEqualTo(LocalDateTime.of(2025, 1, 1, 12, 34, 56, 123_456_789));
  }

  @Test
  void canonicalizeDecodedBoundConvertsTimestampNanosToInstantWhenAdjustedToUtc() {
    Object canonical =
        IcebergPlanner.canonicalizeDecodedBound(
            Types.TimestampNanoType.withZone(), 1_735_734_896_123_456_789L);

    assertThat(canonical).isEqualTo(Instant.parse("2025-01-01T12:34:56.123456789Z"));
  }

  @Test
  void decodeBoundsSkipsVariantBoundsAndKeepsPrimitives() {
    // Iceberg v3 stores variant bounds as a serialized Variant keyed by normalized
    // JSON path, which Conversions (primitives only) rejects with
    // UnsupportedOperationException. Planning must drop the stat, not fail.
    // Payload below is a real DuckDB-written lower bound: root path '$' -> "apple".
    byte[] variantBound = {
      0x11, 0x01, 0x00, 0x01, '$', 0x02, 0x01, 0x00, 0x00, 0x06, 0x15, 'a', 'p', 'p', 'l', 'e'
    };

    Schema schema =
        new Schema(
            1,
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "var", Types.VariantType.get()));
    Table table = tableWithSchema(schema, 1);

    try (IcebergPlanner planner =
        new IcebergPlanner(table, 1L, Set.of(1, 2), Set.of(), null, false)) {
      Map<Integer, Object> decoded =
          planner.decodeBounds(
              Map.of(
                  1, ByteBuffer.wrap(new byte[] {0x2A, 0x00, 0x00, 0x00}),
                  2, ByteBuffer.wrap(variantBound)));

      // int bounds are widened to long by LogicalCoercions.coerceStatValue
      assertThat(decoded).containsEntry(1, 42L).doesNotContainKey(2);
    }
  }

  @Test
  void decodeBoundsReturnsNullWhenOnlyVariantBoundsArePresent() {
    byte[] variantBound = {
      0x11, 0x01, 0x00, 0x01, '$', 0x02, 0x01, 0x00, 0x00, 0x06, 0x15, 'a', 'p', 'p', 'l', 'e'
    };

    Schema schema = new Schema(1, Types.NestedField.optional(2, "var", Types.VariantType.get()));
    Table table = tableWithSchema(schema, 1);

    try (IcebergPlanner planner = new IcebergPlanner(table, 1L, Set.of(2), Set.of(), null, false)) {
      assertThat(planner.decodeBounds(Map.of(2, ByteBuffer.wrap(variantBound)))).isNull();
    }
  }

  private static Table tableWithSchema(Schema schema, int schemaId) {
    Snapshot snapshot =
        (Snapshot)
            Proxy.newProxyInstance(
                Snapshot.class.getClassLoader(),
                new Class<?>[] {Snapshot.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "schemaId" -> schemaId;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "snapshot" -> snapshot;
                  case "schema" -> schema;
                  case "schemas" -> Map.of(schemaId, schema);
                  case "specs" -> Map.of();
                  case "spec" -> null;
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  private static DataFile dataFile(
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    return (DataFile)
        Proxy.newProxyInstance(
            DataFile.class.getClassLoader(),
            new Class<?>[] {DataFile.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "location" -> "file:///tmp/nested.parquet";
                  case "format" -> FileFormat.PARQUET;
                  case "recordCount" -> 100L;
                  case "fileSizeInBytes" -> 1024L;
                  case "valueCounts" -> valueCounts;
                  case "nullValueCounts" -> nullCounts;
                  case "nanValueCounts" -> Map.of();
                  case "lowerBounds" -> lowerBounds;
                  case "upperBounds" -> upperBounds;
                  case "specId" -> 0;
                  case "partition", "fileSequenceNumber" -> null;
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  private static Table tableWithPlannedFile(
      Schema schema, DataFile dataFile, AtomicBoolean includedColumnStats) {
    Snapshot snapshot =
        (Snapshot)
            Proxy.newProxyInstance(
                Snapshot.class.getClassLoader(),
                new Class<?>[] {Snapshot.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "schemaId" -> schema.schemaId();
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    FileScanTask task =
        (FileScanTask)
            Proxy.newProxyInstance(
                FileScanTask.class.getClassLoader(),
                new Class<?>[] {FileScanTask.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "file" -> dataFile;
                      case "deletes" -> List.of();
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    AtomicReference<TableScan> scanReference = new AtomicReference<>();
    TableScan scan =
        (TableScan)
            Proxy.newProxyInstance(
                TableScan.class.getClassLoader(),
                new Class<?>[] {TableScan.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "useSnapshot" -> scanReference.get();
                      case "includeColumnStats" -> {
                        includedColumnStats.set(true);
                        yield scanReference.get();
                      }
                      case "planFiles" -> CloseableIterable.withNoopClose(List.of(task));
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    scanReference.set(scan);
    return (Table)
        Proxy.newProxyInstance(
            Table.class.getClassLoader(),
            new Class<?>[] {Table.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "snapshot" -> snapshot;
                  case "schema" -> schema;
                  case "schemas" -> Map.of(schema.schemaId(), schema);
                  case "specs" -> Map.of();
                  case "spec" -> null;
                  case "newScan" -> scan;
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }
}
