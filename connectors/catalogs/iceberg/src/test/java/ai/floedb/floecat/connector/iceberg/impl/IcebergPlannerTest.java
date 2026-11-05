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

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
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
      assertThat(planner.columnNamesByKey()).containsEntry(2, "name").containsEntry(3, "age");
      assertThat(planner.logicalTypesByKey()).containsKeys(2, 3);
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

  @Test
  void eachDataFileGetsOnlyTheDeletesIcebergAppliesToIt() throws Exception {
    var catalog = new InMemoryCatalog();
    catalog.initialize("mem", Map.of());
    catalog.createNamespace(Namespace.of("ns"));
    var spec = PartitionSpec.unpartitioned();
    Table table =
        catalog.createTable(
            TableIdentifier.of("ns", "t"),
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            spec,
            Map.of("format-version", "2"));
    table
        .newAppend()
        .appendFile(dataFile(spec, "mem://a.parquet"))
        .appendFile(dataFile(spec, "mem://b.parquet"))
        .commit();
    table
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath("mem://d.parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withFormat(FileFormat.PARQUET)
                .withReferencedDataFile("mem://a.parquet")
                .build())
        .commit();

    try (var planner =
        new IcebergPlanner(
            table, table.currentSnapshot().snapshotId(), Set.of(), Set.of(), null, true)) {
      assertThat(planner.deleteLocationsByDataFile())
          .containsEntry("mem://a.parquet", List.of("mem://d.parquet"))
          .containsEntry("mem://b.parquet", List.of());
      // A position delete file does not say how many of a's rows it removes; b has none.
      assertThat(liveRows(planner))
          .containsEntry("mem://a.parquet", null)
          .containsEntry("mem://b.parquet", 2L);
    }

    // A deletion vector's Puffin file holds other data files' vectors too, so its path does not
    // name this one's deletes.
    Table withVectors =
        catalog.createTable(
            TableIdentifier.of("ns", "v"),
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            spec,
            Map.of("format-version", "3"));
    withVectors.newAppend().appendFile(dataFile(spec, "mem://c.parquet")).commit();
    withVectors
        .newRowDelta()
        .addDeletes(
            FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath("mem://v.puffin")
                .withFileSizeInBytes(100)
                .withRecordCount(1)
                .withFormat(FileFormat.PUFFIN)
                .withReferencedDataFile("mem://c.parquet")
                .withContentOffset(4)
                .withContentSizeInBytes(40)
                .build())
        .commit();
    try (var planner =
        new IcebergPlanner(
            withVectors,
            withVectors.currentSnapshot().snapshotId(),
            Set.of(),
            Set.of(),
            null,
            true)) {
      assertThat(planner.deleteLocationsByDataFile()).doesNotContainKey("mem://c.parquet");
      // A deletion vector counts the rows it removes.
      assertThat(liveRows(planner)).containsEntry("mem://c.parquet", 1L);
    }
  }

  private static Map<String, Long> liveRows(IcebergPlanner planner) {
    Map<String, Long> out = new java.util.HashMap<>();
    planner.iterator().forEachRemaining(file -> out.put(file.path(), file.liveRowCount()));
    return out;
  }

  private static DataFile dataFile(PartitionSpec spec, String path) {
    return DataFiles.builder(spec)
        .withPath(path)
        .withFileSizeInBytes(10)
        .withRecordCount(2)
        .withFormat(FileFormat.PARQUET)
        .build();
  }

  /** A planner of a table of schema {@code schema}. */
  private static IcebergPlanner plannerOf(Schema schema) {
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
    Table table =
        (Table)
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
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    return new IcebergPlanner(table, 1L, Set.of(), Set.of(), null, false);
  }

  @Test
  void aStringBoundThatIsNotUtf8OrHoldsUFFFDOrUFFFEIsDropped() {
    Schema schema = new Schema(10, Types.NestedField.optional(2, "s", Types.StringType.get()));
    try (IcebergPlanner planner = plannerOf(schema)) {
      assertThat(planner.decodeBounds(Map.of(2, utf8("a")))).containsEntry(2, "a");
      ByteBuffer cut = ByteBuffer.wrap(new byte[] {0x61, (byte) 0xF0, (byte) 0x9F});
      assertThat(planner.decodeBounds(Map.of(2, cut))).isNull();
      // Decoded leniently by Iceberg from a foreign footer: the cut character as U+FFFD, or as
      // U+FFFE where Iceberg then incremented a maximum it cut after it.
      assertThat(planner.decodeBounds(Map.of(2, utf8("a\uFFFD")))).isNull();
      assertThat(planner.decodeBounds(Map.of(2, utf8("a\uFFFE")))).isNull();
    }
  }

  private static ByteBuffer utf8(String string) {
    return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
  }
}
