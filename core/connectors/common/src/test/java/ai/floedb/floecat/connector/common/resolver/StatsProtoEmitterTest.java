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
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileContent;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.ConnectorFormat;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.query.rpc.SchemaDescriptor;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class StatsProtoEmitterTest {

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
  public void toColumnStats_pathOrdinal_resolvesByCanonicalSchemaPath() {
    // Schema path = a[].b
    // note: for PATH_ORDINAL the actual id is computed from canonical path + ordinal.
    var schema =
        schemaWithColumns(schemaCol("a", "a", 1, 10, false), schemaCol("b", "a[].b", 2, 11, true));

    var tableId = rid("t1");
    long snapshotId = 99L;

    // Incoming stats uses Iceberg-style element path a.element.b
    var in =
        List.of(
            view(ref("b", "a.element.b", 0, 0), 123L),
            // and also exact canonical already
            view(ref("b", "a[].b", 0, 0), 456L));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            tableId,
            snapshotId,
            1700000000000L,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    assertEquals(2, out.size(), "both stats views should resolve");

    TargetStatsRecord s0 = out.get(0);
    TargetStatsRecord s1 = out.get(1);

    assertTrue(s0.getTarget().getColumn().getColumnId() > 0L);
    assertEquals("b", s0.getScalar().getDisplayName());
    assertEquals(123L, s0.getScalar().getValueCount());

    assertTrue(s1.getTarget().getColumn().getColumnId() > 0L);
    assertEquals("b", s1.getScalar().getDisplayName());
    assertEquals(456L, s1.getScalar().getValueCount());

    // Both refer to the *same schema column*, so the computed id must be the same.
    assertEquals(
        s0.getTarget().getColumn().getColumnId(),
        s1.getTarget().getColumn().getColumnId(),
        "canonicalized paths should resolve to same id");
  }

  @Test
  public void toColumnStats_pathOrdinal_skipsUnresolvablePaths() {
    var schema =
        schemaWithColumns(schemaCol("id", "id", 1, 1, true), schemaCol("x", "x", 2, 2, true));

    var tableId = rid("t1");

    var in =
        List.of(
            view(ref("id", "id", 0, 0), 10L), view(ref("missing", "does.not.exist", 0, 0), 20L));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            tableId,
            1L,
            1700000000000L,
            ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            in);

    assertEquals(1, out.size(), "unresolvable column must be skipped safely");
    assertEquals("id", out.get(0).getScalar().getDisplayName());
    assertEquals(10L, out.get(0).getScalar().getValueCount());
  }

  @Test
  public void toColumnStats_fieldId_resolvesOnlyByFieldId() {
    var schema =
        schemaWithColumns(
            // schema fields don't matter for FIELD_ID resolution, but it's required to exist
            schemaCol("id", "id", 1, 100, true), schemaCol("x", "x", 2, 200, true));

    var tableId = rid("t1");
    long snapshotId = 7L;

    var in =
        List.of(
            // FIELD_ID policy uses ref.fieldId directly
            view(ref("whatever", "does.not.matter", 0, 123), 42L),
            view(ref("alsoWhatever", "", 0, 999), 99L));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            tableId,
            snapshotId,
            1700000000000L,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            schema,
            in);

    assertEquals(2, out.size());
    assertEquals(123L, out.get(0).getTarget().getColumn().getColumnId());
    assertEquals(42L, out.get(0).getScalar().getValueCount());

    assertEquals(999L, out.get(1).getTarget().getColumn().getColumnId());
    assertEquals(99L, out.get(1).getScalar().getValueCount());
  }

  @Test
  public void toColumnStats_setsUpstreamProvenanceOnScalarPayload() {
    var schema = schemaWithColumns(schemaCol("id", "id", 1, 1, true));
    long upstreamCreatedAtMs = 1_700_000_123_000L;
    long snapshotId = 3L;

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            rid("t1"),
            snapshotId,
            upstreamCreatedAtMs,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            List.of(view(ref("id", "id", 0, 0), 12L)));

    assertEquals(1, out.size());
    assertTrue(out.get(0).getScalar().hasUpstream());
    assertEquals("3", out.get(0).getScalar().getUpstream().getCommitRef());
    assertEquals(
        upstreamCreatedAtMs / 1000L,
        out.get(0).getScalar().getUpstream().getFetchedAt().getSeconds());
  }

  @Test
  public void toColumnStats_fieldId_skipsWhenFieldIdMissing() {
    var schema = schemaWithColumns(schemaCol("id", "id", 1, 1, true));

    var in =
        List.of(
            view(ref("id", "id", 0, 0), 10L), // fieldId missing => cannot resolve
            view(ref("id", "id", 0, -1), 20L));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            rid("t1"),
            1L,
            1700000000000L,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            schema,
            in);

    assertEquals(0, out.size(), "missing fieldId must be skipped under FIELD_ID algorithm");
  }

  @Test
  public void toTargetFileStats_populatesFileMetadataAndNestedColumns() {
    var schema =
        schemaWithColumns(schemaCol("id", "id", 1, 1, true), schemaCol("b", "a[].b", 2, 2, true));

    var tableId = rid("t1");
    long snapshotId = 123L;

    var perFile =
        List.of(
            new FloecatConnector.FileColumnStatsView(
                "s3://bucket/path/file1.parquet",
                "parquet",
                100,
                2048,
                FileContent.FC_DATA,
                "{\"partitionValues\":[]}",
                0,
                List.of(),
                0L,
                List.of(
                    view(ref("id", "id", 0, 0), 100L),
                    // element path should canonicalize
                    view(ref("b", "a.element.b", 0, 0), 100L))),
            new FloecatConnector.FileColumnStatsView(
                "s3://bucket/path/delete1",
                "",
                5,
                128,
                FileContent.FC_EQUALITY_DELETES,
                "",
                0,
                List.of(1, 2),
                77L,
                List.of()));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetFileStats(
            tableId,
            snapshotId,
            1700000000000L,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            perFile);

    assertEquals(2, out.size());

    assertTrue(out.get(0).hasFile());
    var f0 = out.get(0).getFile();
    assertEquals(tableId, f0.getTableId());
    assertEquals(snapshotId, f0.getSnapshotId());
    assertEquals("s3://bucket/path/file1.parquet", f0.getFilePath());
    assertEquals("parquet", f0.getFileFormat());
    assertEquals(100, f0.getRowCount());
    assertEquals(2048, f0.getSizeBytes());
    assertEquals(FileContent.FC_DATA, f0.getFileContent());
    assertEquals("{\"partitionValues\":[]}", f0.getPartitionDataJson());
    assertEquals(2, f0.getColumnsCount());

    // ensure b resolved
    FileColumnStats colB =
        f0.getColumnsList().stream()
            .filter(c -> c.hasScalar() && "b".equals(c.getScalar().getDisplayName()))
            .findFirst()
            .orElseThrow();
    assertEquals("b", colB.getScalar().getDisplayName());

    assertTrue(out.get(1).hasFile());
    var f1 = out.get(1).getFile();
    assertEquals(FileContent.FC_EQUALITY_DELETES, f1.getFileContent());
    assertEquals(List.of(1, 2), f1.getEqualityFieldIdsList());
    assertEquals(77L, f1.getSequenceNumber());
    assertEquals(0, f1.getColumnsCount(), "delete files should have no per-column stats here");
  }

  @Test
  public void toTargetFileStats_deltaAndIcebergLambdas_produceConsistentFileMetadata() {
    // This test verifies that ConnectorStatsViewBuilder.toFileColumnStatsView()
    // produces consistent FileColumnStatsView records regardless of format-specific lambdas
    var schema = schemaWithColumns(schemaCol("id", "id", 1, 1, true));

    var tableId = rid("t1");
    long snapshotId = 456L;

    // Delta lambdas: name=name, fieldId=0, ordinal from positions map
    var deltaFile =
        new FloecatConnector.FileColumnStatsView(
            "s3://bucket/delta_file.parquet",
            "parquet",
            500L,
            4096L,
            FileContent.FC_DATA,
            "{\"year\":2025}",
            0,
            List.of(),
            null,
            List.of(view(ref("id", "id", 1, 0), 500L)));

    // Iceberg lambdas: name from map, fieldId from map, ordinal from map
    var icebergFile =
        new FloecatConnector.FileColumnStatsView(
            "s3://bucket/iceberg_file.parquet",
            "parquet",
            500L,
            4096L,
            FileContent.FC_DATA,
            "{\"year\":2025}",
            0,
            List.of(),
            null,
            List.of(view(ref("id", "id", 1, 1), 500L)));

    List<TargetStatsRecord> deltaOut =
        StatsProtoEmitter.toTargetFileStats(
            tableId,
            snapshotId,
            1700000000000L,
            ai.floedb.floecat.connector.spi.ConnectorFormat.CF_DELTA,
            ColumnIdAlgorithm.CID_PATH_ORDINAL,
            schema,
            List.of(deltaFile));

    List<TargetStatsRecord> icebergOut =
        StatsProtoEmitter.toTargetFileStats(
            tableId,
            snapshotId,
            1700000000000L,
            ai.floedb.floecat.connector.spi.ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            schema,
            List.of(icebergFile));

    assertEquals(1, deltaOut.size());
    assertEquals(1, icebergOut.size());

    assertTrue(deltaOut.get(0).hasFile());
    assertTrue(icebergOut.get(0).hasFile());
    var deltaProto = deltaOut.get(0).getFile();
    var icebergProto = icebergOut.get(0).getFile();

    // Both should have same file metadata
    assertEquals("s3://bucket/delta_file.parquet", deltaProto.getFilePath());
    assertEquals("s3://bucket/iceberg_file.parquet", icebergProto.getFilePath());
    assertEquals("parquet", deltaProto.getFileFormat());
    assertEquals("parquet", icebergProto.getFileFormat());
    assertEquals(500L, deltaProto.getRowCount());
    assertEquals(500L, icebergProto.getRowCount());
    assertEquals(4096L, deltaProto.getSizeBytes());
    assertEquals(4096L, icebergProto.getSizeBytes());

    // Delta: column is preserved in file payload.
    assertEquals(1, deltaProto.getColumnsCount());
    assertEquals("id", deltaProto.getColumns(0).getScalar().getDisplayName());

    // Iceberg: column is preserved in file payload.
    assertEquals(1, icebergProto.getColumnsCount());
    assertEquals("id", icebergProto.getColumns(0).getScalar().getDisplayName());
  }

  /**
   * Verifies that a column with all-null stats (no valueCount, no min/max) is still emitted as a
   * valid scalar stats entry with target identity and column_name set. This is the new code path
   * exercised when GenericStatsEngine densifies per-file stats with planner.columns() — columns
   * without Iceberg metrics produce all-null ColumnStatsView entries that must not be silently
   * dropped.
   */
  @Test
  public void toColumnStats_fieldId_emitsEntryForColumnWithNullStats() {
    var schema = schemaWithColumns(schemaCol("ts_col", "ts_col", 1, 42, true));

    var in =
        List.of(
            // All stats are null — simulates a date/timestamp column with no Iceberg metrics
            new FloecatConnector.ColumnStatsView(
                ref("ts_col", "ts_col", 1, 42),
                "timestamp",
                null, // valueCount
                null, // nullCount
                null, // nanCount
                null, // min
                null, // max
                null, // ndv
                Map.of()));

    List<TargetStatsRecord> out =
        StatsProtoEmitter.toTargetColumnStats(
            rid("t1"),
            1L,
            1700000000000L,
            ConnectorFormat.CF_ICEBERG,
            ColumnIdAlgorithm.CID_FIELD_ID,
            schema,
            in);

    assertEquals(1, out.size(), "column with null stats must not be dropped");
    TargetStatsRecord cs = out.get(0);
    assertEquals(
        42L, cs.getTarget().getColumn().getColumnId(), "column_id must be resolved from fieldId");
    assertEquals("ts_col", cs.getScalar().getDisplayName(), "column_name must be preserved");
    assertFalse(cs.getScalar().hasNullCount(), "null nullCount must not produce a set proto field");
    assertFalse(cs.getScalar().hasMin(), "null min must not produce a set proto field");
    assertFalse(cs.getScalar().hasMax(), "null max must not produce a set proto field");
  }
}
