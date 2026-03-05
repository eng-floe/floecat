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

package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.types.LogicalComparators;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Regression test suite for {@link ScanPruningUtils}. */
public class ScanPruningUtilsTest {

  /** Build a ScanFile with encoded min/max INT stats. */
  private static ScanFile file(String path, String col, long min, long max) {
    LogicalType t = LogicalType.of(LogicalKind.INT);

    ColumnStats cs =
        ColumnStats.newBuilder()
            .setColumnName(col)
            .setLogicalType("INT")
            .setMin(LogicalTypeProtoAdapter.encodeValue(t, min))
            .setMax(LogicalTypeProtoAdapter.encodeValue(t, max))
            .build();

    return ScanFile.newBuilder()
        .setFilePath(path)
        .setFileFormat("PARQUET")
        .setFileSizeInBytes(10)
        .setRecordCount(1)
        .addColumns(cs)
        .build();
  }

  private static ScanFile fileWithStats(
      String path, String col, LogicalType type, Object min, Object max) {
    ColumnStats.Builder cs =
        ColumnStats.newBuilder()
            .setColumnName(col)
            .setLogicalType(LogicalTypeProtoAdapter.encodeLogicalType(type));

    if (LogicalComparators.isStatsOrderable(type)) {
      cs.setMin(LogicalTypeProtoAdapter.encodeValue(type, min))
          .setMax(LogicalTypeProtoAdapter.encodeValue(type, max));
    }

    return ScanFile.newBuilder()
        .setFilePath(path)
        .setFileFormat("PARQUET")
        .setFileSizeInBytes(10)
        .setRecordCount(1)
        .addColumns(cs.build())
        .build();
  }

  /** Delete-file variant. */
  private static ScanFile deleteFileWithStats(String path, String col, long min, long max) {
    LogicalType t = LogicalType.of(LogicalKind.INT);

    ColumnStats cs =
        ColumnStats.newBuilder()
            .setColumnName(col)
            .setLogicalType("INT")
            .setMin(LogicalTypeProtoAdapter.encodeValue(t, min))
            .setMax(LogicalTypeProtoAdapter.encodeValue(t, max))
            .build();

    return ScanFile.newBuilder()
        .setFilePath(path)
        .setFileContent(
            ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES)
        .addColumns(cs)
        .build();
  }

  /** Delete file with no stats (must not be removed). */
  private static ScanFile deleteFileNoStats(String path) {
    return ScanFile.newBuilder()
        .setFilePath(path)
        .setFileContent(
            ai.floedb.floecat.execution.rpc.ScanFileContent.SCAN_FILE_CONTENT_EQUALITY_DELETES)
        .build();
  }

  private static FloecatConnector.ScanBundle rawBundle(
      List<ScanFile> data, List<ScanFile> deletes) {
    return new FloecatConnector.ScanBundle(data, deletes);
  }

  // ----------------- Tests -----------------

  @Test
  void testEqKeepsMatchingFiles() {
    var f = file("p", "id", 5, 10);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_EQ).addValues("7").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testEqPrunesNonMatchingFiles() {
    var f = file("p", "id", 5, 10);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_EQ).addValues("20").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(0, out.getDataFilesCount());
  }

  @Test
  void testInMatchesAny() {
    var f = file("p", "id", 5, 10);
    var pred =
        Predicate.newBuilder()
            .setColumn("id")
            .setOp(Operator.OP_IN)
            .addValues("1")
            .addValues("7")
            .addValues("20")
            .build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void remapsDeleteFileIndicesAfterPruning() {
    var data =
        file("data", "id", 5, 10).toBuilder()
            .addDeleteFileIndices(0)
            .addDeleteFileIndices(1)
            .build();
    var delete1 = deleteFileWithStats("d1", "id", 1, 2);
    var delete2 = deleteFileWithStats("d2", "id", 7, 9);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_EQ).addValues("7").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(data), List.of(delete1, delete2)), List.of("id"), List.of(pred));

    assertEquals(1, out.getDeleteFilesCount());
    assertEquals("d2", out.getDeleteFiles(0).getFilePath());
    assertEquals(List.of(0), out.getDataFiles(0).getDeleteFileIndicesList());
  }

  @Test
  void testInRejectsAllOutside() {
    var f = file("p", "id", 5, 10);
    var pred =
        Predicate.newBuilder()
            .setColumn("id")
            .setOp(Operator.OP_IN)
            .addValues("20")
            .addValues("30")
            .build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(0, out.getDataFilesCount());
  }

  @Test
  void testBetweenMatching() {
    var f = file("p", "id", 5, 10);
    var pred =
        Predicate.newBuilder()
            .setColumn("id")
            .setOp(Operator.OP_BETWEEN)
            .addValues("8")
            .addValues("20")
            .build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testBetweenPruned() {
    var f = file("p", "id", 5, 10);
    var pred =
        Predicate.newBuilder()
            .setColumn("id")
            .setOp(Operator.OP_BETWEEN)
            .addValues("20")
            .addValues("30")
            .build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(0, out.getDataFilesCount());
  }

  @Test
  void testGtPrunes() {
    var f = file("p", "id", 5, 10);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_GT).addValues("10").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(0, out.getDataFilesCount());
  }

  @Test
  void testGtKeeps() {
    var f = file("p", "id", 5, 10);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_GT).addValues("7").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testNeqRejectsWhenAllValuesSame() {
    var f = file("p", "id", 5, 5);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_NEQ).addValues("5").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(0, out.getDataFilesCount());
  }

  @Test
  void testNeqKeeps() {
    var f = file("p", "id", 5, 5);
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_NEQ).addValues("7").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testMissingColumnStatsKeepsFile() {
    var f = ScanFile.newBuilder().setFilePath("p").build();
    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_EQ).addValues("5").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(pred));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testColumnProjection() {
    var f = file("p", "id", 1, 10);

    var out =
        ScanPruningUtils.pruneBundle(rawBundle(List.of(f), List.of()), List.of("other"), List.of());
    assertEquals(1, out.getDataFilesCount());
    assertEquals(0, out.getDataFiles(0).getColumnsCount());
  }

  @Test
  void testMixedPredicates() {
    var f = file("p", "id", 5, 10);

    var p1 = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_GT).addValues("4").build();
    var p2 = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_LT).addValues("8").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(f), List.of()), List.of("id"), List.of(p1, p2));
    assertEquals(1, out.getDataFilesCount());
  }

  @Test
  void testDeleteFileWithNoStatsAlwaysSurvives() {
    var df = deleteFileNoStats("d");

    var pred =
        Predicate.newBuilder().setColumn("id").setOp(Operator.OP_EQ).addValues("999").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(), List.of(df)), List.of("id"), List.of(pred));
    assertEquals(1, out.getDeleteFilesCount());
  }

  @Test
  void testDeleteFileWithStatsIsPruned() {
    var df = deleteFileWithStats("d", "id", 1, 5);

    var pred = Predicate.newBuilder().setColumn("id").setOp(Operator.OP_GT).addValues("10").build();

    var out =
        ScanPruningUtils.pruneBundle(
            rawBundle(List.of(), List.of(df)), List.of("id"), List.of(pred));
    assertEquals(0, out.getDeleteFilesCount());
  }

  @Test
  void testInputNotMutated() {
    var f = file("p", "id", 1, 10);

    var raw = rawBundle(List.of(f), List.of());
    var out = ScanPruningUtils.pruneBundle(raw, List.of("id"), List.of());

    assertSame(f, raw.dataFiles().get(0));
    assertNotSame(f, out.getDataFiles(0));
  }

  @Test
  void testNonOrderableTypesDoNotCrashPruningAndAreKept() {
    List<LogicalType> nonOrderableTypes =
        List.of(
            LogicalType.of(LogicalKind.JSON),
            LogicalType.of(LogicalKind.INTERVAL),
            LogicalType.of(LogicalKind.ARRAY),
            LogicalType.of(LogicalKind.MAP),
            LogicalType.of(LogicalKind.STRUCT),
            LogicalType.of(LogicalKind.VARIANT));

    for (LogicalType type : nonOrderableTypes) {
      Object min = type.kind() == LogicalKind.JSON ? "{\"a\":1}" : new byte[] {1};
      Object max = type.kind() == LogicalKind.JSON ? "{\"z\":1}" : new byte[] {2};
      ScanFile f = fileWithStats("p-" + type.kind().name(), "col", type, min, max);
      Predicate pred =
          Predicate.newBuilder().setColumn("col").setOp(Operator.OP_EQ).addValues("x").build();

      var out =
          assertDoesNotThrow(
              () ->
                  ScanPruningUtils.pruneBundle(
                      rawBundle(List.of(f), List.of()), List.of("col"), List.of(pred)));
      assertEquals(1, out.getDataFilesCount(), "must keep file for non-orderable type " + type);
    }
  }
}
