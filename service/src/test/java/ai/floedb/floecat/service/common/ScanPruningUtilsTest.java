package ai.floedb.floecat.service.common;

import static org.junit.jupiter.api.Assertions.*;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.common.rpc.Operator;
import ai.floedb.floecat.common.rpc.Predicate;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.execution.rpc.ScanFile;
import ai.floedb.floecat.types.LogicalKind;
import ai.floedb.floecat.types.LogicalType;
import ai.floedb.floecat.types.LogicalTypeProtoAdapter;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Regression test suite for {@link ScanPruningUtils}. */
public class ScanPruningUtilsTest {

  /** Build a ScanFile with encoded min/max INT64 stats. */
  private static ScanFile file(String path, String col, long min, long max) {
    LogicalType t = LogicalType.of(LogicalKind.INT64);

    ColumnStats cs =
        ColumnStats.newBuilder()
            .setColumnName(col)
            .setLogicalType("INT64")
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

  /** Delete-file variant. */
  private static ScanFile deleteFileWithStats(String path, String col, long min, long max) {
    LogicalType t = LogicalType.of(LogicalKind.INT64);

    ColumnStats cs =
        ColumnStats.newBuilder()
            .setColumnName(col)
            .setLogicalType("INT64")
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
}
