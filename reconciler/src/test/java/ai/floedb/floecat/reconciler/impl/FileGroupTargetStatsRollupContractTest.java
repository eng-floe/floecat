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

package ai.floedb.floecat.reconciler.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.types.rpc.ScalarValue;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** A merged column statistic is present only when it holds for every row the merge describes. */
class FileGroupTargetStatsRollupContractTest {
  private static final ResourceId TABLE =
      ResourceId.newBuilder()
          .setAccountId("acc")
          .setId("tbl")
          .setKind(ResourceKind.RK_TABLE)
          .build();
  private static final Set<FloecatConnector.StatsTargetKind> KINDS =
      Set.of(FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.COLUMN);

  @Test
  void aFileOfOnlyNullsNeedsNoBoundButAnyOtherFileDoes() {
    ScalarStats bounded = ints(10).setNullCount(0).setMinValue(i64(1)).setMaxValue(i64(5)).build();
    ScalarStats allNull = ints(4).setNullCount(4).build();
    ScalarStats unbounded = ints(10).setNullCount(2).build();

    ScalarStats withNullFile = mergeFiles(file(bounded), file(allNull)).getScalar();
    ScalarStats withUnboundedFile = mergeFiles(file(bounded), file(unbounded)).getScalar();

    assertEquals(1L, withNullFile.getMinValue().getI64());
    assertEquals(5L, withNullFile.getMaxValue().getI64());
    assertFalse(withUnboundedFile.hasMinValue());
    assertFalse(withUnboundedFile.hasMaxValue());
  }

  @Test
  void aFileGroupVouchesForNoRowCount() {
    // The files a file group plans need not be all of the snapshot's.
    TargetStatsRecord partial = tablePartial(10, 1);
    TargetStatsRecord exactPartial =
        partial.toBuilder().setTable(partial.getTable().toBuilder().setRowCountExact(true)).build();
    assertFalse(mergedTable(exactPartial, exactPartial).getRowCountExact());
  }

  @Test
  void aColumnSomeDataFileDoesNotReportIsPartialAndUnbounded() {
    ScalarStats bounded = ints(10).setNullCount(0).setMinValue(i64(1)).setMaxValue(i64(5)).build();

    TargetStatsRecord merged = mergeFiles(file(bounded), fileWithoutColumn(7));

    assertEquals(StatsCompleteness.SC_PARTIAL, merged.getMetadata().getCompleteness());
    assertFalse(merged.getScalar().hasNullCount());
    assertFalse(merged.getScalar().hasMinValue());
  }

  @Test
  void aColumnSomePartialAggregateDoesNotReportIsPartialAndUnbounded() {
    ScalarStats bounded = ints(10).setNullCount(0).setMinValue(i64(1)).setMaxValue(i64(5)).build();
    List<TargetStatsRecord> partials =
        List.of(tablePartial(10, 1), columnPartial(bounded), tablePartial(7, 1));

    TargetStatsRecord merged =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(TABLE, 1L, KINDS, partials)
            .stream()
            .filter(TargetStatsRecord::hasScalar)
            .findFirst()
            .orElseThrow();

    assertEquals(StatsCompleteness.SC_PARTIAL, merged.getMetadata().getCompleteness());
    assertFalse(merged.getScalar().hasNullCount());
    assertFalse(merged.getScalar().hasMinValue());

    // A partial covering no data file reports no column, and does not count.
    TargetStatsRecord withEmpty =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
                TABLE,
                1L,
                KINDS,
                List.of(tablePartial(10, 1), columnPartial(bounded), tablePartial(0, 0)))
            .stream()
            .filter(TargetStatsRecord::hasScalar)
            .findFirst()
            .orElseThrow();
    assertEquals(StatsCompleteness.SC_COMPLETE, withEmpty.getMetadata().getCompleteness());

    // Without table records nothing counts the partials.
    TargetStatsRecord uncounted =
        FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
                TABLE,
                1L,
                Set.of(FloecatConnector.StatsTargetKind.COLUMN),
                List.of(columnPartial(bounded)))
            .getFirst();
    assertEquals(StatsCompleteness.SC_PARTIAL, uncounted.getMetadata().getCompleteness());
  }

  @Test
  void aTableCountsTheRowsItsDeletesLeaveWhereAFileKnowsThem() {
    TargetStatsRecord withDeletes = fileRecord(10, List.of());
    withDeletes =
        withDeletes.toBuilder()
            .setFile(withDeletes.getFile().toBuilder().setLiveRowCount(7))
            .build();

    TargetStatsRecord table =
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
                TABLE,
                1L,
                Set.of(FloecatConnector.StatsTargetKind.TABLE),
                List.of(withDeletes, fileRecord(5, List.of())))
            .stream()
            .filter(TargetStatsRecord::hasTable)
            .findFirst()
            .orElseThrow();

    assertEquals(12L, table.getTable().getRowCount());

    // A file cannot keep more rows than it holds.
    TargetStatsRecord impossible = fileRecord(10, List.of());
    impossible =
        impossible.toBuilder()
            .setFile(impossible.getFile().toBuilder().setLiveRowCount(20))
            .build();
    assertEquals(
        10L,
        FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
                TABLE, 1L, Set.of(FloecatConnector.StatsTargetKind.TABLE), List.of(impossible))
            .stream()
            .filter(TargetStatsRecord::hasTable)
            .findFirst()
            .orElseThrow()
            .getTable()
            .getRowCount());
  }

  @Test
  void aNegativeZeroMinimumHasNoLegacySpelling() {
    ScalarStats zero =
        ScalarStats.newBuilder()
            .setDisplayName("col")
            .setLogicalType("DOUBLE")
            .setRowCount(10)
            .setNullCount(0)
            .setMinValue(ScalarValue.newBuilder().setF64(0.0))
            .setMaxValue(ScalarValue.newBuilder().setF64(1.0))
            .build();

    // +0.0 is widened to -0.0, which the legacy string would spell as 0.
    ScalarStats merged = mergeFiles(file(zero), file(zero)).getScalar();

    assertTrue(merged.hasMinValue());
    assertFalse(merged.hasMin());
  }

  @Test
  void aMergeClaimsTheWeakestCompletenessOfItsFiles() {
    ScalarStats stats = ints(10).setNullCount(0).build();
    TargetStatsRecord partial = file(stats);
    partial =
        partial.toBuilder()
            .setMetadata(
                partial.getMetadata().toBuilder().setCompleteness(StatsCompleteness.SC_PARTIAL))
            .build();

    // In either order: a merge taking the first or the last file's completeness would claim
    // COMPLETE.
    assertEquals(
        StatsCompleteness.SC_PARTIAL,
        mergeFiles(partial, file(stats)).getMetadata().getCompleteness());
    assertEquals(
        StatsCompleteness.SC_PARTIAL,
        mergeFiles(file(stats), partial).getMetadata().getCompleteness());
    // A file without metadata claims nothing.
    TargetStatsRecord unclaimed = file(stats).toBuilder().clearMetadata().build();
    assertEquals(
        StatsCompleteness.SC_UNSPECIFIED,
        mergeFiles(file(stats), unclaimed).getMetadata().getCompleteness());
  }

  private static ScalarStats.Builder ints(long rows) {
    return ScalarStats.newBuilder().setDisplayName("col").setLogicalType("INT").setRowCount(rows);
  }

  private static ScalarValue i64(long value) {
    return ScalarValue.newBuilder().setI64(value).build();
  }

  private static TargetStatsRecord mergeFiles(TargetStatsRecord... files) {
    return FileGroupTargetStatsRollup.completeSnapshotFromFileRecords(
            TABLE, 1L, Set.of(FloecatConnector.StatsTargetKind.COLUMN), List.of(files))
        .stream()
        .filter(TargetStatsRecord::hasScalar)
        .findFirst()
        .orElseThrow();
  }

  /** A data file reporting {@code scalar} for column 1. */
  private static TargetStatsRecord file(ScalarStats scalar) {
    return fileRecord(
        scalar.getRowCount(),
        List.of(FileColumnStats.newBuilder().setColumnId(1L).setScalar(scalar).build()));
  }

  /** The table record merged from table {@code partials}. */
  private static TableValueStats mergedTable(TargetStatsRecord... partials) {
    return FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            TABLE, 1L, Set.of(FloecatConnector.StatsTargetKind.TABLE), List.of(partials))
        .stream()
        .filter(TargetStatsRecord::hasTable)
        .findFirst()
        .orElseThrow()
        .getTable();
  }

  /** A data file of {@code rows} rows reporting nothing for column 1. */
  private static TargetStatsRecord fileWithoutColumn(long rows) {
    return fileRecord(rows, List.of());
  }

  private static TargetStatsRecord fileRecord(long rows, List<FileColumnStats> columns) {
    return TargetStatsRecord.newBuilder()
        .setTableId(TABLE)
        .setSnapshotId(1L)
        .setMetadata(complete())
        .setFile(
            FileTargetStats.newBuilder()
                .setTableId(TABLE)
                .setSnapshotId(1L)
                .setRowCount(rows)
                .addAllColumns(columns))
        .build();
  }

  /** One file group's table record. */
  private static TargetStatsRecord tablePartial(long rows, long dataFiles) {
    return TargetStatsRecords.tableRecord(
        TABLE,
        1L,
        TableValueStats.newBuilder().setRowCount(rows).setDataFileCount(dataFiles).build(),
        complete());
  }

  /** One file group's record of column 1. */
  private static TargetStatsRecord columnPartial(ScalarStats scalar) {
    return TargetStatsRecords.columnRecord(TABLE, 1L, 1L, scalar, complete());
  }

  private static StatsMetadata complete() {
    return StatsMetadata.newBuilder().setCompleteness(StatsCompleteness.SC_COMPLETE).build();
  }
}
