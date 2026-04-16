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

package ai.floedb.floecat.systemcatalog.statssystable;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StatsTableScannerTest {

  @Test
  void schemaIsAsExpected() {
    assertThat(new StatsTableScanner().schema().stream().map(c -> c.getName()).toList())
        .containsExactly(
            "account_id",
            "catalog",
            "schema",
            "table",
            "table_id",
            "snapshot_id",
            "row_count",
            "file_count",
            "total_bytes",
            "completeness",
            "provenance",
            "confidence",
            "capture_time",
            "refresh_time");
  }

  @Test
  void scanUsesCurrentSnapshotWhenPredicateDoesNotSpecifySnapshot() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(11L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 11L, List.of(tableRecord(orders.id(), 11L)));
    stats.put(orders.id(), 99L, List.of(tableRecord(orders.id(), 99L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    List<Object[]> rows = new StatsTableScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[5]).isEqualTo(11L);
  }

  @Test
  void scanDoesNotPrefetchBySnapshotPredicate() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(77L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 0L, List.of(tableRecord(orders.id(), 0L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Expr predicate = new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("0"));
    List<Object[]> rows =
        new StatsTableScanner()
            .streamRecords(ctx, predicate)
            .map(StatsTableScannerTest::toValues)
            .toList();

    assertThat(rows).isEmpty();
  }

  @Test
  void scanLeavesMissingTableValuesAsNulls() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(5L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 5L, List.of(tableTargetOnlyRecord(orders.id(), 5L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Object[] row = new StatsTableScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[6]).isNull();
    assertThat(row[7]).isNull();
    assertThat(row[8]).isNull();
  }

  private static Object[] toValues(AbstractStatsScanner.StatsScanRecord row) {
    return new StatsTableScanner().toRow(row).values();
  }

  private static TargetStatsRecord tableRecord(ResourceId tableId, long snapshotId) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .setTable(
            ai.floedb.floecat.catalog.rpc.TableValueStats.newBuilder()
                .setRowCount(123L)
                .setDataFileCount(8L)
                .setTotalSizeBytes(1024L)
                .build())
        .build();
  }

  private static TargetStatsRecord tableTargetOnlyRecord(ResourceId tableId, long snapshotId) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .build();
  }
}
