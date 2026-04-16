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
import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
  void scanUsesPinnedSnapshotWhenPredicateDoesNotSpecifySnapshot() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider();
    stats.pin(orders.id(), 11L);
    stats.put(orders.id(), 11L, List.of(tableRecord(orders.id(), 11L)));
    stats.put(orders.id(), 99L, List.of(tableRecord(orders.id(), 99L)));

    SystemObjectScanContext ctx = context(builder, stats);
    List<Object[]> rows = new StatsTableScanner().scan(ctx).map(r -> r.values()).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[5]).isEqualTo(11L);
  }

  @Test
  void scanRespectsSnapshotZeroPredicate() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider();
    stats.pin(orders.id(), 77L);
    stats.put(orders.id(), 0L, List.of(tableRecord(orders.id(), 0L)));

    SystemObjectScanContext ctx = context(builder, stats);
    Expr predicate = new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("0"));
    List<Object[]> rows =
        new StatsTableScanner()
            .streamRecords(ctx, predicate)
            .map(StatsTableScannerTest::toValues)
            .toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst()[5]).isEqualTo(0L);
  }

  @Test
  void scanLeavesMissingTableValuesAsNulls() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider();
    stats.pin(orders.id(), 5L);
    stats.put(orders.id(), 5L, List.of(tableTargetOnlyRecord(orders.id(), 5L)));

    SystemObjectScanContext ctx = context(builder, stats);
    Object[] row = new StatsTableScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[6]).isNull();
    assertThat(row[7]).isNull();
    assertThat(row[8]).isNull();
  }

  private static Object[] toValues(AbstractStatsScanner.StatsScanRecord row) {
    return new StatsTableScanner().toRow(row).values();
  }

  private static SystemObjectScanContext context(
      TestTableScanContextBuilder builder, StatsProvider statsProvider) {
    return new SystemObjectScanContext(
        builder.overlay(),
        NameRef.getDefaultInstance(),
        ResourceId.newBuilder()
            .setAccountId("account")
            .setKind(ResourceKind.RK_CATALOG)
            .setId("catalog")
            .build(),
        EngineContext.empty(),
        statsProvider);
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

  private static final class FakeStatsProvider implements StatsProvider {
    private final Map<ResourceId, Long> pins = new HashMap<>();
    private final Map<Key, List<TargetStatsRecord>> records = new HashMap<>();

    void pin(ResourceId tableId, long snapshotId) {
      pins.put(tableId, snapshotId);
    }

    void put(ResourceId tableId, long snapshotId, List<TargetStatsRecord> items) {
      records.put(new Key(tableId, snapshotId), List.copyOf(items));
    }

    @Override
    public OptionalLong pinnedSnapshotId(ResourceId tableId) {
      Long snapshot = pins.get(tableId);
      return snapshot == null ? OptionalLong.empty() : OptionalLong.of(snapshot);
    }

    @Override
    public TargetStatsPage listPersistedStats(
        ResourceId tableId,
        long snapshotId,
        Optional<String> targetType,
        int limit,
        String pageToken) {
      if (targetType.isPresent() && !"TABLE".equalsIgnoreCase(targetType.get())) {
        return TargetStatsPage.EMPTY;
      }
      return new TargetStatsPage(records.getOrDefault(new Key(tableId, snapshotId), List.of()), "");
    }
  }

  private record Key(ResourceId tableId, long snapshotId) {}
}
