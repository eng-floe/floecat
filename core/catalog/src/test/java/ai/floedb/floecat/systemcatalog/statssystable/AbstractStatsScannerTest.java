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

import ai.floedb.floecat.catalog.rpc.ColumnStatsTarget;
import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.query.rpc.SchemaColumn;
import ai.floedb.floecat.scanner.columnar.AbstractArrowBatchBuilder;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.StatsProvider;
import ai.floedb.floecat.scanner.spi.SystemObjectRow;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.jupiter.api.Test;

class AbstractStatsScannerTest {

  @Test
  void pushdownParsesEqPredicatesFromAndTree() {
    Expr predicate =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("table_id"), new Expr.Literal("public.orders")),
            new Expr.And(
                new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("10")),
                new Expr.And(
                    new Expr.Eq(new Expr.ColumnRef("column_id"), new Expr.Literal("4")),
                    new Expr.Eq(new Expr.ColumnRef("engine_kind"), new Expr.Literal("trino")))));

    AbstractStatsScanner.StatsPushdown pushdown =
        AbstractStatsScanner.StatsPushdown.parse(predicate);

    assertThat(pushdown.impossible()).isFalse();
    assertThat(pushdown.tableFiltered()).isTrue();
    assertThat(pushdown.tableIds()).containsExactly("public.orders");
    assertThat(pushdown.snapshotId().orElseThrow()).isEqualTo(10L);
    assertThat(pushdown.columnId().orElseThrow()).isEqualTo(4L);
    assertThat(pushdown.engineKind().orElseThrow()).isEqualTo("trino");
  }

  @Test
  void pushdownMarksContradictoryEqualsAsImpossible() {
    Expr predicate =
        new Expr.And(
            new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("10")),
            new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("11")));

    AbstractStatsScanner.StatsPushdown pushdown =
        AbstractStatsScanner.StatsPushdown.parse(predicate);
    assertThat(pushdown.impossible()).isTrue();
  }

  @Test
  void iteratorUsesPinnedSnapshotAndSkipsTablesWithoutPin() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));
    builder.addTable(ns, "customers", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider(1);
    stats.pin(orders.id(), 44L);
    stats.put(
        orders.id(),
        44L,
        List.of(columnRecord(orders.id(), 44L, 1L), columnRecord(orders.id(), 44L, 2L)));

    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            builder.overlay(),
            ai.floedb.floecat.common.rpc.NameRef.getDefaultInstance(),
            ResourceId.newBuilder()
                .setAccountId("account")
                .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_CATALOG)
                .setId("catalog")
                .build(),
            ai.floedb.floecat.scanner.utils.EngineContext.empty(),
            stats);

    DummyScanner scanner = new DummyScanner(AbstractStatsScanner.TargetType.COLUMN);
    List<AbstractStatsScanner.StatsScanRecord> rows = scanner.streamRecords(ctx, null).toList();

    assertThat(rows).hasSize(2);
    assertThat(rows).allSatisfy(row -> assertThat(row.table()).isEqualTo("orders"));
    assertThat(rows).extracting(AbstractStatsScanner.StatsScanRecord::snapshotId).containsOnly(44L);
  }

  @Test
  void iteratorAppliesColumnFilterAndConsumesPagedResults() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider(1);
    stats.pin(orders.id(), 55L);
    stats.put(
        orders.id(),
        55L,
        List.of(columnRecord(orders.id(), 55L, 1L), columnRecord(orders.id(), 55L, 2L)));
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            builder.overlay(),
            ai.floedb.floecat.common.rpc.NameRef.getDefaultInstance(),
            ResourceId.newBuilder()
                .setAccountId("account")
                .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_CATALOG)
                .setId("catalog")
                .build(),
            ai.floedb.floecat.scanner.utils.EngineContext.empty(),
            stats);

    Expr predicate = new Expr.Eq(new Expr.ColumnRef("column_id"), new Expr.Literal("2"));
    DummyScanner scanner = new DummyScanner(AbstractStatsScanner.TargetType.COLUMN);
    List<AbstractStatsScanner.StatsScanRecord> rows =
        scanner.streamRecords(ctx, predicate).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().record().getTarget().getColumn().getColumnId()).isEqualTo(2L);
  }

  @Test
  void iteratorAppliesEngineKindFilterForExpressionRows() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders = builder.addTable(ns, "orders", Map.of("id", 1), Map.of("id", "bigint"));

    FakeStatsProvider stats = new FakeStatsProvider(10);
    stats.pin(orders.id(), 66L);
    stats.put(
        orders.id(),
        66L,
        List.of(
            expressionRecord(orders.id(), 66L, "trino", "a"),
            expressionRecord(orders.id(), 66L, "spark", "b")));
    SystemObjectScanContext ctx =
        new SystemObjectScanContext(
            builder.overlay(),
            ai.floedb.floecat.common.rpc.NameRef.getDefaultInstance(),
            ResourceId.newBuilder()
                .setAccountId("account")
                .setKind(ai.floedb.floecat.common.rpc.ResourceKind.RK_CATALOG)
                .setId("catalog")
                .build(),
            ai.floedb.floecat.scanner.utils.EngineContext.empty(),
            stats);

    Expr predicate = new Expr.Eq(new Expr.ColumnRef("engine_kind"), new Expr.Literal("spark"));
    DummyScanner scanner = new DummyScanner(AbstractStatsScanner.TargetType.EXPRESSION);
    List<AbstractStatsScanner.StatsScanRecord> rows =
        scanner.streamRecords(ctx, predicate).toList();

    assertThat(rows).hasSize(1);
    assertThat(rows.getFirst().record().getTarget().getExpression().getEngineKind())
        .isEqualTo("spark");
  }

  private static TargetStatsRecord columnRecord(
      ResourceId tableId, long snapshotId, long columnId) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(
            StatsTarget.newBuilder()
                .setColumn(ColumnStatsTarget.newBuilder().setColumnId(columnId).build())
                .build())
        .build();
  }

  private static TargetStatsRecord expressionRecord(
      ResourceId tableId, long snapshotId, String engineKind, String key) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind(engineKind)
                        .setEngineExpressionKey(com.google.protobuf.ByteString.copyFromUtf8(key))
                        .build())
                .build())
        .build();
  }

  private static final class DummyScanner extends AbstractStatsScanner {

    private final TargetType targetType;

    private DummyScanner(TargetType targetType) {
      this.targetType = targetType;
    }

    @Override
    protected TargetType targetType() {
      return targetType;
    }

    @Override
    public List<SchemaColumn> schema() {
      return List.of();
    }

    @Override
    protected AbstractArrowBatchBuilder newBatchBuilder(
        BufferAllocator allocator, java.util.Set<String> requiredColumns) {
      throw new UnsupportedOperationException("not used in this test");
    }

    @Override
    protected void appendArrowRow(AbstractArrowBatchBuilder builder, StatsScanRecord row) {
      throw new UnsupportedOperationException("not used in this test");
    }

    @Override
    protected SystemObjectRow toRow(StatsScanRecord row) {
      return new SystemObjectRow(new Object[] {row.tableId()});
    }
  }

  private static final class FakeStatsProvider implements StatsProvider {

    private final int forcedPageSize;
    private final Map<ResourceId, Long> pins = new HashMap<>();
    private final Map<Key, List<TargetStatsRecord>> records = new HashMap<>();

    private FakeStatsProvider(int forcedPageSize) {
      this.forcedPageSize = forcedPageSize;
    }

    void pin(ResourceId tableId, long snapshotId) {
      pins.put(tableId, snapshotId);
    }

    void put(ResourceId tableId, long snapshotId, List<TargetStatsRecord> items) {
      records.put(new Key(tableId, snapshotId), List.copyOf(items));
    }

    @Override
    public OptionalLong pinnedSnapshotId(ResourceId tableId) {
      Long snapshot = pins.get(tableId);
      if (snapshot == null) {
        return OptionalLong.empty();
      }
      return OptionalLong.of(snapshot);
    }

    @Override
    public TargetStatsPage listPersistedStats(
        ResourceId tableId,
        long snapshotId,
        Optional<String> targetType,
        int limit,
        String pageToken) {
      List<TargetStatsRecord> items = records.getOrDefault(new Key(tableId, snapshotId), List.of());
      List<TargetStatsRecord> filtered = new ArrayList<>(items.size());
      for (TargetStatsRecord item : items) {
        if (targetType.isPresent() && !matchesType(targetType.get(), item)) {
          continue;
        }
        filtered.add(item);
      }

      int offset = pageToken == null || pageToken.isBlank() ? 0 : Integer.parseInt(pageToken);
      if (offset >= filtered.size()) {
        return TargetStatsPage.EMPTY;
      }
      int pageSize = Math.max(1, Math.min(forcedPageSize, limit));
      int endExclusive = Math.min(filtered.size(), offset + pageSize);
      String nextToken = endExclusive < filtered.size() ? Integer.toString(endExclusive) : "";
      return new TargetStatsPage(filtered.subList(offset, endExclusive), nextToken);
    }

    private static boolean matchesType(String targetType, TargetStatsRecord item) {
      String normalized = targetType.toUpperCase();
      return switch (normalized) {
        case "TABLE" -> item.getTarget().hasTable();
        case "COLUMN" -> item.getTarget().hasColumn();
        case "EXPRESSION" -> item.getTarget().hasExpression();
        default -> true;
      };
    }

    private record Key(ResourceId tableId, long snapshotId) {}
  }
}
