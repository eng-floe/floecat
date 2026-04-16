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
import ai.floedb.floecat.catalog.rpc.Ndv;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StatsColumnScannerTest {

  @Test
  void schemaIsAsExpected() {
    assertThat(new StatsColumnScanner().schema()).hasSize(28);
    assertThat(new StatsColumnScanner().schema().get(6).getName()).isEqualTo("column_id");
    assertThat(new StatsColumnScanner().schema().get(13).getName()).isEqualTo("distinct_count");
  }

  @Test
  void scanMapsScalarValuesAndNdv() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(7L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 7L, List.of(columnRecord(orders.id(), 7L, 1L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Object[] row = new StatsColumnScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[6]).isEqualTo("1");
    assertThat(row[7]).isEqualTo("id");
    assertThat(row[8]).isEqualTo("BIGINT");
    assertThat(row[10]).isEqualTo(100L);
    assertThat(row[11]).isEqualTo(5L);
    assertThat(row[12]).isEqualTo(1L);
    assertThat(row[13]).isEqualTo(42.0d);
    assertThat(row[14]).isEqualTo("0");
    assertThat(row[15]).isEqualTo("999");
  }

  @Test
  void scanReturnsNullsWhenScalarPayloadIsMissing() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(3L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 3L, List.of(columnTargetOnlyRecord(orders.id(), 3L, 1L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Object[] row = new StatsColumnScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[8]).isNull();
    assertThat(row[10]).isNull();
    assertThat(row[11]).isNull();
    assertThat(row[12]).isNull();
    assertThat(row[13]).isNull();
    assertThat(row[14]).isNull();
    assertThat(row[15]).isNull();
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
        .setScalar(
            ScalarStats.newBuilder()
                .setDisplayName("id")
                .setLogicalType("BIGINT")
                .setValueCount(100L)
                .setNullCount(5L)
                .setNanCount(1L)
                .setNdv(Ndv.newBuilder().setExact(42L).build())
                .setMin("0")
                .setMax("999")
                .build())
        .build();
  }

  private static TargetStatsRecord columnTargetOnlyRecord(
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
}
