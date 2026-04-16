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

import ai.floedb.floecat.catalog.rpc.StatsCompleteness;
import ai.floedb.floecat.catalog.rpc.StatsCoverage;
import ai.floedb.floecat.catalog.rpc.StatsMetadata;
import ai.floedb.floecat.catalog.rpc.StatsProducer;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import com.google.protobuf.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StatsSnapshotScannerTest {

  @Test
  void schemaIsAsExpected() {
    assertThat(new StatsSnapshotScanner().schema().stream().map(c -> c.getName()).toList())
        .containsExactly(
            "account_id",
            "catalog",
            "schema",
            "table",
            "table_id",
            "snapshot_id",
            "completeness",
            "provenance",
            "confidence",
            "capture_time",
            "refresh_time",
            "rows_seen_count",
            "files_seen_count",
            "row_groups_seen_count");
  }

  @Test
  void scanMapsMetadataAndCoverageFields() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(12L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 12L, List.of(tableWithMetadataRecord(orders.id(), 12L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Object[] row = new StatsSnapshotScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[6]).isEqualTo("partial");
    assertThat(row[7]).isEqualTo("source_native");
    assertThat(row[8]).isEqualTo(0.75d);
    assertThat(row[11]).isEqualTo(1000L);
    assertThat(row[12]).isEqualTo(20L);
    assertThat(row[13]).isEqualTo(4L);
    assertThat(row[9]).isEqualTo(Timestamp.newBuilder().setSeconds(1710000000L).build());
    assertThat(row[10]).isEqualTo(Timestamp.newBuilder().setSeconds(1710001000L).build());
  }

  @Test
  void scanDoesNotPrefetchBySnapshotPredicateIncludingZero() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(55L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(orders.id(), 0L, List.of(tableWithMetadataRecord(orders.id(), 0L)));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Expr predicate = new Expr.Eq(new Expr.ColumnRef("snapshot_id"), new Expr.Literal("0"));
    List<Object[]> rows =
        new StatsSnapshotScanner()
            .streamRecords(ctx, predicate)
            .map(r -> new StatsSnapshotScanner().toRow(r).values())
            .toList();

    assertThat(rows).isEmpty();
  }

  private static TargetStatsRecord tableWithMetadataRecord(ResourceId tableId, long snapshotId) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .setMetadata(
            StatsMetadata.newBuilder()
                .setProducer(StatsProducer.SPROD_SOURCE_NATIVE)
                .setCompleteness(StatsCompleteness.SC_PARTIAL)
                .setConfidenceLevel(0.75d)
                .setCapturedAt(Timestamp.newBuilder().setSeconds(1710000000L).build())
                .setRefreshedAt(Timestamp.newBuilder().setSeconds(1710001000L).build())
                .setCoverage(
                    StatsCoverage.newBuilder()
                        .setRowsScanned(1000L)
                        .setFilesScanned(20L)
                        .setRowGroupsSampled(4L)
                        .build())
                .build())
        .build();
  }
}
