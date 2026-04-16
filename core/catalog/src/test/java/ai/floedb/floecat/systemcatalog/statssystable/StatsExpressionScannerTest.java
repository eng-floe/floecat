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

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.scanner.expr.Expr;
import ai.floedb.floecat.scanner.spi.SystemObjectScanContext;
import ai.floedb.floecat.systemcatalog.utilities.TestTableScanContextBuilder;
import com.google.protobuf.ByteString;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

class StatsExpressionScannerTest {

  @Test
  void schemaIsAsExpected() {
    assertThat(new StatsExpressionScanner().schema()).hasSize(24);
    assertThat(new StatsExpressionScanner().schema().get(6).getName()).isEqualTo("engine_kind");
    assertThat(new StatsExpressionScanner().schema().get(7).getName()).isEqualTo("expression_key");
  }

  @Test
  void scanMapsExpressionIdentityAndScalar() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(9L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(
        orders.id(),
        9L,
        List.of(expressionRecord(orders.id(), 9L, "trino", ByteString.copyFromUtf8("k1"))));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Object[] row = new StatsExpressionScanner().scan(ctx).findFirst().orElseThrow().values();

    assertThat(row[6]).isEqualTo("trino");
    assertThat(row[7]).isEqualTo("azE");
    assertThat(row[8]).isEqualTo("BIGINT");
    assertThat(row[9]).isEqualTo(200L);
    assertThat(row[10]).isEqualTo(2L);
    assertThat(row[11]).isEqualTo(0L);
    assertThat(row[13]).isEqualTo("1");
    assertThat(row[14]).isEqualTo("9");
  }

  @Test
  void scanDoesNotPrefetchByEngineKindFilter() {
    var builder = TestTableScanContextBuilder.builder("catalog");
    var ns = builder.addNamespace("public");
    var orders =
        builder.addTable(
            ns, "orders", Map.of("id", 1), Map.of("id", "bigint"), OptionalLong.of(4L));

    StatsScannerTestSupport.FakeStatsProvider stats =
        new StatsScannerTestSupport.FakeStatsProvider();
    stats.put(
        orders.id(),
        4L,
        List.of(
            expressionRecord(orders.id(), 4L, "trino", ByteString.copyFromUtf8("k1")),
            expressionRecord(orders.id(), 4L, "spark", ByteString.copyFromUtf8("k2"))));

    SystemObjectScanContext ctx = StatsScannerTestSupport.context(builder, stats);
    Expr predicate = new Expr.Eq(new Expr.ColumnRef("engine_kind"), new Expr.Literal("spark"));
    List<Object[]> rows =
        new StatsExpressionScanner()
            .streamRecords(ctx, predicate)
            .map(r -> new StatsExpressionScanner().toRow(r).values())
            .toList();

    assertThat(rows).hasSize(2);
    assertThat(rows).extracting(r -> r[6]).containsExactly("trino", "spark");
  }

  private static TargetStatsRecord expressionRecord(
      ResourceId tableId, long snapshotId, String engineKind, ByteString key) {
    return TargetStatsRecord.newBuilder()
        .setTableId(tableId)
        .setSnapshotId(snapshotId)
        .setTarget(
            StatsTarget.newBuilder()
                .setExpression(
                    EngineExpressionStatsTarget.newBuilder()
                        .setEngineKind(engineKind)
                        .setEngineExpressionKey(key)
                        .build())
                .build())
        .setScalar(
            ScalarStats.newBuilder()
                .setLogicalType("BIGINT")
                .setValueCount(200L)
                .setNullCount(2L)
                .setNanCount(0L)
                .setMin("1")
                .setMax("9")
                .build())
        .build();
  }
}
