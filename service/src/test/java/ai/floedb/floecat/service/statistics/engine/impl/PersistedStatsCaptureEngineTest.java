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

package ai.floedb.floecat.service.statistics.engine.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.EngineExpressionStatsTarget;
import ai.floedb.floecat.catalog.rpc.FileTargetStats;
import ai.floedb.floecat.catalog.rpc.ScalarStats;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import com.google.protobuf.ByteString;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PersistedStatsCaptureEngineTest {

  @Test
  void capturesTableTargetFromRepository() {
    StatsStore store = Mockito.mock(StatsStore.class);
    PersistedStatsCaptureEngine engine = new PersistedStatsCaptureEngine(store);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("a").setId("t").build();
    var target = StatsTargetIdentity.tableTarget();
    TargetStatsRecord expectedRecord =
        TargetStatsRecord.newBuilder()
            .setTarget(target)
            .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
            .build();
    when(store.getTargetStats(tableId, 101L, target)).thenReturn(Optional.of(expectedRecord));

    StatsCaptureRequest request =
        new StatsCaptureRequest(
            tableId,
            101L,
            target,
            Set.of(StatsKind.ROW_COUNT),
            StatsExecutionMode.SYNC,
            "",
            "corr-table",
            false);

    var result = engine.capture(request);
    assertThat(result).isPresent();
    assertThat(result.get().record()).isEqualTo(expectedRecord);
  }

  @Test
  void capturesColumnTargetFromRepository() {
    StatsStore store = Mockito.mock(StatsStore.class);
    PersistedStatsCaptureEngine engine = new PersistedStatsCaptureEngine(store);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("a").setId("t").build();
    var target = StatsTargetIdentity.columnTarget(7L);
    TargetStatsRecord expectedRecord =
        TargetStatsRecord.newBuilder()
            .setTarget(target)
            .setScalar(ScalarStats.newBuilder().setDisplayName("c7").setLogicalType("BIGINT"))
            .build();
    when(store.getTargetStats(tableId, 101L, target)).thenReturn(Optional.of(expectedRecord));

    StatsCaptureRequest request =
        new StatsCaptureRequest(
            tableId,
            101L,
            target,
            Set.of(StatsKind.NDV),
            StatsExecutionMode.SYNC,
            "",
            "corr-column",
            false);

    var result = engine.capture(request);
    assertThat(result).isPresent();
    assertThat(result.get().record()).isEqualTo(expectedRecord);
  }

  @Test
  void capturesFileTargetFromRepository() {
    StatsStore store = Mockito.mock(StatsStore.class);
    PersistedStatsCaptureEngine engine = new PersistedStatsCaptureEngine(store);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("a").setId("t").build();
    var target = StatsTargetIdentity.fileTarget("/data/f1.parquet");
    TargetStatsRecord expectedRecord =
        TargetStatsRecord.newBuilder()
            .setTarget(target)
            .setFile(FileTargetStats.newBuilder().setFilePath("/data/f1.parquet").build())
            .build();
    when(store.getTargetStats(tableId, 101L, target)).thenReturn(Optional.of(expectedRecord));

    StatsCaptureRequest request =
        new StatsCaptureRequest(
            tableId,
            101L,
            target,
            Set.of(StatsKind.ROW_COUNT),
            StatsExecutionMode.SYNC,
            "",
            "corr-file",
            false);

    var result = engine.capture(request);
    assertThat(result).isPresent();
    assertThat(result.get().record()).isEqualTo(expectedRecord);
  }

  @Test
  void capturesExpressionTargetFromRepository() {
    StatsStore store = Mockito.mock(StatsStore.class);
    PersistedStatsCaptureEngine engine = new PersistedStatsCaptureEngine(store);

    ResourceId tableId = ResourceId.newBuilder().setAccountId("a").setId("t").build();
    EngineExpressionStatsTarget expressionTarget =
        EngineExpressionStatsTarget.newBuilder()
            .setEngineKind("duckdb")
            .setEngineExpressionKey(ByteString.copyFromUtf8("expr-key"))
            .build();
    var target = StatsTargetIdentity.expressionTarget(expressionTarget);

    TargetStatsRecord expectedRecord =
        TargetStatsRecord.newBuilder()
            .setTarget(target)
            .setScalar(ScalarStats.newBuilder().setDisplayName("expr").setLogicalType("BIGINT"))
            .build();

    when(store.getTargetStats(tableId, 101L, target)).thenReturn(Optional.of(expectedRecord));

    StatsCaptureRequest request =
        new StatsCaptureRequest(
            tableId,
            101L,
            target,
            Set.of(StatsKind.NDV),
            StatsExecutionMode.SYNC,
            "",
            "corr-expression",
            false);

    var result = engine.capture(request);

    assertThat(result).isPresent();
    assertThat(result.get().record()).isEqualTo(expectedRecord);
    assertThat(engine.capabilities().targetTypes()).contains(StatsTargetType.EXPRESSION);
  }

  @Test
  void captureReturnsEmptyWhenTargetNotFoundOrValueMismatch() {
    StatsStore store = Mockito.mock(StatsStore.class);
    PersistedStatsCaptureEngine engine = new PersistedStatsCaptureEngine(store);
    ResourceId tableId = ResourceId.newBuilder().setAccountId("a").setId("t").build();

    var tableTarget = StatsTargetIdentity.tableTarget();
    when(store.getTargetStats(tableId, 101L, tableTarget)).thenReturn(Optional.empty());
    var missingRequest =
        new StatsCaptureRequest(
            tableId,
            101L,
            tableTarget,
            Set.of(StatsKind.ROW_COUNT),
            StatsExecutionMode.SYNC,
            "",
            "corr-missing",
            false);
    assertThat(engine.capture(missingRequest)).isEmpty();

    var fileTarget = StatsTargetIdentity.fileTarget("/data/f1.parquet");
    TargetStatsRecord wrongValue =
        TargetStatsRecord.newBuilder()
            .setTarget(fileTarget)
            .setScalar(ScalarStats.newBuilder().setDisplayName("x").setLogicalType("BIGINT"))
            .build();
    when(store.getTargetStats(tableId, 101L, fileTarget)).thenReturn(Optional.of(wrongValue));
    var mismatchRequest =
        new StatsCaptureRequest(
            tableId,
            101L,
            fileTarget,
            Set.of(StatsKind.ROW_COUNT),
            StatsExecutionMode.SYNC,
            "",
            "corr-mismatch",
            false);
    assertThat(engine.capture(mismatchRequest)).isEmpty();
  }
}
