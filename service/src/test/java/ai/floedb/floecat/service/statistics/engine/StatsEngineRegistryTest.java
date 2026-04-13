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

package ai.floedb.floecat.service.statistics.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.spi.StatsCapabilities;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import ai.floedb.floecat.stats.spi.testing.TestStatsCaptureEngine;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StatsEngineRegistryTest {

  @Test
  void routesByPriorityAndReturnsFirstNonEmptyResult() {
    StatsCaptureRequest request = sampleRequest();
    StatsCaptureEngine emptyHighPriority =
        TestStatsCaptureEngine.builder("empty-first")
            .priority(1)
            .capabilities(tableOnlyCaps())
            .fixed(Optional.empty())
            .build();
    TargetStatsRecord expected =
        TargetStatsRecord.newBuilder()
            .setTableId(request.tableId())
            .setSnapshotId(request.snapshotId())
            .setTarget(request.target())
            .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
            .build();
    StatsCaptureEngine hitSecond =
        TestStatsCaptureEngine.builder("hit-second")
            .priority(2)
            .capabilities(tableOnlyCaps())
            .fixed(
                Optional.of(
                    StatsCaptureResult.forRecord("hit-second", expected, Map.of("path", "unit"))))
            .build();

    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(hitSecond, emptyHighPriority));

    Optional<StatsCaptureResult> out = registry.capture(request);
    assertThat(out).isPresent();
    assertThat(out.get().engineId()).isEqualTo("hit-second");
    assertThat(out.get().record()).isEqualTo(expected);
  }

  @Test
  void candidatesFilterUnsupportedEngines() {
    StatsCaptureRequest request = sampleRequest();
    StatsCaptureEngine unsupported =
        TestStatsCaptureEngine.builder("unsupported")
            .priority(1)
            .capabilities(
                StatsCapabilities.builder()
                    .targetTypes(Set.of(StatsTargetType.COLUMN))
                    .statisticKindsByTarget(Map.of(StatsTargetType.COLUMN, Set.of(StatsKind.NDV)))
                    .executionModes(Set.of(StatsExecutionMode.SYNC))
                    .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                    .build())
            .fixed(Optional.empty())
            .build();
    StatsCaptureEngine supported =
        TestStatsCaptureEngine.builder("supported")
            .priority(2)
            .capabilities(tableOnlyCaps())
            .fixed(Optional.empty())
            .build();
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(unsupported, supported));

    assertThat(registry.candidates(request))
        .extracting(StatsCaptureEngine::id)
        .containsExactly("supported");
  }

  @Test
  void captureThrowsNonImplementedWhenNoSelectedEngineSupportsRequest() {
    StatsCaptureRequest request = sampleRequest();
    StatsCaptureEngine columnOnly =
        TestStatsCaptureEngine.builder("column-only")
            .priority(1)
            .capabilities(
                StatsCapabilities.builder()
                    .targetTypes(Set.of(StatsTargetType.COLUMN))
                    .statisticKindsByTarget(
                        Map.of(StatsTargetType.COLUMN, Set.of(StatsKind.ROW_COUNT)))
                    .executionModes(Set.of(StatsExecutionMode.SYNC))
                    .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                    .build())
            .fixed(Optional.empty())
            .build();
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(columnOnly));

    assertThatThrownBy(() -> registry.capture(request))
        .isInstanceOf(StatsUnsupportedTargetException.class)
        .hasMessageContaining("target type TABLE");
  }

  @Test
  void candidatesFilterUnsupportedEnginesForFileTarget() {
    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                100L,
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath("/data/file.parquet").build())
                    .build())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .correlationId("test-corr-file")
            .build();
    StatsCaptureEngine unsupported =
        TestStatsCaptureEngine.builder("unsupported")
            .priority(1)
            .capabilities(
                StatsCapabilities.builder()
                    .targetTypes(Set.of(StatsTargetType.COLUMN))
                    .statisticKindsByTarget(
                        Map.of(StatsTargetType.COLUMN, Set.of(StatsKind.ROW_COUNT)))
                    .executionModes(Set.of(StatsExecutionMode.SYNC))
                    .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                    .build())
            .fixed(Optional.empty())
            .build();
    StatsCaptureEngine supported =
        TestStatsCaptureEngine.builder("supported")
            .priority(2)
            .capabilities(
                StatsCapabilities.builder()
                    .targetTypes(Set.of(StatsTargetType.FILE))
                    .statisticKindsByTarget(
                        Map.of(StatsTargetType.FILE, Set.of(StatsKind.ROW_COUNT)))
                    .executionModes(Set.of(StatsExecutionMode.SYNC))
                    .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                    .build())
            .fixed(Optional.empty())
            .build();
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(unsupported, supported));

    assertThat(registry.candidates(request))
        .extracting(StatsCaptureEngine::id)
        .containsExactly("supported");
  }

  private static StatsCaptureRequest sampleRequest() {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            100L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .requestedKinds(Set.of(StatsKind.ROW_COUNT))
        .executionMode(StatsExecutionMode.SYNC)
        .correlationId("test-corr")
        .build();
  }

  private static StatsCapabilities tableOnlyCaps() {
    return StatsCapabilities.builder()
        .targetTypes(Set.of(StatsTargetType.TABLE))
        .statisticKindsByTarget(Map.of(StatsTargetType.TABLE, Set.of(StatsKind.ROW_COUNT)))
        .executionModes(Set.of(StatsExecutionMode.SYNC))
        .samplingSupport(Set.of(StatsSamplingSupport.NONE))
        .build();
  }
}
