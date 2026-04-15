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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

import ai.floedb.floecat.catalog.rpc.FileStatsTarget;
import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.telemetry.ServiceMetrics;
import ai.floedb.floecat.stats.spi.StatsCapabilities;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsKind;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import ai.floedb.floecat.stats.spi.testing.TestStatsCaptureEngine;
import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.Tag;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StatsEngineRegistryTest {

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

  @Test
  void captureBatchReturnsPerRequestOutcomes() {
    StatsCaptureRequest tableRequest = sampleRequest();
    StatsCaptureRequest unsupportedRequest =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                100L,
                StatsTarget.newBuilder()
                    .setFile(FileStatsTarget.newBuilder().setFilePath("/tmp/f.parquet").build())
                    .build())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .correlationId("corr-file")
            .build();
    StatsCaptureRequest failingRequest =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                101L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .correlationId("corr-fail")
            .build();

    TargetStatsRecord expected =
        TargetStatsRecord.newBuilder()
            .setTableId(tableRequest.tableId())
            .setSnapshotId(tableRequest.snapshotId())
            .setTarget(tableRequest.target())
            .setTable(TableValueStats.newBuilder().setRowCount(10L).build())
            .build();

    StatsCaptureEngine tableEngine =
        TestStatsCaptureEngine.builder("table-engine")
            .priority(1)
            .capabilities(tableOnlyCaps())
            .captureFn(
                req -> {
                  if (req.snapshotId() == 101L) {
                    throw new IllegalStateException("boom");
                  }
                  return Optional.of(
                      StatsCaptureResult.forRecord("table-engine", expected, Map.of()));
                })
            .build();
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(tableEngine));

    StatsCaptureBatchResult result =
        registry.captureBatch(
            StatsCaptureBatchRequest.of(List.of(tableRequest, unsupportedRequest, failingRequest)));

    assertThat(result.results()).hasSize(3);
    assertThat(result.results().get(0).outcome()).isEqualTo(StatsTriggerOutcome.CAPTURED);
    assertThat(result.results().get(1).outcome()).isEqualTo(StatsTriggerOutcome.UNCAPTURABLE);
    assertThat(result.results().get(2).outcome()).isEqualTo(StatsTriggerOutcome.DEGRADED);
  }

  @Test
  void captureBatchEmitsBatchMetrics() {
    StatsCaptureRequest tableRequest = sampleRequest();
    StatsCaptureEngine tableEngine =
        TestStatsCaptureEngine.builder("table-engine")
            .priority(1)
            .capabilities(tableOnlyCaps())
            .fixed(Optional.empty())
            .build();
    Observability observability = Mockito.mock(Observability.class);
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(tableEngine), observability);

    registry.captureBatch(StatsCaptureBatchRequest.of(List.of(tableRequest)));

    verify(observability, atLeastOnce())
        .counter(
            Mockito.eq(ServiceMetrics.Stats.BATCH_GROUPS_TOTAL),
            Mockito.anyDouble(),
            Mockito.any(Tag[].class));
    verify(observability, atLeastOnce())
        .counter(
            Mockito.eq(ServiceMetrics.Stats.ENGINE_BATCH_CALLS_TOTAL),
            Mockito.anyDouble(),
            Mockito.any(Tag[].class));
  }

  @Test
  void captureBatchMarksOrderMismatchesAsDegraded() {
    StatsCaptureRequest req1 = sampleRequest();
    StatsCaptureRequest req2 =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                101L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .requestedKinds(Set.of(StatsKind.ROW_COUNT))
            .executionMode(StatsExecutionMode.SYNC)
            .correlationId("test-corr-2")
            .build();

    StatsCaptureEngine misorderedEngine =
        new StatsCaptureEngine() {
          @Override
          public String id() {
            return "misordered";
          }

          @Override
          public int priority() {
            return 1;
          }

          @Override
          public StatsCapabilities capabilities() {
            return tableOnlyCaps();
          }

          @Override
          public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
            return Optional.empty();
          }

          @Override
          public StatsCaptureBatchResult captureBatch(StatsCaptureBatchRequest batchRequest) {
            return StatsCaptureBatchResult.of(
                List.of(
                    StatsCaptureBatchItemResult.uncapturable(req2, "no capture result"),
                    StatsCaptureBatchItemResult.uncapturable(req1, "no capture result")));
          }
        };

    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(misorderedEngine));
    StatsCaptureBatchResult result =
        registry.captureBatch(StatsCaptureBatchRequest.of(List.of(req1, req2)));

    assertThat(result.results()).hasSize(2);
    assertThat(result.results().get(0).outcome()).isEqualTo(StatsTriggerOutcome.DEGRADED);
    assertThat(result.results().get(0).detail()).contains("order mismatch");
    assertThat(result.results().get(1).outcome()).isEqualTo(StatsTriggerOutcome.DEGRADED);
    assertThat(result.results().get(1).detail()).contains("order mismatch");
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
