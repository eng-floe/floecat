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

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsEngineCapabilities;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsSamplingSupport;
import ai.floedb.floecat.stats.spi.StatsStatisticKind;
import ai.floedb.floecat.stats.spi.StatsTargetType;
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
        new TestEngine("empty-first", 1, tableOnlyCaps(), Optional.empty());
    TableStats expected = TableStats.newBuilder().setSnapshotId(100L).build();
    StatsCaptureEngine hitSecond =
        new TestEngine(
            "hit-second",
            2,
            tableOnlyCaps(),
            Optional.of(
                StatsCaptureResult.forTable(
                    "hit-second", request.target(), expected, Map.of("path", "unit"))));

    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(hitSecond, emptyHighPriority));

    Optional<StatsCaptureResult> out = registry.capture(request);
    assertThat(out).isPresent();
    assertThat(out.get().engineId()).isEqualTo("hit-second");
    assertThat(out.get().value().table()).contains(expected);
  }

  @Test
  void candidatesFilterUnsupportedEngines() {
    StatsCaptureRequest request = sampleRequest();
    StatsCaptureEngine unsupported =
        new TestEngine(
            "unsupported",
            1,
            StatsEngineCapabilities.builder()
                .targetTypes(Set.of(StatsTargetType.COLUMN))
                .statisticKinds(Set.of(StatsStatisticKind.NDV))
                .executionModes(Set.of(StatsExecutionMode.SYNC))
                .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                .snapshotAware(true)
                .build(),
            Optional.empty());
    StatsCaptureEngine supported =
        new TestEngine("supported", 2, tableOnlyCaps(), Optional.empty());
    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(unsupported, supported));

    assertThat(registry.candidates(request))
        .extracting(StatsCaptureEngine::id)
        .containsExactly("supported");
  }

  @Test
  void candidatesExcludeNonSnapshotAwareEngineForConcreteSnapshot() {
    StatsCaptureRequest request = sampleRequest(); // concrete snapshot id: 100
    StatsCaptureEngine notSnapshotAware =
        new TestEngine(
            "not-snapshot-aware",
            1,
            StatsEngineCapabilities.builder()
                .targetTypes(Set.of(StatsTargetType.TABLE))
                .statisticKinds(Set.of(StatsStatisticKind.ROW_COUNT))
                .executionModes(Set.of(StatsExecutionMode.SYNC))
                .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                .snapshotAware(false)
                .build(),
            Optional.empty());
    StatsCaptureEngine snapshotAware =
        new TestEngine("snapshot-aware", 2, tableOnlyCaps(), Optional.empty());

    StatsEngineRegistry registry =
        new StatsEngineRegistry(List.of(notSnapshotAware, snapshotAware));

    assertThat(registry.candidates(request))
        .extracting(StatsCaptureEngine::id)
        .containsExactly("snapshot-aware");
  }

  @Test
  void candidatesAllowNonSnapshotAwareEngineForUnresolvedSnapshotSentinel() {
    StatsCaptureRequest request =
        new StatsCaptureRequest(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            0L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            Set.of(StatsStatisticKind.ROW_COUNT),
            StatsExecutionMode.SYNC,
            "",
            false);
    StatsCaptureEngine notSnapshotAware =
        new TestEngine(
            "not-snapshot-aware",
            1,
            StatsEngineCapabilities.builder()
                .targetTypes(Set.of(StatsTargetType.TABLE))
                .statisticKinds(Set.of(StatsStatisticKind.ROW_COUNT))
                .executionModes(Set.of(StatsExecutionMode.SYNC))
                .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                .snapshotAware(false)
                .build(),
            Optional.empty());

    StatsEngineRegistry registry = new StatsEngineRegistry(List.of(notSnapshotAware));

    assertThat(registry.candidates(request))
        .extracting(StatsCaptureEngine::id)
        .containsExactly("not-snapshot-aware");
  }

  private static StatsCaptureRequest sampleRequest() {
    return new StatsCaptureRequest(
        ResourceId.newBuilder().setAccountId("a").setId("t").build(),
        100L,
        StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
        Set.of(StatsStatisticKind.ROW_COUNT),
        StatsExecutionMode.SYNC,
        "",
        false);
  }

  private static StatsEngineCapabilities tableOnlyCaps() {
    return StatsEngineCapabilities.builder()
        .targetTypes(Set.of(StatsTargetType.TABLE))
        .statisticKinds(Set.of(StatsStatisticKind.ROW_COUNT))
        .executionModes(Set.of(StatsExecutionMode.SYNC))
        .samplingSupport(Set.of(StatsSamplingSupport.NONE))
        .snapshotAware(true)
        .build();
  }

  private record TestEngine(
      String id,
      int priority,
      StatsEngineCapabilities capabilities,
      Optional<StatsCaptureResult> output)
      implements StatsCaptureEngine {

    @Override
    public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
      return output;
    }
  }
}
