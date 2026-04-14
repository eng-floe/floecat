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

package ai.floedb.floecat.stats.spi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StatsCapabilitiesTest {

  @Test
  void normalizesConnectorTypesOnConstruction() {
    StatsCapabilities caps =
        StatsCapabilities.builder()
            .connectors(Set.of(" Iceberg ", "DELTA", ""))
            .targetTypes(Set.of(StatsTargetType.TABLE))
            .statisticKindsByTarget(Map.of(StatsTargetType.TABLE, Set.of(StatsKind.ROW_COUNT)))
            .executionModes(Set.of(StatsExecutionMode.SYNC))
            .samplingSupport(Set.of(StatsSamplingSupport.NONE))
            .snapshotAware(true)
            .build();

    assertThat(caps.connectors()).containsExactlyInAnyOrder("iceberg", "delta");
  }

  @Test
  void normalizedConnectorsMatchNormalizedRequestConnectorType() {
    StatsCapabilities caps =
        StatsCapabilities.builder()
            .connectors(Set.of("ICEBERG"))
            .targetTypes(Set.of(StatsTargetType.TABLE))
            .statisticKindsByTarget(Map.of(StatsTargetType.TABLE, Set.of()))
            .executionModes(Set.of(StatsExecutionMode.SYNC))
            .samplingSupport(Set.of(StatsSamplingSupport.NONE))
            .snapshotAware(true)
            .build();

    StatsCaptureRequest request =
        new StatsCaptureRequest(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            1L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            Set.of(),
            Set.of(),
            StatsExecutionMode.SYNC,
            " iceberg ",
            "",
            false);

    assertThat(caps.supports(request)).isTrue();
  }

  @Test
  void supportsUsesPerTargetStatisticKinds() {
    StatsCapabilities caps =
        StatsCapabilities.builder()
            .targetTypes(Set.of(StatsTargetType.TABLE, StatsTargetType.COLUMN))
            .statisticKindsByTarget(
                Map.of(
                    StatsTargetType.TABLE, Set.of(StatsKind.ROW_COUNT),
                    StatsTargetType.COLUMN, Set.of(StatsKind.NDV)))
            .executionModes(Set.of(StatsExecutionMode.SYNC))
            .samplingSupport(Set.of(StatsSamplingSupport.NONE))
            .snapshotAware(true)
            .build();

    StatsCaptureRequest tableNdvRequest =
        new StatsCaptureRequest(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            1L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            Set.of(),
            Set.of(StatsKind.NDV),
            StatsExecutionMode.SYNC,
            "",
            "",
            false);
    assertThat(caps.supports(tableNdvRequest)).isFalse();
  }

  @Test
  void requiresKindsMappingForEveryDeclaredTargetType() {
    assertThatThrownBy(
            () ->
                StatsCapabilities.builder()
                    .targetTypes(Set.of(StatsTargetType.TABLE, StatsTargetType.COLUMN))
                    .statisticKindsByTarget(
                        Map.of(StatsTargetType.TABLE, Set.of(StatsKind.ROW_COUNT)))
                    .executionModes(Set.of(StatsExecutionMode.SYNC))
                    .samplingSupport(Set.of(StatsSamplingSupport.NONE))
                    .snapshotAware(true)
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must declare supported kinds");
  }
}
