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
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StatsCaptureRequestTest {

  @Test
  void snapshotZeroIsAccepted() {
    StatsCaptureRequest req =
        new StatsCaptureRequest(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            0L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            Set.of(),
            Set.of(),
            StatsExecutionMode.SYNC,
            "",
            false);
    assertThat(req.snapshotId()).isZero();
  }

  @Test
  void negativeSnapshotRejected() {
    assertThatThrownBy(
            () ->
                new StatsCaptureRequest(
                    ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                    -1L,
                    StatsTarget.newBuilder()
                        .setTable(TableStatsTarget.getDefaultInstance())
                        .build(),
                    Set.of(),
                    Set.of(),
                    StatsExecutionMode.SYNC,
                    "",
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("non-negative");
  }

  @Test
  void latencyBudgetOptionalAndPositiveWhenPresent() {
    StatsCaptureRequest withBudget =
        new StatsCaptureRequest(
            ResourceId.newBuilder().setAccountId("a").setId("t").build(),
            1L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build(),
            Set.of(),
            Set.of(),
            StatsExecutionMode.SYNC,
            "",
            false,
            Optional.of(Duration.ofSeconds(1)));
    assertThat(withBudget.latencyBudget()).contains(Duration.ofSeconds(1));

    assertThatThrownBy(
            () ->
                new StatsCaptureRequest(
                    ResourceId.newBuilder().setAccountId("a").setId("t").build(),
                    1L,
                    StatsTarget.newBuilder()
                        .setTable(TableStatsTarget.getDefaultInstance())
                        .build(),
                    Set.of(),
                    Set.of(),
                    StatsExecutionMode.SYNC,
                    "",
                    false,
                    Optional.of(Duration.ZERO)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("latencyBudget");
  }
}
