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

import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class StatsSyncTypesTest {

  private static final TargetStatsRecord RECORD =
      TargetStatsRecord.newBuilder()
          .setTableId(ResourceId.newBuilder().setAccountId("acct").setId("t1").build())
          .setSnapshotId(1L)
          .setTarget(
              ai.floedb.floecat.catalog.rpc.StatsTarget.newBuilder()
                  .setTable(TableStatsTarget.getDefaultInstance())
                  .build())
          .setTable(TableValueStats.newBuilder().setRowCount(42).build())
          .build();

  @Test
  void hitCarriesRecord() {
    StatsResolutionResult r = StatsResolutionResult.hit(RECORD);
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.HIT);
    assertThat(r.stats()).contains(RECORD);
    assertThat(r.hasStats()).isTrue();
    assertThat(r.outcomeDetail()).isEmpty();
  }

  @Test
  void capturedCarriesRecord() {
    StatsResolutionResult r = StatsResolutionResult.captured(RECORD);
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.CAPTURED);
    assertThat(r.stats()).contains(RECORD);
    assertThat(r.hasStats()).isTrue();
  }

  @Test
  void partialIsEmpty() {
    StatsResolutionResult r = StatsResolutionResult.partial("some detail");
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.PARTIAL);
    assertThat(r.stats()).isEmpty();
    assertThat(r.hasStats()).isFalse();
    assertThat(r.outcomeDetail()).isEqualTo("some detail");
  }

  @Test
  void timeoutIsEmpty() {
    StatsResolutionResult r = StatsResolutionResult.timeout("exceeded 1s");
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.TIMEOUT);
    assertThat(r.stats()).isEmpty();
    assertThat(r.outcomeDetail()).isEqualTo("exceeded 1s");
  }

  @Test
  void failedIsEmpty() {
    StatsResolutionResult r = StatsResolutionResult.failed("connector error");
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.FAILED);
    assertThat(r.stats()).isEmpty();
  }

  @Test
  void skippedIsEmpty() {
    StatsResolutionResult r = StatsResolutionResult.skipped("async_mode");
    assertThat(r.outcome()).isEqualTo(StatsSyncOutcome.SKIPPED);
    assertThat(r.stats()).isEmpty();
    assertThat(r.outcomeDetail()).isEqualTo("async_mode");
  }

  @Test
  void nullDetailDefaultsToEmpty() {
    StatsResolutionResult r =
        new StatsResolutionResult(Optional.empty(), StatsSyncOutcome.SKIPPED, null, false);
    assertThat(r.outcomeDetail()).isEmpty();
  }

  @Test
  void nullStatsThrows() {
    assertThatThrownBy(() -> new StatsResolutionResult(null, StatsSyncOutcome.SKIPPED, "", false))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void nullOutcomeThrows() {
    assertThatThrownBy(() -> new StatsResolutionResult(Optional.empty(), null, "", false))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void hitRequiresNonNullRecord() {
    assertThatThrownBy(() -> StatsResolutionResult.hit(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void allOutcomeValuesExist() {
    assertThat(StatsSyncOutcome.values())
        .containsExactlyInAnyOrder(
            StatsSyncOutcome.HIT,
            StatsSyncOutcome.CAPTURED,
            StatsSyncOutcome.PARTIAL,
            StatsSyncOutcome.TIMEOUT,
            StatsSyncOutcome.FAILED,
            StatsSyncOutcome.SKIPPED);
  }
}
