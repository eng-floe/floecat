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
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StatsCaptureBatchTypesTest {

  @Test
  void batchRequestRequiresAtLeastOneItemAndIsImmutable() {
    assertThatThrownBy(() -> StatsCaptureBatchRequest.of(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be empty");

    StatsCaptureRequest request = request("tbl-1", 10L);
    List<StatsCaptureRequest> mutable = new ArrayList<>();
    mutable.add(request);
    StatsCaptureBatchRequest batch = StatsCaptureBatchRequest.of(mutable);
    mutable.add(request("tbl-2", 11L));

    assertThat(batch.requests()).containsExactly(request);
    assertThatThrownBy(() -> batch.requests().add(request("tbl-3", 12L)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void batchItemResultCapturesOutcomeAndPayload() {
    StatsCaptureRequest request = request("tbl-1", 10L);
    TargetStatsRecord record =
        TargetStatsRecord.newBuilder()
            .setTableId(request.tableId())
            .setSnapshotId(request.snapshotId())
            .setTarget(request.target())
            .setTable(TableValueStats.newBuilder().setRowCount(7L).build())
            .build();
    StatsCaptureResult captureResult = StatsCaptureResult.forRecord("engine-1", record, Map.of());

    StatsCaptureBatchItemResult captured =
        StatsCaptureBatchItemResult.captured(request, captureResult);
    StatsCaptureBatchItemResult queued =
        StatsCaptureBatchItemResult.queued(request, "queued for async worker");

    assertThat(captured.outcome()).isEqualTo(StatsTriggerOutcome.CAPTURED);
    assertThat(captured.captureResult()).contains(captureResult);
    assertThat(queued.outcome()).isEqualTo(StatsTriggerOutcome.QUEUED);
    assertThat(queued.captureResult()).isEmpty();
    assertThat(queued.detail()).contains("queued");
  }

  @Test
  void batchResultIsImmutable() {
    StatsCaptureRequest request = request("tbl-1", 10L);
    List<StatsCaptureBatchItemResult> mutable = new ArrayList<>();
    mutable.add(StatsCaptureBatchItemResult.uncapturable(request, "unsupported"));
    StatsCaptureBatchResult result = StatsCaptureBatchResult.of(mutable);
    mutable.clear();

    assertThat(result.results()).hasSize(1);
    assertThatThrownBy(() -> result.results().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private static StatsCaptureRequest request(String tableId, long snapshotId) {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("acct").setId(tableId).build(),
            snapshotId,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .executionMode(StatsExecutionMode.SYNC)
        .connectorType("iceberg")
        .correlationId("corr")
        .build();
  }
}
