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

package ai.floedb.floecat.service.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ai.floedb.floecat.catalog.rpc.StatsTarget;
import ai.floedb.floecat.catalog.rpc.TableStatsTarget;
import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import ai.floedb.floecat.stats.spi.StatsTriggerOutcome;
import ai.floedb.floecat.stats.spi.StatsTriggerResult;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StatsCaptureControlPlaneAdapterTest {

  @Test
  void delegatesCaptureToOrchestrator() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request = request();
    StatsCaptureResult expected =
        StatsCaptureResult.forRecord(
            "test",
            TargetStatsRecord.newBuilder()
                .setTableId(request.tableId())
                .setSnapshotId(request.snapshotId())
                .setTarget(request.target())
                .setTable(TableValueStats.newBuilder().setRowCount(1L).build())
                .build(),
            Map.of());
    when(orchestrator.trigger(request)).thenReturn(StatsTriggerResult.captured(expected));

    StatsTriggerResult out = adapter.trigger(request);

    assertThat(out.outcome()).isEqualTo(StatsTriggerOutcome.CAPTURED);
    assertThat(out.captureResult()).contains(expected);
    verify(orchestrator).trigger(request);
  }

  @Test
  void delegatesQueuedOutcome() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request = request();
    when(orchestrator.trigger(request)).thenReturn(StatsTriggerResult.queued("queued for worker"));

    StatsTriggerResult out = adapter.trigger(request);

    assertThat(out.outcome()).isEqualTo(StatsTriggerOutcome.QUEUED);
    assertThat(out.captureResult()).isEmpty();
    assertThat(out.detail()).isEqualTo("queued for worker");
    verify(orchestrator).trigger(request);
  }

  @Test
  void delegatesUncapturableOutcome() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request = request();
    when(orchestrator.trigger(request))
        .thenReturn(StatsTriggerResult.uncapturable("engine unsupported"));

    StatsTriggerResult out = adapter.trigger(request);

    assertThat(out.outcome()).isEqualTo(StatsTriggerOutcome.UNCAPTURABLE);
    assertThat(out.captureResult()).isEmpty();
    assertThat(out.detail()).isEqualTo("engine unsupported");
    verify(orchestrator).trigger(request);
  }

  @Test
  void delegatesDegradedOutcome() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request = request();
    when(orchestrator.trigger(request)).thenReturn(StatsTriggerResult.degraded("runtime failure"));

    StatsTriggerResult out = adapter.trigger(request);

    assertThat(out.outcome()).isEqualTo(StatsTriggerOutcome.DEGRADED);
    assertThat(out.captureResult()).isEmpty();
    assertThat(out.detail()).isEqualTo("runtime failure");
    verify(orchestrator).trigger(request);
  }

  private static StatsCaptureRequest request() {
    return StatsCaptureRequest.builder(
            ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build(),
            42L,
            StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
        .executionMode(StatsExecutionMode.ASYNC)
        .connectorType("iceberg")
        .correlationId("corr-1")
        .build();
  }
}
