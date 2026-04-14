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
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StatsCaptureControlPlaneAdapterTest {

  @Test
  void delegatesCaptureToOrchestrator() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request =
        StatsCaptureRequest.builder(
                ResourceId.newBuilder().setAccountId("acct").setId("tbl-1").build(),
                42L,
                StatsTarget.newBuilder().setTable(TableStatsTarget.getDefaultInstance()).build())
            .executionMode(StatsExecutionMode.ASYNC)
            .connectorType("iceberg")
            .correlationId("corr-1")
            .build();
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
    when(orchestrator.capture(request)).thenReturn(Optional.of(expected));

    Optional<StatsCaptureResult> out = adapter.capture(request);

    assertThat(out).contains(expected);
    verify(orchestrator).capture(request);
  }
}
