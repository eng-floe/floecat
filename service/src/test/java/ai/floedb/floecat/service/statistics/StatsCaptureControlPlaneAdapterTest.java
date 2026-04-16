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
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsExecutionMode;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StatsCaptureControlPlaneAdapterTest {

  @Test
  void delegatesBatchTrigger() {
    StatsOrchestrator orchestrator = Mockito.mock(StatsOrchestrator.class);
    StatsCaptureControlPlaneAdapter adapter = new StatsCaptureControlPlaneAdapter(orchestrator);
    StatsCaptureRequest request = request();
    StatsCaptureBatchRequest batchRequest = StatsCaptureBatchRequest.of(request);
    StatsCaptureBatchResult expected =
        StatsCaptureBatchResult.of(
            List.of(StatsCaptureBatchItemResult.uncapturable(request, "unsupported")));
    when(orchestrator.triggerBatch(batchRequest)).thenReturn(expected);

    StatsCaptureBatchResult out = adapter.triggerBatch(batchRequest);

    assertThat(out).isEqualTo(expected);
    verify(orchestrator).triggerBatch(batchRequest);
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
