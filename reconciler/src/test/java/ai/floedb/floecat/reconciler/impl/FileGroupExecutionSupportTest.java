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

package ai.floedb.floecat.reconciler.impl;

import static org.assertj.core.api.Assertions.assertThat;

import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FileGroupExecutionSupportTest {

  @Test
  void requestedFileGroupStatsTargetKindsIncludesFileForTableOnlyPolicy() {
    ReconcileCapturePolicy capturePolicy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS));

    assertThat(FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy))
        .containsExactlyInAnyOrder(
            FloecatConnector.StatsTargetKind.TABLE, FloecatConnector.StatsTargetKind.FILE);
  }

  @Test
  void requestedFileGroupStatsTargetKindsIncludesFileForColumnOnlyPolicy() {
    ReconcileCapturePolicy capturePolicy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.COLUMN_STATS));

    assertThat(FileGroupExecutionSupport.requestedFileGroupStatsTargetKinds(capturePolicy))
        .containsExactlyInAnyOrder(
            FloecatConnector.StatsTargetKind.COLUMN, FloecatConnector.StatsTargetKind.FILE);
  }

  @Test
  void requestedStatsTargetKindsKeepsDirectTableOnlyPolicyUnchanged() {
    ReconcileCapturePolicy capturePolicy =
        ReconcileCapturePolicy.of(List.of(), Set.of(ReconcileCapturePolicy.Output.TABLE_STATS));

    assertThat(FileGroupExecutionSupport.requestedStatsTargetKinds(capturePolicy))
        .containsExactly(FloecatConnector.StatsTargetKind.TABLE);
  }
}
