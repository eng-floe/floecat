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

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.List;

public record StandaloneFileGroupExecutionResult(
    String resultId,
    List<TargetStatsRecord> statsRecords,
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
  public StandaloneFileGroupExecutionResult {
    resultId = resultId == null ? "" : resultId.trim();
    statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
    stagedIndexArtifacts =
        stagedIndexArtifacts == null ? List.of() : List.copyOf(stagedIndexArtifacts);
  }

  public static StandaloneFileGroupExecutionResult empty(String resultId) {
    return new StandaloneFileGroupExecutionResult(resultId, List.of(), List.of());
  }
}
