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

package ai.floedb.floecat.reconciler.spi.capture;

import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.ReconcilerBackend;
import java.util.List;

/**
 * Captured outputs for one file-group execution.
 *
 * <p>Results are expected to be semantically complete for the requested file group. Engines may
 * obtain those outputs however they want, but callers should not need any follow-up aggregation to
 * make the result persistable.
 */
public record CaptureEngineResult(
    List<TargetStatsRecord> statsRecords,
    List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries,
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
  public CaptureEngineResult {
    statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
    pageIndexEntries = pageIndexEntries == null ? List.of() : List.copyOf(pageIndexEntries);
    stagedIndexArtifacts =
        stagedIndexArtifacts == null ? List.of() : List.copyOf(stagedIndexArtifacts);
  }

  public static CaptureEngineResult of(
      List<TargetStatsRecord> statsRecords,
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    return new CaptureEngineResult(statsRecords, pageIndexEntries, stagedIndexArtifacts);
  }

  public static CaptureEngineResult empty() {
    return new CaptureEngineResult(List.of(), List.of(), List.of());
  }
}
