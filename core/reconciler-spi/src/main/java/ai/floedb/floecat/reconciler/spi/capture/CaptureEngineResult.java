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
 * Terminal captured outputs for one file-group execution.
 *
 * <p>File-scoped outputs are delivered progressively through {@link CaptureFileResultConsumer} and
 * must not be retained here. Stats records in this result are compact file-group aggregate
 * partials.
 */
public record CaptureEngineResult(
    List<TargetStatsRecord> statsRecords,
    List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries,
    List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts,
    List<String> realizedStatsSelectors) {
  public CaptureEngineResult {
    statsRecords = statsRecords == null ? List.of() : List.copyOf(statsRecords);
    pageIndexEntries = pageIndexEntries == null ? List.of() : List.copyOf(pageIndexEntries);
    stagedIndexArtifacts =
        stagedIndexArtifacts == null ? List.of() : List.copyOf(stagedIndexArtifacts);
    realizedStatsSelectors =
        realizedStatsSelectors == null
            ? List.of()
            : realizedStatsSelectors.stream()
                .filter(selector -> selector != null && !selector.isBlank())
                .map(String::trim)
                .distinct()
                .sorted()
                .toList();
  }

  public static CaptureEngineResult of(
      List<TargetStatsRecord> statsRecords,
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts) {
    return new CaptureEngineResult(statsRecords, pageIndexEntries, stagedIndexArtifacts, List.of());
  }

  public static CaptureEngineResult of(
      List<TargetStatsRecord> statsRecords,
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries,
      List<ReconcilerBackend.StagedIndexArtifact> stagedIndexArtifacts,
      List<String> realizedStatsSelectors) {
    return new CaptureEngineResult(
        statsRecords, pageIndexEntries, stagedIndexArtifacts, realizedStatsSelectors);
  }

  public static CaptureEngineResult empty() {
    return new CaptureEngineResult(List.of(), List.of(), List.of(), List.of());
  }
}
