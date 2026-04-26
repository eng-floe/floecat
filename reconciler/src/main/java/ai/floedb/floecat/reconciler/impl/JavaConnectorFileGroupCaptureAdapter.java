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
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineRequest;
import ai.floedb.floecat.reconciler.spi.capture.CaptureEngineResult;
import java.util.List;
import java.util.Set;

/**
 * Adapts the current Java connector SPI to the unified file-group capture contract.
 *
 * <p>This class is intentionally Java-specific glue. It turns the connector's existing snapshot-
 * and file-group-scoped primitives into a semantically complete {@link CaptureEngineResult}. A
 * future Rust engine should replace this adapter by returning the same contract directly from its
 * own runtime.
 */
final class JavaConnectorFileGroupCaptureAdapter {
  private final FileGroupTargetStatsRollup statsRollup = new FileGroupTargetStatsRollup();

  CaptureEngineResult capture(FloecatConnector source, CaptureEngineRequest request) {
    FloecatConnector.FileGroupCaptureResult captured =
        source.capturePlannedFileGroup(
            request.sourceNamespace(),
            request.sourceTable(),
            request.tableId(),
            request.snapshotId(),
            Set.copyOf(request.plannedFilePaths()),
            request.statsColumns(),
            request.requestedStatsTargetKinds(),
            request.capturePageIndex());
    return CaptureEngineResult.of(
        completeStats(request, captured.statsRecords()),
        filterPageIndexEntries(captured.pageIndexEntries(), request.indexColumns()),
        List.of());
  }

  private List<TargetStatsRecord> completeStats(
      CaptureEngineRequest request, List<TargetStatsRecord> capturedStats) {
    if (!request.requestsStats() || request.plannedFilePaths().isEmpty()) {
      return List.of();
    }
    return statsRollup.complete(
        request.tableId(),
        request.snapshotId(),
        request.requestedStatsTargetKinds(),
        capturedStats);
  }

  private static List<FloecatConnector.ParquetPageIndexEntry> filterPageIndexEntries(
      List<FloecatConnector.ParquetPageIndexEntry> entries, Set<String> indexColumns) {
    if (entries == null || entries.isEmpty() || indexColumns == null || indexColumns.isEmpty()) {
      return entries == null ? List.of() : entries;
    }
    return entries.stream()
        .filter(entry -> entry != null && indexColumns.contains(entry.columnName()))
        .toList();
  }
}
