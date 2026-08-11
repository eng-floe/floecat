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
import ai.floedb.floecat.reconciler.spi.capture.CaptureFileResultConsumer;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;

/**
 * Adapts the current Java connector SPI to the unified file-group capture contract.
 *
 * <p>This class is intentionally Java-specific glue. It normalizes Java connector file-group
 * capture to the same contract used by the Rust executor: file-scoped stats plus optional
 * page-index outputs. A future Rust engine should replace this adapter by returning the same
 * contract directly from its own runtime.
 */
final class JavaConnectorFileGroupCaptureAdapter {
  CaptureEngineResult capture(
      FloecatConnector source,
      CaptureEngineRequest request,
      CaptureFileResultConsumer fileResultConsumer) {
    CaptureFileResultConsumer output =
        java.util.Objects.requireNonNull(fileResultConsumer, "fileResultConsumer");
    Set<String> publishedFileTargets = new HashSet<>();
    Set<String> realizedStatsSelectors = new java.util.TreeSet<>();
    List<TargetStatsRecord> partialAggregates = List.of();
    throwIfCancellationRequested(request);
    FloecatConnector.FileGroupCaptureResult captured =
        source.capturePlannedFileGroup(
            request.sourceNamespace(),
            request.sourceTable(),
            request.tableId(),
            request.snapshotId(),
            new java.util.LinkedHashSet<>(request.plannedFilePaths()),
            request.statsColumns(),
            request.requestedStatsTargetKinds(),
            request.capturePageIndex(),
            request.columnSelectorPolicy());
    List<TargetStatsRecord> capturedFileStats =
        uniqueFileStats(captured.statsRecords(), publishedFileTargets);
    realizedStatsSelectors.addAll(captured.realizedStatsSelectors());
    List<FloecatConnector.ParquetPageIndexEntry> capturedPageIndexEntries =
        request.capturePageIndex()
            ? source
                .selectPageIndexEntries(
                    request.sourceNamespace(),
                    request.sourceTable(),
                    request.snapshotId(),
                    request.indexColumns(),
                    request.columnSelectorPolicy(),
                    new java.util.LinkedHashSet<>(request.plannedFilePaths()),
                    captured.pageIndexEntries(),
                    captured.pageIndexRowGroups())
                .orElseGet(
                    () ->
                        filterPageIndexEntries(
                            captured.pageIndexEntries(),
                            request.indexColumns(),
                            request.columnSelectorPolicy()))
            : List.of();
    Map<String, List<TargetStatsRecord>> statsByFile = new LinkedHashMap<>();
    Map<String, List<FloecatConnector.ParquetPageIndexEntry>> indexesByFile = new LinkedHashMap<>();
    Map<String, String> fileByStatsTarget = new java.util.HashMap<>();
    for (String filePath : request.plannedFilePaths()) {
      statsByFile.put(filePath, new java.util.ArrayList<>());
      indexesByFile.put(filePath, new java.util.ArrayList<>());
      fileByStatsTarget.put(
          StatsTargetIdentity.storageId(StatsTargetIdentity.fileTarget(filePath)), filePath);
    }
    List<TargetStatsRecord> auxiliaryFileStats = new java.util.ArrayList<>();
    for (TargetStatsRecord fileStat : capturedFileStats) {
      String filePath = fileByStatsTarget.get(StatsTargetIdentity.storageId(fileStat.getTarget()));
      if (filePath != null) {
        statsByFile.get(filePath).add(fileStat);
      } else {
        // Connectors may return file targets attached to a planned data file without listing those
        // auxiliary files as independent plan entries. Iceberg position/equality delete files are
        // the canonical example. Keep them in this file-group output; finalization validates their
        // target identities against the immutable execution plan.
        auxiliaryFileStats.add(fileStat);
      }
    }
    if (!auxiliaryFileStats.isEmpty()) {
      statsByFile.get(request.plannedFilePaths().getFirst()).addAll(auxiliaryFileStats);
    }
    for (FloecatConnector.ParquetPageIndexEntry entry : capturedPageIndexEntries) {
      if (entry != null && indexesByFile.containsKey(entry.filePath())) {
        indexesByFile.get(entry.filePath()).add(entry);
      }
    }
    for (String filePath : request.plannedFilePaths()) {
      throwIfCancellationRequested(request);
      List<TargetStatsRecord> fileStats = List.copyOf(statsByFile.get(filePath));
      List<FloecatConnector.ParquetPageIndexEntry> pageIndexEntries =
          List.copyOf(indexesByFile.get(filePath));
      if (!fileStats.isEmpty()) {
        partialAggregates = mergePartialAggregates(request, partialAggregates, fileStats);
      }
      if (!fileStats.isEmpty() || !pageIndexEntries.isEmpty()) {
        output.accept(fileStats, pageIndexEntries);
      }
      throwIfCancellationRequested(request);
    }
    return CaptureEngineResult.of(
        partialAggregates, List.of(), List.of(), List.copyOf(realizedStatsSelectors));
  }

  private static void throwIfCancellationRequested(CaptureEngineRequest request) {
    if (request.shouldStop().getAsBoolean()) {
      throw new CancellationException("file-group execution cancelled");
    }
  }

  private static List<TargetStatsRecord> uniqueFileStats(
      List<TargetStatsRecord> capturedStats, Set<String> publishedFileTargets) {
    if (capturedStats == null || capturedStats.isEmpty()) {
      return List.of();
    }
    LinkedHashMap<String, TargetStatsRecord> uniqueFileStats = new LinkedHashMap<>();
    for (TargetStatsRecord fileStat : capturedStats) {
      if (fileStat == null || !fileStat.hasFile()) {
        continue;
      }
      String storageId = StatsTargetIdentity.storageId(fileStat.getTarget());
      if (publishedFileTargets.add(storageId)) {
        uniqueFileStats.put(storageId, fileStat);
      }
    }
    return List.copyOf(uniqueFileStats.values());
  }

  private static List<TargetStatsRecord> mergePartialAggregates(
      CaptureEngineRequest request,
      List<TargetStatsRecord> partialAggregates,
      List<TargetStatsRecord> fileStats) {
    if (!request.requestsStats()) {
      return List.of();
    }
    List<TargetStatsRecord> next =
        FileGroupTargetStatsRollup.partialAggregatesFromFileRecords(
            request.tableId(),
            request.snapshotId(),
            request.requestedStatsTargetKinds(),
            fileStats);
    if (partialAggregates.isEmpty()) {
      return next;
    }
    List<TargetStatsRecord> combined =
        new java.util.ArrayList<>(partialAggregates.size() + next.size());
    combined.addAll(partialAggregates);
    combined.addAll(next);
    return FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
        request.tableId(), request.snapshotId(), request.requestedStatsTargetKinds(), combined);
  }

  private static List<FloecatConnector.ParquetPageIndexEntry> filterPageIndexEntries(
      List<FloecatConnector.ParquetPageIndexEntry> entries,
      Set<String> indexColumns,
      FloecatConnector.ColumnSelectorPolicy columnSelectorPolicy) {
    if (entries == null || entries.isEmpty()) {
      return List.of();
    }
    List<String> availableColumns =
        entries.stream()
            .filter(java.util.Objects::nonNull)
            .map(FloecatConnector.ParquetPageIndexEntry::columnName)
            .filter(name -> name != null && !name.isBlank())
            .map(String::trim)
            .distinct()
            .toList();
    Set<String> selectedColumns =
        FloecatConnector.resolveIncludedColumns(
            availableColumns, indexColumns, columnSelectorPolicy);
    if (selectedColumns.isEmpty()) {
      return List.of();
    }
    return entries.stream()
        .filter(
            entry ->
                entry != null
                    && entry.columnName() != null
                    && selectedColumns.contains(entry.columnName().trim()))
        .toList();
  }
}
