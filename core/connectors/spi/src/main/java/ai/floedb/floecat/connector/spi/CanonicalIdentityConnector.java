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

package ai.floedb.floecat.connector.spi;

import ai.floedb.floecat.catalog.rpc.ColumnIdentityMap;
import ai.floedb.floecat.common.rpc.ResourceId;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Opt-in connector capability for authoritative canonical column identity.
 *
 * <p>A connector that implements only {@link FloecatConnector} keeps the existing {@link
 * ai.floedb.floecat.catalog.rpc.ColumnIdAlgorithm} semantics and is never asked to consume a column
 * identity map. Implement this interface only when every identity-bearing operation uses the
 * supplied snapshot map.
 */
public interface CanonicalIdentityConnector extends FloecatConnector {
  @Override
  default FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<String> indexColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes,
      ColumnSelectorPolicy columnSelectorPolicy) {
    return capturePlannedFileGroup(
        namespaceFq,
        tableName,
        destinationTableId,
        snapshotId,
        plannedFilePaths,
        includeColumns,
        indexColumns,
        includeTargetKinds,
        captureIndexes,
        columnSelectorPolicy,
        ColumnIdentityMap.getDefaultInstance());
  }

  default Optional<DirectSnapshotStatsCapture> captureSnapshotTargetStatsDirect(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> includeColumns,
      Set<StatsTargetKind> includeTargetKinds,
      ColumnSelectorPolicy columnSelectorPolicy,
      ColumnIdentityMap columnIdentityMap) {
    return Optional.empty();
  }

  FileGroupCaptureResult capturePlannedFileGroup(
      String namespaceFq,
      String tableName,
      ResourceId destinationTableId,
      long snapshotId,
      Set<String> plannedFilePaths,
      Set<String> includeColumns,
      Set<String> indexColumns,
      Set<StatsTargetKind> includeTargetKinds,
      boolean captureIndexes,
      ColumnSelectorPolicy columnSelectorPolicy,
      ColumnIdentityMap columnIdentityMap);

  default Optional<List<ParquetPageIndexEntry>> selectPageIndexEntries(
      String namespaceFq,
      String tableName,
      long snapshotId,
      Set<String> selectors,
      ColumnSelectorPolicy columnSelectorPolicy,
      List<ParquetPageIndexEntry> entries,
      ColumnIdentityMap columnIdentityMap) {
    return selectPageIndexEntries(
        namespaceFq,
        tableName,
        snapshotId,
        selectors,
        columnSelectorPolicy,
        pageIndexPlannedFilePaths(entries),
        entries,
        pageIndexRowGroups(entries),
        columnIdentityMap);
  }

  default Optional<List<ParquetPageIndexEntry>> selectPageIndexEntries(
      String namespaceFq,
      String tableName,
      long snapshotId,
      Set<String> selectors,
      ColumnSelectorPolicy columnSelectorPolicy,
      Set<String> plannedFilePaths,
      List<ParquetPageIndexEntry> entries,
      List<ParquetRowGroup> rowGroups,
      ColumnIdentityMap columnIdentityMap) {
    return Optional.empty();
  }

  private static Set<String> pageIndexPlannedFilePaths(List<ParquetPageIndexEntry> entries) {
    return entries == null
        ? Set.of()
        : entries.stream()
            .filter(java.util.Objects::nonNull)
            .map(ParquetPageIndexEntry::filePath)
            .filter(path -> path != null && !path.isBlank())
            .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
  }

  private static List<ParquetRowGroup> pageIndexRowGroups(List<ParquetPageIndexEntry> entries) {
    Map<String, Map<Integer, Integer>> rowGroupsByFile = new LinkedHashMap<>();
    if (entries != null) {
      for (ParquetPageIndexEntry entry : entries) {
        if (entry == null || entry.filePath().isBlank()) {
          continue;
        }
        long rowGroupEnd = entry.firstRowIndex() + entry.rowCount();
        int rowCount =
            rowGroupEnd >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) Math.max(0L, rowGroupEnd);
        rowGroupsByFile
            .computeIfAbsent(entry.filePath(), ignored -> new LinkedHashMap<>())
            .merge(entry.rowGroup(), rowCount, Math::max);
      }
    }
    List<ParquetRowGroup> rowGroups = new java.util.ArrayList<>();
    rowGroupsByFile.forEach(
        (filePath, groups) ->
            groups.forEach(
                (rowGroup, rowCount) ->
                    rowGroups.add(new ParquetRowGroup(filePath, rowGroup, rowCount))));
    return List.copyOf(rowGroups);
  }
}
