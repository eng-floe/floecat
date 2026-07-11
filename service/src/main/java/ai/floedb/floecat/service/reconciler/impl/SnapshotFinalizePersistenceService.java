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

package ai.floedb.floecat.service.reconciler.impl;

import ai.floedb.floecat.catalog.rpc.TableValueStats;
import ai.floedb.floecat.catalog.rpc.TargetStatsRecord;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.connector.spi.FloecatConnector;
import ai.floedb.floecat.reconciler.impl.FileGroupTargetStatsRollup;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.service.catalog.impl.TableRootWriter;
import ai.floedb.floecat.service.statistics.StatsOrchestrator;
import ai.floedb.floecat.stats.identity.StatsTargetIdentity;
import ai.floedb.floecat.stats.identity.TargetStatsRecords;
import ai.floedb.floecat.stats.spi.StatsStore;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class SnapshotFinalizePersistenceService {
  @Inject StatsStore statsStore;
  @Inject StatsOrchestrator statsOrchestrator;
  @Inject TableRootWriter rootWriter;

  public long replaceAllStatsForSnapshot(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> canonical = canonicalize(records);
    statsStore.replaceAllStatsForSnapshot(tableId, snapshotId, canonical);
    statsOrchestrator.invalidateStatsCache(tableId, snapshotId);
    commitGenerationToRoot(tableId, snapshotId);
    return canonical.size();
  }

  public long replaceFileGroupStatsForSnapshot(
      ResourceId tableId,
      long snapshotId,
      List<String> expectedFilePaths,
      List<TargetStatsRecord> aggregateRecords) {
    List<TargetStatsRecord> replacement = new ArrayList<>();
    for (String filePath : expectedFilePaths == null ? List.<String>of() : expectedFilePaths) {
      if (filePath == null || filePath.isBlank()) {
        continue;
      }
      statsStore
          .getTargetStats(tableId, snapshotId, StatsTargetIdentity.fileTarget(filePath))
          .ifPresent(replacement::add);
    }
    if (aggregateRecords != null && !aggregateRecords.isEmpty()) {
      replacement.addAll(aggregateRecords);
    }
    return replaceAllStatsForSnapshot(tableId, snapshotId, replacement);
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    boolean deleted = statsStore.deleteAllStatsForSnapshot(tableId, snapshotId);
    statsOrchestrator.invalidateStatsCache(tableId, snapshotId);
    commitGenerationToRoot(tableId, snapshotId);
    return deleted;
  }

  public long persistStats(List<TargetStatsRecord> records) {
    long processed = 0L;
    List<TargetStatsRecord> canonical = canonicalize(records);
    LinkedHashSet<TableSnapshot> touched = new LinkedHashSet<>();
    for (TargetStatsRecord record : canonical) {
      statsStore.putTargetStats(record);
      statsOrchestrator.invalidateStatsCache(
          record.getTableId(), record.getSnapshotId(), record.getTarget());
      touched.add(new TableSnapshot(record.getTableId(), record.getSnapshotId()));
      processed++;
    }
    // The first put on a snapshot may have created its active generation; the commit no-ops when
    // the root already carries the generation's ref.
    for (TableSnapshot pair : touched) {
      commitGenerationToRoot(pair.tableId(), pair.snapshotId());
    }
    return processed;
  }

  private record TableSnapshot(ResourceId tableId, long snapshotId) {}

  /** Record the snapshot's (possibly new or removed) active stats generation on the table root. */
  private void commitGenerationToRoot(ResourceId tableId, long snapshotId) {
    if (rootWriter != null) {
      rootWriter.commitStatsGeneration(tableId, snapshotId);
    }
  }

  public long persistEmptySnapshotCompletionMarker(
      ResourceId tableId, long snapshotId, boolean fullRescan) {
    TargetStatsRecord zeroMarker =
        TargetStatsRecords.tableRecord(
            tableId,
            snapshotId,
            TableValueStats.newBuilder()
                .setRowCount(0L)
                .setDataFileCount(0L)
                .setTotalSizeBytes(0L)
                .build(),
            null);
    if (fullRescan) {
      statsStore.replaceAllStatsForSnapshot(
          tableId, snapshotId, List.of(TargetStatsRecords.canonicalize(zeroMarker)));
      statsOrchestrator.invalidateStatsCache(tableId, snapshotId);
      commitGenerationToRoot(tableId, snapshotId);
      return 1L;
    }
    if (statsStore
        .getTargetStats(tableId, snapshotId, StatsTargetIdentity.tableTarget())
        .isPresent()) {
      return 0L;
    }
    if (statsStore.putTargetStatsIfAbsent(zeroMarker)) {
      statsOrchestrator.invalidateStatsCache(tableId, snapshotId, zeroMarker.getTarget());
      commitGenerationToRoot(tableId, snapshotId);
      return 1L;
    }
    if (statsStore
        .getTargetStats(tableId, snapshotId, StatsTargetIdentity.tableTarget())
        .isPresent()) {
      return 0L;
    }
    throw new IllegalStateException(
        "snapshot finalization failed to persist empty completion marker for table "
            + tableId.getId()
            + " snapshot "
            + snapshotId);
  }

  public List<TargetStatsRecord> listFileStats(ResourceId tableId, long snapshotId) {
    List<TargetStatsRecord> out = new ArrayList<>();
    String pageToken = "";
    do {
      StatsStore.StatsStorePage page =
          statsStore.listTargetStats(
              tableId, snapshotId, Optional.of(StatsTargetType.FILE), 256, pageToken);
      out.addAll(page.records());
      pageToken = page.nextPageToken();
    } while (pageToken != null && !pageToken.isBlank());
    return List.copyOf(out);
  }

  public List<TargetStatsRecord> listSnapshotStats(ResourceId tableId, long snapshotId) {
    List<TargetStatsRecord> out = new ArrayList<>();
    String pageToken = "";
    do {
      StatsStore.StatsStorePage page =
          statsStore.listTargetStats(tableId, snapshotId, Optional.empty(), 256, pageToken);
      out.addAll(page.records());
      pageToken = page.nextPageToken();
    } while (pageToken != null && !pageToken.isBlank());
    return List.copyOf(out);
  }

  public List<TargetStatsRecord> buildAggregateStats(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds,
      List<TargetStatsRecord> fileStats) {
    return FileGroupTargetStatsRollup.partialAggregatesFromFileRecords(
            tableId, snapshotId, aggregateKinds, fileStats)
        .stream()
        .map(TargetStatsRecords::canonicalize)
        .toList();
  }

  public List<TargetStatsRecord> completeStatsWithAggregates(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds,
      List<TargetStatsRecord> capturedStats) {
    return new FileGroupTargetStatsRollup()
        .complete(tableId, snapshotId, aggregateKinds, capturedStats).stream()
            .map(TargetStatsRecords::canonicalize)
            .toList();
  }

  public List<TargetStatsRecord> mergeAggregatePartials(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds,
      List<TargetStatsRecord> partials) {
    return FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
            tableId, snapshotId, aggregateKinds, partials)
        .stream()
        .map(TargetStatsRecords::canonicalize)
        .toList();
  }

  public List<TargetStatsRecord> mergeCompletedGroupPartials(
      ResourceId tableId,
      long snapshotId,
      Set<FloecatConnector.StatsTargetKind> aggregateKinds,
      List<ReconcileFileGroupTask> completedGroups) {
    if (aggregateKinds == null
        || aggregateKinds.isEmpty()
        || completedGroups == null
        || completedGroups.isEmpty()) {
      return List.of();
    }
    List<TargetStatsRecord> normalizedGroupPartials = new ArrayList<>();
    for (ReconcileFileGroupTask group : completedGroups) {
      if (group == null || group.partialAggregateRecords().isEmpty()) {
        continue;
      }
      normalizedGroupPartials.addAll(
          FileGroupTargetStatsRollup.mergeSnapshotAggregatePartials(
              tableId, snapshotId, aggregateKinds, group.partialAggregateRecords()));
    }
    return mergeAggregatePartials(tableId, snapshotId, aggregateKinds, normalizedGroupPartials);
  }

  public List<TargetStatsRecord> validateAggregateStats(
      List<TargetStatsRecord> aggregateStats, ResourceId tableId, long snapshotId) {
    return validateFinalizeStats(
        aggregateStats,
        tableId,
        snapshotId,
        "aggregate stats",
        "aggregate stats target",
        TargetTypeConstraint.NON_FILE,
        "snapshot finalize submission must not include file-target stats");
  }

  public List<TargetStatsRecord> validateReplacementStats(
      List<TargetStatsRecord> records, ResourceId tableId, long snapshotId) {
    return validateFinalizeStats(
        records,
        tableId,
        snapshotId,
        "replacement stats",
        "replacement stats target",
        TargetTypeConstraint.ANY,
        null);
  }

  public List<TargetStatsRecord> validateIncrementalDeltaFileStats(
      List<TargetStatsRecord> records, ResourceId tableId, long snapshotId) {
    return validateFinalizeStats(
        records,
        tableId,
        snapshotId,
        "incremental delta stats",
        "incremental file stats target",
        TargetTypeConstraint.FILE_ONLY,
        "incremental snapshot finalize submission must include only file-target stats");
  }

  /** Target-type rule applied to each record in a snapshot-finalize submission. */
  private enum TargetTypeConstraint {
    ANY,
    NON_FILE,
    FILE_ONLY
  }

  /**
   * Canonicalizes and validates a snapshot-finalize submission: every record must have a target of
   * the required kind, match the leased table/snapshot, and be unique by storage id. {@code label}
   * and {@code duplicateLabel} shape the error messages; {@code typeErrorMessage} is thrown when
   * the target-type constraint is violated.
   */
  private List<TargetStatsRecord> validateFinalizeStats(
      List<TargetStatsRecord> records,
      ResourceId tableId,
      long snapshotId,
      String label,
      String duplicateLabel,
      TargetTypeConstraint typeConstraint,
      String typeErrorMessage) {
    if (records == null || records.isEmpty()) {
      return List.of();
    }
    List<TargetStatsRecord> validated =
        records.stream()
            .filter(java.util.Objects::nonNull)
            .map(TargetStatsRecords::canonicalize)
            .peek(
                record -> {
                  if (!record.hasTarget()) {
                    throw new IllegalArgumentException(label + " target is required");
                  }
                  boolean isFile = StatsTargetType.from(record.getTarget()) == StatsTargetType.FILE;
                  if ((typeConstraint == TargetTypeConstraint.NON_FILE && isFile)
                      || (typeConstraint == TargetTypeConstraint.FILE_ONLY && !isFile)) {
                    throw new IllegalArgumentException(typeErrorMessage);
                  }
                  if (!tableId.equals(record.getTableId())) {
                    throw new IllegalArgumentException(
                        label + " table_id does not match leased snapshot table");
                  }
                  if (record.getSnapshotId() != snapshotId) {
                    throw new IllegalArgumentException(
                        label + " snapshot_id does not match leased snapshot");
                  }
                })
            .toList();
    LinkedHashSet<String> targetIds = new LinkedHashSet<>();
    for (TargetStatsRecord record : validated) {
      String targetId = StatsTargetIdentity.storageId(record.getTarget());
      if (!targetIds.add(targetId)) {
        throw new IllegalArgumentException(
            "duplicate " + duplicateLabel + " in snapshot finalize submission: " + targetId);
      }
    }
    return validated;
  }

  private static List<TargetStatsRecord> canonicalize(List<TargetStatsRecord> records) {
    if (records == null || records.isEmpty()) {
      return List.of();
    }
    return records.stream()
        .filter(java.util.Objects::nonNull)
        .map(TargetStatsRecords::canonicalize)
        .toList();
  }
}
