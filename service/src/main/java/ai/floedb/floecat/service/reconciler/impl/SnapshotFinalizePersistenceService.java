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

  public long replaceAllStatsForSnapshot(
      ResourceId tableId, long snapshotId, List<TargetStatsRecord> records) {
    List<TargetStatsRecord> canonical = canonicalize(records);
    statsStore.replaceAllStatsForSnapshot(tableId, snapshotId, canonical);
    return canonical.size();
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    return statsStore.deleteAllStatsForSnapshot(tableId, snapshotId);
  }

  public long persistStats(List<TargetStatsRecord> records) {
    long processed = 0L;
    for (TargetStatsRecord record : canonicalize(records)) {
      statsStore.putTargetStats(record);
      processed++;
    }
    return processed;
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
      return 1L;
    }
    if (statsStore
        .getTargetStats(tableId, snapshotId, StatsTargetIdentity.tableTarget())
        .isPresent()) {
      return 0L;
    }
    if (statsStore.putTargetStatsIfAbsent(zeroMarker)) {
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

  public List<TargetStatsRecord> validateAggregateStats(
      List<TargetStatsRecord> aggregateStats, ResourceId tableId, long snapshotId) {
    if (aggregateStats == null || aggregateStats.isEmpty()) {
      return List.of();
    }
    List<TargetStatsRecord> validated =
        aggregateStats.stream()
            .filter(java.util.Objects::nonNull)
            .map(TargetStatsRecords::canonicalize)
            .peek(
                record -> {
                  if (!record.hasTarget()) {
                    throw new IllegalArgumentException("aggregate stats target is required");
                  }
                  if (StatsTargetType.from(record.getTarget()) == StatsTargetType.FILE) {
                    throw new IllegalArgumentException(
                        "snapshot finalize submission must not include file-target stats");
                  }
                  if (!tableId.equals(record.getTableId())) {
                    throw new IllegalArgumentException(
                        "aggregate stats table_id does not match leased snapshot table");
                  }
                  if (record.getSnapshotId() != snapshotId) {
                    throw new IllegalArgumentException(
                        "aggregate stats snapshot_id does not match leased snapshot");
                  }
                })
            .toList();
    LinkedHashSet<String> targetIds = new LinkedHashSet<>();
    for (TargetStatsRecord record : validated) {
      String targetId = StatsTargetIdentity.storageId(record.getTarget());
      if (!targetIds.add(targetId)) {
        throw new IllegalArgumentException(
            "duplicate aggregate stats target in snapshot finalize submission: " + targetId);
      }
    }
    return validated;
  }

  public List<TargetStatsRecord> validateReplacementStats(
      List<TargetStatsRecord> records, ResourceId tableId, long snapshotId) {
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
                    throw new IllegalArgumentException("replacement stats target is required");
                  }
                  if (!tableId.equals(record.getTableId())) {
                    throw new IllegalArgumentException(
                        "replacement stats table_id does not match leased snapshot table");
                  }
                  if (record.getSnapshotId() != snapshotId) {
                    throw new IllegalArgumentException(
                        "replacement stats snapshot_id does not match leased snapshot");
                  }
                })
            .toList();
    LinkedHashSet<String> targetIds = new LinkedHashSet<>();
    for (TargetStatsRecord record : validated) {
      String targetId = StatsTargetIdentity.storageId(record.getTarget());
      if (!targetIds.add(targetId)) {
        throw new IllegalArgumentException(
            "duplicate replacement stats target in snapshot finalize submission: " + targetId);
      }
    }
    return validated;
  }

  public List<TargetStatsRecord> validateIncrementalDeltaFileStats(
      List<TargetStatsRecord> records, ResourceId tableId, long snapshotId) {
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
                    throw new IllegalArgumentException(
                        "incremental delta stats target is required");
                  }
                  if (StatsTargetType.from(record.getTarget()) != StatsTargetType.FILE) {
                    throw new IllegalArgumentException(
                        "incremental snapshot finalize submission must include only file-target stats");
                  }
                  if (!tableId.equals(record.getTableId())) {
                    throw new IllegalArgumentException(
                        "incremental delta stats table_id does not match leased snapshot table");
                  }
                  if (record.getSnapshotId() != snapshotId) {
                    throw new IllegalArgumentException(
                        "incremental delta stats snapshot_id does not match leased snapshot");
                  }
                })
            .toList();
    LinkedHashSet<String> targetIds = new LinkedHashSet<>();
    for (TargetStatsRecord record : validated) {
      String targetId = StatsTargetIdentity.storageId(record.getTarget());
      if (!targetIds.add(targetId)) {
        throw new IllegalArgumentException(
            "duplicate incremental file stats target in snapshot finalize submission: " + targetId);
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
