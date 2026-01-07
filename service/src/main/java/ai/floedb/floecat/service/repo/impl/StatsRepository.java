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

package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.ColumnStats;
import ai.floedb.floecat.catalog.rpc.FileColumnStats;
import ai.floedb.floecat.catalog.rpc.TableStats;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.ColumnStatsKey;
import ai.floedb.floecat.service.repo.model.FileColumnStatsKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.TableStatsKey;
import ai.floedb.floecat.service.repo.util.ColumnStatsNormalizer;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class StatsRepository {

  private final GenericResourceRepository<TableStats, TableStatsKey> tableStatsRepo;
  private final GenericResourceRepository<ColumnStats, ColumnStatsKey> columnStatsRepo;
  private final GenericResourceRepository<FileColumnStats, FileColumnStatsKey> fileStatsRepo;

  @Inject
  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.tableStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE_STATS,
            TableStats::parseFrom,
            TableStats::toByteArray,
            "application/x-protobuf");

    this.columnStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.COLUMN_STATS,
            ColumnStats::parseFrom,
            ColumnStats::toByteArray,
            "application/x-protobuf");

    this.fileStatsRepo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.FILE_COLUMN_STATS,
            FileColumnStats::parseFrom,
            FileColumnStats::toByteArray,
            "application/x-protobuf");
  }

  public void putTableStats(ResourceId tableId, long snapshotId, TableStats value) {
    tableStatsRepo.create(value);
  }

  public Optional<TableStats> getTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.getByKey(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public boolean deleteTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.delete(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public void putColumnStats(ResourceId tableId, long snapshotId, ColumnStats value) {
    columnStatsRepo.create(value);
  }

  public Optional<ColumnStats> getColumnStats(ResourceId tableId, long snapshotId, int columnId) {
    return columnStatsRepo.getByKey(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  public int putColumnStats(ResourceId tableId, long snapshotId, List<ColumnStats> batch) {
    int created = 0;
    for (ColumnStats cs : batch) {
      ColumnStatsKey key =
          new ColumnStatsKey(
              cs.getTableId().getAccountId(),
              cs.getTableId().getId(),
              cs.getSnapshotId(),
              cs.getColumnId(),
              "");
      if (columnStatsRepo.getByKey(key).isEmpty()) {
        created++;
      }
      columnStatsRepo.create(cs);
    }
    return created;
  }

  public List<ColumnStats> list(
      ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return columnStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  public int count(ResourceId tableId, long snapshotId) {
    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return columnStatsRepo.countByPrefix(prefix);
  }

  public void putFileColumnStats(ResourceId tableId, long snapshotId, FileColumnStats value) {
    fileStatsRepo.create(value);
  }

  public Optional<FileColumnStats> getFileColumnStats(
      ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.getByKey(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }

  public int putFileColumnStats(ResourceId tableId, long snapshotId, List<FileColumnStats> batch) {
    int created = 0;
    for (FileColumnStats fs : batch) {
      FileColumnStatsKey key =
          new FileColumnStatsKey(
              fs.getTableId().getAccountId(),
              fs.getTableId().getId(),
              fs.getSnapshotId(),
              fs.getFilePath(),
              "");
      if (fileStatsRepo.getByKey(key).isEmpty()) {
        created++;
      }
      fileStatsRepo.create(fs);
    }
    return created;
  }

  public List<FileColumnStats> listFileStats(
      ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    String prefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return fileStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  public int countFileStats(ResourceId tableId, long snapshotId) {
    String prefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    return fileStatsRepo.countByPrefix(prefix);
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    deleteTableStats(tableId, snapshotId);

    String colPrefix =
        Keys.snapshotColumnStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      List<ColumnStats> page = columnStatsRepo.listByPrefix(colPrefix, 200, token, next);
      for (ColumnStats cs : page) {
        String sha = ColumnStatsNormalizer.sha256Hex(cs.toByteArray());
        ColumnStatsKey key =
            new ColumnStatsKey(
                cs.getTableId().getAccountId(),
                cs.getTableId().getId(),
                cs.getSnapshotId(),
                cs.getColumnId(),
                sha);
        columnStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    String filePrefix =
        Keys.snapshotFileStatsPrefix(tableId.getAccountId(), tableId.getId(), snapshotId);
    token = "";
    next.setLength(0);

    do {
      List<FileColumnStats> page = fileStatsRepo.listByPrefix(filePrefix, 200, token, next);
      for (FileColumnStats fs : page) {
        String sha = ColumnStatsNormalizer.sha256Hex(fs.toByteArray());
        FileColumnStatsKey key =
            new FileColumnStatsKey(
                fs.getTableId().getAccountId(),
                fs.getTableId().getId(),
                fs.getSnapshotId(),
                fs.getFilePath(),
                sha);
        fileStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""), nowTs);
  }

  public MutationMeta metaForTableStatsSafe(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaForSafe(
        new TableStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, ""));
  }

  public MutationMeta metaForColumnStats(ResourceId tableId, long snapshotId, int columnId) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  public MutationMeta metaForColumnStats(
      ResourceId tableId, long snapshotId, int columnId, Timestamp nowTs) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""),
        nowTs);
  }

  public MutationMeta metaForColumnStatsSafe(ResourceId tableId, long snapshotId, int columnId) {
    return columnStatsRepo.metaForSafe(
        new ColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, columnId, ""));
  }

  public MutationMeta metaForFileColumnStats(ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.metaFor(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }

  public MutationMeta metaForFileColumnStats(
      ResourceId tableId, long snapshotId, String filePath, Timestamp nowTs) {
    return fileStatsRepo.metaFor(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""),
        nowTs);
  }

  public MutationMeta metaForFileColumnStatsSafe(
      ResourceId tableId, long snapshotId, String filePath) {
    return fileStatsRepo.metaForSafe(
        new FileColumnStatsKey(tableId.getAccountId(), tableId.getId(), snapshotId, filePath, ""));
  }
}
