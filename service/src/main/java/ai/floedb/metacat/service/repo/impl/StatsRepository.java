package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.ColumnStatsKey;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.model.TableStatsKey;
import ai.floedb.metacat.service.repo.util.GenericRepository;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class StatsRepository {

  private final GenericRepository<TableStats, TableStatsKey> tableStatsRepo;
  private final GenericRepository<ColumnStats, ColumnStatsKey> columnStatsRepo;

  @Inject
  public StatsRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.tableStatsRepo =
        new GenericRepository<>(
            pointerStore,
            blobStore,
            Schemas.TABLE_STATS,
            TableStats::parseFrom,
            TableStats::toByteArray,
            "application/x-protobuf");

    this.columnStatsRepo =
        new GenericRepository<>(
            pointerStore,
            blobStore,
            Schemas.COLUMN_STATS,
            ColumnStats::parseFrom,
            ColumnStats::toByteArray,
            "application/x-protobuf");
  }

  public void putTableStats(ResourceId tableId, long snapshotId, TableStats value) {
    tableStatsRepo.create(value);
  }

  public Optional<TableStats> getTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.getByKey(
        new TableStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public boolean deleteTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.delete(
        new TableStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public void putColumnStats(ResourceId tableId, long snapshotId, ColumnStats value) {
    columnStatsRepo.create(value);
  }

  public Optional<ColumnStats> getColumnStats(
      ResourceId tableId, long snapshotId, String columnId) {
    return columnStatsRepo.getByKey(
        new ColumnStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId, columnId));
  }

  public int putColumnStatsBatch(ResourceId tableId, long snapshotId, List<ColumnStats> batch) {
    int created = 0;
    for (ColumnStats cs : batch) {
      ColumnStatsKey key =
          new ColumnStatsKey(
              cs.getTableId().getTenantId(),
              cs.getTableId().getId(),
              cs.getSnapshotId(),
              cs.getColumnId());
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
        Keys.snapshotColumnStatsPrefix(tableId.getTenantId(), tableId.getId(), snapshotId);
    return columnStatsRepo.listByPrefix(prefix, limit, token, nextOut);
  }

  public int count(ResourceId tableId, long snapshotId) {
    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getTenantId(), tableId.getId(), snapshotId);
    return columnStatsRepo.countByPrefix(prefix);
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    deleteTableStats(tableId, snapshotId);

    String prefix =
        Keys.snapshotColumnStatsPrefix(tableId.getTenantId(), tableId.getId(), snapshotId);
    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      List<ColumnStats> page = columnStatsRepo.listByPrefix(prefix, 200, token, next);
      for (ColumnStats cs : page) {
        ColumnStatsKey key =
            new ColumnStatsKey(
                cs.getTableId().getTenantId(),
                cs.getTableId().getId(),
                cs.getSnapshotId(),
                cs.getColumnId());
        columnStatsRepo.delete(key);
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return tableStatsRepo.metaFor(
        new TableStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId), nowTs);
  }

  public MutationMeta metaForTableStatsSafe(ResourceId tableId, long snapshotId) {
    return tableStatsRepo.metaForSafe(
        new TableStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaForColumnStats(ResourceId tableId, long snapshotId, String columnId) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId, columnId));
  }

  public MutationMeta metaForColumnStats(
      ResourceId tableId, long snapshotId, String columnId, Timestamp nowTs) {
    return columnStatsRepo.metaFor(
        new ColumnStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId, columnId), nowTs);
  }

  public MutationMeta metaForColumnStatsSafe(ResourceId tableId, long snapshotId, String columnId) {
    return columnStatsRepo.metaForSafe(
        new ColumnStatsKey(tableId.getTenantId(), tableId.getId(), snapshotId, columnId));
  }
}
