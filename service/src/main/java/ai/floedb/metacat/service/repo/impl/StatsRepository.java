package ai.floedb.metacat.service.repo.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import com.google.protobuf.Timestamp;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.TableStats;
import ai.floedb.metacat.catalog.rpc.ColumnStats;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class StatsRepository extends BaseRepository<TableStats> {
  public StatsRepository() {
    super();
  }

  @Inject
  public StatsRepository(PointerStore pointerStore, BlobStore blobs) {
    super(pointerStore, blobs, TableStats::parseFrom,
        TableStats::toByteArray, "application/x-protobuf");
  }

  public Optional<TableStats> getTableStats(ResourceId tableId, long snapshotId) {
    final String key = Keys.snapTableStatsPtr(tableId.getTenantId(), tableId.getId(), snapshotId);
    return get(key);
  }

  public void putTableStats(ResourceId tableId, long snapshotId, TableStats stats) {
    final String tenantId = tableId.getTenantId();
    final String key = Keys.snapTableStatsPtr(tenantId, tableId.getId(), snapshotId);
    final String blobUri = Keys.snapTableStatsBlob(tenantId, tableId.getId(), snapshotId);

    putBlob(blobUri, stats);
    reserveAllOrRollback(key, blobUri);
  }

  public boolean deleteTableStats(ResourceId tableId, long snapshotId) {
    final String tenantId = tableId.getTenantId();
    final String key = Keys.snapTableStatsPtr(tenantId, tableId.getId(), snapshotId);
    final String blobUri = Keys.snapTableStatsBlob(tenantId, tableId.getId(), snapshotId);

    pointerStore.get(key).ifPresent(pointer -> compareAndDeleteOrFalse(key, pointer.getVersion()));
    deleteQuietly(() -> blobStore.delete(blobUri));
    return true;
  }

  public Optional<ColumnStats> getColumnStats(ResourceId tableId, long snapshotId, String columnId) {
    final String key = Keys.snapColStatsPtr(
        tableId.getTenantId(), tableId.getId(), snapshotId, columnId);

    var pointer = pointerStore.get(key);
    if (pointer.isEmpty()) {
      return Optional.empty();
    }

    try {
      return Optional.of(ColumnStats.parseFrom(blobStore.get(pointer.get().getBlobUri())));
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + pointer.get().getBlobUri(), e);
    }
  }

  public void putColumnStats(ResourceId tableId, long snapshotId, ColumnStats cs) {
    final String tenantId = tableId.getTenantId();
    final String key = Keys.snapColStatsPtr(
        tenantId, tableId.getId(), snapshotId, cs.getColumnId());
    final String blobUri = Keys.snapColStatsBlob(
        tenantId, tableId.getId(), snapshotId, cs.getColumnId());

    putBlobStrictBytes(blobUri, cs.toByteArray());
    reserveAllOrRollback(key, blobUri);
  }

  public int putColumnStatsBatch(ResourceId tableId,
      long snapshotId, List<ColumnStats> columnStatsBatch) {
    int created = 0;
    for (var columnStats : columnStatsBatch) {
      final var key = Keys.snapColStatsPtr(
          tableId.getTenantId(), tableId.getId(), snapshotId, columnStats.getColumnId());
      if (pointerStore.get(key).isEmpty()) {
        created++;
      }
      putColumnStats(tableId, snapshotId, columnStats);
    }
    return created;
  }

  public List<ColumnStats> listColumnStats(ResourceId tableId,
      long snapshotId, int limit, String token, StringBuilder nextOut) {
    final String columnStatsPrefix = Keys.snapColStatsPrefix(
        tableId.getTenantId(), tableId.getId(), snapshotId);
    var rows = pointerStore.listPointersByPrefix(
        columnStatsPrefix, Math.max(1, limit), token, nextOut);

    var uris = new ArrayList<String>(rows.size());
    for (var row : rows) {
      uris.add(row.blobUri());
    }

    var blobsMap = blobStore.getBatch(uris);
    var columnStats = new ArrayList<ColumnStats>(rows.size());
    for (var row : rows) {
      byte[] bytes = blobsMap.get(row.blobUri());
      if (bytes == null) {
        continue;
      }
      try {
        columnStats.add(ColumnStats.parseFrom(bytes));
      }
      catch (Exception e) {
        throw new CorruptionException("parse failed: " + row.blobUri(), e);
      }
    }
    return columnStats;
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    final String tenantId = tableId.getTenantId();

    deleteTableStats(tableId, snapshotId);

    final String pfx = Keys.snapColStatsPrefix(tenantId, tableId.getId(), snapshotId);
    String token = "";
    var next = new StringBuilder();
    do {
      var rows = pointerStore.listPointersByPrefix(pfx, 200, token, next);
      for (var row : rows) {
        pointerStore.get(row.key()).ifPresent(
            pointer -> compareAndDeleteOrFalse(row.key(), pointer.getVersion()));

        deleteQuietly(() -> blobStore.delete(row.blobUri()));
      }
      token = next.toString(); next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    final String tenantId = tableId.getTenantId();
    final String key = Keys.snapTableStatsPtr(tenantId, tableId.getId(), snapshotId);
    final var pointer = pointerStore.get(key).orElseThrow(() ->
        new IllegalStateException("Pointer missing for table-stats: "
            + tableId.getId() + "@" + snapshotId));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForColumnStats(
      ResourceId tableId,
      long snapshotId,
      String columnId,
      Timestamp nowTs) {
    final String tenantId = tableId.getTenantId();
    final String key = Keys.snapColStatsPtr(tenantId, tableId.getId(), snapshotId, columnId);
    final var pointer = pointerStore.get(key).orElseThrow(() ->
        new IllegalStateException(
            "Pointer missing for column-stats: "
                + tableId.getId() + "/" + columnId + "@" + snapshotId));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }
}
