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

  protected StatsRepository() { super(); }

  @Inject
  public StatsRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, TableStats::parseFrom, TableStats::toByteArray, "application/x-protobuf");
  }

  public Optional<TableStats> getTableStats(ResourceId tableId, long snapshotId) {
    final String key = Keys.snapTableStatsPtr(tableId.getTenantId(), tableId.getId(), snapshotId);
    return get(key);
  }

  public void putTableStats(ResourceId tableId, long snapshotId, TableStats stats) {
    final String tid = tableId.getTenantId();
    final String tbl = tableId.getId();
    final String key = Keys.snapTableStatsPtr(tid, tbl, snapshotId);
    final String blob = Keys.snapTableStatsBlob(tid, tbl, snapshotId);

    reserveIndexOrIdempotent(key, blob);
    putBlob(blob, stats);
  }

  public boolean deleteTableStats(ResourceId tableId, long snapshotId) {
    final String tid = tableId.getTenantId();
    final String tbl = tableId.getId();
    final String key = Keys.snapTableStatsPtr(tid, tbl, snapshotId);
    final String blob = Keys.snapTableStatsBlob(tid, tbl, snapshotId);

    ptr.get(key).ifPresent(p -> compareAndDeleteOrFalse(key, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public Optional<ColumnStats> getColumnStats(ResourceId tableId, long snapshotId, String columnId) {
    final String key = Keys.snapColStatsPtr(tableId.getTenantId(), tableId.getId(), snapshotId, columnId);
    var p = ptr.get(key);
    if (p.isEmpty()) return Optional.empty();
    try {
      return Optional.of(ColumnStats.parseFrom(blobs.get(p.get().getBlobUri())));
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + p.get().getBlobUri(), e);
    }
  }

  public void putColumnStats(ResourceId tableId, long snapshotId, ColumnStats cs) {
    final String tid = tableId.getTenantId();
    final String tbl = tableId.getId();
    final String key = Keys.snapColStatsPtr(tid, tbl, snapshotId, cs.getColumnId());
    final String blob = Keys.snapColStatsBlob(tid, tbl, snapshotId, cs.getColumnId());
    reserveIndexOrIdempotent(key, blob);
    putBlobStrictBytes(blob, cs.toByteArray());
  }

  public void putColumnStatsBatch(ResourceId tableId, long snapshotId, List<ColumnStats> cols) {
    for (var cs : cols) {
      putColumnStats(tableId, snapshotId, cs);
    }
  }

  public List<ColumnStats> listColumnStats(ResourceId tableId, long snapshotId, int limit, String token, StringBuilder nextOut) {
    final String pfx = Keys.snapColStatsPrefix(tableId.getTenantId(), tableId.getId(), snapshotId);
    var rows = ptr.listPointersByPrefix(pfx, Math.max(1, limit), token, nextOut);

    var uris = new ArrayList<String>(rows.size());
    for (var r : rows) uris.add(r.blobUri());

    var blobsMap = blobs.getBatch(uris);
    var out = new ArrayList<ColumnStats>(rows.size());
    for (var r : rows) {
      byte[] bytes = blobsMap.get(r.blobUri());
      if (bytes == null) {
        continue;
      }
      try { out.add(ColumnStats.parseFrom(bytes)); }
      catch (Exception e) {
        throw new CorruptionException("parse failed: " + r.blobUri(), e);
      }
    }
    return out;
  }

  public boolean deleteAllStatsForSnapshot(ResourceId tableId, long snapshotId) {
    final String tid = tableId.getTenantId();
    final String tbl = tableId.getId();

    deleteTableStats(tableId, snapshotId);

    final String pfx = Keys.snapColStatsPrefix(tid, tbl, snapshotId);
    String token = ""; var next = new StringBuilder();
    do {
      var rows = ptr.listPointersByPrefix(pfx, 200, token, next);
      for (var r : rows) {
        ptr.get(r.key()).ifPresent(p -> compareAndDeleteOrFalse(r.key(), p.getVersion()));
        deleteQuietly(() -> blobs.delete(r.blobUri()));
      }
      token = next.toString(); next.setLength(0);
    } while (!token.isEmpty());

    return true;
  }

  public MutationMeta metaForTableStats(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    final String t = tableId.getTenantId();
    final String tbl = tableId.getId();
    final String key = Keys.snapTableStatsPtr(t, tbl, snapshotId);
    final var p = ptr.get(key).orElseThrow(() ->
        new IllegalStateException("Pointer missing for table-stats: " + tbl + "@" + snapshotId));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForColumnStats(ResourceId tableId, long snapshotId, String columnId, Timestamp nowTs) {
    final String t = tableId.getTenantId();
    final String tbl = tableId.getId();
    final String key = Keys.snapColStatsPtr(t, tbl, snapshotId, columnId);
    final var p = ptr.get(key).orElseThrow(() ->
        new IllegalStateException(
            "Pointer missing for column-stats: " + tbl + "/" + columnId + "@" + snapshotId));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }
}
