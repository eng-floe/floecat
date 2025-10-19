package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class SnapshotRepository extends BaseRepository<Snapshot> {

  protected SnapshotRepository() { super(); }

  @Inject
  public SnapshotRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Snapshot::parseFrom, Snapshot::toByteArray, "application/x-protobuf");
  }

  public Optional<Snapshot> get(ResourceId tableId, long snapshotId) {
    String key = Keys.snapPtrById(tableId.getTenantId(), tableId.getId(), snapshotId);
    return get(key);
  }

  public List<Snapshot> list(ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapPtrByIdPrefix(tableId.getTenantId(), tableId.getId());
    return listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapPtrByIdPrefix(tableId.getTenantId(), tableId.getId());
    return countByPrefix(prefix);
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    final String pfx = Keys.snapPtrByTimePrefix(tableId.getTenantId(), tableId.getId());
    final String token = "";
    final StringBuilder next = new StringBuilder();

    var rows = ptr.listPointersByPrefix(pfx, 1, token, next);
    if (rows.isEmpty()) {
      return Optional.empty();
    }

    var latest = rows.get(0);
    try {
      return Optional.of(Snapshot.parseFrom(blobs.get(latest.blobUri())));
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + latest.blobUri(), e);
    }
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    final String tenantId = tableId.getTenantId();
    final String tableUUID = tableId.getId();
    final String pfx = Keys.snapPtrByTimePrefix(tenantId, tableUUID);

    final long asOfMs = Timestamps.toMillis(asOf);

    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      var rows = ptr.listPointersByPrefix(pfx, 200, token, next);
      for (var r : rows) {
        final byte[] bytes;
        try {
          bytes = blobs.get(r.blobUri());
        } catch (Exception e) {
          throw new CorruptionException("parse failed: " + r.blobUri(), e);
        }
        final Snapshot snap;
        try {
          snap = Snapshot.parseFrom(bytes);
        } catch (Exception e) {
          throw new CorruptionException("parse failed: " + r.blobUri(), e);
        }

        long createdMs = Timestamps.toMillis(snap.getUpstreamCreatedAt());
        if (createdMs <= asOfMs) {
          return Optional.of(snap);
        }
      }

      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return Optional.empty();
  }

  public void create(Snapshot snapshot) {
    var tableId = snapshot.getTableId();
    var tenantId = tableId.getTenantId();
    var tableUUID = tableId.getId();
    long snapId = snapshot.getSnapshotId();
    long created = com.google.protobuf.util.Timestamps.toMillis(snapshot.getUpstreamCreatedAt());

    String byId = Keys.snapPtrById(tenantId, tableUUID, snapId);
    String byTime = Keys.snapPtrByTime(tenantId, tableUUID, snapId, created);
    String blob = Keys.snapBlob(tenantId, tableUUID, snapId);

    reserveIndexOrIdempotent(byId,   blob);
    reserveIndexOrIdempotent(byTime, blob);
    putBlob(blob, snapshot);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    var tid = tableId.getTenantId();
    var tbl = tableId.getId();

    String byId = Keys.snapPtrById(tid, tbl, snapshotId);
    long createdMs = get(byId)
        .map(s -> com.google.protobuf.util.Timestamps.toMillis(s.getUpstreamCreatedAt()))
        .orElse(0L);
    String byTime = Keys.snapPtrByTime(tid, tbl, snapshotId, createdMs);
    String blob = Keys.snapBlob(tid, tbl, snapshotId);

    ptr.get(byTime).ifPresent(p -> compareAndDeleteOrFalse(byTime, p.getVersion()));
    ptr.get(byId).ifPresent(p -> compareAndDeleteOrFalse(byId, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long snapshotId, long expectedVersion) {
    var tid = tableId.getTenantId();
    var tbl = tableId.getId();

    String byId = Keys.snapPtrById(tid, tbl, snapshotId);
    long createdMs = get(byId)
        .map(s -> com.google.protobuf.util.Timestamps.toMillis(s.getUpstreamCreatedAt()))
        .orElse(0L);
    String byTime = Keys.snapPtrByTime(tid, tbl, snapshotId, createdMs);
    String blob = Keys.snapBlob(tid, tbl, snapshotId);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) return false;
    ptr.get(byTime).ifPresent(p -> compareAndDeleteOrFalse(byTime, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    var t = tableId.getTenantId();
    var key = Keys.snapPtrById(t, tableId.getId(), snapshotId);
    var p = ptr.get(key).orElseThrow(
        () -> new IllegalStateException(
            "Pointer missing for snapshot: " + Long.toString(snapshotId)));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    var t = tableId.getTenantId();
    var key = Keys.snapPtrById(t, tableId.getId(), snapshotId);
    var blob = Keys.snapBlob(t, tableId.getId(), snapshotId);
    return safeMetaOrDefault(key, blob, nowTs);
  }
}
