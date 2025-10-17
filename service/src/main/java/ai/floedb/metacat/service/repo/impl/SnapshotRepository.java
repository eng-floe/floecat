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
      throw new RuntimeException("parse failed: " + latest.blobUri(), e);
    }
  }

  public void create(Snapshot snapshot) {
    var tableId = snapshot.getTableId();
    var tenantId = tableId.getTenantId();
    var tableUUID = tableId.getId();
    long snapshotId = snapshot.getSnapshotId();
    long upstreamCreatedAt = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());

    String byId = Keys.snapPtrById(tenantId, tableUUID, snapshotId);
    String byTime = Keys.snapPtrByTime(tenantId, tableUUID, snapshotId, upstreamCreatedAt);
    String blob = Keys.snapBlob(tenantId, tableUUID, snapshotId);

    putCas(byTime, blob);
    try {
      putBlob(blob, snapshot);
      putCas(byId, blob);
    } catch (RuntimeException e) {
      deleteQuietly(() -> ptr.delete(byTime));
      throw e;
    }
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    var tid = tableId.getTenantId();
    var tbl = tableId.getId();
    var byId = Keys.snapPtrById(tid, tbl, snapshotId);
    var snapshotOpt = get(byId);
    long upstreamCreatedAtMs = 0L;
    if (snapshotOpt.isPresent()) {
      upstreamCreatedAtMs = Timestamps.toMillis(snapshotOpt.get().getUpstreamCreatedAt());
    }
    var byTime = Keys.snapPtrByTime(tid, tbl, snapshotId, upstreamCreatedAtMs);
    var blob = Keys.snapBlob(tid, tbl, snapshotId);

    deleteQuietly(() -> ptr.delete(byId));
    deleteQuietly(() -> ptr.delete(byTime));
    deleteQuietly(() -> blobs.delete(blob));
    return ptr.get(byId).isEmpty() && ptr.get(byTime).isEmpty() && blobs.head(blob).isEmpty();
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long snapshotId, long expectedVersion) {
    var tid = tableId.getTenantId();
    var tbl = tableId.getId();

    String byId = Keys.snapPtrById(tid, tbl, snapshotId);
    var snapshotOpt = get(byId);
    long upstreamCreatedAtMs = 0L;
    if (snapshotOpt.isPresent()) {
      upstreamCreatedAtMs = Timestamps.toMillis(snapshotOpt.get().getUpstreamCreatedAt());
    }
    String byTime = Keys.snapPtrByTime(tid, tbl, snapshotId, upstreamCreatedAtMs);
    String blob = Keys.snapBlob(tid, tbl, snapshotId);

    if (!compareAndDeleteOrFalse(ptr, byId, expectedVersion)) {
      return false;
    }
    deleteQuietly(() -> ptr.delete(byTime));
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
