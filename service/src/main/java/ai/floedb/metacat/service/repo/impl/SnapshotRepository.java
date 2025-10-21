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
  public SnapshotRepository(PointerStore pointerStore, BlobStore blobs) {
    super(pointerStore, blobs, Snapshot::parseFrom,
        Snapshot::toByteArray, "application/x-protobuf");
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

    var rows = pointerStore.listPointersByPrefix(pfx, 1, token, next);
    if (rows.isEmpty()) {
      return Optional.empty();
    }

    var latest = rows.get(0);
    try {
      return Optional.of(Snapshot.parseFrom(blobStore.get(latest.blobUri())));
    } catch (Exception e) {
      throw new CorruptionException("parse failed: " + latest.blobUri(), e);
    }
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    final String tenantId = tableId.getTenantId();
    final String pfx = Keys.snapPtrByTimePrefix(tenantId, tableId.getId());

    final long asOfMs = Timestamps.toMillis(asOf);

    String token = "";
    StringBuilder next = new StringBuilder();

    do {
      var rows = pointerStore.listPointersByPrefix(pfx, 200, token, next);
      for (var row : rows) {
        final byte[] bytes;
        try {
          bytes = blobStore.get(row.blobUri());
        } catch (Exception e) {
          throw new CorruptionException("parse failed: " + row.blobUri(), e);
        }
        final Snapshot snapshot;
        try {
          snapshot = Snapshot.parseFrom(bytes);
        } catch (Exception e) {
          throw new CorruptionException("parse failed: " + row.blobUri(), e);
        }

        long createdMs = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());
        if (createdMs <= asOfMs) {
          return Optional.of(snapshot);
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
    long snapId = snapshot.getSnapshotId();
    long created = Timestamps.toMillis(snapshot.getUpstreamCreatedAt());

    String byId = Keys.snapPtrById(tenantId, tableId.getId(), snapId);
    String byTime = Keys.snapPtrByTime(tenantId, tableId.getId(), snapId, created);
    String blobUri = Keys.snapBlob(tenantId, tableId.getId(), snapId);

    putBlob(blobUri, snapshot);
    reserveAllOrRollback(byId, blobUri, byTime, blobUri);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    var tenantId = tableId.getTenantId();

    String byId = Keys.snapPtrById(tenantId, tableId.getId(), snapshotId);
    long createdMs = get(byId)
        .map(s -> com.google.protobuf.util.Timestamps.toMillis(s.getUpstreamCreatedAt()))
        .orElse(0L);
    String byTime = Keys.snapPtrByTime(tenantId, tableId.getId(), snapshotId, createdMs);
    String blobUri = Keys.snapBlob(tenantId, tableId.getId(), snapshotId);

    pointerStore.get(byTime).ifPresent(
        pointer -> compareAndDeleteOrFalse(byTime, pointer.getVersion()));
    pointerStore.get(byId).ifPresent(
        pointer -> compareAndDeleteOrFalse(byId, pointer.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tableId, long snapshotId, long expectedVersion) {
    var tenantId = tableId.getTenantId();

    String byId = Keys.snapPtrById(tenantId, tableId.getId(), snapshotId);
    long createdMs = get(byId)
        .map(s -> Timestamps.toMillis(s.getUpstreamCreatedAt()))
        .orElse(0L);
    String byTime = Keys.snapPtrByTime(tenantId, tableId.getId(), snapshotId, createdMs);
    String blobUri = Keys.snapBlob(tenantId, tableId.getId(), snapshotId);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) {
      return false;
    }

    pointerStore.get(byTime).ifPresent(
        pointer -> compareAndDeleteOrFalse(byTime, pointer.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    var tenantId = tableId.getTenantId();
    var key = Keys.snapPtrById(tenantId, tableId.getId(), snapshotId);

    var pointer = pointerStore.get(key).orElseThrow(
        () -> new IllegalStateException(
            "Pointer missing for snapshot: " + Long.toString(snapshotId)));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    var tenantId = tableId.getTenantId();
    var key = Keys.snapPtrById(tenantId, tableId.getId(), snapshotId);
    var blobUri = Keys.snapBlob(tenantId, tableId.getId(), snapshotId);
    return safeMetaOrDefault(key, blobUri, nowTs);
  }
}
