package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.util.Timestamps;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
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
    String key = Keys.snapPtr(tableId.getTenantId(), tableId.getId(), snapshotId);
    return get(key);
  }

  public List<Snapshot> list(ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapPrefix(tableId.getTenantId(), tableId.getId());
    return listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapPrefix(tableId.getTenantId(), tableId.getId());
    return countByPrefix(prefix);
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    String prefix = Keys.snapPrefix(tableId.getTenantId(), tableId.getId());
    StringBuilder ignore = new StringBuilder();

    var snapshots = listByPrefix(prefix, Integer.MAX_VALUE, "", ignore);
    if (snapshots.isEmpty()) return Optional.empty();

    return snapshots.stream().max((a, b) -> Timestamps.compare(a.getCreatedAt(), b.getCreatedAt()));
  }

  public void put(ResourceId tableId, Snapshot snapshot) {
    var tenantId = tableId.getTenantId();
    var tableUUID = tableId.getId();
    long snapshotId = snapshot.getSnapshotId();

    String key = Keys.snapPtr(tenantId, tableUUID, snapshotId);
    String uri = Keys.snapBlob(tenantId, tableUUID, snapshotId);

    put(key, uri, snapshot);
  }
}
