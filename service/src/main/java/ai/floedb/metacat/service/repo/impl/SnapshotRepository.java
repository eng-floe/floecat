package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Snapshot;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.model.SnapshotKey;
import ai.floedb.metacat.service.repo.util.GenericResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class SnapshotRepository {

  private final GenericResourceRepository<Snapshot, SnapshotKey> repo;

  @Inject
  public SnapshotRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.SNAPSHOT,
            Snapshot::parseFrom,
            Snapshot::toByteArray,
            "application/x-protobuf");
  }

  public void create(Snapshot snapshot) {
    repo.create(snapshot);
  }

  public boolean update(Snapshot snapshot, long expectedPointerVersion) {
    return repo.update(snapshot, expectedPointerVersion);
  }

  public boolean delete(ResourceId tableId, long snapshotId) {
    return repo.delete(new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public boolean deleteWithPrecondition(
      ResourceId tableId, long snapshotId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId),
        expectedPointerVersion);
  }

  public Optional<Snapshot> getById(ResourceId tableId, long snapshotId) {
    return repo.getByKey(new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public Optional<Snapshot> getCurrentSnapshot(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getTenantId(), tableId.getId());
    StringBuilder next = new StringBuilder();
    List<Snapshot> page = repo.listByPrefix(prefix, 1, "", next);
    if (page.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(page.get(0));
  }

  public Optional<Snapshot> getAsOf(ResourceId tableId, Timestamp asOf) {
    String prefix = Keys.snapshotPointerByTimePrefix(tableId.getTenantId(), tableId.getId());
    long asOfMs = Timestamps.toMillis(asOf);

    String token = "";
    StringBuilder next = new StringBuilder();
    do {
      List<Snapshot> batch = repo.listByPrefix(prefix, 200, token, next);
      for (Snapshot s : batch) {
        long createdMs = Timestamps.toMillis(s.getUpstreamCreatedAt());
        if (createdMs <= asOfMs) {
          return Optional.of(s);
        }
      }
      token = next.toString();
      next.setLength(0);
    } while (!token.isEmpty());

    return Optional.empty();
  }

  public List<Snapshot> list(
      ResourceId tableId, int limit, String pageToken, StringBuilder nextOut) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getTenantId(), tableId.getId());
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(ResourceId tableId) {
    String prefix = Keys.snapshotPointerByIdPrefix(tableId.getTenantId(), tableId.getId());
    return repo.countByPrefix(prefix);
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId) {
    return repo.metaFor(new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }

  public MutationMeta metaFor(ResourceId tableId, long snapshotId, Timestamp nowTs) {
    return repo.metaFor(new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tableId, long snapshotId) {
    return repo.metaForSafe(new SnapshotKey(tableId.getTenantId(), tableId.getId(), snapshotId));
  }
}
