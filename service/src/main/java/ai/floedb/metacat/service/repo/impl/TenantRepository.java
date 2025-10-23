package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import ai.floedb.metacat.tenancy.rpc.Tenant;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TenantRepository extends BaseRepository<Tenant> {
  public TenantRepository() {
    super();
  }

  @Inject
  public TenantRepository(PointerStore pointerStore, BlobStore blobs) {
    super(pointerStore, blobs, Tenant::parseFrom, Tenant::toByteArray, "application/x-protobuf");
  }

  public Optional<Tenant> getById(ResourceId tenantId) {
    String key = Keys.tenPtr(tenantId.getId());
    return get(key);
  }

  public Optional<Tenant> getByName(String displayName) {
    String key = Keys.tenByNamePtr(displayName);
    return get(key);
  }

  public List<Tenant> listByName(
      int limit, String namePrefix, String pageToken, StringBuilder nextOut) {
    String pfx = Keys.tenByNamePrefix() + (namePrefix == null ? "" : namePrefix);
    return listByPrefix(pfx, limit, pageToken, nextOut);
  }

  public int count() {
    return countByPrefix(Keys.tenByNamePrefix());
  }

  public void create(Tenant t) {
    final var rid = t.getResourceId();
    final String id = rid.getId();

    final String canonical = Keys.tenPtr(id);
    final String byName = Keys.tenByNamePtr(t.getDisplayName());
    final String blobUri = Keys.tenBlob(id);

    putBlob(blobUri, t);
    reserveAllOrRollback(canonical, blobUri, byName, blobUri);
  }

  public boolean update(Tenant updated, long expectedVersion) {
    final var rid = updated.getResourceId();
    final String id = rid.getId();

    final String canonical = Keys.tenPtr(id);
    final String blobUri = Keys.tenBlob(id);

    putBlob(blobUri, updated);
    advancePointer(canonical, blobUri, expectedVersion);
    return true;
  }

  public boolean rename(ResourceId tenantId, String newDisplayName, long expectedVersion) {
    final String id = tenantId.getId();

    var cur =
        getById(tenantId)
            .orElseThrow(() -> new BaseRepository.NotFoundException("tenant not found"));

    if (newDisplayName.equals(cur.getDisplayName())) return true;

    final String blobUri = Keys.tenBlob(id);
    final String canonical = Keys.tenPtr(id);

    var updated = cur.toBuilder().setDisplayName(newDisplayName).build();

    putBlob(blobUri, updated);

    final String newByName = Keys.tenByNamePtr(newDisplayName);
    reserveAllOrRollback(newByName, blobUri);

    try {
      advancePointer(canonical, blobUri, expectedVersion);
    } catch (PreconditionFailedException e) {
      pointerStore
          .get(newByName)
          .ifPresent(p -> compareAndDeleteOrFalse(newByName, p.getVersion()));
      return false;
    }

    final String oldByName = Keys.tenByNamePtr(cur.getDisplayName());
    pointerStore.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(oldByName, p.getVersion()));
    return true;
  }

  public boolean delete(ResourceId tenantRid) {
    final String id = tenantRid.getId();

    final String canonical = Keys.tenPtr(id);
    final String blobUri = Keys.tenBlob(id);

    var tOpt = getById(tenantRid);
    final String byName = tOpt.map(t -> Keys.tenByNamePtr(t.getDisplayName())).orElse(null);

    if (byName != null) {
      pointerStore.get(byName).ifPresent(p -> compareAndDeleteOrFalse(byName, p.getVersion()));
    }
    pointerStore.get(canonical).ifPresent(p -> compareAndDeleteOrFalse(canonical, p.getVersion()));
    deleteQuietly(() -> blobStore.delete(blobUri));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId tenantRid, long expectedVersion) {
    final String id = tenantRid.getId();

    final String canonical = Keys.tenPtr(id);
    final String blobUri = Keys.tenBlob(id);

    var tOpt = getById(tenantRid);
    final String byName = tOpt.map(t -> Keys.tenByNamePtr(t.getDisplayName())).orElse(null);

    if (!compareAndDeleteOrFalse(canonical, expectedVersion)) return false;

    if (byName != null) {
      pointerStore.get(byName).ifPresent(p -> compareAndDeleteOrFalse(byName, p.getVersion()));
    }
    deleteQuietly(() -> blobStore.delete(blobUri));
    return true;
  }

  public MutationMeta metaFor(ResourceId rid) {
    return metaFor(rid, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(ResourceId rid, Timestamp nowTs) {
    final String id = rid.getId();

    final String key = Keys.tenPtr(id);
    var pointer =
        pointerStore
            .get(key)
            .orElseThrow(
                () -> new BaseRepository.NotFoundException("Pointer missing for tenant: " + id));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId rid) {
    return metaForSafe(rid, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(ResourceId rid, Timestamp nowTs) {
    final String id = rid.getId();
    final String key = Keys.tenPtr(id);
    final String blobUri = Keys.tenBlob(id);
    return safeMetaOrDefault(key, blobUri, nowTs);
  }
}
