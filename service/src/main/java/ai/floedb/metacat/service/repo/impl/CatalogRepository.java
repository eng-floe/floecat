package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Catalog;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class CatalogRepository extends BaseRepository<Catalog> {
  public CatalogRepository() {
    super();
  }

  @Inject
  public CatalogRepository(PointerStore pointerStore, BlobStore blobs) {
    super(pointerStore, blobs, Catalog::parseFrom, Catalog::toByteArray, "application/x-protobuf");
  }

  public Optional<Catalog> getById(ResourceId rid) {
    return get(Keys.catPtr(rid.getTenantId(), rid.getId()));
  }

  public Optional<Catalog> getByName(String tenantId, String displayName) {
    return get(Keys.catByNamePtr(tenantId, displayName));
  }

  public List<Catalog> listByName(String tenantId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.catByNamePrefix(tenantId), limit, token, next);
  }

  public int count(String tenantId) {
    return countByPrefix(Keys.catByNamePrefix(tenantId));
  }

  public void create(Catalog cat) {
    var rid = cat.getResourceId();
    var tid = rid.getTenantId();

    var byName = Keys.catByNamePtr(tid, cat.getDisplayName());
    var byId = Keys.catPtr(tid, rid.getId());
    var blobUri = Keys.catBlob(tid, rid.getId());

    putBlob(blobUri, cat);
    reserveAllOrRollback(byId, blobUri, byName, blobUri);
  }

  public boolean update(Catalog updated, long expectedPointerVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();
    var byId = Keys.catPtr(tid, rid.getId());
    var blobUri = Keys.catBlob(tid, rid.getId());

    putBlob(blobUri, updated);
    advancePointer(byId, blobUri, expectedPointerVersion);
    return true;
  }

  public boolean rename(String tenant, ResourceId catId, String newName, long expectedVersion) {
    var byId = Keys.catPtr(tenant, catId.getId());
    var catalog =
        get(byId).orElseThrow(() -> new NotFoundException("catalog not found: " + catId.getId()));

    if (newName.equals(catalog.getDisplayName())) {
      return true;
    }

    var blobUri = Keys.catBlob(tenant, catId.getId());
    var newByName = Keys.catByNamePtr(tenant, newName);

    var updated = catalog.toBuilder().setDisplayName(newName).build();
    putBlob(blobUri, updated);

    reserveAllOrRollback(newByName, blobUri);

    try {
      advancePointer(byId, blobUri, expectedVersion);
    } catch (PreconditionFailedException e) {
      pointerStore
          .get(newByName)
          .ifPresent(p -> compareAndDeleteOrFalse(newByName, p.getVersion()));
      return false;
    }

    var oldByName = Keys.catByNamePtr(tenant, catalog.getDisplayName());
    pointerStore.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(oldByName, p.getVersion()));

    return true;
  }

  public boolean delete(ResourceId catalogId) {
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blobUri = Keys.catBlob(tid, catalogId.getId());

    var pointerIdOpt = pointerStore.get(byId);
    if (pointerIdOpt.isEmpty()) {
      deleteQuietly(() -> blobStore.delete(blobUri));
      return true;
    }
    var pointerId = pointerIdOpt.get();

    var catalogOpt = getById(catalogId);
    var byName =
        catalogOpt.map(catalog -> Keys.catByNamePtr(tid, catalog.getDisplayName())).orElse(null);

    if (byName != null) {
      pointerStore
          .get(byName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byName, pointer.getVersion()));
    }

    if (!compareAndDeleteOrFalse(byId, pointerId.getVersion())) {
      return false;
    }

    deleteQuietly(() -> blobStore.delete(blobUri));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId catalogId, long expectedVersion) {
    var tid = catalogId.getTenantId();
    var byId = Keys.catPtr(tid, catalogId.getId());
    var blobUri = Keys.catBlob(tid, catalogId.getId());

    var catalogOpt = getById(catalogId);
    var byName =
        catalogOpt.map(catalog -> Keys.catByNamePtr(tid, catalog.getDisplayName())).orElse(null);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) {
      return false;
    }

    if (byName != null) {
      pointerStore
          .get(byName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byName, pointer.getVersion()));
    }

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public MutationMeta metaFor(ResourceId id) {
    return metaFor(id, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(ResourceId catalogId, Timestamp nowTs) {
    var tenant = catalogId.getTenantId();
    var key = Keys.catPtr(tenant, catalogId.getId());
    var pointer =
        pointerStore
            .get(key)
            .orElseThrow(
                () ->
                    new BaseRepository.NotFoundException(
                        "Pointer missing for catalog: " + catalogId.getId()));
    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId id) {
    return metaForSafe(id, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(ResourceId catalogId, Timestamp nowTs) {
    var tenant = catalogId.getTenantId();
    var key = Keys.catPtr(tenant, catalogId.getId());
    var blobUri = Keys.catBlob(tenant, catalogId.getId());
    return safeMetaOrDefault(key, blobUri, nowTs);
  }
}
