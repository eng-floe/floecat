package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.Namespace;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class NamespaceRepository extends BaseRepository<Namespace> {
  public NamespaceRepository() {
    super();
  }

  @Inject
  public NamespaceRepository(PointerStore pointerStore, BlobStore blobs) {
    super(
        pointerStore,
        blobs,
        Namespace::parseFrom,
        Namespace::toByteArray,
        "application/x-protobuf");
  }

  public Optional<Namespace> getById(ResourceId namespaceId) {
    return get(Keys.nsPtr(namespaceId.getTenantId(), namespaceId.getId()));
  }

  public Optional<ResourceId> getByPath(String tenantId, ResourceId catalogId, List<String> path) {
    var key = Keys.nsByPathPtr(tenantId, catalogId.getId(), path);

    return get(key).map(Namespace::getResourceId);
  }

  public List<Namespace> listByName(
      ResourceId catalogId, List<String> pathPrefix, int limit, String token, StringBuilder next) {
    var pfx =
        Keys.nsByPathPrefix(
            catalogId.getTenantId(),
            catalogId.getId(),
            pathPrefix == null ? List.of() : pathPrefix);

    return listByPrefix(pfx, limit, token, next);
  }

  public int count(ResourceId catalogId) {
    var pfx = Keys.nsByPathPrefix(catalogId.getTenantId(), catalogId.getId(), List.of());
    return countByPrefix(pfx);
  }

  public void create(Namespace namespace) {
    var catalogId = namespace.getCatalogId();
    requireCatalogId(catalogId);
    var namespaceId = namespace.getResourceId();
    var tenantId = namespaceId.getTenantId();

    var byId = Keys.nsPtr(tenantId, namespaceId.getId());
    var blobUri = Keys.nsBlob(tenantId, namespaceId.getId());
    var fullNamespacePath = new ArrayList<>(namespace.getParentsList());
    if (!namespace.getDisplayName().isBlank()) {
      fullNamespacePath.add(namespace.getDisplayName());
    }
    var byPath = Keys.nsByPathPtr(tenantId, catalogId.getId(), fullNamespacePath);
    ;
    putBlob(blobUri, namespace);
    reserveAllOrRollback(byId, blobUri, byPath, blobUri);
  }

  public boolean update(Namespace updated, ResourceId catalogId, long expectedVersion) {
    requireCatalogId(catalogId);
    var tenantId = updated.getResourceId().getTenantId();
    var byId = Keys.nsPtr(tenantId, updated.getResourceId().getId());
    var blobUri = Keys.nsBlob(tenantId, updated.getResourceId().getId());

    putBlob(blobUri, updated);
    advancePointer(byId, blobUri, expectedVersion);
    return true;
  }

  public boolean renameOrMove(
      Namespace updated,
      ResourceId oldCatalogId,
      List<String> oldParents,
      String oldLeaf,
      ResourceId newCatalogId,
      List<String> newParents,
      long expectedVersion) {

    requireCatalogId(oldCatalogId);
    requireCatalogId(newCatalogId);

    var tenantId = updated.getResourceId().getTenantId();
    var sameCatalog = oldCatalogId.getId().equals(newCatalogId.getId());
    var oldPath = new ArrayList<>(oldParents);
    oldPath.add(oldLeaf);
    var newPath = new ArrayList<>(newParents);
    newPath.add(updated.getDisplayName());
    var newByPath = Keys.nsByPathPtr(tenantId, newCatalogId.getId(), newPath);

    if (sameCatalog && oldPath.equals(newPath)) {
      return true;
    }

    var namespaceId = updated.getResourceId().getId();
    var blobUri = Keys.nsBlob(tenantId, namespaceId);

    putBlob(blobUri, updated);

    var oldById = Keys.nsPtr(tenantId, namespaceId);
    if (sameCatalog) {
      reserveAllOrRollback(newByPath, blobUri);
      try {
        advancePointer(oldById, blobUri, expectedVersion);
      } catch (PreconditionFailedException e) {
        pointerStore
            .get(newByPath)
            .ifPresent(pointer -> compareAndDeleteOrFalse(newByPath, pointer.getVersion()));
        return false;
      }
    } else {
      var newById = Keys.nsPtr(tenantId, namespaceId);
      reserveAllOrRollback(newById, blobUri, newByPath, blobUri);
      if (!compareAndDeleteOrFalse(oldById, expectedVersion)) {
        pointerStore
            .get(newById)
            .ifPresent(pointer -> compareAndDeleteOrFalse(newById, pointer.getVersion()));
        pointerStore
            .get(newByPath)
            .ifPresent(pointer -> compareAndDeleteOrFalse(newByPath, pointer.getVersion()));

        return false;
      }
    }

    var oldByPath = Keys.nsByPathPtr(tenantId, oldCatalogId.getId(), oldPath);
    pointerStore
        .get(oldByPath)
        .ifPresent(pointer -> compareAndDeleteOrFalse(oldByPath, pointer.getVersion()));

    return true;
  }

  public boolean delete(ResourceId catalogId, ResourceId namespaceId) {
    var tenantId = namespaceId.getTenantId();
    var byId = Keys.nsPtr(tenantId, namespaceId.getId());
    var blobUri = Keys.nsBlob(tenantId, namespaceId.getId());

    var namespaceOpt = getById(namespaceId);
    var byPath =
        namespaceOpt
            .map(
                ns -> {
                  var fullNamespacePath = new ArrayList<>(ns.getParentsList());
                  fullNamespacePath.add(ns.getDisplayName());
                  return Keys.nsByPathPtr(tenantId, catalogId.getId(), fullNamespacePath);
                })
            .orElse(null);

    if (byPath != null) {
      pointerStore
          .get(byPath)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byPath, pointer.getVersion()));
    }
    pointerStore
        .get(byId)
        .ifPresent(pointer -> compareAndDeleteOrFalse(byId, pointer.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public boolean deleteWithPrecondition(
      ResourceId catalogId, ResourceId namespaceId, long expectedVersion) {
    var tenantId = namespaceId.getTenantId();
    var byId = Keys.nsPtr(tenantId, namespaceId.getId());
    var blobUri = Keys.nsBlob(tenantId, namespaceId.getId());

    var namespaceOpt = getById(namespaceId);
    var byPath =
        namespaceOpt
            .map(
                ns -> {
                  var fullNamespacePath = new ArrayList<>(ns.getParentsList());
                  fullNamespacePath.add(ns.getDisplayName());
                  return Keys.nsByPathPtr(tenantId, catalogId.getId(), fullNamespacePath);
                })
            .orElse(null);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) {
      return false;
    }

    if (byPath != null) {
      pointerStore
          .get(byPath)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byPath, pointer.getVersion()));
    }

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId) {
    return metaFor(catalogId, namespaceId, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaFor(ResourceId catalogId, ResourceId namespaceId, Timestamp nowTs) {
    var tenantId = namespaceId.getTenantId();
    var key = Keys.nsPtr(tenantId, namespaceId.getId());
    var pointer =
        pointerStore
            .get(key)
            .orElseThrow(
                () ->
                    new BaseRepository.NotFoundException(
                        "Pointer missing for namespace: " + namespaceId.getId()));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogId, ResourceId namespaceId) {
    return metaForSafe(catalogId, namespaceId, Timestamps.fromMillis(clock.millis()));
  }

  public MutationMeta metaForSafe(ResourceId catalogId, ResourceId namespaceId, Timestamp nowTs) {
    var tenantId = namespaceId.getTenantId();
    var key = Keys.nsPtr(tenantId, namespaceId.getId());
    var blobUri = Keys.nsBlob(tenantId, namespaceId.getId());

    return safeMetaOrDefault(key, blobUri, nowTs);
  }

  private static void requireCatalogId(ResourceId catalogId) {
    if (catalogId == null || catalogId.getId() == null || catalogId.getId().isBlank()) {
      throw new IllegalArgumentException("namespace requires non-empty catalog_id");
    }
  }
}
