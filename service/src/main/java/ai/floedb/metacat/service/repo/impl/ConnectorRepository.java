package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ConnectorRepository extends BaseRepository<Connector> {
  public ConnectorRepository() {
    super();
  }

  @Inject
  public ConnectorRepository(PointerStore pointerStore, BlobStore blobs) {
    super(
        pointerStore,
        blobs,
        Connector::parseFrom,
        Connector::toByteArray,
        "application/x-protobuf");
  }

  public Optional<Connector> getById(ResourceId rid) {
    return get(Keys.connByIdPtr(rid.getTenantId(), rid.getId()));
  }

  public Optional<Connector> getByName(String tenantId, String displayName) {
    return get(Keys.connByNamePtr(tenantId, displayName));
  }

  public List<Connector> listByName(String tenantId, int limit, String token, StringBuilder next) {
    return listByPrefix(Keys.connByNamePrefix(tenantId), limit, token, next);
  }

  public int countAll(String tenantId) {
    return countByPrefix(Keys.connByNamePrefix(tenantId));
  }

  public void create(Connector connector) {
    var resourceId = connector.getResourceId();
    var tenantId = resourceId.getTenantId();
    var byId = Keys.connByIdPtr(tenantId, resourceId.getId());
    var byName = Keys.connByNamePtr(tenantId, connector.getDisplayName());
    var blobUri = Keys.connBlob(tenantId, resourceId.getId());

    putBlob(blobUri, connector);
    reserveAllOrRollback(byId, blobUri, byName, blobUri);
  }

  public boolean update(Connector updated, long expectedPointerVersion) {
    var resourceId = updated.getResourceId();
    var tenantId = resourceId.getTenantId();

    var byId = Keys.connByIdPtr(tenantId, resourceId.getId());
    var blobUri = Keys.connBlob(tenantId, resourceId.getId());

    putBlob(blobUri, updated);
    advancePointer(byId, blobUri, expectedPointerVersion);

    return true;
  }

  public boolean rename(Connector updated, String oldDisplayName, long expectedVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();

    var newByName = Keys.connByNamePtr(tid, updated.getDisplayName());
    var oldByName = Keys.connByNamePtr(tid, oldDisplayName);
    var blobUri = Keys.connBlob(tid, rid.getId());

    putBlob(blobUri, updated);
    reserveAllOrRollback(newByName, blobUri);
    pointerStore
        .get(oldByName)
        .ifPresent(pointer -> compareAndDeleteOrFalse(oldByName, pointer.getVersion()));

    return true;
  }

  public boolean delete(ResourceId rid) {
    var tid = rid.getTenantId();
    var byId = Keys.connByIdPtr(tid, rid.getId());
    var blobUri = Keys.connBlob(tid, rid.getId());

    var connectorOpt = getById(rid);
    var byName = connectorOpt.map(c -> Keys.connByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (byName != null) {
      pointerStore
          .get(byName)
          .ifPresent(pointer -> compareAndDeleteOrFalse(byName, pointer.getVersion()));
    }

    pointerStore
        .get(byId)
        .ifPresent(pointer -> compareAndDeleteOrFalse(byId, pointer.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public boolean deleteWithPrecondition(ResourceId rid, long expectedVersion) {
    var tid = rid.getTenantId();
    var byId = Keys.connByIdPtr(tid, rid.getId());
    var blobUri = Keys.connBlob(tid, rid.getId());

    var connectorOpt = getById(rid);
    var byName = connectorOpt.map(c -> Keys.connByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (!compareAndDeleteOrFalse(byId, expectedVersion)) {
      return false;
    }

    if (byName != null)
      pointerStore.get(byName).ifPresent(p -> compareAndDeleteOrFalse(byName, p.getVersion()));

    deleteQuietly(() -> blobStore.delete(blobUri));

    return true;
  }

  public MutationMeta metaFor(ResourceId resourceId, Timestamp nowTs) {
    var tenantId = resourceId.getTenantId();
    var key = Keys.connByIdPtr(tenantId, resourceId.getId());

    var pointer =
        pointerStore
            .get(key)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Pointer missing for connector: " + resourceId.getId()));

    return safeMetaOrDefault(key, pointer.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId resourceId, Timestamp nowTs) {
    var tenantId = resourceId.getTenantId();
    var key = Keys.connByIdPtr(tenantId, resourceId.getId());
    var blobUri = Keys.connBlob(tenantId, resourceId.getId());

    return safeMetaOrDefault(key, blobUri, nowTs);
  }
}
