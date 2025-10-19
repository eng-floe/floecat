package ai.floedb.metacat.service.repo.impl;

import java.util.List;
import java.util.Optional;

import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import ai.floedb.metacat.catalog.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.connector.rpc.Connector;
import ai.floedb.metacat.service.repo.util.BaseRepository;
import ai.floedb.metacat.service.repo.util.Keys;
import ai.floedb.metacat.service.storage.BlobStore;
import ai.floedb.metacat.service.storage.PointerStore;

@ApplicationScoped
public class ConnectorRepository extends BaseRepository<Connector> {

  protected ConnectorRepository() { super(); }

  @Inject
  public ConnectorRepository(PointerStore ptr, BlobStore blobs) {
    super(ptr, blobs, Connector::parseFrom, Connector::toByteArray, "application/x-protobuf");
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

  public void create(Connector c) {
    var rid = c.getResourceId();
    var tid = rid.getTenantId();
    var byId = Keys.connByIdPtr(tid, rid.getId());
    var byName = Keys.connByNamePtr(tid, c.getDisplayName());
    var blob = Keys.connBlob(tid, rid.getId());

    reserveIndexOrIdempotent(byId, blob);
    reserveIndexOrIdempotent(byName, blob);
    putBlob(blob, c);
  }

  public boolean update(Connector updated, long expectedPointerVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();

    var byId = Keys.connByIdPtr(tid, rid.getId());
    var blob = Keys.connBlob(tid, rid.getId());

    putBlob(blob, updated);
    advancePointer(byId, blob, expectedPointerVersion);
    return true;
  }

  public boolean rename(Connector updated, String oldDisplayName, long expectedVersion) {
    var rid = updated.getResourceId();
    var tid = rid.getTenantId();

    var byId = Keys.connByIdPtr(tid, rid.getId());
    var newByName = Keys.connByNamePtr(tid, updated.getDisplayName());
    var oldByName = Keys.connByNamePtr(tid, oldDisplayName);
    var blob = Keys.connBlob(tid, rid.getId());

    putBlob(blob, updated);
    reserveIndexOrIdempotent(newByName, blob);
    try {
      advancePointer(byId, blob, expectedVersion);
    } catch (RuntimeException e) {
      ptr.get(newByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, newByName, p.getVersion()));
      throw e;
    }
    ptr.get(oldByName).ifPresent(p -> compareAndDeleteOrFalse(ptr, oldByName, p.getVersion()));
    return true;
  }

  public boolean delete(ResourceId rid) {
    var tid = rid.getTenantId();
    var byId = Keys.connByIdPtr(tid, rid.getId());
    var blob = Keys.connBlob(tid, rid.getId());

    var cOpt = getById(rid);
    var byName = cOpt.map(c -> Keys.connByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (byName != null) ptr.get(byName).ifPresent(p -> compareAndDeleteOrFalse(ptr, byName, p.getVersion()));
    ptr.get(byId).ifPresent(p -> compareAndDeleteOrFalse(ptr, byId, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public boolean deleteWithPrecondition(ResourceId rid, long expectedVersion) {
    var tid = rid.getTenantId();
    var byId = Keys.connByIdPtr(tid, rid.getId());
    var blob = Keys.connBlob(tid, rid.getId());

    var cOpt = getById(rid);
    var byName = cOpt.map(c -> Keys.connByNamePtr(tid, c.getDisplayName())).orElse(null);

    if (!compareAndDeleteOrFalse(ptr, byId, expectedVersion)) return false;
    if (byName != null) ptr.get(byName).ifPresent(p -> compareAndDeleteOrFalse(ptr, byName, p.getVersion()));
    deleteQuietly(() -> blobs.delete(blob));
    return true;
  }

  public MutationMeta metaFor(ResourceId rid, Timestamp nowTs) {
    var t = rid.getTenantId();
    var key = Keys.connByIdPtr(t, rid.getId());
    var p = ptr.get(key).orElseThrow(() -> new IllegalStateException("Pointer missing for connector: " + rid.getId()));
    return safeMetaOrDefault(key, p.getBlobUri(), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId rid, Timestamp nowTs) {
    var t = rid.getTenantId();
    var key = Keys.connByIdPtr(t, rid.getId());
    var blob = Keys.connBlob(t, rid.getId());
    return safeMetaOrDefault(key, blob, nowTs);
  }
}
