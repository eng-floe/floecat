package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.model.TenantKey;
import ai.floedb.metacat.service.repo.util.GenericResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import ai.floedb.metacat.tenant.rpc.Tenant;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class TenantRepository {

  private final GenericResourceRepository<Tenant, TenantKey> repo;

  @Inject
  public TenantRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.TENANT,
            Tenant::parseFrom,
            Tenant::toByteArray,
            "application/x-protobuf");
  }

  public void create(Tenant tenant) {
    repo.create(tenant);
  }

  public boolean update(Tenant tenant, long expectedPointerVersion) {
    return repo.update(tenant, expectedPointerVersion);
  }

  public boolean delete(ResourceId tenantResourceId) {
    return repo.delete(new TenantKey(tenantResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId tenantResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new TenantKey(tenantResourceId.getId()), expectedPointerVersion);
  }

  public Optional<Tenant> getById(ResourceId tenantResourceId) {
    return repo.getByKey(new TenantKey(tenantResourceId.getId()));
  }

  public Optional<Tenant> getByName(String displayName) {
    return repo.get(Keys.tenantPointerByName(displayName));
  }

  public List<Tenant> list(int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.tenantPointerByNamePrefix(), limit, pageToken, nextOut);
  }

  public int count() {
    return repo.countByPrefix(Keys.tenantPointerByNamePrefix());
  }

  public MutationMeta metaFor(ResourceId tenantResourceId) {
    return repo.metaFor(new TenantKey(tenantResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId tenantResourceId, Timestamp nowTs) {
    return repo.metaFor(new TenantKey(tenantResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId tenantResourceId) {
    return repo.metaForSafe(new TenantKey(tenantResourceId.getId()));
  }
}
