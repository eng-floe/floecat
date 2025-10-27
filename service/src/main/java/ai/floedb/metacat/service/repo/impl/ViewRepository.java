package ai.floedb.metacat.service.repo.impl;

import ai.floedb.metacat.catalog.rpc.View;
import ai.floedb.metacat.common.rpc.MutationMeta;
import ai.floedb.metacat.common.rpc.ResourceId;
import ai.floedb.metacat.service.repo.model.Keys;
import ai.floedb.metacat.service.repo.model.Schemas;
import ai.floedb.metacat.service.repo.model.ViewKey;
import ai.floedb.metacat.service.repo.util.GenericResourceRepository;
import ai.floedb.metacat.storage.BlobStore;
import ai.floedb.metacat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class ViewRepository {

  private final GenericResourceRepository<View, ViewKey> repo;

  @Inject
  public ViewRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.VIEW,
            View::parseFrom,
            View::toByteArray,
            "application/x-protobuf");
  }

  public void create(View view) {
    repo.create(view);
  }

  public boolean update(View view, long expectedPointerVersion) {
    return repo.update(view, expectedPointerVersion);
  }

  public boolean delete(ResourceId viewResourceId) {
    return repo.delete(new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId viewResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()), expectedPointerVersion);
  }

  public Optional<View> getById(ResourceId viewResourceId) {
    return repo.getByKey(new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()));
  }

  public Optional<View> getByName(
      String tenantId, String catalogId, String namespaceId, String viewName) {
    return repo.get(Keys.viewPointerByName(tenantId, catalogId, namespaceId, viewName));
  }

  public List<View> list(
      String tenantId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.viewPointerByNamePrefix(tenantId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String tenantId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.viewPointerByNamePrefix(tenantId, catalogId, namespaceId));
  }

  public MutationMeta metaFor(ResourceId viewResourceId) {
    return repo.metaFor(new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId viewResourceId, Timestamp nowTs) {
    return repo.metaFor(new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId viewResourceId) {
    return repo.metaForSafe(new ViewKey(viewResourceId.getTenantId(), viewResourceId.getId()));
  }
}
