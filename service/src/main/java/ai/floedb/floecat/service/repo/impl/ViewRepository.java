package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.model.ViewKey;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
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
    return repo.delete(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId viewResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()), expectedPointerVersion);
  }

  public Optional<View> getById(ResourceId viewResourceId) {
    return repo.getByKey(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public Optional<View> getByName(
      String accountId, String catalogId, String namespaceId, String viewName) {
    return repo.get(Keys.viewPointerByName(accountId, catalogId, namespaceId, viewName));
  }

  public List<View> list(
      String accountId,
      String catalogId,
      String namespaceId,
      int limit,
      String pageToken,
      StringBuilder nextOut) {
    String prefix = Keys.viewPointerByNamePrefix(accountId, catalogId, namespaceId);
    return repo.listByPrefix(prefix, limit, pageToken, nextOut);
  }

  public int count(String accountId, String catalogId, String namespaceId) {
    return repo.countByPrefix(Keys.viewPointerByNamePrefix(accountId, catalogId, namespaceId));
  }

  public MutationMeta metaFor(ResourceId viewResourceId) {
    return repo.metaFor(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId viewResourceId, Timestamp nowTs) {
    return repo.metaFor(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId viewResourceId) {
    return repo.metaForSafe(new ViewKey(viewResourceId.getAccountId(), viewResourceId.getId()));
  }
}
