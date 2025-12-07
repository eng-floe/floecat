package ai.floedb.floecat.service.repo.impl;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.CatalogKey;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.Schemas;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository;
import ai.floedb.floecat.storage.BlobStore;
import ai.floedb.floecat.storage.PointerStore;
import com.google.protobuf.Timestamp;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class CatalogRepository {

  private final GenericResourceRepository<Catalog, CatalogKey> repo;

  @Inject
  public CatalogRepository(PointerStore pointerStore, BlobStore blobStore) {
    this.repo =
        new GenericResourceRepository<>(
            pointerStore,
            blobStore,
            Schemas.CATALOG,
            Catalog::parseFrom,
            Catalog::toByteArray,
            "application/x-protobuf");
  }

  public void create(Catalog catalog) {
    repo.create(catalog);
  }

  public boolean update(Catalog catalog, long expectedPointerVersion) {
    return repo.update(catalog, expectedPointerVersion);
  }

  public boolean delete(ResourceId catalogResourceId) {
    return repo.delete(new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public boolean deleteWithPrecondition(ResourceId catalogResourceId, long expectedPointerVersion) {
    return repo.deleteWithPrecondition(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()),
        expectedPointerVersion);
  }

  public Optional<Catalog> getById(ResourceId catalogResourceId) {
    return repo.getByKey(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public Optional<Catalog> getByName(String accountId, String displayName) {
    return repo.get(Keys.catalogPointerByName(accountId, displayName));
  }

  public List<Catalog> list(String accountId, int limit, String pageToken, StringBuilder nextOut) {
    return repo.listByPrefix(Keys.catalogPointerByNamePrefix(accountId), limit, pageToken, nextOut);
  }

  public int count(String accountId) {
    return repo.countByPrefix(Keys.catalogPointerByNamePrefix(accountId));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId) {
    return repo.metaFor(new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }

  public MutationMeta metaFor(ResourceId catalogResourceId, Timestamp nowTs) {
    return repo.metaFor(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()), nowTs);
  }

  public MutationMeta metaForSafe(ResourceId catalogResourceId) {
    return repo.metaForSafe(
        new CatalogKey(catalogResourceId.getAccountId(), catalogResourceId.getId()));
  }
}
