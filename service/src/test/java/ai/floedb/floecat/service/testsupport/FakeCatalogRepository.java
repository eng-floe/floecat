package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.Catalog;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.CatalogRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class FakeCatalogRepository extends CatalogRepository {
  private final Map<ResourceId, Catalog> entries = new HashMap<>();
  private final Map<ResourceId, MutationMeta> metas = new HashMap<>();
  private final Map<ResourceId, Integer> gets = new HashMap<>();

  public FakeCatalogRepository() {
    super(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  public void put(Catalog catalog, MutationMeta meta) {
    entries.put(catalog.getResourceId(), catalog);
    metas.put(catalog.getResourceId(), meta);
  }

  public void putMeta(ResourceId id, MutationMeta meta) {
    metas.put(id, meta);
  }

  @Override
  public Optional<Catalog> getById(ResourceId id) {
    gets.merge(id, 1, Integer::sum);
    return Optional.ofNullable(entries.get(id));
  }

  @Override
  public Optional<Catalog> getByName(String accountId, String displayName) {
    return entries.values().stream()
        .filter(
            c ->
                accountId.equals(c.getResourceId().getAccountId())
                    && displayName.equals(c.getDisplayName()))
        .findFirst();
  }

  @Override
  public MutationMeta metaForSafe(ResourceId id) {
    MutationMeta meta = metas.get(id);
    if (meta == null) {
      throw new StorageNotFoundException("missing catalog meta");
    }
    return meta;
  }

  public int getByIdCount(ResourceId id) {
    return gets.getOrDefault(id, 0);
  }
}
