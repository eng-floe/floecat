package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.storage.InMemoryBlobStore;
import ai.floedb.floecat.storage.InMemoryPointerStore;
import ai.floedb.floecat.storage.errors.StorageNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class FakeNamespaceRepository extends NamespaceRepository {
  private final Map<ResourceId, Namespace> entries = new HashMap<>();
  private final Map<ResourceId, MutationMeta> metas = new HashMap<>();

  public FakeNamespaceRepository() {
    super(new InMemoryPointerStore(), new InMemoryBlobStore());
  }

  public void put(Namespace namespace, MutationMeta meta) {
    entries.put(namespace.getResourceId(), namespace);
    metas.put(namespace.getResourceId(), meta);
  }

  @Override
  public Optional<Namespace> getById(ResourceId id) {
    return Optional.ofNullable(entries.get(id));
  }

  @Override
  public Optional<Namespace> getByPath(String accountId, String catalogId, List<String> path) {
    return entries.values().stream()
        .filter(
            ns ->
                accountId.equals(ns.getResourceId().getAccountId())
                    && catalogId.equals(ns.getCatalogId().getId())
                    && matchesPath(ns, path))
        .findFirst();
  }

  @Override
  public MutationMeta metaForSafe(ResourceId id) {
    MutationMeta meta = metas.get(id);
    if (meta == null) {
      throw new StorageNotFoundException("missing namespace meta");
    }
    return meta;
  }

  private boolean matchesPath(Namespace namespace, List<String> path) {
    if (path.isEmpty()) {
      return namespace.getDisplayName().isBlank() && namespace.getParentsCount() == 0;
    }
    List<String> parents = path.subList(0, path.size() - 1);
    String name = path.get(path.size() - 1);
    return parents.equals(namespace.getParentsList()) && name.equals(namespace.getDisplayName());
  }
}
