package ai.floedb.floecat.service.repo.util;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.storage.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MarkerStore {
  private static final int CAS_MAX = BaseResourceRepository.CAS_MAX;

  @Inject PointerStore pointerStore;

  public long catalogMarkerVersion(ResourceId catalogId) {
    String key = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }

  public long namespaceMarkerVersion(ResourceId namespaceId) {
    String key = Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }

  public void bumpCatalogMarker(ResourceId catalogId) {
    String key = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    bumpMarker(key);
  }

  public void bumpNamespaceMarker(ResourceId namespaceId) {
    String key = Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    bumpMarker(key);
  }

  public boolean advanceCatalogMarker(ResourceId catalogId, long expectedVersion) {
    String key = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    return advanceMarker(key, expectedVersion);
  }

  public boolean advanceNamespaceMarker(ResourceId namespaceId, long expectedVersion) {
    String key = Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return advanceMarker(key, expectedVersion);
  }

  private void bumpMarker(String key) {
    for (int i = 0; i < CAS_MAX; i++) {
      var current = pointerStore.get(key).orElse(null);
      long expected = current == null ? 0L : current.getVersion();
      if (advanceMarker(key, expected)) {
        return;
      }
    }
  }

  private boolean advanceMarker(String key, long expectedVersion) {
    var next =
        Pointer.newBuilder().setKey(key).setBlobUri(key).setVersion(expectedVersion + 1).build();
    return pointerStore.compareAndSet(key, expectedVersion, next);
  }
}
