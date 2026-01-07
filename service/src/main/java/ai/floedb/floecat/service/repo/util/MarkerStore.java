/*
 * Copyright 2026 Yellowbrick Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
