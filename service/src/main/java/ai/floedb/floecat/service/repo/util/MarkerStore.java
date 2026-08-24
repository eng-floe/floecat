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
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

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
    bumpMarker(catalogId.getAccountId(), key);
  }

  public void bumpNamespaceMarker(ResourceId namespaceId) {
    String key = Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    bumpMarker(namespaceId.getAccountId(), key);
  }

  public boolean advanceCatalogMarker(ResourceId catalogId, long expectedVersion) {
    String key = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    return advanceMarker(catalogId.getAccountId(), key, expectedVersion);
  }

  public boolean advanceNamespaceMarker(ResourceId namespaceId, long expectedVersion) {
    String key = Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return advanceMarker(namespaceId.getAccountId(), key, expectedVersion);
  }

  private void bumpMarker(String accountId, String key) {
    for (int i = 0; i < CAS_MAX; i++) {
      if (pointerStore.get(Keys.accountDeletionMarker(accountId)).isPresent()) {
        return;
      }
      var current = pointerStore.get(key).orElse(null);
      long expected = current == null ? 0L : current.getVersion();
      if (tryAdvanceMarker(accountId, key, expected)) {
        return;
      }
    }
  }

  private boolean advanceMarker(String accountId, String key, long expectedVersion) {
    String fenceKey = Keys.accountDeletionMarker(accountId);
    if (pointerStore.get(fenceKey).isPresent()) {
      throw new BaseResourceRepository.AccountDeletionInProgressException(accountId);
    }
    boolean advanced = tryAdvanceMarker(accountId, key, expectedVersion);
    if (!advanced && pointerStore.get(fenceKey).isPresent()) {
      throw new BaseResourceRepository.AccountDeletionInProgressException(accountId);
    }
    return advanced;
  }

  private boolean tryAdvanceMarker(String accountId, String key, long expectedVersion) {
    var next = PointerReferences.opaqueMarkerPointer(key, key, expectedVersion + 1);
    return pointerStore.compareAndSetBatch(
        List.of(
            new PointerStore.CasCheckAbsent(Keys.accountDeletionMarker(accountId)),
            new PointerStore.CasUpsert(key, expectedVersion, next)));
  }
}
