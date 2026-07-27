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
    var next = PointerReferences.opaqueMarkerPointer(key, key, expectedVersion + 1);
    return pointerStore.compareAndSet(key, expectedVersion, next);
  }

  /**
   * Guard for publishing a child into {@code namespaceId} — a table, a view, or a child namespace,
   * whether created outright or moved in by a reparent.
   *
   * <p>Its ops advance the namespace's children marker <em>in the publishing batch itself</em> and
   * pin the namespace pointer to the version the caller resolved. Paired with {@link
   * #namespaceDeleteGuard}, that makes "child published" and "namespace deleted" mutually
   * exclusive: a delete whose emptiness scan missed this child can no longer commit, because its
   * own batch checks the marker this one moved.
   *
   * <p>Must be built <em>after</em> the caller has resolved and authorized the namespace: the
   * pointer version captured here is the one the publish is pinned to, and a namespace that
   * disappeared in between is refused outright rather than pinned to "absent".
   */
  public BatchGuard namespaceChildGuard(ResourceId namespaceId) {
    String namespacePointerKey =
        Keys.namespacePointerById(namespaceId.getAccountId(), namespaceId.getId());
    long namespacePointerVersion = versionOf(namespacePointerKey);
    if (namespacePointerVersion <= 0L) {
      // Refuse to build a guard that would otherwise degrade to "check absent" and happily publish
      // a child into a namespace that is already gone. Retryable: the enclosing RPC re-resolves the
      // namespace and reports the natural NOT_FOUND.
      throw new BaseResourceRepository.BatchGuardFailedException(
          "namespace " + namespaceId.getId() + " no longer exists");
    }
    String markerKey =
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return new NamespaceChildGuard(
        namespaceId.getId(), namespacePointerKey, namespacePointerVersion, markerKey);
  }

  /**
   * Guard for deleting {@code namespaceId}: asserts, atomically with the pointer removal, that its
   * children marker is still at {@code expectedMarkerVersion} — where the caller's emptiness scan
   * left it. Any child published since (each of which advances the marker via {@link
   * #namespaceChildGuard}) invalidates the scan and the delete cannot commit.
   */
  public BatchGuard namespaceDeleteGuard(ResourceId namespaceId, long expectedMarkerVersion) {
    String markerKey =
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return new NamespaceDeleteGuard(namespaceId.getId(), markerKey, expectedMarkerVersion);
  }

  private long versionOf(String key) {
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }

  private final class NamespaceChildGuard implements BatchGuard {
    private final String namespaceId;
    private final String namespacePointerKey;
    private final long namespacePointerVersion;
    private final String markerKey;
    private long markerVersion;

    private NamespaceChildGuard(
        String namespaceId,
        String namespacePointerKey,
        long namespacePointerVersion,
        String markerKey) {
      this.namespaceId = namespaceId;
      this.namespacePointerKey = namespacePointerKey;
      this.namespacePointerVersion = namespacePointerVersion;
      this.markerKey = markerKey;
      this.markerVersion = versionOf(markerKey);
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      return List.of(
          new PointerStore.CasCheck(namespacePointerKey, namespacePointerVersion),
          new PointerStore.CasUpsert(
              markerKey,
              markerVersion,
              PointerReferences.opaqueMarkerPointer(markerKey, markerKey, markerVersion + 1)));
    }

    @Override
    public Outcome reevaluate() {
      if (versionOf(namespacePointerKey) != namespacePointerVersion) {
        // The namespace pointer is pinned to an exact version because CasCheck cannot express
        // "exists at any version", so a plain rename trips this too, not only a delete. That is
        // deliberately conservative and costs nothing: BROKEN is retryable, and a retry re-resolves
        // the namespace — succeeding against the new version, or reporting NOT_FOUND if it is truly
        // gone.
        return Outcome.BROKEN;
      }
      long current = versionOf(markerKey);
      if (current != markerVersion) {
        // A sibling published into the same namespace. The parent is untouched, so this is ordinary
        // contention on the marker: re-capture and let the caller re-run the batch.
        markerVersion = current;
        return Outcome.RETRY;
      }
      return Outcome.HOLDS;
    }

    @Override
    public String describe() {
      return "namespace " + namespaceId;
    }
  }

  private final class NamespaceDeleteGuard implements BatchGuard {
    private final String namespaceId;
    private final String markerKey;
    private final long expectedMarkerVersion;

    private NamespaceDeleteGuard(String namespaceId, String markerKey, long expectedMarkerVersion) {
      this.namespaceId = namespaceId;
      this.markerKey = markerKey;
      this.expectedMarkerVersion = expectedMarkerVersion;
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      // A namespace that never had a child has no marker pointer at all; "still absent" is the
      // correct precondition there, and CasCheck cannot express version 0.
      return List.of(
          expectedMarkerVersion == 0L
              ? new PointerStore.CasCheckAbsent(markerKey)
              : new PointerStore.CasCheck(markerKey, expectedMarkerVersion));
    }

    @Override
    public Outcome reevaluate() {
      // Never RETRY: a moved marker means a child may now exist, and only the caller's emptiness
      // scan can decide whether the delete is still legal.
      return versionOf(markerKey) == expectedMarkerVersion ? Outcome.HOLDS : Outcome.BROKEN;
    }

    @Override
    public String describe() {
      return "namespace " + namespaceId;
    }
  }
}
