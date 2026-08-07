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
import java.util.Optional;

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

  /**
   * Advances a catalog's children marker on its own, outside any batch that publishes a namespace.
   * A test seam and not how a child is published — see {@link #advanceNamespaceMarker} for why.
   */
  public void bumpCatalogMarker(ResourceId catalogId) {
    String key = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    bumpMarker(key);
  }

  /**
   * Removes a namespace's children marker, which is a pointer row in its own right and otherwise
   * outlives the namespace it belongs to. Its key sits under {@code /accounts/{a}/namespaces/{n}/},
   * outside every prefix the pointer GC and account teardown sweep, so an abandoned one is
   * unreachable for good — and the emptiness gate creates one even for a namespace that never had a
   * child.
   *
   * <p>Call only once the namespace pointer is gone. Until then a concurrent publish may still be
   * fencing against this key; afterwards none can, because {@link #namespaceChildGuard} refuses to
   * build a guard for a namespace with no live pointer.
   */
  public void deleteNamespaceMarker(ResourceId namespaceId) {
    pointerStore.delete(
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId()));
  }

  /**
   * Removes a catalog's children marker, the exact counterpart of {@link #deleteNamespaceMarker}.
   *
   * <p>Its key, {@code /accounts/{a}/catalogs/{c}/markers/children}, sits under the catalog root
   * but outside both pointer families — by-id and by-name — so nothing sweeps it: not the pointer
   * GC, and not the account teardown, which deliberately scopes itself to those two families rather
   * than the root it shares with every nested namespace, table and view (see {@code
   * CatalogRepository#deleteResidualRows}). The emptiness gate creates one for every catalog that
   * ever held a namespace, so without this the row outlives the catalog for good.
   *
   * <p>Call only once the catalog pointer is gone, for the same reason as the namespace case: until
   * then a concurrent namespace publish may still be advancing this key.
   */
  public void deleteCatalogMarker(ResourceId catalogId) {
    pointerStore.delete(Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId()));
  }

  /**
   * Releases every children marker left in an account, for the end of account teardown.
   *
   * <p>Both {@link #deleteCatalogMarker} and {@link #deleteNamespaceMarker} are reached by identity
   * — a walk that resolves the resource and removes its marker alongside its pointer. So a marker
   * whose resource's canonical pointer is already gone is never visited: the catalog walk
   * enumerates by-id rows, and a catalog missing that row is not enumerated. Nothing else sweeps
   * markers either. The catalog residual sweep is scoped to the by-id and by-name families, and
   * {@code /accounts/{a}/namespaces/} — where namespace markers live — has no residual sweep at
   * all. The row then outlives the account for good, unreachable by every path.
   *
   * <p>Filtered to marker rows rather than wiping either root, which keeps the guarantee simple: a
   * marker is a fence and never a handle, so nothing a retried teardown needs to reach a resource
   * can be removed here. That filter is structural and has to be — see {@link
   * #isResourceMarkerKey}, which is what keeps a namespace whose path runs through a segment called
   * {@code markers} from being read as one.
   *
   * <p>{@code accountGone} joins every removal, and this is not the belt-and-braces it might look
   * like. Removing a <em>live</em> namespace's children marker is not untidy, it breaks the fence:
   * a create publishing into that namespace advances a marker this sweep has taken, and a delete
   * whose emptiness scan found the namespace empty then contends on an absent key rather than on
   * that advance — so both commit and the newly created table is orphaned under a namespace that is
   * gone. Exactly the hazard {@link #deleteNamespaceMarker} refuses to risk by requiring the
   * resource pointer to be gone first. Account ids are caller-supplied and reusable, so "the
   * account is gone" is the only thing standing between this sweep and a live namespace's fence,
   * and it has to hold at the removal rather than merely before it. The row versions come from the
   * scan, so pinning each one costs no extra read.
   *
   * @return how many marker rows were released
   */
  public int deleteAccountMarkers(String accountId, BatchGuard accountGone) {
    return deleteAccountMarkers(
        accountId, accountGone, new BaseResourceRepository.GuardedDeleteProgress());
  }

  public int deleteAccountMarkers(
      String accountId,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    return deleteMarkersUnder(Keys.catalogRootPrefix(accountId), accountGone, deleteProgress)
        + deleteMarkersUnder(Keys.namespaceRootPrefix(accountId), accountGone, deleteProgress);
  }

  private int deleteMarkersUnder(
      String rootPrefix,
      BatchGuard accountGone,
      BaseResourceRepository.GuardedDeleteProgress deleteProgress) {
    int deleted = 0;
    var seenTokens = new java.util.HashSet<String>();
    String token = "";
    while (true) {
      var next = new StringBuilder();
      for (var row :
          pointerStore.listPointersByPrefix(
              rootPrefix, MARKER_SWEEP_PAGE_SIZE, token, next, true)) {
        if (isResourceMarkerKey(rootPrefix, row.getKey())) {
          if (BaseResourceRepository.deletePointerWithGuard(
              pointerStore, row, accountGone, deleteProgress.hasPriorWrite())) {
            deleted++;
            deleteProgress.recordWrite();
          }
        }
      }
      token = next.toString();
      if (token.isBlank()) {
        return deleted;
      }
      // A store returning a non-advancing cursor would spin here with nothing observable.
      if (!seenTokens.add(token)) {
        throw new IllegalStateException(
            "pointer scan did not advance; repeated page token: " + token);
      }
    }
  }

  /**
   * Whether {@code key} is the marker row of a resource sitting directly under {@code rootPrefix},
   * rather than any row that merely has a path segment called {@code markers} somewhere inside it.
   *
   * <p>A substring test on {@link Keys#SEG_MARKERS} is not enough, and not only in theory. A
   * namespace by-path row lives under the catalog root this sweep walks — {@code
   * /accounts/{a}/catalogs/{c}/namespaces/by-path/...} — and everything after {@code by-path/} is
   * caller-chosen path segments. A namespace named {@code markers} therefore produces a key
   * containing {@code /markers/}, and that key is the only handle every later walk has for reaching
   * the namespace. Deleting it here would strand that namespace and its whole subtree, on a sweep
   * whose entire remit is rows that fence nothing.
   *
   * <p>Marker keys are exactly {@code <root><id>/markers/<name>}: one id segment, then the fixed
   * marker directory, then a leaf. Nothing nested deeper is one, and neither is anything whose
   * second segment is something else.
   */
  private static boolean isResourceMarkerKey(String rootPrefix, String key) {
    if (key == null || !key.startsWith(rootPrefix)) {
      return false;
    }
    String rest = key.substring(rootPrefix.length());
    int idEnd = rest.indexOf('/');
    if (idEnd <= 0) {
      return false;
    }
    String afterId = rest.substring(idEnd);
    return afterId.startsWith(Keys.SEG_MARKERS)
        && afterId.indexOf('/', Keys.SEG_MARKERS.length()) < 0;
  }

  private static final int MARKER_SWEEP_PAGE_SIZE = 200;

  /**
   * Advances a namespace's children marker on its own, outside any batch that publishes a child.
   *
   * <p><strong>Not how a child is published.</strong> This is the shape {@link BatchGuard} exists
   * to replace — its own note calls a marker advanced outside the publishing batch "an advisory
   * hint rather than a fence", because a delete can pass its emptiness scan in the gap between the
   * advance and the publish. Production code takes {@link #namespaceChildGuard} instead, which
   * advances the marker inside the create's batch. What is left here is a test seam: it lets a test
   * move a marker the way a racing sibling would, which is the one thing no fenced call can do.
   */
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
   * #namespaceChildrenUnchangedGuard}, that makes "child published" and "namespace deleted"
   * mutually exclusive: a delete whose emptiness scan missed this child can no longer commit,
   * because its own batch checks the marker this one moved.
   *
   * <p>Must be built <em>after</em> the caller has resolved and authorized the namespace: the
   * pointer version captured here is the one the publish is pinned to, and a namespace that
   * disappeared in between is refused outright rather than pinned to "absent".
   */
  public BatchGuard namespaceChildGuard(ResourceId namespaceId) {
    return namespaceChildGuard(namespaceId, null);
  }

  /**
   * Builds a child-publish guard only if the namespace pointer still names the blob the caller
   * resolved and authorized.
   *
   * <p>The blob check closes the gap between membership validation and guard construction: a
   * namespace move rewrites its blob, so a publisher validated in the old catalog cannot capture
   * and pin the post-move pointer version.
   */
  public BatchGuard namespaceChildGuard(ResourceId namespaceId, String expectedBlobUri) {
    String namespacePointerKey =
        Keys.namespacePointerById(namespaceId.getAccountId(), namespaceId.getId());
    var namespacePointer = pointerStore.get(namespacePointerKey).orElse(null);
    if (namespacePointer == null) {
      // Refuse to build a guard that would otherwise degrade to "check absent" and happily publish
      // a child into a namespace that is already gone. Retryable: the enclosing RPC re-resolves the
      // namespace and reports the natural NOT_FOUND.
      throw new BaseResourceRepository.BatchGuardFailedException(
          "namespace " + namespaceId.getId() + " no longer exists");
    }
    if (expectedBlobUri != null && !expectedBlobUri.equals(namespacePointer.getBlobUri())) {
      throw new BaseResourceRepository.BatchGuardFailedException(
          "namespace " + namespaceId.getId() + " changed after it was resolved");
    }
    String markerKey =
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return new ChildGuard(
        "namespace " + namespaceId.getId(),
        namespacePointerKey,
        namespacePointer.getVersion(),
        markerKey);
  }

  /**
   * Catalog counterpart of {@link #namespaceChildGuard}: publishing any namespace pins the catalog
   * pointer and advances its children marker in that same batch.
   */
  public BatchGuard catalogChildGuard(ResourceId catalogId) {
    String catalogPointerKey = Keys.catalogPointerById(catalogId.getAccountId(), catalogId.getId());
    long catalogPointerVersion = versionOf(catalogPointerKey);
    if (catalogPointerVersion <= 0L) {
      throw new BaseResourceRepository.BatchGuardFailedException(
          "catalog " + catalogId.getId() + " no longer exists");
    }
    String markerKey = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    return new ChildGuard(
        "catalog " + catalogId.getId(), catalogPointerKey, catalogPointerVersion, markerKey);
  }

  /**
   * Asserts, atomically with the caller's own mutation, that {@code namespaceId} has had no child
   * published since its children marker was at {@code expectedMarkerVersion} — where the caller's
   * scan left it. Any child published since advances the marker via {@link #namespaceChildGuard},
   * which invalidates the scan and makes this batch fail.
   *
   * <p>Two mutations need this, for the same reason: a scan decided something about the namespace's
   * children and a scan cannot join a CAS batch. A delete established that the namespace is empty.
   * A relocation established that it has no children to leave behind at the path it is about to
   * vacate — and unlike the delete it also carries a fence for its <em>destination</em>, so the two
   * are combined with {@link BatchGuard#all}.
   */
  public BatchGuard namespaceChildrenUnchangedGuard(
      ResourceId namespaceId, long expectedMarkerVersion) {
    String markerKey =
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId());
    return new ChildrenUnchangedGuard(
        "namespace " + namespaceId.getId(), markerKey, expectedMarkerVersion);
  }

  /** Catalog counterpart of {@link #namespaceChildrenUnchangedGuard}. */
  public BatchGuard catalogChildrenUnchangedGuard(
      ResourceId catalogId, long expectedMarkerVersion) {
    String markerKey = Keys.catalogChildrenMarker(catalogId.getAccountId(), catalogId.getId());
    return new ChildrenUnchangedGuard(
        "catalog " + catalogId.getId(), markerKey, expectedMarkerVersion);
  }

  /**
   * Guard that pins a namespace to an exact pointer version without touching its children marker,
   * for mutations that must only proceed while that namespace is unchanged.
   *
   * <p>Used by recursive delete to bind every removal inside a namespace to the namespace it
   * scanned: a reparent moves a namespace by advancing its canonical pointer, so if it left the
   * subtree mid-drop, the batches destroying its contents fail instead of emptying a namespace that
   * escaped.
   */
  public BatchGuard namespacePinnedGuard(ResourceId namespaceId, long expectedPointerVersion) {
    return new NamespacePinnedGuard(
        namespaceId.getId(),
        Keys.namespacePointerById(namespaceId.getAccountId(), namespaceId.getId()),
        expectedPointerVersion);
  }

  /**
   * Pins a table-owned publication to the exact live table definition it was issued against. Table
   * deletion removes that canonical pointer before sweeping snapshots and root state, so a
   * publisher paused before its pointer batch cannot resume after the sweep and recreate owned rows
   * beneath a deleted table.
   */
  public BatchGuard tableLiveGuard(ResourceId tableId) {
    String key = Keys.tablePointerById(tableId.getAccountId(), tableId.getId());
    long version = versionOf(key);
    if (version == 0L) {
      throw new BaseResourceRepository.BatchGuardFailedException(
          "table " + tableId.getId() + " no longer exists");
    }
    return new PointerPinnedGuard("table " + tableId.getId(), key, version);
  }

  /**
   * Pins an arbitrary pointer row to an exact version, for a mutation whose justification rests on
   * that row rather than on the resource it names.
   *
   * <p>Used where a scan is the only membership evidence there is: a relation whose blob cannot be
   * read cannot say which namespace owns it, so the by-name row the scan followed is what says it,
   * and a reparent removes that row. Pinning the canonical pointer alone is not enough there — it
   * only catches a move that happens after the canonical read, not one that completed before it and
   * left the read reporting the version the relation now has in its new home.
   *
   * @param subject what the guard is protecting, for the message on a broken guard
   */
  public BatchGuard pointerPinnedGuard(String subject, String pointerKey, long expectedVersion) {
    return new PointerPinnedGuard(subject, pointerKey, expectedVersion);
  }

  /**
   * Asserts, atomically with the caller's own mutation, that {@code pointerKey} is still absent.
   *
   * <p>The mirror image of {@link #pointerPinnedGuard}: where that one binds work to a row that
   * must not change, this binds work to a row that must not come back. Used by account teardown,
   * whose whole justification is that the account pointer is gone — every batch it issues
   * enumerates by an account id the caller supplied and can therefore reuse, so a recreate landing
   * mid-sweep would otherwise have the new account's resources torn down by a sweep that was
   * authorized against the old one.
   *
   * @param subject what the guard is protecting, for the message on a broken guard
   */
  public BatchGuard pointerAbsentGuard(String subject, String pointerKey) {
    return new PointerPinnedGuard(subject, pointerKey, 0L);
  }

  /**
   * Pins the account a top-level resource is about to be published into, so that resource cannot
   * become visible under an account that teardown has already swept.
   *
   * <p>The account is the root of the fence chain — a catalog pins it, a namespace pins its
   * catalog, a table or view pins its namespace — and it was the one level with nothing above it.
   * Account teardown removes the account pointer first and enumerates by prefix afterwards, so a
   * create holding no precondition on that pointer could be authorized before the delete, pause,
   * and commit after the sweep had already passed its prefix: a live catalog or connector under an
   * account that no longer exists, reachable by nothing, and reported to the caller as a completed
   * teardown.
   *
   * <p>Empty when the pointer is already gone. There is no version to pin and nothing left to
   * publish into, so the caller must refuse rather than proceed unguarded — a create that finds the
   * pointer absent is precisely a create the sweep can no longer see. Pinning an exact version is
   * conservative in the same way {@link #catalogChildGuard} is: an unrelated {@code UpdateAccount}
   * trips it too, and the retry re-resolves the account and succeeds against its new version.
   */
  public Optional<BatchGuard> accountLiveGuard(String accountId) {
    String key = Keys.accountPointerById(accountId);
    long version = versionOf(key);
    return version == 0L
        ? Optional.empty()
        : Optional.of(new PointerPinnedGuard("account " + accountId, key, version));
  }

  private long versionOf(String key) {
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }

  /**
   * The precondition for "this row is still exactly as it was read", where version 0 means it was
   * not there.
   *
   * <p>{@link PointerStore.CasCheck} cannot express version 0, and absence is a real thing to pin:
   * a marker the namespace never had, or a canonical pointer this call is holding gone. Version 0
   * is what absence means everywhere else here — {@link #versionOf} reports it for a missing row —
   * so it means the same in a guard rather than being rejected by a constructor.
   */
  private static PointerStore.CasOp stillAt(String key, long expectedVersion) {
    return expectedVersion == 0L
        ? new PointerStore.CasCheckAbsent(key)
        : new PointerStore.CasCheck(key, expectedVersion);
  }

  private final class PointerPinnedGuard implements BatchGuard {
    private final String subject;
    private final String pointerKey;
    private final long expectedVersion;

    private PointerPinnedGuard(String subject, String pointerKey, long expectedVersion) {
      this.subject = subject;
      this.pointerKey = pointerKey;
      this.expectedVersion = expectedVersion;
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      return List.of(stillAt(pointerKey, expectedVersion));
    }

    @Override
    public Outcome reevaluate() {
      // Never RETRY: this row is the evidence the work was chosen on, so its moving invalidates the
      // decision rather than merely delaying it.
      return versionOf(pointerKey) == expectedVersion ? Outcome.HOLDS : Outcome.BROKEN;
    }

    @Override
    public String describe() {
      return subject;
    }
  }

  private final class NamespacePinnedGuard implements BatchGuard {
    private final String namespaceId;
    private final String namespacePointerKey;
    private final long expectedPointerVersion;

    private NamespacePinnedGuard(
        String namespaceId, String namespacePointerKey, long expectedPointerVersion) {
      this.namespaceId = namespaceId;
      this.namespacePointerKey = namespacePointerKey;
      this.expectedPointerVersion = expectedPointerVersion;
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      return List.of(stillAt(namespacePointerKey, expectedPointerVersion));
    }

    @Override
    public Outcome reevaluate() {
      // Never RETRY: the namespace moving or changing invalidates the scan that chose this work.
      return versionOf(namespacePointerKey) == expectedPointerVersion
          ? Outcome.HOLDS
          : Outcome.BROKEN;
    }

    @Override
    public String describe() {
      return "namespace " + namespaceId;
    }
  }

  private final class ChildGuard implements BatchGuard {
    private final String subject;
    private final String parentPointerKey;
    private final long parentPointerVersion;
    private final String markerKey;
    private long markerVersion;

    private ChildGuard(
        String subject, String parentPointerKey, long parentPointerVersion, String markerKey) {
      this.subject = subject;
      this.parentPointerKey = parentPointerKey;
      this.parentPointerVersion = parentPointerVersion;
      this.markerKey = markerKey;
      this.markerVersion = versionOf(markerKey);
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      return List.of(
          new PointerStore.CasCheck(parentPointerKey, parentPointerVersion),
          new PointerStore.CasUpsert(
              markerKey,
              markerVersion,
              PointerReferences.opaqueMarkerPointer(markerKey, markerKey, markerVersion + 1)));
    }

    @Override
    public Outcome reevaluate() {
      if (versionOf(parentPointerKey) != parentPointerVersion) {
        // The parent pointer is pinned to an exact version because CasCheck cannot express
        // "exists at any version", so a plain rename trips this too, not only a delete. That is
        // deliberately conservative and costs nothing: BROKEN is retryable, and a retry
        // re-resolves the parent — succeeding against the new version, or reporting NOT_FOUND if
        // it is truly gone.
        return Outcome.BROKEN;
      }
      long current = versionOf(markerKey);
      if (current != markerVersion) {
        // A sibling published into the same parent. The parent is untouched, so this is ordinary
        // contention on the marker: re-capture and let the caller re-run the batch.
        markerVersion = current;
        return Outcome.RETRY;
      }
      return Outcome.HOLDS;
    }

    @Override
    public String describe() {
      return subject;
    }
  }

  private final class ChildrenUnchangedGuard implements BatchGuard {
    private final String subject;
    private final String markerKey;
    private final long expectedMarkerVersion;

    private ChildrenUnchangedGuard(String subject, String markerKey, long expectedMarkerVersion) {
      this.subject = subject;
      this.markerKey = markerKey;
      this.expectedMarkerVersion = expectedMarkerVersion;
    }

    @Override
    public List<PointerStore.CasOp> ops() {
      // A parent that never had a child has no marker pointer at all, and "still absent" is the
      // correct precondition there. See stillAt.
      return List.of(stillAt(markerKey, expectedMarkerVersion));
    }

    @Override
    public Outcome reevaluate() {
      // Never RETRY: a moved marker means a child may now exist, and only the caller's emptiness
      // scan can decide whether the delete is still legal.
      return versionOf(markerKey) == expectedMarkerVersion ? Outcome.HOLDS : Outcome.BROKEN;
    }

    @Override
    public String describe() {
      return subject;
    }
  }
}
