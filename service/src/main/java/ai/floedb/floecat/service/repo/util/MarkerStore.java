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
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.GenericResourceRepository.PointerConditions;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Version markers for invariants about rows a write does not itself touch.
 *
 * <p>Two kinds of condition live here and they are not interchangeable. A fence -- {@link
 * #childNamespacesFence} and {@link #relationsFence} -- returns a condition the caller folds into
 * its own atomic batch, so the invariant and the write commit together. That is the one to reach
 * for: it is the only form that excludes a concurrent writer.
 *
 * <p>A read dependency ({@link #namespaceStillExistsFence} and the catalog equivalent) requires a
 * pointer at the version just read without advancing it, so writes carrying it do not contend with
 * one another. It excludes the writer that deletes that pointer rather than one racing for the same
 * marker -- which is the case a marker cannot catch, because a write arriving after a delete
 * samples the post-delete version and matches it.
 *
 * <p>Every marker here is asserted inside the caller's own batch. A marker advanced after a commit
 * excludes nothing: between the write and the advance the row exists and the marker has not moved.
 */
@ApplicationScoped
public class MarkerStore {
  @Inject PointerStore pointerStore;

  /**
   * The condition that fences a namespace's set of child namespaces.
   *
   * <p>Reads each marker and, folded into the caller's atomic batch, requires it unchanged and
   * advances it. So two writes that both change the same namespace's child set cannot both commit
   * -- adding a child to it, and changing the identity those children derive their keys from.
   *
   * <p>Checking and then writing cannot substitute for this, and neither can sampling the version
   * after the check: the expected version has to predate the check it fences, or a write that
   * landed in between is the version this reads.
   */
  public PointerConditions childNamespacesFence(ResourceId... namespaceIds) {
    return fence(Keys::namespaceChildrenMarker, namespaceIds);
  }

  /**
   * The condition that fences a namespace's set of relations, on the same terms.
   *
   * <p>Separate from the child-namespace marker because the operations differ. A rename re-keys
   * only what derives its key from the path, which is the child namespaces; a relation carries the
   * namespace id and is untouched. Sharing one marker would make every rename contend with ordinary
   * table traffic. An operation that disturbs both -- a catalog move, a delete -- joins the two.
   *
   * <p>Taken by writers that ADD a relation, and by a namespace delete. Not by a relation delete: a
   * namespace delete racing one can only find the namespace emptier than it counted, which orphans
   * nothing, so asserting it there would cost every relation delete a write to a hot key for an
   * exclusion it does not need.
   */
  public PointerConditions relationsFence(ResourceId... namespaceIds) {
    return fence(Keys::namespaceRelationsMarker, namespaceIds);
  }

  /**
   * The fence for joining a parent's child set: its child marker, its row, and its path.
   *
   * <p>Three conditions, because none of them alone covers the others. The marker is advanced, so
   * two writes changing the same child set cannot both commit. The parent's row is checked, which
   * refuses a write whose parent was deleted -- and a path reused after that delete, since an id
   * never recurs while a by-path version restarts at 1. The parent's path is checked, which refuses
   * a RENAME: that deletes the old by-path key, where it only bumps the row.
   *
   * <p>Identity and version both come out of the one pointer the caller passes, so the path
   * condition and the marker cannot end up describing different namespaces. Taking them as separate
   * arguments would let a caller pair an identity resolved before a path was reused with a version
   * read after -- fencing one namespace while writing under another.
   */
  public PointerConditions childSetFenceForParent(String parentByPathKey, Pointer parentByPath) {
    ResourceId parent = parentByPath.getResourceId();
    if (parent.getId().isBlank()) {
      throw new IllegalArgumentException(
          "parent by-path pointer must carry its resource id: " + parentByPathKey);
    }
    return childNamespacesFence(parent)
        .and(namespaceStillExistsFence(parent))
        .and(
            new PointerConditions(
                Map.of(parentByPathKey, parentByPath.getVersion()), Set.of(), Map.of()));
  }

  /**
   * The condition that a namespace still exists at the version just read.
   *
   * <p>A read-dependency, not an advance: it compiles to a CAS check, so relation writes carrying
   * it do not contend with one another. What it exists to exclude is a namespace delete, whose
   * batch deletes this very pointer -- so a write holding this condition cannot commit across that
   * delete.
   *
   * <p>It is stricter than its name, and deliberately left that way. The condition is on the
   * canonical pointer's version, which ANY namespace update bumps -- a rename, a re-parent, even a
   * property edit -- so a benign concurrent update also costs a relation write its CAS. The cost is
   * a retry, not a failure: {@code FenceRetry} re-samples and tries again, and namespace updates
   * are rare next to relation writes. Narrowing it would mean giving the namespace a separate
   * liveness pointer that only a delete moves, which is a schema change and a second thing to keep
   * in step with the row; the version check needs nothing to stay true.
   *
   * <p>The existence check a service performs before writing cannot stand in for it. That check
   * resolves through the metadata graph, which caches nodes per process and is not invalidated by a
   * delete on another instance, so it can pass for a namespace that is already gone. This is read
   * from the pointer and settled by the same batch as the write.
   *
   * @throws BaseResourceRepository.NotFoundException when the namespace is already gone. Requiring
   *     version 0 would be a requirement that it be ABSENT, which such a write would satisfy -- so
   *     absence has to be refused here rather than folded into the condition.
   */
  public PointerConditions namespaceStillExistsFence(ResourceId namespaceId) {
    return stillExistsFence(
        Keys.namespacePointerById(namespaceId.getAccountId(), namespaceId.getId()),
        ResourceKind.RK_NAMESPACE,
        namespaceId);
  }

  /**
   * The same condition on a catalog's own row. See {@link #namespaceStillExistsFence}.
   *
   * <p>Private, unlike its namespace counterpart, because nothing outside this class composes it
   * directly -- and a loose read-dependency helper is what a caller reaches for instead of the
   * composed fence that also carries the marker.
   */
  private PointerConditions catalogStillExistsFence(ResourceId catalogId) {
    return stillExistsFence(
        Keys.catalogPointerById(catalogId.getAccountId(), catalogId.getId()),
        ResourceKind.RK_CATALOG,
        catalogId);
  }

  private PointerConditions stillExistsFence(String canonical, ResourceKind kind, ResourceId id) {
    long version =
        pointerStore
            .get(canonical)
            .map(Pointer::getVersion)
            .orElseThrow(
                () ->
                    new BaseResourceRepository.NotFoundException(
                        kind, kind.name() + " no longer exists: " + id.getId()));
    return new PointerConditions(Map.of(canonical, version), Set.of(), Map.of());
  }

  /**
   * A namespace's own shape markers, as one sampled observation split by what each requires.
   *
   * <p>The same shape as {@link #catalogChildSetMarkers} one level down, and for the same reason: a
   * delete removes the markers rather than advancing them, because the row they count for is going
   * and an advanced marker would be left counting nothing. Exclusion is unaffected -- requiring a
   * key at a version and then writing it excludes a concurrent writer whether the write is an
   * advance or a removal.
   */
  public MarkerRemoval namespaceShapeMarkers(ResourceId namespaceId) {
    return sampleMarkers(
        Keys.namespaceChildrenMarker(namespaceId.getAccountId(), namespaceId.getId()),
        Keys.namespaceRelationsMarker(namespaceId.getAccountId(), namespaceId.getId()));
  }

  /**
   * The fence for adding a namespace to a catalog.
   *
   * <p>The same two halves as {@link #relationCreateFence}, one level up: the catalog's children
   * marker is advanced, which is what a catalog delete racing this write loses to, and the
   * catalog's own row is checked, which refuses a write against a catalog whose delete already
   * finished -- the marker cannot catch that, because a write arriving afterwards samples the
   * post-delete version and matches it.
   */
  public PointerConditions catalogChildNamespacesFence(ResourceId catalogId) {
    return fence(Keys::catalogChildrenMarker, catalogId).and(catalogStillExistsFence(catalogId));
  }

  /**
   * Markers asserted and then removed with the row they count for, as one sampled observation.
   *
   * <p>Carries the conditions already composed rather than the raw absent-marker set, so the shape
   * that expresses "required absent" exists in one place. A caller with a fence of its own adds it
   * with {@link #and}; a caller without one passes this straight through.
   *
   * <p>The split behind {@code conditions} is this class's knowledge, not the caller's: a marker
   * that has never been written reads as version zero, so it is required ABSENT -- the writer that
   * adds the first child writes it, and that write then loses the batch carrying this. One that
   * exists is required at its version and removed, because the resource it counts for is going and
   * advancing would leave a row counting nothing -- or, for a marker never written, would CREATE
   * one.
   *
   * @param conditions what must still hold for the removal to commit
   * @param toDelete markers to remove in the same batch, at the versions read
   */
  public record MarkerRemoval(PointerConditions conditions, Map<String, Long> toDelete) {

    /** The same removal, additionally requiring {@code extra}. */
    public MarkerRemoval and(PointerConditions extra) {
      return new MarkerRemoval(conditions.and(extra), toDelete);
    }
  }

  /**
   * Samples both of a catalog's child-set markers together.
   *
   * <p>Both, because a catalog holds two kinds of child counted separately -- namespaces and
   * overlays -- and a delete asserting only one can be raced by the other.
   *
   * <p>A delete asserting these is not gated on account deletion: the repository adds that check to
   * the batches it builds for creates and updates, not to the one behind a delete. This delete
   * creates no pointer, so nothing it removes can be resurrected by a teardown running alongside
   * it.
   *
   * <p>Sampled by the caller BEFORE the emptiness checks it guards. A version read after those is
   * the version a concurrent create already moved, so the CAS would confirm that create instead of
   * losing to it.
   */
  public MarkerRemoval catalogChildSetMarkers(ResourceId catalogId) {
    String account = catalogId.getAccountId();
    String id = catalogId.getId();
    return sampleMarkers(
        Keys.catalogChildrenMarker(account, id), Keys.catalogOverlaysMarker(account, id));
  }

  /**
   * Samples the given markers together and splits them by what each requires.
   *
   * <p>A marker that has never been written reads as absent and is required absent: the writer that
   * adds the first child writes it, and that write then loses the batch carrying this. One that
   * exists is required at its version and removed with the row it counts for.
   */
  private MarkerRemoval sampleMarkers(String... markerKeys) {
    var absent = new LinkedHashSet<String>();
    var toDelete = new LinkedHashMap<String, Long>();
    for (String marker : markerKeys) {
      long version = versionOf(marker);
      if (version == 0L) {
        absent.add(marker);
      } else {
        toDelete.put(marker, version);
      }
    }
    return new MarkerRemoval(new PointerConditions(Map.of(), absent, Map.of()), toDelete);
  }

  /**
   * The fence for a relation update that may be changing where the relation lives.
   *
   * <p>An update that leaves a relation where it is changes no namespace's relation set, so there
   * is nothing for a namespace delete or catalog move to be excluded from -- and asserting anyway
   * would serialize every ordinary relation update on the namespace's marker.
   *
   * <p>"Where it is" is the container the relation is COUNTED in, which is the catalog and the
   * namespace together -- not the namespace alone. The marker is keyed by namespace, but the
   * emptiness check it guards counts by {@code (account, catalog, namespace)}, so an update that
   * changes only the catalog still moves the relation from one count to another while leaving the
   * namespace id alone. Reading that as "not moving" would assert nothing on a write that re-keys
   * the relation -- the non-participating writer this protocol says voids the guard rather than
   * weakening it.
   *
   * <p>"The same namespace" means the same account AND the same id. A resource id is unique only
   * within its account, and every key this fence asserts is account-scoped, so comparing the local
   * id alone would read a move between two accounts that happen to share one as a no-op.
   *
   * @param changesCatalog whether the update also re-keys the relation into another catalog
   */
  public PointerConditions relationMoveFence(
      ResourceId from, ResourceId to, boolean changesCatalog) {
    boolean sameNamespace =
        from.getAccountId().equals(to.getAccountId()) && from.getId().equals(to.getId());
    if (sameNamespace && !changesCatalog) {
      return PointerConditions.none();
    }
    // When only the catalog moves, both sides name one namespace, so relationsFence collapses to
    // that single marker -- exactly the assertion a create into it would make.
    return relationsFence(from, to).and(namespaceStillExistsFence(to));
  }

  /**
   * The fence for adding a relation to a namespace.
   *
   * <p>Two conditions, because they exclude different writers. The marker is advanced, which is
   * what a namespace delete racing this write loses to. The namespace's own pointer version is only
   * checked, which is what refuses a write against a namespace whose delete already finished -- the
   * marker cannot catch that one, because a write arriving afterwards samples the post-delete
   * version and matches it.
   */
  public PointerConditions relationCreateFence(ResourceId namespaceId) {
    return relationsFence(namespaceId).and(namespaceStillExistsFence(namespaceId));
  }

  private PointerConditions fence(
      BiFunction<String, String, String> markerKey, ResourceId... namespaceIds) {
    Map<String, Long> markers = new LinkedHashMap<>();
    for (ResourceId namespaceId : namespaceIds) {
      Objects.requireNonNull(namespaceId, "namespaceId");
      if (namespaceId.getAccountId().isBlank() || namespaceId.getId().isBlank()) {
        throw new IllegalArgumentException("namespaceId must carry an account and an id to fence");
      }
      markers.computeIfAbsent(
          markerKey.apply(namespaceId.getAccountId(), namespaceId.getId()), this::versionOf);
    }
    return new PointerConditions(Map.of(), Set.of(), markers);
  }

  private long versionOf(String markerKey) {
    return pointerStore.get(markerKey).map(Pointer::getVersion).orElse(0L);
  }

  public long catalogIntegrationOverlaysMarkerVersion(ResourceId integrationId) {
    String key =
        Keys.catalogIntegrationOverlaysMarker(integrationId.getAccountId(), integrationId.getId());
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }

  public long catalogOverlaysMarkerVersion(ResourceId catalogId) {
    String key = Keys.catalogOverlaysMarker(catalogId.getAccountId(), catalogId.getId());
    return pointerStore.get(key).map(Pointer::getVersion).orElse(0L);
  }
}
