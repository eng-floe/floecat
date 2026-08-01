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

package ai.floedb.floecat.service.catalog.impl;

import ai.floedb.floecat.catalog.rpc.Namespace;
import ai.floedb.floecat.catalog.rpc.Table;
import ai.floedb.floecat.catalog.rpc.View;
import ai.floedb.floecat.common.rpc.MutationMeta;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.scanner.spi.TopologyGraph;
import ai.floedb.floecat.service.metagraph.overlay.user.UserGraph;
import ai.floedb.floecat.service.repo.impl.NamespaceRepository;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.impl.ViewRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.BatchGuard;
import ai.floedb.floecat.service.repo.util.MarkerStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.jboss.logging.Logger;

/** Removes the dependent state owned by tables and namespace trees. */
@ApplicationScoped
public class RecursiveResourceDropper {

  // Per-resource audit trail for irreversible teardown (account delete and recursive namespace
  // delete). Each purged namespace, table, view, and snapshot prefix is named so the destruction
  // can be reconstructed after the fact.
  private static final Logger CLEANUP_LOG = Logger.getLogger(RecursiveResourceDropper.class);

  @Inject NamespaceRepository namespaceRepo;
  @Inject TableRepository tableRepo;
  @Inject TableRootRepository tableRoots;
  @Inject ViewRepository viewRepo;
  @Inject StatsRepository statsRepo;
  @Inject UserGraph metadataGraph;
  @Inject TopologyGraph topology;
  @Inject MarkerStore markerStore;
  @Inject PointerStore pointerStore;

  /** Drops all descendants of {@code root}, leaving the root namespace for its caller to delete. */
  public DropSummary dropNamespaceContents(Namespace root) {
    return dropNamespaceContents(root, new DropSummary());
  }

  /**
   * Guarded {@link #dropNamespaceContents(Namespace)} that accumulates into {@code summary} instead
   * of returning a fresh one.
   *
   * <p>For callers that retry: this drop is irreversible but can abort part-way, and it can be
   * re-entered after a concurrent child publish. Counting into a summary the caller owns means what
   * an aborted attempt destroyed is still visible to the attempt that finally reports the outcome —
   * a per-call summary would report only the last attempt, which sees an already-emptied subtree.
   */
  public DropSummary dropNamespaceContents(Namespace root, DropSummary summary) {
    return dropNamespaceContents(root, true, summary);
  }

  /**
   * Drops all descendants of {@code root}, leaving the root namespace for its caller to delete.
   *
   * <p>When {@code guarded} is true, each descendant is verified empty and stable under the
   * namespace-marker protocol before removal, so a concurrently created table or immediate child
   * cannot be orphaned; the caller is expected to have advanced the root marker first. When false —
   * account teardown, where the whole tree (and its account pointer) is going away — descendants
   * are dropped unconditionally so cleanup never raises {@link
   * BaseResourceRepository.AbortRetryableException}, which would otherwise retry the account delete
   * after its pointer is gone and skip cleanup entirely.
   *
   * <p>In both modes, no removal advances any namespace's children marker (see {@link
   * #deleteNamespace(Namespace, BatchGuard, long)}), so a guarded caller can assert the root marker
   * moved exactly once — its own advance — and still detect a genuine concurrent write to the
   * root's immediate children.
   */
  public DropSummary dropNamespaceContents(Namespace root, boolean guarded) {
    return dropNamespaceContents(root, guarded, new DropSummary());
  }

  private DropSummary dropNamespaceContents(Namespace root, boolean guarded, DropSummary summary) {
    var rootId = root.getResourceId();
    var rootPath = new ArrayList<>(root.getParentsList());
    rootPath.add(root.getDisplayName());
    // Only the root's own subtree, by path prefix, rather than the whole catalog — and from the
    // by-path pointer rows, not their content: a row carries the id and full path the drop needs,
    // while a content-bearing scan would fail outright on one unparseable namespace blob, mid-drop,
    // after everything deeper had already been destroyed.
    forEachNamespaceDeepestFirst(
        rootId.getAccountId(),
        root.getCatalogId(),
        rootPath,
        ref ->
            dropNamespace(namespaceFromRef(ref, root.getCatalogId()), summary, rootPath, guarded));
    // The root's own relations are pinned to the root the caller resolved, so a concurrent reparent
    // of the root cannot have its contents emptied out from under it either.
    dropNamespaceRelations(root, summary, guarded, subtreePin(rootId, guarded));
    return summary;
  }

  /** Drops a namespace and every object it owns, including descendant namespaces. */
  public DropSummary dropNamespaceTree(Namespace root) {
    // Account teardown: the whole tree is going away, so drop unconditionally rather than under the
    // marker guard. A guarded drop can raise AbortRetryableException, which would retry the account
    // delete after its pointer is already gone and skip cleanup entirely, orphaning resources.
    var summary = dropNamespaceContents(root, false);
    if (deleteNamespace(root)) {
      summary.namespacesDeleted++;
    }
    return summary;
  }

  /**
   * Tears down every namespace in a catalog, and everything they own, deepest-first.
   *
   * <p>Enumerated from by-path pointer rows rather than content, because this runs after the
   * account pointer is gone and so cannot be retried: a namespace whose blob does not parse must
   * still be removed, not abort the sweep. Every namespace in the catalog is visited, not only the
   * top-level ones — a damaged tree can leave a deep namespace whose ancestors are already gone,
   * and nothing else would ever reach it.
   */
  public DropSummary dropCatalogNamespaces(String accountId, ResourceId catalogId) {
    var summary = new DropSummary();
    forEachNamespaceDeepestFirst(
        accountId,
        catalogId,
        List.of(),
        ref -> dropNamespace(namespaceFromRef(ref, catalogId), summary, List.of(), false));
    return summary;
  }

  /**
   * Hands every namespace strictly under {@code rootPath} to {@code drop}, each one only after
   * everything beneath it, while holding no more than one namespace per level of depth.
   *
   * <p>A namespace can only be deleted once its children are gone, and a by-path scan runs the
   * other way: parents first. Collecting the subtree to reverse it made peak memory proportional to
   * the subtree — the one unbounded allocation on a teardown path that runs after the account
   * pointer is gone, where exhausting the heap orphans whatever the sweep had not reached,
   * permanently.
   *
   * <p>No collection is needed, but the test for "this one's subtree is finished" has to be about
   * key ranges rather than about descent. A by-path key ends at its last segment with no trailing
   * delimiter, so a namespace's descendants are <em>not</em> the only rows that follow it: {@code
   * db/orders}, {@code db/orders-2024} and {@code db/orders/archive} sort in that order, because
   * the separator {@code /} (0x2F) sorts after the {@code -} that {@code Keys.encode} leaves
   * literal. Popping {@code db/orders} when the sibling arrives would drop it while its child is
   * still to come — and for a guarded recursive delete that is fatal rather than untidy: the
   * emptiness gate then fails on the surviving child, deterministically, on that attempt and on
   * every retry.
   *
   * <p>So a namespace stays open until a key arrives that does not begin with its key at all. Every
   * descendant's key begins with {@code key + "/"}, keys sharing a prefix are contiguous, and the
   * stack is LIFO — so when one is finally popped, everything beneath it has already been dropped.
   * A name that merely extends another ({@code orders-2024} over {@code orders}) rides on top of it
   * and is dropped first, which is harmless. The stack holds one entry per key that is a prefix of
   * the next, bounded by key length rather than by the size of the subtree.
   */
  private void forEachNamespaceDeepestFirst(
      String accountId,
      ResourceId catalogId,
      List<String> rootPath,
      java.util.function.Consumer<TopologyGraph.NamespaceRef> drop) {
    var open = new java.util.ArrayDeque<OpenNamespace>();
    namespaceRepo.forEachRefUnder(
        accountId,
        catalogId.getId(),
        rootPath,
        ref -> {
          // The prefix scan over-returns the root itself; it is the caller's to delete, not ours.
          if (!isDescendantPath(ref.pathSegments(), rootPath)) {
            return;
          }
          String key =
              Keys.namespacePointerByPath(accountId, catalogId.getId(), ref.pathSegments());
          while (!open.isEmpty() && !key.startsWith(open.peek().key())) {
            drop.accept(open.pop().ref());
          }
          open.push(new OpenNamespace(key, ref));
        },
        unresolvable -> reclaimUnresolvableNamespacePath(accountId, rootPath, unresolvable));
    while (!open.isEmpty()) {
      drop.accept(open.pop().ref());
    }
  }

  /**
   * A namespace the walk has seen but cannot drop yet, with the key whose range holds its subtree.
   */
  private record OpenNamespace(String key, TopologyGraph.NamespaceRef ref) {}

  /**
   * Removes a table after its pointer has already been deleted through a public mutation.
   *
   * <p>Deliberately does not touch the owning namespace's children marker. That marker is the
   * child-publish fence and nothing else: {@code namespaceChildGuard} advances it inside the batch
   * that publishes a child, and {@code namespaceDeleteGuard} refuses a namespace delete whose
   * emptiness scan predates such an advance. A table leaving the namespace is the opposite of a
   * publish, so advancing the marker here would break a concurrent {@code DeleteNamespace} that
   * nothing had invalidated — and losing that guard is not retryable, so a non-recursive delete
   * would report NAMESPACE_CHILDREN_CHANGED to a caller racing nothing but a table going away.
   */
  public void cleanupDeletedTable(ResourceId tableId) {
    topology.evict(tableId);
    metadataGraph.invalidate(tableId);
    pointerStore.deleteByPrefix(Keys.snapshotRootPrefix(tableId.getAccountId(), tableId.getId()));
    // Per-snapshot stats live under /snapshots/ (removed by the prefix above); the table-level and
    // per-target "latest committed" stats pointers/blobs live outside it and must be purged too, or
    // they outlive the deleted table as durable orphans.
    statsRepo.deleteAllStatsForTable(tableId);
    tableRoots.purgeRoot(tableId);
    pointerStore.delete(Keys.rootResyncPendingPointer(tableId.getAccountId(), tableId.getId()));
  }

  private void dropNamespace(
      Namespace namespace, DropSummary summary, List<String> rootPath, boolean guarded) {
    var namespaceId = namespace.getResourceId();

    if (!guarded) {
      dropNamespaceRelations(namespace, summary, false, BatchGuard.NONE);
      // Count only a namespace this call actually removed — teardown reaches namespaces an earlier
      // subtree already took, and this number is the reconstruction record for an irreversible
      // operation. dropNamespaceTree already worked this way; this branch did not.
      if (deleteNamespace(namespace, BatchGuard.NONE, UNPINNED)) {
        summary.namespacesDeleted++;
      }
      return;
    }

    // Confirm this scanned descendant is STILL inside the subtree before destroying anything in it,
    // and capture the pointer version that proved it. A reparent out of the subtree advances that
    // pointer, so pinning every removal below to this version means a namespace that escaped
    // mid-drop
    // is neither emptied nor deleted.
    var pinned = pinDescendantToSubtree(namespace, rootPath);
    if (pinned.isEmpty()) {
      // The namespace itself is gone, but the by-path row this walk followed is not — a concurrent
      // delete that could only remove the canonical pointer leaves exactly that. Every
      // immediate-child probe counts that row, so leaving it wedges the drop for good: the parent
      // reports a child that cannot be resolved, let alone deleted.
      CLEANUP_LOG.infof(
          "recursive_drop_namespace_skipped_absent account_id=%s namespace_id=%s",
          namespaceId.getAccountId(), namespaceId.getId());
      // Its relations do NOT go with it: they are keyed by namespace id, not by path, so they
      // outlive the namespace's own pointer. Drop them first and release the by-path row only
      // afterwards — that row is the only handle any later scan has for reaching this namespace, so
      // releasing it while tables remain would strand them where no emptiness gate and no teardown
      // sweep can ever walk to them again. Nothing survives to pin against, so removals rest on
      // each
      // relation's own canonical version, which still fails a CAS for anything reparented out.
      dropNamespaceRelations(namespace, summary, true, BatchGuard.NONE);
      reclaimStrandedNamespacePath(namespace);
      // Its children marker is a pointer row in its own right, outside every prefix the GC and
      // teardown sweep, so whoever removed the canonical pointer without it left a row nothing can
      // ever reach again. This is the last walk that knows the id.
      markerStore.deleteNamespaceMarker(namespaceId);
      return;
    }
    // Both guards below also assert the placement proof, when there is one: for a namespace whose
    // blob cannot be read, the by-path row is the only thing that says where it lives, so every
    // batch that destroys something has to contend on it rather than trust a read that preceded it.
    var placement = pinned.get().placement();
    var subtreePin =
        withPlacement(
            markerStore.namespacePinnedGuard(namespaceId, pinned.get().pointerVersion()),
            placement);

    // Everything below works from the namespace the pin resolved, not the row the walk started
    // from. They differ after an in-place rename: the scan's path is then stale, and probing for
    // children under a stale path would miss one created under the new name and delete this
    // namespace out from over a live child.
    var resolved = pinned.get().resolved();
    dropNamespaceRelations(resolved, summary, true, subtreePin);
    var childrenGuard = withPlacement(requireNamespaceEmptyAndStable(resolved), placement);
    deleteNamespace(resolved, childrenGuard, pinned.get().pointerVersion());
    summary.namespacesDeleted++;
  }

  /** Sentinel for "no pointer version pinned" — account teardown, or a resource already gone. */
  private static final long UNPINNED = -1L;

  /**
   * Pins the root's own relations to the root pointer the caller resolved, so a concurrent reparent
   * of the root cannot have its contents emptied. Unguarded teardown pins nothing.
   */
  private BatchGuard subtreePin(ResourceId namespaceId, boolean guarded) {
    if (!guarded) {
      return BatchGuard.NONE;
    }
    long version = namespaceRepo.metaForSafe(namespaceId).getPointerVersion();
    if (version == 0L) {
      throw namespaceChanged(namespaceId);
    }
    return markerStore.namespacePinnedGuard(namespaceId, version);
  }

  /**
   * Re-reads a scanned descendant and returns the canonical pointer version that proves it is still
   * under {@code rootPath}, together with the namespace that version resolved to — empty when it no
   * longer exists at all.
   *
   * <p>The version and the content come from a single pointer read, so the pair is coherent: the
   * namespace path checked here is exactly the one that version resolved to. Verifying membership
   * against a separately-read version could otherwise pin a resource that had already moved.
   *
   * <p>Handing the resolved namespace back matters as much as the version. The row the walk started
   * from carries the path as it was scanned, and an in-place rename makes that stale while leaving
   * the namespace inside the subtree — so callers must work from what was verified here, not from
   * what was scanned.
   */
  private Optional<DescendantPin> pinDescendantToSubtree(Namespace scanned, List<String> rootPath) {
    var namespaceId = scanned.getResourceId();
    var meta = namespaceRepo.metaForSafe(namespaceId);
    if (meta.getPointerVersion() == 0L) {
      return Optional.empty();
    }
    Optional<Namespace> live;
    try {
      live = namespaceRepo.getByBlobUri(meta.getBlobUri());
    } catch (BaseResourceRepository.CorruptionException unparseable) {
      // Same reasoning as a relation with an unreadable blob (see pinToNamespace), and the stakes
      // are higher here: descendants are dropped deepest-first, so refusing at this point aborts an
      // operation that has already destroyed everything below this namespace.
      CLEANUP_LOG.warnf(
          "recursive_drop_namespace_blob_unparseable account_id=%s namespace_id=%s blob_uri=%s",
          namespaceId.getAccountId(), namespaceId.getId(), meta.getBlobUri());
      return pinToScannedPath(scanned, meta.getPointerVersion(), rootPath);
    }
    if (live.isEmpty()) {
      // The blob is gone rather than unreadable, which is just as stable: aborting here would
      // report
      // NAMESPACE_RECURSIVE_PARTIAL on every attempt, after this namespace's own descendants are
      // already destroyed, with nothing a retry could change. Same treatment as the unparseable
      // case.
      CLEANUP_LOG.warnf(
          "recursive_drop_namespace_blob_absent account_id=%s namespace_id=%s blob_uri=%s",
          namespaceId.getAccountId(), namespaceId.getId(), meta.getBlobUri());
      return pinToScannedPath(scanned, meta.getPointerVersion(), rootPath);
    }
    if (!isDescendant(live.get(), scanned.getCatalogId(), rootPath)) {
      throw movedOutOfSubtree(namespaceId, String.join(".", rootPath));
    }
    return Optional.of(new DescendantPin(meta.getPointerVersion(), live.get()));
  }

  /**
   * Placement for a namespace that cannot state its own: proved by the by-path row the walk
   * followed still naming it, and carried into every batch below so a concurrent move loses.
   *
   * <p>The canonical version alone proves nothing about location. It is read here, after any move
   * that already happened, so a namespace reparented out of the subtree before this read passes the
   * CAS and would be destroyed where it now lives — and with an unreadable blob there is no path in
   * the content to fall back on. The by-path row is the only remaining evidence: a reparent removes
   * the row at the old path and writes one at the new, so a row still naming this namespace at the
   * scanned path is proof it has not moved, and pinning that row's version keeps it that way for
   * the batches that follow.
   *
   * <p>A row that names something else — or nothing — means a concurrent move or rename, which is a
   * genuine conflict rather than a permanent state: refuse, and let the retry re-scan and find it
   * wherever it now is.
   */
  private Optional<DescendantPin> pinToScannedPath(
      Namespace scanned, long pointerVersion, List<String> rootPath) {
    var namespaceId = scanned.getResourceId();
    var path = new ArrayList<>(scanned.getParentsList());
    path.add(scanned.getDisplayName());
    String byPathKey =
        Keys.namespacePointerByPath(
            namespaceId.getAccountId(), scanned.getCatalogId().getId(), path);
    var row = pointerStore.get(byPathKey).orElse(null);
    if (row == null || !namespaceId.getId().equals(ownerIdOf(row))) {
      throw movedOutOfSubtree(namespaceId, String.join(".", rootPath));
    }
    return Optional.of(
        new DescendantPin(
            pointerVersion, scanned, new PlacementProof(byPathKey, row.getVersion())));
  }

  /** The by-path row that proves where a blob-less namespace lives, at the version proved. */
  private record PlacementProof(String key, long version) {}

  /**
   * {@code guard} with the placement proof folded in, so the batch it protects commits only while
   * the by-path row that placed this namespace is still exactly as read. A no-op when there is no
   * proof to carry — a namespace whose blob parses states its own path and is checked against it.
   *
   * <p>The outcome of the combined guard is the more severe of the two: a moved placement row is
   * never benign contention, so it reports BROKEN and the drop refuses rather than retrying into
   * the same conflict.
   */
  private BatchGuard withPlacement(BatchGuard guard, PlacementProof placement) {
    if (placement == null) {
      return guard;
    }
    return new BatchGuard() {
      @Override
      public List<PointerStore.CasOp> ops() {
        var ops = new ArrayList<>(guard.ops());
        ops.add(new PointerStore.CasCheck(placement.key(), placement.version()));
        return ops;
      }

      @Override
      public Outcome reevaluate() {
        var row = pointerStore.get(placement.key()).orElse(null);
        if (row == null || row.getVersion() != placement.version()) {
          return Outcome.BROKEN;
        }
        return guard.reevaluate();
      }

      @Override
      public String describe() {
        return guard.describe() + " at " + placement.key();
      }
    };
  }

  /**
   * The canonical pointer version a scanned descendant is pinned to, and the namespace that version
   * resolved to — the live one where its blob could be read, the scanned row where it could not.
   * Emptiness probes and the delete itself must both use {@code resolved}: it carries the path the
   * namespace has now, which an in-place rename makes differ from the path the walk found it under.
   *
   * <p>{@code placement} is set only for the blob-less cases, where the row is the sole evidence of
   * location; a readable namespace states its own path and is checked against it directly.
   */
  private record DescendantPin(long pointerVersion, Namespace resolved, PlacementProof placement) {
    DescendantPin(long pointerVersion, Namespace resolved) {
      this(pointerVersion, resolved, null);
    }
  }

  /**
   * Uses the same marker protocol as ordinary namespace deletion. This prevents a concurrently
   * created table or immediate child namespace from being orphaned by a recursive parent drop.
   *
   * @return the fence to carry into this descendant's delete batch, asserting the marker is still
   *     where these scans left it. The scans alone cannot close the window — they are reads, and a
   *     read cannot join a CAS batch — so the delete itself has to contend on the marker that every
   *     child-publishing write advances (see {@link BatchGuard}).
   */
  private BatchGuard requireNamespaceEmptyAndStable(Namespace namespace) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    var parentPath = new ArrayList<>(namespace.getParentsList());
    parentPath.add(namespace.getDisplayName());
    long markerVersion = markerStore.namespaceMarkerVersion(namespaceId);

    if (hasRelations(namespaceId, catalogId) || hasImmediateChildren(catalogId, parentPath)) {
      throw namespaceChanged(namespaceId);
    }
    if (!markerStore.advanceNamespaceMarker(namespaceId, markerVersion)) {
      throw namespaceChanged(namespaceId);
    }
    if (markerStore.namespaceMarkerVersion(namespaceId) != markerVersion + 1
        || hasRelations(namespaceId, catalogId)
        || hasImmediateChildren(catalogId, parentPath)) {
      throw namespaceChanged(namespaceId);
    }
    return markerStore.namespaceDeleteGuard(namespaceId, markerVersion + 1);
  }

  private boolean hasRelations(ResourceId namespaceId, ResourceId catalogId) {
    return tableRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0
        || viewRepo.count(namespaceId.getAccountId(), catalogId.getId(), namespaceId.getId()) > 0;
  }

  /**
   * Whether {@code parentPath} has a direct child namespace. Reads by-path rows rather than
   * content: this gates a delete, so an unparseable child must be able to block it — but not by
   * failing the probe outright.
   */
  private boolean hasImmediateChildren(ResourceId catalogId, List<String> parentPath) {
    return namespaceRepo.hasChildUnder(catalogId.getAccountId(), catalogId.getId(), parentPath);
  }

  private static BaseResourceRepository.AbortRetryableException namespaceChanged(
      ResourceId namespaceId) {
    return new BaseResourceRepository.AbortRetryableException(
        "namespace children changed during recursive delete: " + namespaceId.getId());
  }

  private static BaseResourceRepository.AbortRetryableException relationChanged(
      ResourceId relationId) {
    return new BaseResourceRepository.AbortRetryableException(
        "relation changed during recursive delete: " + relationId.getId());
  }

  private static BaseResourceRepository.AbortRetryableException movedOutOfSubtree(
      ResourceId resourceId, String from) {
    return new BaseResourceRepository.AbortRetryableException(
        "resource moved out of the subtree during recursive delete: "
            + resourceId.getId()
            + " no longer under "
            + from);
  }

  /**
   * Resolves the pointer version to delete a scanned resource at, but only while it still belongs
   * to the part of the tree being dropped.
   *
   * <p>The subtree scans that feed this dropper produce ids, and deleting by id resolves whatever
   * that id points at <em>now</em>. A concurrent reparent that moved the resource out between the
   * scan and the delete would therefore have it destroyed in its new home, along with its owned
   * state — a silent data loss that no marker can prevent, because a marker can only be checked
   * after the fact.
   *
   * <p>The fix is to make membership and the delete precondition come from the same observation.
   * {@code metaReader} performs a single canonical-pointer read, yielding a coherent {@code
   * (blobUri, pointerVersion)} pair; {@code ownerOfBlob} then reads the content <em>that exact
   * version</em> pointed at and reports which namespace it declared. Deleting pinned to that
   * version means any later mutation — every reparent advances the canonical pointer — loses the
   * CAS instead of destroying a resource that had legitimately moved away.
   *
   * @return the version to delete at, or empty when the resource is already gone (a concurrent
   *     delete won and ran the same cleanup, so there is nothing left to do)
   */
  private Optional<RelationPin> pinToNamespace(
      ResourceId resourceId,
      ResourceId expectedOwner,
      Function<ResourceId, MutationMeta> metaReader,
      Function<String, Optional<ResourceId>> ownerOfBlob) {
    var meta = metaReader.apply(resourceId);
    if (meta.getPointerVersion() == 0L) {
      return Optional.empty();
    }
    Optional<ResourceId> owner;
    try {
      owner = ownerOfBlob.apply(meta.getBlobUri());
    } catch (BaseResourceRepository.CorruptionException unparseable) {
      // Present but unparseable, so the relation cannot state which namespace owns it. Membership
      // still rests on evidence rather than assumption: this relation was reached through a by-name
      // pointer under THIS namespace's prefix, and the delete is pinned to the canonical version
      // read here — a reparent advances that pointer, so a relation that moved out loses the CAS
      // instead of being destroyed in its new home. Refusing here would instead let one corrupt
      // blob make the whole subtree permanently undeletable, which is the state a recursive delete
      // exists to clear.
      CLEANUP_LOG.warnf(
          "recursive_drop_relation_blob_unparseable account_id=%s namespace_id=%s resource_id=%s"
              + " blob_uri=%s",
          resourceId.getAccountId(), expectedOwner.getId(), resourceId.getId(), meta.getBlobUri());
      return Optional.of(new RelationPin(meta.getPointerVersion(), false));
    }
    if (owner.isEmpty()) {
      // Either the pointer moved between the version read and the content read, or it dangles. The
      // first is a race and the second never resolves on its own, so aborting would burn the whole
      // retry budget and leave the namespace undeletable — the same dead end an unparseable blob
      // used
      // to produce. Treat it like that case and delete on pointer evidence: pinning to the version
      // read above means a pointer that really did move loses its CAS instead.
      CLEANUP_LOG.warnf(
          "recursive_drop_relation_blob_absent account_id=%s namespace_id=%s resource_id=%s"
              + " blob_uri=%s",
          resourceId.getAccountId(), expectedOwner.getId(), resourceId.getId(), meta.getBlobUri());
      return Optional.of(new RelationPin(meta.getPointerVersion(), false));
    }
    if (!owner.get().getId().equals(expectedOwner.getId())) {
      throw movedOutOfSubtree(resourceId, expectedOwner.getId());
    }
    return Optional.of(new RelationPin(meta.getPointerVersion(), true));
  }

  /**
   * The canonical pointer version a scanned relation is pinned to, and whether its blob could be
   * read to confirm the namespace it declares. An unverified pin still deletes — see {@link
   * #pinToNamespace} — but leaves the relation's index rows behind, since the repository can only
   * drop the canonical pointer when it cannot parse the resource.
   */
  private record RelationPin(long pointerVersion, boolean membershipVerified) {}

  /**
   * Drops every relation in {@code namespace}, enumerating them through their by-name pointer rows
   * rather than their content.
   *
   * <p>The content-bearing listing ({@code tableRepo.list}) fetches and parses every blob in the
   * namespace before the first relation can be dropped, so one present-but-unparseable blob makes
   * the whole listing — and with it the entire recursive delete — fail with a non-retryable {@code
   * CorruptionException}. Cleaning up damaged state is exactly what a recursive delete is for, so
   * enumeration must not depend on that state being readable. The by-name rows are also what the
   * emptiness gate counts, so this enumerates precisely the set that has to reach zero, and it
   * costs one pointer scan instead of a blob fetch per relation.
   */
  private void dropNamespaceRelations(
      Namespace namespace, DropSummary summary, boolean guarded, BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    String accountId = namespaceId.getAccountId();

    // Streamed, not materialized: a namespace can hold any number of relations, and each row is
    // handled and then gone. Deleting the row just handed over does not disturb the scan — page
    // tokens resume from the last key seen.
    tableRepo.forEachNamePointer(
        accountId,
        catalogId.getId(),
        namespaceId.getId(),
        namePointer -> dropTable(namespace, namePointer, summary, guarded, subtreePin));
    viewRepo.forEachNamePointer(
        accountId,
        catalogId.getId(),
        namespaceId.getId(),
        namePointer -> dropView(namespace, namePointer, summary, guarded, subtreePin));
  }

  /** The relation a by-name row names, or empty when the row identifies no owner. */
  private static Optional<ResourceId> relationIdOf(
      Pointer namePointer, String accountId, ResourceKind kind) {
    String ownerId = ownerIdOf(namePointer);
    if (ownerId.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        ResourceId.newBuilder().setAccountId(accountId).setKind(kind).setId(ownerId).build());
  }

  /** Both relation kinds — for callers authorized to write tables and views alike. */
  public static final Set<ResourceKind> ALL_RELATION_KINDS =
      Set.of(ResourceKind.RK_TABLE, ResourceKind.RK_VIEW);

  /**
   * Reconciles leftover by-name index rows for a namespace whose emptiness a caller is about to
   * decide, outside any drop. Best-effort: a row that changes under the batch is left for the
   * caller's own re-check rather than aborting the request.
   */
  public int reclaimStrandedRelationNames(Namespace namespace) {
    return reclaimStrandedRelationNames(namespace, ALL_RELATION_KINDS);
  }

  /**
   * Same, restricted to {@code kinds}.
   *
   * <p>Releasing a row is a relation-scoped write, so a caller may only spend the grants it holds:
   * a CreateTable authorized by {@code table.write} alone can clear the stranded table rows
   * blocking its own name, but not view rows. A name still held after a restricted sweep is
   * reported as taken, which is the honest answer for a caller who cannot clear what is holding it.
   */
  public int reclaimStrandedRelationNames(Namespace namespace, Set<ResourceKind> kinds) {
    return reclaimStrandedRelationNames(namespace, kinds, BatchGuard.NONE);
  }

  /**
   * Whether {@code displayName} in this namespace is held by a relation that still exists.
   *
   * <p>A bounded answer — at most the shared relation-name claim and the two kind-specific by-name
   * rows, plus one canonical read for each owner they name — to the question a create asks after a
   * name collision. The collision itself does not distinguish "taken by something live" from "taken
   * by a row whose relation is gone": only the second is worth reconciling, and the first is the
   * common case, since a name held by a live relation of either kind collides deterministically on
   * every attempt. Answering here keeps that case from paying for a sweep of the whole namespace.
   */
  public boolean relationNameHeld(Namespace namespace, String displayName) {
    var namespaceId = namespace.getResourceId();
    String accountId = namespaceId.getAccountId();
    String catalogId = namespace.getCatalogId().getId();
    var claim =
        pointerStore.get(
            Keys.relationPointerByName(accountId, catalogId, namespaceId.getId(), displayName));
    if (claim.isPresent() && !claimIsStranded(accountId, claim.get())) {
      return true;
    }
    // The claim postdates some rows, so fall back to the kind-specific indexes it would have
    // covered.
    return resolvesByName(
            Keys.tablePointerByName(accountId, catalogId, namespaceId.getId(), displayName),
            accountId,
            ResourceKind.RK_TABLE)
        || resolvesByName(
            Keys.viewPointerByName(accountId, catalogId, namespaceId.getId(), displayName),
            accountId,
            ResourceKind.RK_VIEW);
  }

  private boolean resolvesByName(String nameKey, String accountId, ResourceKind kind) {
    return pointerStore.get(nameKey).map(row -> resolves(row, accountId, kind)).orElse(false);
  }

  /**
   * Whether any by-name row in this namespace still names a relation that exists, answering at the
   * first one it finds.
   *
   * <p>Lets a caller deciding emptiness skip {@link #reclaimStrandedRelationNames} in the ordinary
   * case. That sweep exists only to release rows nothing can resolve, so a namespace holding
   * anything live has nothing to gain from it — and paying for it turned the common "not empty"
   * rejection into a full row scan plus a point read per relation. Here the first live relation
   * ends the walk, which for a namespace that holds tables is one page and one read.
   *
   * <p>Ending it is a return value, not a thrown exception: the enumeration is observed, and a scan
   * abandoned by throwing marks its span failed and counts a repository error — on the successful
   * answer, which is also the common one.
   */
  public boolean hasResolvableRelation(Namespace namespace) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    String accountId = namespaceId.getAccountId();
    return tableRepo.anyNamePointer(
            accountId,
            catalogId.getId(),
            namespaceId.getId(),
            namePointer -> resolves(namePointer, accountId, ResourceKind.RK_TABLE))
        || viewRepo.anyNamePointer(
            accountId,
            catalogId.getId(),
            namespaceId.getId(),
            namePointer -> resolves(namePointer, accountId, ResourceKind.RK_VIEW));
  }

  /** Whether a by-name row names a relation whose canonical pointer still exists. */
  private boolean resolves(Pointer namePointer, String accountId, ResourceKind kind) {
    String ownerId = ownerIdOf(namePointer);
    if (ownerId.isEmpty()) {
      // Names nothing, so it resolves to nothing: reclaimUnresolvableName's case, not a live
      // relation.
      return false;
    }
    String canonicalKey =
        kind == ResourceKind.RK_TABLE
            ? Keys.tablePointerById(accountId, ownerId)
            : Keys.viewPointerById(accountId, ownerId);
    return pointerStore.get(canonicalKey).isPresent();
  }

  /**
   * Releases by-name pointers whose relation no longer exists, so the emptiness gate and the
   * removal step agree on what counts as a relation.
   *
   * <p>{@link #hasRelations} counts by-name pointers, while every removal resolves its relation
   * through the canonical by-id pointer. A relation whose canonical pointer is gone but whose
   * by-name pointer survived — the corrupt-blob delete path in {@code GenericResourceRepository}
   * removes only the canonical pointer, and legacy partial state has the same shape — is therefore
   * invisible to the drop and permanently fatal to the gate: nothing left can remove it, so every
   * retry re-counts it and the namespace can never be deleted, recursively or otherwise. Reclaiming
   * it here is what makes the subtree deletable, and it destroys nothing reachable: no live
   * relation resolves through a pointer whose owner is already gone.
   *
   * <p>The drop path reclaims these rows inline as it walks them; this sweep exists for callers who
   * only need to decide emptiness, such as a non-recursive delete.
   */
  private int reclaimStrandedRelationNames(
      Namespace namespace, Set<ResourceKind> kinds, BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    String accountId = namespaceId.getAccountId();

    var reclaimed = new int[1];
    if (kinds.contains(ResourceKind.RK_TABLE)) {
      tableRepo.forEachNamePointer(
          accountId,
          catalogId.getId(),
          namespaceId.getId(),
          pointer -> {
            if (reclaimIfOrphaned(pointer, namespace, ResourceKind.RK_TABLE, subtreePin)) {
              reclaimed[0]++;
            }
          });
    }
    if (kinds.contains(ResourceKind.RK_VIEW)) {
      viewRepo.forEachNamePointer(
          accountId,
          catalogId.getId(),
          namespaceId.getId(),
          pointer -> {
            if (reclaimIfOrphaned(pointer, namespace, ResourceKind.RK_VIEW, subtreePin)) {
              reclaimed[0]++;
            }
          });
    }
    return reclaimed[0];
  }

  /**
   * Deletes one by-name pointer, and the relation-name claim it shares, if and only if the relation
   * they name has no canonical pointer left.
   *
   * <p>The batch asserts that absence rather than trusting the read, so a relation being created or
   * restored under this name right now keeps its index intact. Under a guarded drop the removal
   * also carries {@code subtreePin}, binding it to the namespace the scan observed.
   *
   * <p>Best-effort by design: losing this CAS is not a reason to abort the operation that called
   * it. The common case is a concurrent {@code DeleteTable} that removed the canonical pointer and
   * this row together, and aborting would turn that benign race into a retry. Nothing downstream
   * trusts the reclaim either — the emptiness gate re-counts these rows and is the authority on
   * whether the namespace may go.
   *
   * <p>A row that names no relation at all is handled by {@link #reclaimUnresolvableName}, which
   * cannot assert an absent owner because there is no owner to name.
   */
  private boolean reclaimIfOrphaned(
      Pointer namePointer, Namespace namespace, ResourceKind kind, BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    String accountId = namespaceId.getAccountId();
    String ownerId = ownerIdOf(namePointer);
    if (ownerId.isEmpty()) {
      return reclaimUnresolvableName(namePointer, namespace, kind, subtreePin);
    }
    String canonicalKey =
        kind == ResourceKind.RK_TABLE
            ? Keys.tablePointerById(accountId, ownerId)
            : Keys.viewPointerById(accountId, ownerId);
    if (pointerStore.get(canonicalKey).isPresent()) {
      return false;
    }

    var ops = new ArrayList<PointerStore.CasOp>();
    ops.add(new PointerStore.CasCheckAbsent(canonicalKey));
    ops.add(new PointerStore.CasDelete(namePointer.getKey(), namePointer.getVersion()));

    String displayName =
        namePointer.getDisplayName().isEmpty()
            ? Keys.extractLastSegment(namePointer.getKey())
            : namePointer.getDisplayName();
    String claimKey =
        Keys.relationPointerByName(
            accountId, namespace.getCatalogId().getId(), namespaceId.getId(), displayName);
    pointerStore
        .get(claimKey)
        .filter(claim -> ownerId.equals(claim.getResourceId().getId()))
        .ifPresent(claim -> ops.add(new PointerStore.CasDelete(claimKey, claim.getVersion())));

    ops.addAll(subtreePin.ops());

    if (!pointerStore.compareAndSetBatch(ops)) {
      // The row changed under us — most often a concurrent delete that removed it already. Leave it
      // for the gate to re-count.
      CLEANUP_LOG.infof(
          "recursive_drop_reclaim_stranded_name_contended account_id=%s namespace_id=%s"
              + " pointer_key=%s",
          accountId, namespaceId.getId(), namePointer.getKey());
      return false;
    }
    CLEANUP_LOG.infof(
        "recursive_drop_reclaimed_stranded_name account_id=%s namespace_id=%s kind=%s"
            + " relation_id=%s pointer_key=%s",
        accountId, namespaceId.getId(), kind.name(), ownerId, namePointer.getKey());
    return true;
  }

  /**
   * Releases a by-name row that names no relation at all — neither a ref nor a parseable blob URI.
   *
   * <p>Such a row cannot be acted on: every removal resolves its relation through the owner this
   * row does not carry, so no delete, recursive or otherwise, can ever take it away. The emptiness
   * gate counts it all the same, which made it permanently fatal to a recursive delete: the drop
   * skipped the row, the gate then reported the namespace non-empty, and under {@code --recursive}
   * that is a retryable abort — so every attempt destroyed the rest of the subtree, exhausted its
   * retries, and reported partial teardown, with no path left to finish the job.
   *
   * <p>Releasing it destroys nothing reachable. A lookup by name needs the owner reference to
   * return a relation, so a row without one cannot be the index of anything live; it is unusable
   * state whose only effect is to wedge the namespace. The delete is still pinned to the exact
   * version read, so a writer republishing this key right now wins instead.
   *
   * <p>The shared relation-name claim goes too, but only when it is provably stranded on its own
   * terms — its owner is absent, or missing entirely. A claim naming a live relation is left alone:
   * it belongs to that relation, not to this row.
   */
  private boolean reclaimUnresolvableName(
      Pointer namePointer, Namespace namespace, ResourceKind kind, BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    String accountId = namespaceId.getAccountId();

    var ops = new ArrayList<PointerStore.CasOp>();
    ops.add(new PointerStore.CasDelete(namePointer.getKey(), namePointer.getVersion()));

    String displayName =
        namePointer.getDisplayName().isEmpty()
            ? Keys.extractLastSegment(namePointer.getKey())
            : namePointer.getDisplayName();
    String claimKey =
        Keys.relationPointerByName(
            accountId, namespace.getCatalogId().getId(), namespaceId.getId(), displayName);
    pointerStore
        .get(claimKey)
        .filter(claim -> claimIsStranded(accountId, claim))
        .ifPresent(claim -> ops.add(new PointerStore.CasDelete(claimKey, claim.getVersion())));

    ops.addAll(subtreePin.ops());

    if (!pointerStore.compareAndSetBatch(ops)) {
      CLEANUP_LOG.infof(
          "recursive_drop_reclaim_unresolvable_name_contended account_id=%s namespace_id=%s"
              + " pointer_key=%s",
          accountId, namespaceId.getId(), namePointer.getKey());
      return false;
    }
    CLEANUP_LOG.warnf(
        "recursive_drop_reclaimed_unresolvable_name account_id=%s namespace_id=%s kind=%s"
            + " pointer_key=%s",
        accountId, namespaceId.getId(), kind.name(), namePointer.getKey());
    return true;
  }

  /** Whether a relation-name claim names nothing, or names a relation whose pointer is gone. */
  private boolean claimIsStranded(String accountId, Pointer claim) {
    String ownerId = claim.getResourceId().getId();
    if (ownerId.isEmpty()) {
      return true;
    }
    return pointerStore.get(Keys.tablePointerById(accountId, ownerId)).isEmpty()
        && pointerStore.get(Keys.viewPointerById(accountId, ownerId)).isEmpty();
  }

  /** The relation a by-name pointer names: its ref if present, else parsed from its blob URI. */
  private static String ownerIdOf(Pointer namePointer) {
    String fromRef = namePointer.getResourceId().getId();
    return fromRef.isEmpty()
        ? Keys.extractResourceIdFromBlobUri(namePointer.getBlobUri())
        : fromRef;
  }

  private void dropTable(
      Namespace namespace,
      Pointer namePointer,
      DropSummary summary,
      boolean guarded,
      BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    var resolved = relationIdOf(namePointer, namespaceId.getAccountId(), ResourceKind.RK_TABLE);
    if (resolved.isEmpty()) {
      // Neither the row's ref nor its blob URI names a table, so there is no table to delete — but
      // the row itself must still go, or the emptiness gate counts a relation nothing can ever
      // remove and the namespace becomes undeletable for good.
      CLEANUP_LOG.warnf(
          "recursive_drop_table_unresolvable account_id=%s namespace_id=%s pointer_key=%s",
          namespaceId.getAccountId(), namespaceId.getId(), namePointer.getKey());
      reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_TABLE, subtreePin);
      return;
    }
    var tableId = resolved.get();

    if (guarded) {
      var pinned =
          pinToNamespace(
              tableId,
              namespaceId,
              tableRepo::metaForSafe,
              blobUri -> tableRepo.getByBlobUri(blobUri).map(Table::getNamespaceId));
      if (pinned.isEmpty()) {
        // Already gone — a concurrent DeleteTable won and ran this same cleanup, or the canonical
        // pointer was removed on its own by a corrupt-blob delete. Either way the by-name row this
        // scan followed is now orphaned index state, and the emptiness gate counts it, so release
        // it here rather than leaving the namespace permanently undeletable.
        CLEANUP_LOG.infof(
            "recursive_drop_table_skipped_absent account_id=%s namespace_id=%s table_id=%s",
            namespaceId.getAccountId(), namespaceId.getId(), tableId.getId());
        reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_TABLE, subtreePin);
        return;
      }
      // Delete pinned to the exact pointer version the membership check observed. Any concurrent
      // mutation — crucially a reparent that moved this table out of the subtree — advances the
      // canonical pointer, so the CAS fails and no owned state is purged. Deleting by id alone
      // would resolve the table's CURRENT pointer and destroy it in whatever namespace it had just
      // moved to.
      if (!tableRepo.deleteWithPrecondition(tableId, pinned.get().pointerVersion(), subtreePin)) {
        throw relationChanged(tableId);
      }
      if (!pinned.get().membershipVerified()) {
        // The repository could not parse the table, so its delete batch could only remove the
        // canonical pointer — the by-name row and relation claim it owned are still there. Release
        // them now that the canonical pointer is provably gone, or the gate below still counts a
        // table that no longer exists.
        reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_TABLE, subtreePin);
      }
      cleanupDeletedTable(tableId);
      summary.tablesDeleted++;
      summary.snapshotPrefixesDeleted++;
      CLEANUP_LOG.infof(
          "recursive_drop_table account_id=%s catalog_id=%s namespace_id=%s table_id=%s"
              + " membership_verified=%s committed=true",
          namespaceId.getAccountId(),
          catalogId.getId(),
          namespaceId.getId(),
          tableId.getId(),
          pinned.get().membershipVerified());
      return;
    }

    boolean committed = tableRepo.delete(tableId);
    // Teardown deletes by id and tolerates a corrupt blob (the repository drops the canonical
    // pointer alone in that case), so the index rows can survive. The account is going away, so
    // release them rather than leaving durable orphans behind the deleted namespace.
    reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_TABLE, subtreePin);
    // Committed delete, or unguarded account teardown. In teardown there is no survivor to protect
    // — the account, its catalogs, and namespaces are all going away — so purge owned state
    // unconditionally even when delete lost the CAS, or the table pointer and its root-resync
    // marker outlive account deletion as durable orphans.
    cleanupDeletedTable(tableId);
    // Purged unconditionally above, but counted only when this call removed the table. A retried
    // account delete re-runs cleanup over tables an earlier pass already took, and counting those
    // again inflates the audit record of an irreversible operation.
    if (committed) {
      summary.tablesDeleted++;
      summary.snapshotPrefixesDeleted++;
    }
    CLEANUP_LOG.infof(
        "recursive_drop_table account_id=%s catalog_id=%s namespace_id=%s table_id=%s committed=%s",
        namespaceId.getAccountId(),
        catalogId.getId(),
        namespaceId.getId(),
        tableId.getId(),
        committed);
  }

  private void dropView(
      Namespace namespace,
      Pointer namePointer,
      DropSummary summary,
      boolean guarded,
      BatchGuard subtreePin) {
    var namespaceId = namespace.getResourceId();
    var catalogId = namespace.getCatalogId();
    var resolved = relationIdOf(namePointer, namespaceId.getAccountId(), ResourceKind.RK_VIEW);
    if (resolved.isEmpty()) {
      // Same as dropTable: no view to delete, but the row still has to go or it wedges the gate.
      CLEANUP_LOG.warnf(
          "recursive_drop_view_unresolvable account_id=%s namespace_id=%s pointer_key=%s",
          namespaceId.getAccountId(), namespaceId.getId(), namePointer.getKey());
      reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_VIEW, subtreePin);
      return;
    }
    var viewId = resolved.get();

    if (guarded) {
      // Same stale-scan hazard as tables: delete pinned to the version whose content was verified
      // to still live here, so a view that moved out cannot be destroyed in its new namespace. And
      // the same corrupt-blob tolerance — an unreadable view must not wedge the subtree.
      var pinned =
          pinToNamespace(
              viewId,
              namespaceId,
              viewRepo::metaForSafe,
              blobUri -> viewRepo.getByBlobUri(blobUri).map(View::getNamespaceId));
      if (pinned.isEmpty()) {
        CLEANUP_LOG.infof(
            "recursive_drop_view_skipped_absent account_id=%s namespace_id=%s view_id=%s",
            namespaceId.getAccountId(), namespaceId.getId(), viewId.getId());
        reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_VIEW, subtreePin);
        return;
      }
      if (!viewRepo.deleteWithPrecondition(viewId, pinned.get().pointerVersion(), subtreePin)) {
        throw relationChanged(viewId);
      }
      if (!pinned.get().membershipVerified()) {
        reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_VIEW, subtreePin);
      }
      topology.evict(viewId);
      metadataGraph.invalidate(viewId);
      summary.viewsDeleted++;
      CLEANUP_LOG.infof(
          "recursive_drop_view account_id=%s catalog_id=%s namespace_id=%s view_id=%s"
              + " membership_verified=%s committed=true",
          namespaceId.getAccountId(),
          catalogId.getId(),
          namespaceId.getId(),
          viewId.getId(),
          pinned.get().membershipVerified());
      return;
    }

    boolean committed = viewRepo.delete(viewId);
    reclaimIfOrphaned(namePointer, namespace, ResourceKind.RK_VIEW, subtreePin);
    topology.evict(viewId);
    metadataGraph.invalidate(viewId);
    // Counted only when this call removed it — see dropTable.
    if (committed) {
      summary.viewsDeleted++;
    }
    CLEANUP_LOG.infof(
        "recursive_drop_view account_id=%s catalog_id=%s namespace_id=%s view_id=%s committed=%s",
        namespaceId.getAccountId(),
        catalogId.getId(),
        namespaceId.getId(),
        viewId.getId(),
        committed);
  }

  private boolean deleteNamespace(Namespace namespace) {
    // Account teardown: the whole tree and its account pointer are going away, so there is nothing
    // for a fence to protect and nothing a late child could be orphaned under.
    return deleteNamespace(namespace, BatchGuard.NONE, UNPINNED);
  }

  /**
   * Deletes {@code namespace}, leaving every ancestor's children marker alone.
   *
   * <p>Those markers are delete fences that only a child publish may move, and a descendant going
   * away is the opposite. Advancing them here used to break the enclosing recursive delete itself —
   * hence the former root-skipping parameter — and, one namespace up, any concurrent delete of an
   * ancestor that had already scanned. See {@link MarkerStore#namespaceChildGuard}.
   *
   * <p>{@code childrenGuard} makes the pointer removal atomic with the emptiness the caller
   * established: a child published since then raises {@link
   * BaseResourceRepository.BatchGuardFailedException}, which is retryable and re-runs the drop.
   *
   * <p>{@code expectedVersion} pins the removal to the pointer that was proven to be inside the
   * subtree ({@link #UNPINNED} in account teardown, where there is no subtree to leave). Without
   * it, deleting by id would resolve the namespace's current pointer and remove one that had been
   * reparented out of the subtree since the scan.
   *
   * @return whether this call removed the namespace's pointer. False in teardown when it was
   *     already gone — the owned state is still purged, but the caller must not count a deletion it
   *     did not perform, since that count is the audit record for an irreversible operation.
   */
  private boolean deleteNamespace(
      Namespace namespace, BatchGuard childrenGuard, long expectedVersion) {
    var namespaceId = namespace.getResourceId();
    boolean removed;
    if (expectedVersion == UNPINNED) {
      removed = namespaceRepo.delete(namespaceId, childrenGuard);
    } else if (namespaceRepo.deleteWithPrecondition(namespaceId, expectedVersion, childrenGuard)) {
      removed = true;
    } else {
      throw namespaceChanged(namespaceId);
    }
    reclaimStrandedNamespacePath(namespace);
    markerStore.deleteNamespaceMarker(namespaceId);
    topology.evictRelationRefs(namespaceId);
    topology.evictNamespaceRefs(namespace.getCatalogId());
    metadataGraph.invalidate(namespaceId);
    CLEANUP_LOG.infof(
        "recursive_drop_namespace account_id=%s catalog_id=%s namespace_id=%s display_name=%s"
            + " removed=%s",
        namespaceId.getAccountId(),
        namespace.getCatalogId().getId(),
        namespaceId.getId(),
        namespace.getDisplayName(),
        removed);
    return removed;
  }

  /**
   * Releases a by-path row that names no namespace at all — neither a ref nor a parseable blob URI.
   *
   * <p>The namespace counterpart of {@link #reclaimUnresolvableName}, and the same dead end: the
   * subtree walk cannot act on a row it cannot resolve, while the immediate-child probe counts rows
   * by key shape and does count it. Left in place it is a child that the emptiness gate sees and no
   * drop can remove — so a recursive delete destroys the subtree and then reports partial teardown
   * on that attempt and every retry, and account teardown leaves the row behind for good.
   *
   * <p>Releasing it destroys nothing reachable: no namespace resolves through a row that names
   * none, and the delete is pinned to the version read, so a writer republishing this path wins
   * instead.
   */
  private void reclaimUnresolvableNamespacePath(
      String accountId, List<String> rootPath, Pointer row) {
    CLEANUP_LOG.warnf(
        "recursive_drop_namespace_path_unresolvable account_id=%s root_path=%s pointer_key=%s",
        accountId, String.join(".", rootPath), row.getKey());
    if (!pointerStore.compareAndSetBatch(
        List.of(new PointerStore.CasDelete(row.getKey(), row.getVersion())))) {
      CLEANUP_LOG.infof(
          "recursive_drop_reclaim_unresolvable_path_contended account_id=%s pointer_key=%s",
          accountId, row.getKey());
      return;
    }
    CLEANUP_LOG.warnf(
        "recursive_drop_reclaimed_unresolvable_path account_id=%s pointer_key=%s",
        accountId, row.getKey());
  }

  /**
   * Releases a deleted namespace's by-path row when the repository could only remove its canonical
   * pointer — the case for a namespace whose blob does not parse, since the repository cannot read
   * the secondary keys it would otherwise remove in the same batch.
   *
   * <p>That row is what every immediate-child probe counts, so leaving it makes the parent look
   * non-empty forever and the subtree undeletable. A no-op on the ordinary path: a parseable
   * namespace has its by-path row removed atomically with the canonical pointer, so there is
   * nothing here to find.
   *
   * <p>Only ever removes a row that still names the namespace just deleted — a namespace recreated
   * at the same path owns its own row, and the absent-canonical check alone would not notice.
   */
  private void reclaimStrandedNamespacePath(Namespace namespace) {
    var namespaceId = namespace.getResourceId();
    String accountId = namespaceId.getAccountId();
    var path = new ArrayList<>(namespace.getParentsList());
    path.add(namespace.getDisplayName());
    String byPathKey =
        Keys.namespacePointerByPath(accountId, namespace.getCatalogId().getId(), path);
    var row = pointerStore.get(byPathKey).orElse(null);
    if (row == null || !namespaceId.getId().equals(ownerIdOf(row))) {
      return;
    }
    String canonicalKey = Keys.namespacePointerById(accountId, namespaceId.getId());
    var ops =
        List.<PointerStore.CasOp>of(
            new PointerStore.CasCheckAbsent(canonicalKey),
            new PointerStore.CasDelete(byPathKey, row.getVersion()));
    if (!pointerStore.compareAndSetBatch(ops)) {
      CLEANUP_LOG.infof(
          "recursive_drop_reclaim_stranded_path_contended account_id=%s namespace_id=%s"
              + " pointer_key=%s",
          accountId, namespaceId.getId(), byPathKey);
      return;
    }
    CLEANUP_LOG.infof(
        "recursive_drop_reclaimed_stranded_path account_id=%s namespace_id=%s pointer_key=%s",
        accountId, namespaceId.getId(), byPathKey);
  }

  /**
   * The scanned namespace as the drop needs to see it, from a by-path pointer row alone.
   *
   * <p>Identity, placement, and name are all recoverable from the row: the key encodes the full
   * path, so the parents are that path minus its last segment. Nothing below reads any other field,
   * so this stands in for the stored proto without depending on the blob being parseable.
   */
  private static Namespace namespaceFromRef(TopologyGraph.NamespaceRef ref, ResourceId catalogId) {
    var path = ref.pathSegments();
    return Namespace.newBuilder()
        .setResourceId(ref.id())
        .setCatalogId(catalogId)
        .setDisplayName(ref.name())
        .addAllParents(path.isEmpty() ? List.of() : path.subList(0, path.size() - 1))
        .build();
  }

  /**
   * Whether {@code namespace} is inside the subtree being dropped: under {@code rootPath}, and in
   * the same catalog.
   *
   * <p>A path alone does not identify a namespace. Two catalogs can hold the same path, and a
   * reparent may move a namespace across catalogs without changing it — so a membership test on
   * segments alone would let a namespace that had left the catalog entirely still look like a
   * descendant, and the drop would destroy it, and everything under it, where it now lives.
   */
  private static boolean isDescendant(
      Namespace namespace, ResourceId rootCatalogId, java.util.List<String> rootPath) {
    if (!namespace.getCatalogId().getId().equals(rootCatalogId.getId())) {
      return false;
    }
    var parents = namespace.getParentsList();
    return parents.size() >= rootPath.size()
        && parents.subList(0, rootPath.size()).equals(rootPath);
  }

  /**
   * The same test against a by-path row's full path: strictly below {@code rootPath}, so the root's
   * own row — which the prefix scan also returns — is not treated as a descendant of itself.
   */
  private static boolean isDescendantPath(
      java.util.List<String> fullPath, java.util.List<String> rootPath) {
    return fullPath.size() > rootPath.size()
        && fullPath.subList(0, rootPath.size()).equals(rootPath);
  }

  public static final class DropSummary {
    public int namespacesDeleted;
    public int tablesDeleted;
    public int viewsDeleted;
    public int snapshotPrefixesDeleted;

    /**
     * Resources destroyed, for callers deciding whether a failed operation was still destructive.
     */
    public int total() {
      return namespacesDeleted + tablesDeleted + viewsDeleted;
    }
  }
}
