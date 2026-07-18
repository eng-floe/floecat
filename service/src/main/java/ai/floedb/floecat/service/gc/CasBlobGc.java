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

package ai.floedb.floecat.service.gc;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;
import org.jboss.logging.Logger;

/**
 * Sweeps unreferenced CAS blobs per account. Roots come from three sources: every live pointer
 * (shared store) and the pin/resolving roots of live query contexts — which are <b>node-local</b>
 * (in-process {@code QueryContextStore}), the only root source that is.
 *
 * <p>Single-GC-writer assumption: because pin roots are node-local, this sweep is only safe where
 * the process running it can see every live pin — i.e. a single-node deployment, or one where all
 * query traffic is served by the node running GC. A multi-node deployment must either disable this
 * sweep on nodes serving queries ({@code floecat.gc.cas.enabled=false}) or share pin roots across
 * nodes first; the in-process context store already makes queries node-sticky, so this constraint
 * travels with the existing query-routing one.
 *
 * <p>Defense in depth: independent of how the referenced set was computed, the delete phase
 * re-reads each candidate's OWNING pointer ({@link Keys#ownerPointerKeyForBlob}) immediately before
 * deleting, and refuses — loudly — to delete a blob a live pointer still references. Such a rescue
 * keeps the blob, counts {@code blobs-rescued}, and WARNs, then CONTINUES the pass — it does NOT
 * poison the sweep. A stale referenced set can only cause false KEEPS here, never false deletes,
 * because every owner-derivable candidate self-rechecks and every no-owner candidate is deferred
 * and re-proven per table; poisoning the sweep on a rescue would instead let one table's persistent
 * flip-flop starve the whole account's no-owner collection. A miss here is catalog data loss with
 * no self-heal (reads fail with "dangling pointer, missing blob" and a reconcile re-references the
 * dead URI). Deletes are version-targeted (the exact version the pass age-checked), and the sweep
 * fails closed unless the store reports immutable version ids (S3 bucket versioning Enabled), so a
 * concurrent re-PUT always survives as a new version the targeted delete cannot touch. Families
 * with no owner pointer derivable from the key (manifest pages, per-target and file stats records)
 * cannot be rescued individually, and lexicographic listing puts some of them before any blob whose
 * rescue could reveal the stale set — so their deletion is DEFERRED, and the flush independently
 * re-proves liveness per owning table against the settled store (root-chain re-walk plus
 * constraints/stats pointer re-scan) before deleting. Superseded-but-pinned blobs are outside the
 * recheck's reach (their owner pointer has moved on) and remain guarded by pin roots and
 * walk-failure poisoning alone.
 *
 * <p><b>Versioned-bucket operations note.</b> This sweep only ever deletes the CURRENT version it
 * age-checked; it never visits noncurrent versions. With the always-PUT write path, an oscillating
 * live URI accrues a version per rewrite, and a garbage blob with N versions needs N sweep ticks to
 * disappear (each targeted delete promotes the previous version to current). Neither is reclaimed
 * fully by this GC: versioned deployments REQUIRE a bucket lifecycle rule expiring noncurrent
 * versions (which also finishes off delete markers left by the unversioned {@code delete(key)}
 * fallback, which reclaims nothing physically on a versioned bucket). This is the deliberate trade:
 * versioning is what makes the delete race closable at all, and it doubles as the
 * corruption-recovery mechanism (staging was healed by restoring deleted versions).
 */
@ApplicationScoped
public class CasBlobGc {

  private static final Logger LOG = Logger.getLogger(CasBlobGc.class);
  private static final int CHAIN_READ_ATTEMPTS = 3;

  @Inject BlobStore blobStore;
  @Inject PointerStore pointerStore;
  @Inject QueryContextStore queryContextStore;
  @Inject TableRootRepository tableRootRepo;
  @Inject StatsRepository statsRepository;

  /**
   * One sweep's tallies. On a versioned store {@code blobsDeleted} counts VERSION deletes, not
   * whole-blob removals: deleting the current version of an N-version blob leaves it readable with
   * N-1 versions, so this metric must not be read as "garbage fully reclaimed" (see the class note
   * on lifecycle rules).
   */
  public record Result(
      int pointersScanned,
      int blobsScanned,
      int blobsDeleted,
      int blobsRescued,
      int referenced,
      int tablesScanned,
      boolean poisoned,
      boolean deletesUnsupported) {}

  public Result runForAccount(String accountId) {
    if (!blobStore.supportsVersionedDeletes()) {
      // Fail closed: without immutable version identities every delete is the
      // eng-floe/core#1904 race (an S3 bucket whose versioning is not Enabled overwrites version
      // state in place), so nothing may be collected. One warn per pass; the scheduler gauges
      // this so a misconfigured bucket is noticed rather than silently never collecting.
      LOG.warnf(
          "cas gc for account %s skipped: blob store cannot delete by immutable version"
              + " (on S3 the bucket's versioning status must be Enabled)",
          accountId);
      return new Result(0, 0, 0, 0, 0, 0, false, true);
    }

    final var cfg = ConfigProvider.getConfig();
    final int pageSize =
        cfg.getOptionalValue("floecat.gc.cas.page-size", Integer.class).orElse(500);
    final long minAgeMs =
        cfg.getOptionalValue("floecat.gc.cas.min-age-ms", Long.class).orElse(30_000L);
    final long nowMs = System.currentTimeMillis();

    Set<String> referenced = new HashSet<>();
    List<String> tableIds = new ArrayList<>();
    int pointersScanned = 0;

    var accountPtr = pointerStore.get(Keys.accountPointerById(accountId)).orElse(null);
    if (accountPtr != null && !accountPtr.getBlobUri().isBlank()) {
      referenced.add(normalizeKey(accountPtr.getBlobUri()));
      pointersScanned++;
    }

    pointersScanned +=
        collectPointers(Keys.catalogPointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.namespacePointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.tablePointerByIdPrefix(accountId), referenced, tableIds, pageSize);
    pointersScanned +=
        collectPointers(Keys.viewPointerByIdPrefix(accountId), referenced, null, pageSize);
    pointersScanned +=
        collectPointers(Keys.connectorPointerByIdPrefix(accountId), referenced, null, pageSize);

    // A pinned ROOT protects everything it references, not just its own blob: a query pinned to a
    // superseded root must keep reading that root's pages, snapshot blobs, generation manifests,
    // and constraints bundles until it ends. `walkedPinRoots` remembers which pin roots have had
    // their chains walked so pins registered mid-sweep can be rooted incrementally.
    // `walkFailures` poisons the sweep: manifest pages and per-entry refs are reachable ONLY
    // through chain walks, so a walk that could not complete (missing blob, storage error) means
    // the referenced set is not trustworthy and nothing may be deleted this pass.
    int[] walkFailures = {0};
    Set<String> walkedPinRoots = new HashSet<>();
    rootLivePinChains(referenced, walkedPinRoots, walkFailures);

    int tablesScanned = 0;
    for (String tableId : tableIds) {
      tablesScanned++;
      String snapshotsById = Keys.snapshotPointerByIdPrefix(accountId, tableId);
      pointersScanned += collectPointers(snapshotsById, referenced, null, pageSize);

      // The current table root and EVERYTHING it references are GC roots: the root blob, every
      // manifest page, and each entry's definition/snapshot/generation-manifest/constraints blobs.
      // This MUST happen before the generation reclaim below — a generation the current root still
      // references is protected even when the live active pointer has already moved past it (the
      // finalize's pointer flip and root commit are not atomic). Superseded root chains no live
      // pin references are unreferenced and swept below.
      var rootPtr = pointerStore.get(Keys.tableRootByTable(accountId, tableId)).orElse(null);
      if (rootPtr != null && !rootPtr.getBlobUri().isBlank()) {
        pointersScanned++;
        if (!rootTableRootChain(rootPtr.getBlobUri(), referenced)) {
          walkFailures[0]++;
        }
      }

      // Reclaim superseded stats generations BEFORE collecting stats pointers as roots, so a
      // doomed generation's record blobs are swept in this same pass. On a miss the predicate
      // re-roots pins registered since the sweep started — a pin protects its root's whole chain,
      // including the generation manifests its entries reference, not just the root blob.
      var rid =
          ResourceId.newBuilder()
              .setAccountId(accountId)
              .setId(tableId)
              .setKind(ResourceKind.RK_TABLE)
              .build();
      statsRepository.deleteUnreferencedGenerations(
          rid,
          manifestUri -> {
            if (walkFailures[0] > 0) {
              return true; // poisoned: an incomplete walk means protection is unknowable
            }
            String normalized = normalizeKey(manifestUri);
            if (referenced.contains(normalized)) {
              return true;
            }
            rootLivePinChains(referenced, walkedPinRoots, walkFailures);
            return walkFailures[0] > 0 || referenced.contains(normalized);
          },
          nowMs,
          minAgeMs);

      String snapshotsRoot = Keys.snapshotRootPrefix(accountId, tableId);
      pointersScanned +=
          collectPointers(
              snapshotsRoot,
              referenced,
              null,
              pageSize,
              p -> p.getKey() != null && p.getKey().contains(Keys.SEG_STATS));

      // Constraints pointers live under a SIBLING prefix (/constraints/by-snapshot/), not under
      // /snapshots/, so they need their own scan. A constraints blob is deletable (it matches the
      // delete predicate) and its pointer goes live before commitConstraints records the ref on the
      // root — this scan protects the blob during that window, symmetric with the stats pointers.
      pointersScanned +=
          collectPointers(
              Keys.snapshotConstraintsPointerPrefix(accountId, tableId),
              referenced,
              null,
              pageSize);
    }

    // Active query pins are GC roots: an immutable blob a live query pinned must survive even after
    // the current catalog pointers have advanced past it, until that query (and its scan lease)
    // ends. Pin blob URIs share MutationMeta.getBlobUri()'s shape, so they normalize identically to
    // the pointer-derived roots above. This snapshot seeds the root set; because a sweep can run
    // for a while and pins are registered continuously, the delete passes below also re-read the
    // (in-memory, cheap) pin roots per page so a pin taken mid-sweep still protects its blob.

    int blobsScanned = 0;
    int blobsDeleted = 0;
    int blobsRescued = 0;

    if (walkFailures[0] > 0) {
      // A chain walk failed: manifest pages and every ref inside them are reachable only through
      // the walks, so the referenced set is incomplete and ANY delete could destroy a live root's
      // chain. Skip the whole delete phase; the next pass retries with (hopefully) healthy reads.
      LOG.warnf(
          "cas gc for account %s skipped its delete phase: %d root-chain walk(s) failed",
          accountId, walkFailures[0]);
      return new Result(pointersScanned, 0, 0, 0, referenced.size(), tablesScanned, true, false);
    }

    var account =
        deleteUnreferenced(
            Keys.accountBlobPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_ACCOUNT),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += account.scanned();
    blobsDeleted += account.deleted();
    blobsRescued += account.rescued();

    var catalogs =
        deleteUnreferenced(
            Keys.catalogRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_CATALOG),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += catalogs.scanned();
    blobsDeleted += catalogs.deleted();
    blobsRescued += catalogs.rescued();

    var namespaces =
        deleteUnreferenced(
            Keys.namespaceRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_NAMESPACE),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += namespaces.scanned();
    blobsDeleted += namespaces.deleted();
    blobsRescued += namespaces.rescued();

    // One LIST over the table subtree covers all five blob families that live under it — the
    // segment filters are mutually exclusive key shapes, so a combined pass deletes exactly what
    // five per-family passes would, at a fifth of the listing cost. Chain-walked families with no
    // owner pointer are only COLLECTED here; they are flushed after every pass finished clean
    // (see the flush below).
    var deferredNoOwner = new ArrayList<DeferredCandidate>();
    var tableTree =
        deleteUnreferenced(
            Keys.tableRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            // Deliberately enumerated: only families whose superseded blobs the referenced-set
            // computation above actually tracks may be delete candidates. Families NOT listed
            // (index-artifact sidecars, compat, storage-authority) are LISTed but never deleted
            // here — widening this filter without also teaching the referenced-set to root their
            // live blobs would delete live data. Bringing those families under GC (with
            // referenced-set support) is a separate follow-up; today they are out of scope for
            // this sweep, not a data-loss risk.
            key ->
                key.contains(Keys.SEG_TABLE)
                    || (key.contains(Keys.SEG_SNAPSHOTS) && key.contains(Keys.SEG_SNAPSHOT))
                    || key.contains(Keys.SEG_TARGET_STATS)
                    || key.contains(Keys.SEG_FILE_STATS)
                    || key.contains(Keys.SEG_CONSTRAINTS)
                    || key.contains(Keys.SEG_TABLE_ROOT),
            deferredNoOwner,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += tableTree.scanned();
    blobsDeleted += tableTree.deleted();
    blobsRescued += tableTree.rescued();

    var views =
        deleteUnreferenced(
            Keys.viewRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_VIEW),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += views.scanned();
    blobsDeleted += views.deleted();
    blobsRescued += views.rescued();

    var connectors =
        deleteUnreferenced(
            Keys.connectorRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_CONNECTOR),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += connectors.scanned();
    blobsDeleted += connectors.deleted();
    blobsRescued += connectors.rescued();

    // Flush the chain-walked candidates LAST, and never on the sweep-long stale set alone. These
    // families cannot rescue themselves: manifest pages have no pointer at all, and the
    // per-target/file stats records are referenced by per-snapshot pointers not reconstructible
    // from the blob key — a flip-flop or scan miss on those produces no inline rescue anywhere
    // (and the head/min-age skip paths can swallow a top-level rescue signal). So the flush
    // proves liveness for itself: for each table that owns deferred candidates, it re-reads that
    // table's root chain and re-scans its constraints and stats pointer prefixes against the
    // SETTLED store, and only deletes candidates this fresh, scoped re-mark still leaves
    // unreferenced. The zero-rescue/zero-poison gate remains as a cheap early out — any rescue
    // already means the whole set is suspect — but correctness rests on the re-mark, not on
    // rescue-signal completeness. Cost is proportional to actual garbage (tables with deferred
    // candidates), not to steady-state traffic.
    int remarkFailures = 0;
    // Gated on walkFailures only, NOT on blobsRescued: a rescue means the referenced set is
    // stale, but the flush re-proves liveness per table against the SETTLED store (it does not
    // trust the stale set for the delete decision), so a rescue on one table must not starve every
    // table's no-owner garbage collection. A pin-chain or root-chain walk FAILURE does block it.
    if (walkFailures[0] == 0 && !deferredNoOwner.isEmpty()) {
      var deferredByTable = new HashMap<String, List<DeferredCandidate>>();
      for (DeferredCandidate candidate : deferredNoOwner) {
        String key = candidate.key();
        String tableId = Keys.extractResourceIdFromBlobUri(key.startsWith("/") ? key : "/" + key);
        deferredByTable.computeIfAbsent(tableId, k -> new ArrayList<>()).add(candidate);
      }
      for (var entry : deferredByTable.entrySet()) {
        String tableId = entry.getKey();
        if (tableId.isEmpty()) {
          continue; // unparseable key shape: fail safe, leave it for a future pass
        }
        // Snapshot the live pins before re-marking this table so remarkTable runs against a
        // pin-aware referenced set; the per-CANDIDATE re-snapshot below is what actually closes
        // the late-pin window before each delete. A PIN-chain walk failure is global — the
        // referenced set itself is unprovable — so it aborts the whole flush, unlike a per-table
        // re-mark failure below.
        rootLivePinChains(referenced, walkedPinRoots, walkFailures);
        if (walkFailures[0] > 0) {
          break;
        }
        var fresh = new HashSet<String>();
        if (!remarkTable(accountId, tableId, fresh, pageSize)) {
          // This table's chain is unprovable: keep ITS candidates and move on. Each table's
          // deletion is gated on its own re-mark, so one transiently unreadable root must not
          // starve every other table's independently-proven flush — but the failure still reaches
          // the poisoned gauge (below), because unprovable garbage was left behind.
          remarkFailures++;
          continue;
        }
        var deleted = new ArrayList<String>();
        boolean pinWalkFailed = false;
        for (DeferredCandidate candidate : entry.getValue()) {
          String normalized = normalizeKey(candidate.key());
          if (referenced.contains(normalized) || fresh.contains(normalized)) {
            continue; // rooted by a late pin walk, or live per the settled re-mark
          }
          // Final guard: re-snapshot pins immediately before THIS delete, not just once per table.
          // A query pinning a superseded root after the per-table snapshot would otherwise lose a
          // manifest page reachable only through it (pin publication is read-only, so version
          // targeting cannot catch it). A pin-chain walk failure aborts the flush.
          if (keepForLatePin(normalized, referenced, walkedPinRoots, walkFailures)) {
            if (walkFailures[0] > 0) {
              pinWalkFailed = true;
              break;
            }
            continue; // a pin published since the per-table snapshot now protects it
          }
          // Version-targeted like the inline path: a concurrent re-PUT mints a new version this
          // delete cannot touch, so on a versioned store the re-referenced blob survives.
          if (blobStore.delete(candidate.key(), candidate.versionId())) {
            blobsDeleted++;
            deleted.add(normalized);
          }
        }
        if (pinWalkFailed) {
          break; // reachability unprovable mid-flush: stop, the next tick retries
        }
        // Residual-race detector for UNVERSIONED stores, mirroring the inline path's post-delete
        // probe: a second re-mark sees the settled state; a candidate it now references AND whose
        // key is actually gone was destroyed by a delete that degraded to unconditional — name it
        // at ERROR so the operator knows exactly what to repair.
        if (!deleted.isEmpty()) {
          var after = new HashSet<String>();
          if (remarkTable(accountId, tableId, after, pageSize)) {
            for (String normalized : deleted) {
              if (after.contains(normalized) && blobStore.head(normalized).isEmpty()) {
                LOG.errorf(
                    "cas gc deleted deferred blob %s of table %s that a concurrent commit"
                        + " re-referenced during the flush: the resource is corrupted — repair"
                        + " (re-create/re-sync) required",
                    normalized, tableId);
              }
            }
          }
        }
      }
    }

    // Report the FINAL poison state, not a static false: the delete passes re-run rootLivePinChains
    // per page, so a pin registered mid-sweep whose chain walk fails raises walkFailures[0] and
    // aborts that pass (protecting data). That poison must reach the scheduler's gauges — reporting
    // clean here would reset the clean-sweep clock and skip the poisoned-account count.
    return new Result(
        pointersScanned,
        blobsScanned,
        blobsDeleted,
        blobsRescued,
        referenced.size(),
        tablesScanned,
        walkFailures[0] > 0 || remarkFailures > 0,
        false);
  }

  /**
   * Root a table-root chain: the root blob itself, every snapshot-manifest page, and each entry's
   * definition/snapshot/stats-generation-manifest/constraints blob URIs. Returns whether the walk
   * COMPLETED — manifest pages and the refs inside them have no pointer-based protection of their
   * own, so an incomplete walk (unreadable root or page, transient storage error) must poison the
   * sweep: deleting against a partially-rooted set could destroy a live chain permanently.
   */
  private boolean rootTableRootChain(String rootBlobUri, Set<String> referenced) {
    referenced.add(normalizeKey(rootBlobUri));
    try {
      var root =
          readChainObject("root blob " + rootBlobUri, () -> tableRootRepo.getByBlobUri(rootBlobUri))
              .orElse(null);
      if (root == null) {
        LOG.warnf("cas gc could not read root blob %s; sweep will be skipped", rootBlobUri);
        return false;
      }
      if (root.hasDefinitionRef() && !root.getDefinitionRef().getUri().isBlank()) {
        referenced.add(normalizeKey(root.getDefinitionRef().getUri()));
      }
      Set<String> walkedPages = new HashSet<>();
      var pageRef = root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null;
      while (pageRef != null && !pageRef.getUri().isBlank()) {
        if (!walkedPages.add(pageRef.getUri())) {
          // Content-addressed pages are acyclic by construction, so a repeated URI means a corrupt
          // or malformed chain. Fail safe (poison the sweep) instead of looping until OOM on the
          // background GC thread, which has no request timeout to rescue it.
          LOG.warnf(
              "cas gc manifest chain of root %s is cyclic at %s; poisoning the sweep",
              rootBlobUri, pageRef.getUri());
          return false;
        }
        referenced.add(normalizeKey(pageRef.getUri()));
        var pageRefForRead = pageRef;
        var page =
            readChainObject(
                    "manifest page " + pageRef.getUri() + " of root " + rootBlobUri,
                    () -> tableRootRepo.getManifestPage(pageRefForRead))
                .orElse(null);
        if (page == null) {
          LOG.warnf(
              "cas gc could not read manifest page %s of root %s; sweep will be skipped",
              pageRef.getUri(), rootBlobUri);
          return false;
        }
        for (var entry : page.getEntriesList()) {
          if (entry.hasSnapshotRef() && !entry.getSnapshotRef().getUri().isBlank()) {
            referenced.add(normalizeKey(entry.getSnapshotRef().getUri()));
          }
          if (entry.hasStatsGenerationRef() && !entry.getStatsGenerationRef().getUri().isBlank()) {
            referenced.add(normalizeKey(entry.getStatsGenerationRef().getUri()));
          }
          if (entry.hasConstraintsRef() && !entry.getConstraintsRef().getUri().isBlank()) {
            referenced.add(normalizeKey(entry.getConstraintsRef().getUri()));
          }
        }
        pageRef = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
      }
      return true;
    } catch (RuntimeException e) {
      LOG.warnf(e, "cas gc chain walk failed for root %s; sweep will be skipped", rootBlobUri);
      return false;
    }
  }

  private <T> Optional<T> readChainObject(String description, Supplier<Optional<T>> read) {
    BaseResourceRepository.AbortRetryableException last = null;
    for (int attempt = 1; attempt <= CHAIN_READ_ATTEMPTS; attempt++) {
      try {
        return read.get();
      } catch (BaseResourceRepository.AbortRetryableException e) {
        last = e;
        if (attempt < CHAIN_READ_ATTEMPTS) {
          LOG.debugf(e, "cas gc retrying %s read after retryable storage failure", description);
        }
      }
    }
    if (last == null) {
      throw new IllegalStateException("cas gc chain read had no attempts: " + description);
    }
    throw last;
  }

  private int collectPointers(
      String prefix, Set<String> referenced, List<String> tableIds, int pageSize) {
    return collectPointers(prefix, referenced, tableIds, pageSize, null);
  }

  private int collectPointers(
      String prefix,
      Set<String> referenced,
      List<String> tableIds,
      int pageSize,
      Predicate<Pointer> filter) {
    String token = "";
    int scanned = 0;

    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : pointers) {
        if (filter != null && !filter.test(p)) {
          continue;
        }
        scanned++;
        if (p.getBlobUri() != null && !p.getBlobUri().isBlank()) {
          referenced.add(normalizeKey(p.getBlobUri()));
        }
        if (tableIds != null) {
          String id = decodeSuffix(prefix, p.getKey());
          if (id != null && !id.isBlank()) {
            tableIds.add(id);
          }
        }
      }

      token = next.toString();
      if (token.isEmpty()) {
        break;
      }
    }
    return scanned;
  }

  /** Current pin-root URIs, normalized like every other root. */
  private Set<String> normalizedPinRoots() {
    Set<String> roots = new HashSet<>();
    for (String pinUri : queryContextStore.referencedPinBlobUris()) {
      if (pinUri != null && !pinUri.isBlank()) {
        roots.add(normalizeKey(pinUri));
      }
    }
    return roots;
  }

  /**
   * Roots every currently-live pin: the pin blob URIs themselves, plus — for pin roots not yet
   * walked this pass — the whole table-root chain they reference. Re-reading the (in-memory,
   * node-local) pin set is cheap; chain walks happen at most once per distinct pin root per pass,
   * so pins registered mid-sweep are rooted incrementally without re-walking known chains.
   */
  private void rootLivePinChains(
      Set<String> referenced, Set<String> walkedPinRoots, int[] walkFailures) {
    for (String pinRoot : normalizedPinRoots()) {
      referenced.add(pinRoot);
      if (pinRoot.contains(Keys.SEG_TABLE_ROOT) && walkedPinRoots.add(pinRoot)) {
        if (!rootTableRootChain(pinRoot, referenced)) {
          walkFailures[0]++;
        }
      }
    }
  }

  private record DeleteResult(int scanned, int deleted, int rescued) {}

  /** A deferred no-owner candidate: the key plus the exact version the sweep age-checked. */
  private record DeferredCandidate(String key, String versionId) {}

  private DeleteResult deleteUnreferenced(
      String prefix,
      Set<String> referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures,
      Predicate<String> isCandidate,
      List<DeferredCandidate> deferNoOwnerTo,
      int pageSize,
      long nowMs,
      long minAgeMs) {
    String token = "";
    int scanned = 0;
    int deleted = 0;
    int rescued = 0;

    while (true) {
      BlobStore.Page page = blobStore.list(prefix, pageSize, token);
      // Re-root the live pins once per page: the root set captured at the start of the run goes
      // stale over a long sweep, and a pin registered mid-sweep (whose blobs may have just lost
      // their pointer root) must still protect its blob AND its root's whole chain. The pin set is
      // node-local memory and chains walk at most once per pin root, so this stays cheap.
      rootLivePinChains(referenced, walkedPinRoots, walkFailures);
      if (walkFailures[0] > 0) {
        // A pin-chain walk failed mid-phase (reachability is now unknowable): stop deleting
        // immediately (see the pass-level gate). A rescue does NOT reach here — it keeps the blob
        // and continues without raising walkFailures.
        LOG.warnf("cas gc delete pass over %s aborted: pin-chain walk failed mid-phase", prefix);
        return new DeleteResult(scanned, deleted, rescued);
      }
      for (String key : page.keys()) {
        scanned++;
        String normalized = normalizeKey(key);
        if (!isCandidate.test(normalized)) {
          continue;
        }
        if (!referenced.contains(normalized)) {
          var header = blobStore.head(key).orElse(null);
          if (header == null) {
            // No header (transient HEAD failure, or read-after-write metadata lag): we cannot
            // prove the blob is old enough, so fail SAFE and skip it, matching the generation
            // reclaim. The next pass retries once the metadata is readable.
            continue;
          }
          long lastModified =
              com.google.protobuf.util.Timestamps.toMillis(header.getLastModifiedAt());
          // nowMs is FROZEN at pass start, so this grace is anchored to pass-start, NOT to
          // wall-clock-now. That protects a root (or any blob) committed AFTER its table's one-time
          // reference mark: its lastModified is STRICTLY later than nowMs, so the difference is
          // negative — below min-age — and it is skipped no matter how long the sweep runs. This
          // runs unconditionally, not only when min-age > 0. (The lone edge is a blob whose
          // lastModified == nowMs exactly: at min-age=0, 0 < 0 is false, so it is eligible. That is
          // unreachable in practice — nowMs is stamped before any blob the sweep could race, and
          // the default min-age is 30s — but note the fence is exact only for min-age > 0.) A blob
          // is deletable only if it was already unreferenced-and-old at pass start.
          if (nowMs - lastModified < minAgeMs) {
            continue;
          }
          // Second fence against the mark/CAS race: a pointer CAS can re-target an OLD existing
          // blob after this pass's one-time mark, invisible to both the mark and (if the writer
          // left LastModified stale) the age fence. For pointer-rooted families the key encodes
          // its owner (see Keys.ownerPointerKeyForBlob) — re-read that pointer right before the
          // delete. The recheck runs on every owner-derivable delete candidate — and those are
          // NOT rare: every commit supersedes its table/root/snapshot blobs, so each such
          // deletion pays up to two extra pointer reads (this recheck + the post-delete probe).
          // That is the deliberate price of never deleting a live blob.
          String ownerPointerKey = Keys.ownerPointerKeyForBlob(normalized);
          String versionId = header.getVersionId();
          if (versionId.isBlank()) {
            // Cannot name the version this pass age-checked (unexpected on a store that reports
            // supportsVersionedDeletes): fail closed and leave the blob for a future pass — an
            // unconditional delete here would reintroduce the race.
            continue;
          }
          if (ownerPointerKey == null && deferNoOwnerTo != null) {
            // No owner pointer derivable from the key (chain-walked manifest pages, and the
            // per-target/file stats records whose per-snapshot pointers are not reconstructible
            // from the blob key alone) — and blob listing is lexicographic, so these often sort
            // BEFORE any blob whose rescue could reveal a stale referenced set. Deleting them
            // inline would destroy a missed table's chain before anything can object. Defer them
            // (with the version this pass age-checked): the flush below re-proves liveness per
            // table against the settled store before deleting (see the flush comment for why the
            // rescue signal alone is not sufficient proof).
            deferNoOwnerTo.add(new DeferredCandidate(key, versionId));
            continue;
          }
          if (ownerPointerKey != null && ownedBy(ownerPointerKey, normalized)) {
            // The owner pointer still references this blob: the referenced set missed a live
            // reference (canonically the content-addressed pointer flip-flop A -> B -> A across the
            // sweep, the revert skipping the blob write so min-age is blind). Keep it and count the
            // rescue — but do NOT poison or abort the pass. A stale referenced set can only cause
            // false KEEPS here, never false deletes: every owner-derivable candidate self-rechecks
            // (this branch), every no-owner candidate is deferred and re-proven per-table against
            // the settled store, so continuing is safe and lets the pass finish collecting
            // deferrals. Poisoning on rescue would let one table's persistent flip-flop starve the
            // whole account's no-owner garbage collection. blobs-rescued is the operator signal.
            rescued++;
            LOG.warnf(
                "cas gc rescued live blob %s: owner pointer %s still references it but the"
                    + " referenced set missed it (stale mark, e.g. a pointer flip during the sweep)"
                    + " — keeping it; the referenced set is stale but the recheck protects each"
                    + " delete independently",
                key, ownerPointerKey);
            continue;
          }
          if (ownerPointerKey == null) {
            // Reached only for a candidate with no derivable owner in a pass that does NOT defer
            // (account/catalog/namespace/view/connector families are all owner-derivable for
            // well-formed keys, so this is an unexpected/malformed shape). Fail safe: never take
            // the unconditional-delete path below, which would skip the owner-recheck invariant.
            // Leave it for investigation rather than risk deleting live data.
            LOG.debugf(
                "cas gc skipped %s: no derivable owner pointer and no deferral in this pass", key);
            continue;
          }
          // Final guard before the irreversible delete: a query may have published a pin to a
          // superseded root since this page's pin snapshot. Re-read the live pins and keep the blob
          // if it is now pin-reachable; a pin-chain walk failure aborts the pass (reachability
          // unprovable).
          if (keepForLatePin(normalized, referenced, walkedPinRoots, walkFailures)) {
            if (walkFailures[0] > 0) {
              return new DeleteResult(scanned, deleted, rescued);
            }
            continue;
          }
          // Delete exactly the version this pass age-checked: the fences above are still stale
          // reads — a writer can re-PUT the blob and CAS its pointer between them and this delete
          // — but that re-PUT mints a NEW version a targeted delete cannot touch, so the check and
          // the act name the same immutable object and the pointer stays resolvable in every
          // interleaving.
          if (blobStore.delete(key, versionId)) {
            deleted++;
            // Defensive post-delete corruption detector. The sweep only reaches here on a
            // versioned store (unversioned/blank-version blobs fail closed above), where a
            // version-targeted delete keeps any concurrent re-PUT as a new version — so this should
            // never fire. If it does (owner pointer now references this key AND the key is actually
            // gone), a live blob was destroyed: log at ERROR with both keys so the operator knows
            // exactly what to repair.
            if (ownerPointerKey != null
                && ownedBy(ownerPointerKey, normalized)
                && blobStore.head(key).isEmpty()) {
              LOG.errorf(
                  "cas gc deleted blob %s while owner pointer %s concurrently flipped to it:"
                      + " the pointer now dangles and the resource is corrupted — repair"
                      + " (re-create/re-sync) required",
                  key, ownerPointerKey);
            }
          }
        }
      }
      token = page.nextToken();
      if (token == null || token.isBlank()) {
        break;
      }
    }

    return new DeleteResult(scanned, deleted, rescued);
  }

  /**
   * Re-snapshots the live query pins immediately before an irreversible delete and reports whether
   * the blob must be kept. A query can publish a pin to a SUPERSEDED root at any instant — the
   * root's manifest pages and chain blobs are then absent from the sweep-time referenced set and
   * from the current-root re-mark, yet must survive. Pin publication is read-only (no re-PUT), so
   * the version fence cannot catch it either. Re-reading the (node-local, cheap) pin set here and
   * expanding any newly-pinned root's chain narrows the exposure to the microseconds between this
   * read and the delete — the same residual the owner recheck accepts. A pin-chain walk failure
   * makes reachability unprovable, so it too returns "keep" (and the caller aborts). Fully closing
   * the window needs atomic pin/GC coordination (see the node-local-pin constraint in the class
   * header).
   */
  private boolean keepForLatePin(
      String normalizedKey,
      Set<String> referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures) {
    rootLivePinChains(referenced, walkedPinRoots, walkFailures);
    return walkFailures[0] > 0 || referenced.contains(normalizedKey);
  }

  /** Whether the given owner pointer currently references exactly this normalized blob key. */
  private boolean ownedBy(String ownerPointerKey, String normalizedKey) {
    var owner = pointerStore.get(ownerPointerKey).orElse(null);
    return owner != null && normalizedKey.equals(normalizeKey(owner.getBlobUri()));
  }

  /**
   * Re-proves one table's chain-referenced liveness against the SETTLED store: re-reads the
   * table-root pointer and re-walks its chain, then re-scans the table's stats and constraints
   * pointer prefixes, accumulating every referenced URI into {@code fresh}. Returns false when the
   * chain walk could not complete — the caller must then treat the table's candidates as unprovable
   * and keep them.
   *
   * <p>Honesty about the guarantee: only the chain walk carries a completeness signal. The
   * stats/constraints re-scans rely on {@code listPointersByPrefix} either returning the full
   * settled listing or THROWING (which aborts the whole sweep) — a silently partial, non-throwing
   * listing is undetectable at this layer and would leave {@code fresh} incomplete. The flush
   * therefore still requires a candidate to be absent from BOTH the sweep's original referenced set
   * and this re-mark before deleting; that is two independent scans, not a proof.
   */
  private boolean remarkTable(String accountId, String tableId, Set<String> fresh, int pageSize) {
    var rootPtr = pointerStore.get(Keys.tableRootByTable(accountId, tableId)).orElse(null);
    if (rootPtr != null && !rootPtr.getBlobUri().isBlank()) {
      if (!rootTableRootChain(rootPtr.getBlobUri(), fresh)) {
        return false;
      }
    }
    collectPointers(
        Keys.snapshotRootPrefix(accountId, tableId),
        fresh,
        null,
        pageSize,
        ptr -> ptr.getKey() != null && ptr.getKey().contains(Keys.SEG_STATS));
    collectPointers(
        Keys.snapshotConstraintsPointerPrefix(accountId, tableId), fresh, null, pageSize);
    return true;
  }

  private static String decodeSuffix(String prefix, String fullKey) {
    if (fullKey == null || !fullKey.startsWith(prefix)) {
      return null;
    }
    String suffix = fullKey.substring(prefix.length());
    if (suffix.isBlank()) {
      return null;
    }
    return URLDecoder.decode(suffix, StandardCharsets.UTF_8);
  }

  private static String normalizeKey(String key) {
    if (key == null) {
      return "";
    }
    return key.startsWith("/") ? key.substring(1) : key;
  }
}
