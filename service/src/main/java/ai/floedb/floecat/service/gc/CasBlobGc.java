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

  public record Result(
      int pointersScanned,
      int blobsScanned,
      int blobsDeleted,
      int referenced,
      int tablesScanned,
      boolean poisoned) {}

  public Result runForAccount(String accountId) {
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

    if (walkFailures[0] > 0) {
      // A chain walk failed: manifest pages and every ref inside them are reachable only through
      // the walks, so the referenced set is incomplete and ANY delete could destroy a live root's
      // chain. Skip the whole delete phase; the next pass retries with (hopefully) healthy reads.
      LOG.warnf(
          "cas gc for account %s skipped its delete phase: %d root-chain walk(s) failed",
          accountId, walkFailures[0]);
      return new Result(pointersScanned, 0, 0, referenced.size(), tablesScanned, true);
    }

    var account =
        deleteUnreferenced(
            Keys.accountBlobPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_ACCOUNT),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += account.scanned();
    blobsDeleted += account.deleted();

    var catalogs =
        deleteUnreferenced(
            Keys.catalogRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_CATALOG),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += catalogs.scanned();
    blobsDeleted += catalogs.deleted();

    var namespaces =
        deleteUnreferenced(
            Keys.namespaceRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_NAMESPACE),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += namespaces.scanned();
    blobsDeleted += namespaces.deleted();

    // One LIST over the table subtree covers all five blob families that live under it — the
    // segment filters are mutually exclusive key shapes, so a combined pass deletes exactly what
    // five per-family passes would, at a fifth of the listing cost.
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
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += tableTree.scanned();
    blobsDeleted += tableTree.deleted();

    var views =
        deleteUnreferenced(
            Keys.viewRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_VIEW),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += views.scanned();
    blobsDeleted += views.deleted();

    var connectors =
        deleteUnreferenced(
            Keys.connectorRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_CONNECTOR),
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += connectors.scanned();
    blobsDeleted += connectors.deleted();

    // Report the FINAL poison state, not a static false: the delete passes re-run rootLivePinChains
    // per page, so a pin registered mid-sweep whose chain walk fails raises walkFailures[0] and
    // aborts that pass (protecting data). That poison must reach the scheduler's gauges — reporting
    // clean here would reset the clean-sweep clock and skip the poisoned-account count.
    return new Result(
        pointersScanned,
        blobsScanned,
        blobsDeleted,
        referenced.size(),
        tablesScanned,
        walkFailures[0] > 0);
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

  private record DeleteResult(int scanned, int deleted) {}

  private DeleteResult deleteUnreferenced(
      String prefix,
      Set<String> referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures,
      Predicate<String> isCandidate,
      int pageSize,
      long nowMs,
      long minAgeMs) {
    String token = "";
    int scanned = 0;
    int deleted = 0;

    while (true) {
      BlobStore.Page page = blobStore.list(prefix, pageSize, token);
      // Re-root the live pins once per page: the root set captured at the start of the run goes
      // stale over a long sweep, and a pin registered mid-sweep (whose blobs may have just lost
      // their pointer root) must still protect its blob AND its root's whole chain. The pin set is
      // node-local memory and chains walk at most once per pin root, so this stays cheap.
      rootLivePinChains(referenced, walkedPinRoots, walkFailures);
      if (walkFailures[0] > 0) {
        // A pin-chain walk failed mid-phase: stop deleting immediately (see the pass-level gate).
        LOG.warnf("cas gc delete pass over %s aborted: pin-chain walk failed mid-sweep", prefix);
        return new DeleteResult(scanned, deleted);
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
          // Last line of defense against the mark/CAS race: a pointer CAS can re-target an OLD
          // existing blob after this pass's one-time mark, invisible to both the mark and (if the
          // writer left LastModified stale) the age fence. For pointer-rooted families the key
          // encodes its owner (see Keys.ownerPointerKeyForBlob) — re-read that pointer right
          // before the delete and keep the blob it currently references.
          if (referencedByOwnerPointer(normalized)) {
            continue;
          }
          if (blobStore.delete(key)) {
            deleted++;
          }
        }
      }
      token = page.nextToken();
      if (token == null || token.isBlank()) {
        break;
      }
    }

    return new DeleteResult(scanned, deleted);
  }

  private boolean referencedByOwnerPointer(String normalizedKey) {
    String ownerKey = Keys.ownerPointerKeyForBlob(normalizedKey);
    if (ownerKey == null) {
      return false;
    }
    var owner = pointerStore.get(ownerKey).orElse(null);
    return owner != null && normalizedKey.equals(normalizeKey(owner.getBlobUri()));
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
