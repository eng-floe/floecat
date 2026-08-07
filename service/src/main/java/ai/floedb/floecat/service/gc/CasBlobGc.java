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

import ai.floedb.floecat.catalog.rpc.IndexArtifactRecord;
import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.common.rpc.ResourceId;
import ai.floedb.floecat.common.rpc.ResourceKind;
import ai.floedb.floecat.reconciler.rpc.ReusableArtifactBundlePayload;
import ai.floedb.floecat.service.query.QueryContextStore;
import ai.floedb.floecat.service.repo.impl.StatsRepository;
import ai.floedb.floecat.service.repo.impl.TableRootRepository;
import ai.floedb.floecat.service.repo.model.Keys;
import ai.floedb.floecat.service.repo.model.PointerReferences;
import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import ai.floedb.floecat.service.repo.util.TableBlobReachabilityGuard;
import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
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
 * with no owner pointer derivable from the key (manifest pages and shared index sidecars) cannot be
 * rescued individually, and lexicographic listing puts some of them before any blob whose rescue
 * could reveal the stale set — so their deletion is DEFERRED, and the flush independently re-proves
 * liveness per owning table against the settled store (root-chain re-walk plus constraints/stats
 * pointer re-scan) before deleting. The re-mark retains an exact table publication epoch, and the
 * epoch check plus version-targeted deletes run under the same table entry used by root, shared
 * sidecar, and resolving-pin publishers. A publication before the proof is included by the fresh
 * re-mark; one during or after it invalidates the proof and forces a complete restart. This closes
 * the late-publication window for ownerless manifest pages and shared sidecars.
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
  @Inject TableBlobReachabilityGuard reachabilityGuard;

  private static final String SCAN_COMPLETE = "\u0000";
  private PassContinuation continuation;
  private long activeDeadlineMs = Long.MAX_VALUE;

  private static final class DeferredPageState {
    private final String prefix;
    private final List<DeferredCandidate> candidates;
    private final boolean prefixPending;
    private final List<String> deletedKeys = new ArrayList<>();
    private ReferenceIndex fresh;
    private CandidateSetProbeReferenceIndex verificationProbe;
    private int deleteIndex;
    private int verifyIndex;
    private int deleted;
    private TableBlobReachabilityGuard.Proof remarkProof;
    private StatsRepository.GenerationGcContinuation generationRefresh;
    private boolean remarkComplete;
    private boolean verificationRemarkComplete;

    private DeferredPageState(
        String prefix, List<DeferredCandidate> candidates, boolean prefixPending) {
      this.prefix = prefix;
      this.candidates = List.copyOf(candidates);
      this.prefixPending = prefixPending;
      this.generationRefresh = new StatsRepository.GenerationGcContinuation();
    }

    private void close() {
      if (fresh != null) {
        fresh.close();
      }
      if (verificationProbe != null) {
        verificationProbe.close();
      }
      if (remarkProof != null) {
        remarkProof.close();
      }
    }
  }

  private enum Phase {
    ACCOUNT_MARK,
    TABLES,
    ACCOUNT_SWEEP
  }

  private static final class PassContinuation {
    private final String accountId;
    private final long passStartedAtMs;
    private final ReferenceIndex referenced;
    private final long referenceCapacity;
    private final double referenceFalsePositiveRate;
    private final int maxTableIds;
    private final int maxGenerationKeys;
    private final List<String> tableIds = new ArrayList<>();
    private Set<String> tableIdSet = new HashSet<>();
    private final StorageEstimate storageEstimate = new StorageEstimate();
    private final Set<String> walkedPinRoots = new HashSet<>();
    private final int[] walkFailures = {0};
    private final Map<String, String> pointerTokens = new LinkedHashMap<>();
    private final Map<String, String> blobTokens = new LinkedHashMap<>();
    private final Map<String, String> chainPageUris = new HashMap<>();
    private final Map<String, Integer> chainEntryIndexes = new HashMap<>();
    private ReferenceIndex chainVisited;
    private String chainRoot = "";
    private boolean accountMarked;
    private boolean tablesPrepared;
    private int tableIndex;
    private String currentTableId = "";
    private ReferenceIndex tableReferenced;
    private final Set<String> tableWalkedPinRoots = new HashSet<>();
    private final Set<Keys.GenerationKey> tableGenerationKeys = new HashSet<>();
    private final int[] tableWalkFailures = {0};
    private StatsRepository.GenerationGcContinuation generationGcContinuation;
    private DeferredPageState deferredPage;
    private int blobsScanned;
    private int blobsDeleted;
    private int blobsRescued;
    private int tablesScanned;
    private long completedTableReferenceInsertions;
    private double maxReferenceIndexSaturation;
    private double maxReferenceIndexFalsePositiveProbability;
    private boolean generationCleanupPending;
    private boolean poisoned;
    private Phase phase = Phase.ACCOUNT_MARK;

    private PassContinuation(
        String accountId,
        long passStartedAtMs,
        ReferenceIndex referenced,
        long referenceCapacity,
        double referenceFalsePositiveRate,
        int maxTableIds,
        int maxGenerationKeys) {
      this.accountId = accountId;
      this.passStartedAtMs = passStartedAtMs;
      this.referenced = referenced;
      this.referenceCapacity = referenceCapacity;
      this.referenceFalsePositiveRate = referenceFalsePositiveRate;
      this.maxTableIds = maxTableIds;
      this.maxGenerationKeys = maxGenerationKeys;
    }

    private void addTableId(String tableId) {
      if (tableIdSet.contains(tableId)) {
        return;
      }
      if (tableIdSet.size() >= maxTableIds) {
        throw new ReferenceIndex.CapacityExceededException(
            "table-id continuation capacity exceeded: capacity=" + maxTableIds);
      }
      tableIdSet.add(tableId);
      tableIds.add(tableId);
    }

    private void addGenerationKey(Keys.GenerationKey generation) {
      if (tableGenerationKeys.contains(generation)) {
        return;
      }
      if (tableGenerationKeys.size() >= maxGenerationKeys) {
        throw new ReferenceIndex.CapacityExceededException(
            "table generation-key capacity exceeded: capacity=" + maxGenerationKeys);
      }
      tableGenerationKeys.add(generation);
    }

    private void close() {
      referenced.close();
      if (chainVisited != null) {
        chainVisited.close();
      }
      if (tableReferenced != null) {
        tableReferenced.close();
      }
      if (deferredPage != null) {
        deferredPage.close();
      }
    }
  }

  private static final class DeadlineReached extends RuntimeException {
    private static final DeadlineReached INSTANCE = new DeadlineReached();

    private DeadlineReached() {
      super(null, null, false, false);
    }
  }

  /**
   * One sweep's tallies. {@code blobsDeleted} counts successful version-delete CALLS, not blobs
   * physically removed: (a) on a versioned store, deleting the current version of an N-version blob
   * leaves it readable with N-1 versions, so it is not "garbage fully reclaimed" (see the class
   * note on lifecycle rules); and (b) a version-targeted delete returns success — and is counted —
   * even for an already-absent blob (another actor removed it between this pass's head() and the
   * delete, or a 404). So the count can read slightly high; treat it as a delete-throughput signal,
   * not a reclaimed-bytes measure.
   */
  public record Result(
      int pointersScanned,
      long referencedBytes,
      int sizedBlobPointers,
      int blobPointers,
      int blobsScanned,
      int blobsDeleted,
      int blobsRescued,
      int referenced,
      int referenceIndexSaturationPpm,
      int referenceIndexEstimatedFalsePositivePpb,
      int tablesScanned,
      boolean poisoned,
      boolean deletesUnsupported,
      boolean generationCleanupPending) {
    Result(
        int pointersScanned,
        long referencedBytes,
        int sizedBlobPointers,
        int blobPointers,
        int blobsScanned,
        int blobsDeleted,
        int blobsRescued,
        int referenced,
        int tablesScanned,
        boolean poisoned,
        boolean deletesUnsupported,
        boolean generationCleanupPending) {
      this(
          pointersScanned,
          referencedBytes,
          sizedBlobPointers,
          blobPointers,
          blobsScanned,
          blobsDeleted,
          blobsRescued,
          referenced,
          0,
          0,
          tablesScanned,
          poisoned,
          deletesUnsupported,
          generationCleanupPending);
    }
  }

  /**
   * Cheap logical-storage estimate collected only from pointer rows the mark phase already reads.
   * No additional pointer listing or blob HEAD is permitted here.
   */
  private static final class StorageEstimate {
    private int pointers;
    private int blobPointers;
    private int sizedBlobPointers;
    private long referencedBytes;

    private void observe(Pointer pointer) {
      if (pointer == null) {
        return;
      }
      pointers++;
      if (!PointerReferences.isBlobPointer(pointer)
          || pointer.getBlobUri() == null
          || pointer.getBlobUri().isBlank()) {
        return;
      }
      blobPointers++;
      if (pointer.hasReferencedObjectSizeBytes()) {
        sizedBlobPointers++;
        referencedBytes =
            saturatedAdd(referencedBytes, Math.max(0L, pointer.getReferencedObjectSizeBytes()));
      }
    }

    private static long saturatedAdd(long left, long right) {
      if (right > Long.MAX_VALUE - left) {
        return Long.MAX_VALUE;
      }
      return left + right;
    }
  }

  public Result runForAccount(String accountId) {
    return runForAccount(accountId, Long.MAX_VALUE);
  }

  ReferenceIndex newReferenceIndex(long capacity, double falsePositiveRate, long seed) {
    return new BloomReferenceIndex(capacity, falsePositiveRate, seed);
  }

  public synchronized Result runForAccount(String accountId, long deadlineMs) {
    if (continuation != null && !continuation.accountId.equals(accountId)) {
      // Only one local mark epoch is retained, which gives the process a hard memory bound. The
      // scheduler prioritizes this account on the next tick; callers reaching another account in
      // the same tick receive a safe pending result without replacing the incomplete mark.
      return new Result(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, false, true);
    }
    activeDeadlineMs = deadlineMs;
    try {
      Result result = runPass(accountId, deadlineMs);
      clearContinuation();
      return result;
    } catch (DeadlineReached ignored) {
      return incompleteResult(continuation);
    } catch (ReferenceIndex.CapacityExceededException
        | StatsRepository.GenerationGcCapacityExceededException e) {
      PassContinuation failed = continuation;
      LOG.errorf(
          e,
          "cas gc mark epoch for account %s exceeded its bounded reference index; deleting is"
              + " stopped and the epoch will restart",
          accountId);
      Result result = poisonedResult(failed);
      clearContinuation();
      return result;
    } catch (RuntimeException e) {
      // Losing local continuation state is safe only by abandoning the epoch. The next invocation
      // starts a new mark and cannot resume deletion against an incomplete old mark.
      clearContinuation();
      throw e;
    } finally {
      activeDeadlineMs = Long.MAX_VALUE;
    }
  }

  Optional<String> continuationAccountId() {
    return continuation == null ? Optional.empty() : Optional.of(continuation.accountId);
  }

  synchronized void abandonContinuation() {
    if (continuation != null) {
      LOG.infof(
          "cas gc abandoning local mark epoch for account %s to preserve account fairness",
          continuation.accountId);
      clearContinuation();
    }
  }

  synchronized void abandonContinuationIfAccountMissing(Set<String> presentAccountIds) {
    if (continuation != null && !presentAccountIds.contains(continuation.accountId)) {
      LOG.infof(
          "cas gc abandoning local mark epoch for deleted account %s", continuation.accountId);
      clearContinuation();
    }
  }

  private static int referenceInsertions(PassContinuation pass) {
    long total = pass.completedTableReferenceInsertions + pass.referenced.insertions();
    if (pass.tableReferenced != null) {
      total += pass.tableReferenced.insertions();
    }
    return Math.toIntExact(Math.min(Integer.MAX_VALUE, total));
  }

  private static int saturationPpm(PassContinuation pass) {
    double saturation = Math.max(pass.maxReferenceIndexSaturation, pass.referenced.saturation());
    if (pass.tableReferenced != null) {
      saturation = Math.max(saturation, pass.tableReferenced.saturation());
    }
    return (int) Math.min(1_000_000L, Math.round(saturation * 1_000_000.0d));
  }

  private static int falsePositivePpb(PassContinuation pass) {
    double probability =
        Math.max(
            pass.maxReferenceIndexFalsePositiveProbability,
            pass.referenced.estimatedFalsePositiveProbability());
    if (pass.tableReferenced != null) {
      probability = Math.max(probability, pass.tableReferenced.estimatedFalsePositiveProbability());
    }
    return (int) Math.min(1_000_000_000L, Math.round(probability * 1_000_000_000.0d));
  }

  private static void observeReferenceIndex(PassContinuation pass, ReferenceIndex index) {
    pass.maxReferenceIndexSaturation =
        Math.max(pass.maxReferenceIndexSaturation, index.saturation());
    pass.maxReferenceIndexFalsePositiveProbability =
        Math.max(
            pass.maxReferenceIndexFalsePositiveProbability,
            index.estimatedFalsePositiveProbability());
  }

  private Result incompleteResult(PassContinuation pass) {
    if (pass == null) {
      return new Result(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, false, true);
    }
    return new Result(
        pass.storageEstimate.pointers,
        pass.storageEstimate.referencedBytes,
        pass.storageEstimate.sizedBlobPointers,
        pass.storageEstimate.blobPointers,
        0,
        0,
        0,
        referenceInsertions(pass),
        saturationPpm(pass),
        falsePositivePpb(pass),
        pass.tableIds.size(),
        false,
        false,
        true);
  }

  private Result poisonedResult(PassContinuation pass) {
    if (pass == null) {
      return new Result(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, true, false, true);
    }
    return new Result(
        pass.storageEstimate.pointers,
        pass.storageEstimate.referencedBytes,
        pass.storageEstimate.sizedBlobPointers,
        pass.storageEstimate.blobPointers,
        0,
        0,
        0,
        referenceInsertions(pass),
        saturationPpm(pass),
        falsePositivePpb(pass),
        pass.tableIds.size(),
        true,
        false,
        true);
  }

  private void clearContinuation() {
    if (continuation != null) {
      continuation.close();
      continuation = null;
    }
  }

  private void checkDeadline() {
    if (System.currentTimeMillis() >= activeDeadlineMs) {
      throw DeadlineReached.INSTANCE;
    }
  }

  private boolean isRetainedContinuationIndex(ReferenceIndex index) {
    return continuation != null
        && (index == continuation.referenced
            || index == continuation.tableReferenced
            || (continuation.deferredPage != null
                && (index == continuation.deferredPage.fresh
                    || index == continuation.deferredPage.verificationProbe)));
  }

  private String continuationIndexScope(ReferenceIndex index) {
    if (continuation == null) {
      return "transient";
    }
    if (index == continuation.referenced) {
      return "account";
    }
    if (index == continuation.tableReferenced) {
      return "table:" + continuation.currentTableId;
    }
    if (continuation.deferredPage != null && index == continuation.deferredPage.fresh) {
      return "remark:" + continuation.currentTableId + ":" + continuation.deferredPage.prefix;
    }
    if (continuation.deferredPage != null && index == continuation.deferredPage.verificationProbe) {
      return "verify:" + continuation.currentTableId + ":" + continuation.deferredPage.prefix;
    }
    return "transient";
  }

  private Result runPass(String accountId, long deadlineMs) {
    if (!blobStore.supportsVersionedDeletes()) {
      // Fail closed: without immutable version identities every delete is the
      // eng-floe/core#1904 race (an S3 bucket whose versioning is not Enabled overwrites version
      // state in place), so nothing may be collected. One warn per pass; the scheduler gauges
      // this so a misconfigured bucket is noticed rather than silently never collecting.
      LOG.warnf(
          "cas gc for account %s skipped: blob store cannot delete by immutable version"
              + " (on S3 the bucket's versioning status must be Enabled)",
          accountId);
      return new Result(0, 0L, 0, 0, 0, 0, 0, 0, 0, 0, 0, false, true, false);
    }

    final var cfg = ConfigProvider.getConfig();
    final int pageSize =
        Math.max(1, cfg.getOptionalValue("floecat.gc.cas.page-size", Integer.class).orElse(500));
    final long minAgeMs =
        Math.max(0L, cfg.getOptionalValue("floecat.gc.cas.min-age-ms", Long.class).orElse(30_000L));
    int remainingGenerationBlobDeletes =
        Math.max(
            1,
            cfg.getOptionalValue(
                    "floecat.gc.cas.stats-generation-blob-deletes-per-account", Integer.class)
                .orElse(1000));
    final long passStart = System.currentTimeMillis();

    final long referenceCapacity =
        Math.max(
            1L,
            cfg.getOptionalValue("floecat.gc.cas.reference-index.expected-capacity", Long.class)
                .orElse(2_000_000L));
    final double referenceFalsePositiveRate =
        cfg.getOptionalValue("floecat.gc.cas.reference-index.false-positive-rate", Double.class)
            .orElse(0.0000001d);
    final int maxTableIds =
        Math.max(
            1,
            cfg.getOptionalValue("floecat.gc.cas.max-table-ids-per-account", Integer.class)
                .orElse(100_000));
    final int maxGenerationKeys =
        Math.max(
            1,
            cfg.getOptionalValue("floecat.gc.cas.max-stats-generations-per-table", Integer.class)
                .orElse(100_000));
    if (continuation == null) {
      continuation =
          new PassContinuation(
              accountId,
              passStart,
              newReferenceIndex(
                  referenceCapacity,
                  referenceFalsePositiveRate,
                  ThreadLocalRandom.current().nextLong()),
              referenceCapacity,
              referenceFalsePositiveRate,
              maxTableIds,
              maxGenerationKeys);
    }
    PassContinuation pass = continuation;
    final long nowMs = pass.passStartedAtMs;
    ReferenceIndex referenced = pass.referenced;
    List<String> tableIds = pass.tableIds;
    int pointersScanned = 0;
    StorageEstimate storageEstimate = pass.storageEstimate;

    checkDeadline();
    if (!pass.accountMarked) {
      var accountPtr = pointerStore.get(Keys.accountPointerById(accountId)).orElse(null);
      if (accountPtr != null && !accountPtr.getBlobUri().isBlank()) {
        referenced.add(normalizeKey(accountPtr.getBlobUri()));
        pointersScanned++;
        storageEstimate.observe(accountPtr);
      }
      pass.accountMarked = true;
    }

    pointersScanned +=
        collectPointers(
            Keys.catalogPointerByIdPrefix(accountId),
            referenced,
            null,
            pageSize,
            null,
            storageEstimate);
    pointersScanned +=
        collectPointers(
            Keys.namespacePointerByIdPrefix(accountId),
            referenced,
            null,
            pageSize,
            null,
            storageEstimate);
    pointersScanned +=
        collectPointers(
            Keys.tablePointerByIdPrefix(accountId),
            referenced,
            tableIds,
            pageSize,
            null,
            storageEstimate,
            false);
    pointersScanned +=
        collectPointers(
            Keys.viewPointerByIdPrefix(accountId),
            referenced,
            null,
            pageSize,
            null,
            storageEstimate);
    pointersScanned +=
        collectPointers(
            Keys.connectorPointerByIdPrefix(accountId),
            referenced,
            null,
            pageSize,
            null,
            storageEstimate);
    pointersScanned +=
        collectPointers(
            Keys.storageAuthorityPointerByIdPrefix(accountId),
            referenced,
            null,
            pageSize,
            null,
            storageEstimate);
    collectTableBlobIds(accountId, tableIds, pageSize);

    // A pinned ROOT protects everything it references, not just its own blob: a query pinned to a
    // superseded root must keep reading that root's pages, snapshot blobs, generation manifests,
    // and constraints bundles until it ends. `walkedPinRoots` remembers which pin roots have had
    // their chains walked so pins registered mid-sweep can be rooted incrementally.
    // `walkFailures` poisons the sweep: manifest pages and per-entry refs are reachable ONLY
    // through chain walks, so a walk that could not complete (missing blob, storage error) means
    // the referenced set is not trustworthy and nothing may be deleted this pass.
    int[] walkFailures;
    Set<String> walkedPinRoots = pass.walkedPinRoots;
    walkFailures = pass.walkFailures;
    rootLivePinChains(referenced, walkedPinRoots, walkFailures);
    pass.phase = Phase.TABLES;

    if (!pass.tablesPrepared) {
      if (!tableIds.isEmpty()) {
        String cursor = generationCursor(accountId);
        int start = cursor.isBlank() ? 0 : tableIds.indexOf(cursor);
        if (start < 0) {
          start = 0;
        }
        Collections.rotate(tableIds, -start);
      }
      // Discovery needs a set for deduplication, but table processing only needs the ordered list.
      // Release the duplicate hash structure for the potentially long-lived continuation.
      pass.tableIdSet = null;
      pass.tablesPrepared = true;
    }
    for (int tableIndex = pass.tableIndex; tableIndex < tableIds.size(); tableIndex++) {
      checkDeadline();
      String tableId = tableIds.get(tableIndex);
      if (!tableId.equals(pass.currentTableId)) {
        if (pass.tableReferenced != null) {
          pass.tableReferenced.close();
        }
        pass.currentTableId = tableId;
        pass.tableReferenced =
            newReferenceIndex(
                pass.referenceCapacity,
                pass.referenceFalsePositiveRate,
                ThreadLocalRandom.current().nextLong());
        pass.generationGcContinuation = new StatsRepository.GenerationGcContinuation();
        pass.tableWalkedPinRoots.clear();
        pass.tableWalkFailures[0] = 0;
      }
      ReferenceIndex tableReferenced = pass.tableReferenced;
      var tablePointer = pointerStore.get(Keys.tablePointerById(accountId, tableId)).orElse(null);
      if (tablePointer != null && !tablePointer.getBlobUri().isBlank()) {
        tableReferenced.add(normalizeKey(tablePointer.getBlobUri()));
      }
      var currentSnapshotPointer =
          pointerStore.get(Keys.currentSnapshotPointerByTable(accountId, tableId)).orElse(null);
      if (currentSnapshotPointer != null && !currentSnapshotPointer.getBlobUri().isBlank()) {
        tableReferenced.add(normalizeKey(currentSnapshotPointer.getBlobUri()));
        pointersScanned++;
        storageEstimate.observe(currentSnapshotPointer);
      }
      rootLivePinChains(tableReferenced, pass.tableWalkedPinRoots, pass.tableWalkFailures);
      String snapshotsById = Keys.snapshotPointerByIdPrefix(accountId, tableId);
      pointersScanned +=
          collectPointers(snapshotsById, tableReferenced, null, pageSize, null, storageEstimate);

      // The current table root and EVERYTHING it references are GC roots: the root blob, every
      // manifest page, and each entry's definition/snapshot/generation-manifest/constraints blobs.
      // This MUST happen before the generation reclaim below — a generation the current root still
      // references is protected even when the live active pointer has already moved past it (the
      // finalize's pointer flip and root commit are not atomic). Superseded root chains no live
      // pin references are unreferenced and swept below.
      var rootPtr = pointerStore.get(Keys.tableRootByTable(accountId, tableId)).orElse(null);
      if (rootPtr != null && !rootPtr.getBlobUri().isBlank()) {
        pointersScanned++;
        storageEstimate.observe(rootPtr);
        if (!rootTableRootChain(rootPtr.getBlobUri(), tableReferenced)) {
          pass.tableWalkFailures[0]++;
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
      // Keep the potentially wide identity scan outside the publication lock. Once complete, the
      // bounded delete slice below is serialized with resolving-pin publication: delete-first
      // makes pin validation fail closed, while pin-first makes the protection predicate keep the
      // generation and every shared sidecar its wrappers reference.
      if (!statsRepository.discoverGenerationKeys(rid, deadlineMs, pass.generationGcContinuation)) {
        checkDeadline();
        throw DeadlineReached.INSTANCE;
      }
      for (Keys.GenerationKey generation : pass.generationGcContinuation.generations()) {
        pass.addGenerationKey(generation);
      }
      int generationDeleteBudget = remainingGenerationBlobDeletes;
      StatsRepository.GenerationGcResult generationGc =
          reachabilityGuard.exclusive(
              rid,
              () ->
                  statsRepository.deleteUnreferencedGenerations(
                      rid,
                      manifestUri -> {
                        if (pass.tableWalkFailures[0] > 0) {
                          return true; // an incomplete walk makes protection unknowable
                        }
                        String normalized = normalizeKey(manifestUri);
                        if (tableReferenced.mightContain(normalized)) {
                          return true;
                        }
                        rootLivePinChains(
                            tableReferenced, pass.tableWalkedPinRoots, pass.tableWalkFailures);
                        return pass.tableWalkFailures[0] > 0
                            || tableReferenced.mightContain(normalized);
                      },
                      nowMs,
                      minAgeMs,
                      generationDeleteBudget,
                      deadlineMs,
                      pass.generationGcContinuation));
      remainingGenerationBlobDeletes =
          Math.max(0, remainingGenerationBlobDeletes - generationGc.blobDeleteAttempts());
      pass.blobsDeleted += generationGc.blobsDeleted();
      pass.generationCleanupPending |= generationGc.pending();
      // Generation-owned wrappers and reuse bundles are reclaimed only by StatsRepository. Their
      // IndexArtifactRecord payloads can, however, reference shared content-addressed sidecars.
      // Expand only those indirect shared references into the generic mark index; never add the
      // generation-owned wrapper URI itself.
      pointersScanned +=
          collectSharedIndexArtifactReferences(
              accountId, tableId, tableReferenced, pageSize, storageEstimate);

      // Constraints pointers live under a SIBLING prefix (/constraints/by-snapshot/), not under
      // /snapshots/, so they need their own scan. A constraints blob is deletable (it matches the
      // delete predicate) and its pointer goes live before commitConstraints records the ref on the
      // root — this scan protects the blob during that window, symmetric with the stats pointers.
      pointersScanned +=
          collectPointers(
              Keys.snapshotConstraintsPointerPrefix(accountId, tableId),
              tableReferenced,
              null,
              pageSize,
              null,
              storageEstimate);
      if (pass.tableWalkFailures[0] == 0) {
        DeleteResult tableSweep =
            sweepTable(
                accountId,
                tableId,
                tableReferenced,
                pass.tableWalkedPinRoots,
                pass.tableWalkFailures,
                pageSize,
                nowMs,
                minAgeMs,
                referenceCapacity,
                referenceFalsePositiveRate);
        pass.blobsScanned += tableSweep.scanned();
        pass.blobsDeleted += tableSweep.deleted();
        pass.blobsRescued += tableSweep.rescued();
        pass.generationCleanupPending |= tableSweep.pending();
        pass.poisoned |= pass.tableWalkFailures[0] > 0;
      } else {
        pass.poisoned = true;
      }
      pass.tablesScanned++;
      clearCompletedTableState(accountId, tableId, pass);
      pass.tableIndex = tableIndex + 1;
      // This is the durable checkpoint used when the scheduler abandons an in-memory continuation
      // to give another account a turn. Advance only after the entire table has completed, so a
      // deadline in the middle of a table always resumes that table rather than skipping ahead.
      advanceGenerationCursor(accountId, tableIds.get((tableIndex + 1) % tableIds.size()));
    }

    // Active query pins are GC roots: an immutable blob a live query pinned must survive even after
    // the current catalog pointers have advanced past it, until that query (and its scan lease)
    // ends. Pin blob URIs share MutationMeta.getBlobUri()'s shape, so they normalize identically to
    // the pointer-derived roots above. This snapshot seeds the root set; because a sweep can run
    // for a while and pins are registered continuously, the delete passes below also re-read the
    // (in-memory, cheap) pin roots per page so a pin taken mid-sweep still protects its blob.

    int blobsScanned = pass.blobsScanned;
    int blobsDeleted = pass.blobsDeleted;
    int blobsRescued = pass.blobsRescued;

    if (walkFailures[0] > 0) {
      // A chain walk failed: manifest pages and every ref inside them are reachable only through
      // the walks, so the referenced set is incomplete and ANY delete could destroy a live root's
      // chain. Skip the whole delete phase; the next pass retries with (hopefully) healthy reads.
      LOG.warnf(
          "cas gc for account %s skipped its delete phase: %d root-chain walk(s) failed",
          accountId, walkFailures[0]);
      return new Result(
          storageEstimate.pointers,
          storageEstimate.referencedBytes,
          storageEstimate.sizedBlobPointers,
          storageEstimate.blobPointers,
          0,
          0,
          0,
          referenceInsertions(pass),
          saturationPpm(pass),
          falsePositivePpb(pass),
          pass.tablesScanned,
          true,
          false,
          pass.generationCleanupPending);
    }

    pass.phase = Phase.ACCOUNT_SWEEP;

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

    var storageAuthorities =
        deleteUnreferenced(
            Keys.storageAuthorityRootPrefix(accountId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key -> key.contains(Keys.SEG_STORAGE_AUTHORITY),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    blobsScanned += storageAuthorities.scanned();
    blobsDeleted += storageAuthorities.deleted();
    blobsRescued += storageAuthorities.rescued();

    // Report the FINAL poison state, not a static false: the delete passes re-run rootLivePinChains
    // per page, so a pin registered mid-sweep whose chain walk fails raises walkFailures[0] and
    // aborts that pass (protecting data). That poison must reach the scheduler's gauges — reporting
    // clean here would reset the clean-sweep clock and skip the poisoned-account count.
    return new Result(
        storageEstimate.pointers,
        storageEstimate.referencedBytes,
        storageEstimate.sizedBlobPointers,
        storageEstimate.blobPointers,
        blobsScanned,
        blobsDeleted,
        blobsRescued,
        referenceInsertions(pass),
        saturationPpm(pass),
        falsePositivePpb(pass),
        pass.tablesScanned,
        walkFailures[0] > 0 || pass.poisoned,
        false,
        pass.generationCleanupPending);
  }

  /**
   * Sweeps only the generic blob families owned by one table. Target-stats, generation-owned index
   * wrappers, worker uploads, and finalizer outputs live under {@code /target-stats/}; that prefix
   * is deliberately absent because {@link StatsRepository} is their sole collector.
   */
  private DeleteResult sweepTable(
      String accountId,
      String tableId,
      ReferenceIndex referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures,
      int pageSize,
      long nowMs,
      long minAgeMs,
      long referenceCapacity,
      double referenceFalsePositiveRate) {
    int scanned = 0;
    int deleted = 0;
    int rescued = 0;

    List<String> directPrefixes =
        List.of(
            Keys.tableDefinitionBlobPrefix(accountId, tableId),
            Keys.tableConstraintsBlobPrefix(accountId, tableId));
    for (String prefix : directPrefixes) {
      DeleteResult result =
          deleteUnreferenced(
              prefix,
              referenced,
              walkedPinRoots,
              walkFailures,
              key -> true,
              null,
              pageSize,
              nowMs,
              minAgeMs);
      scanned += result.scanned();
      deleted += result.deleted();
      rescued += result.rescued();
      if (walkFailures[0] > 0) {
        return new DeleteResult(scanned, deleted, rescued, true);
      }
    }

    DeleteResult snapshots =
        deleteUnreferenced(
            Keys.tableSnapshotBlobPrefix(accountId, tableId),
            referenced,
            walkedPinRoots,
            walkFailures,
            key ->
                key.contains(Keys.SEG_SNAPSHOT)
                    || key.contains("/snapshots/current/")
                    || key.contains(Keys.SEG_INDEX_CAPTURE_MANIFESTS),
            null,
            pageSize,
            nowMs,
            minAgeMs);
    scanned += snapshots.scanned();
    deleted += snapshots.deleted();
    rescued += snapshots.rescued();

    for (String prefix :
        List.of(
            Keys.tableRootBlobPrefix(accountId, tableId),
            Keys.tableIndexSidecarBlobPrefix(accountId, tableId))) {
      boolean more;
      do {
        if (continuation.deferredPage == null) {
          var deferred = new ArrayList<DeferredCandidate>(pageSize);
          DeleteResult listed =
              deleteUnreferenced(
                  prefix,
                  referenced,
                  walkedPinRoots,
                  walkFailures,
                  key -> true,
                  deferred,
                  pageSize,
                  nowMs,
                  minAgeMs);
          scanned += listed.scanned();
          deleted += listed.deleted();
          rescued += listed.rescued();
          if (walkFailures[0] > 0) {
            return new DeleteResult(scanned, deleted, rescued, true);
          }
          if (deferred.isEmpty()) {
            more = listed.pending();
            continue;
          }
          continuation.deferredPage = new DeferredPageState(prefix, deferred, listed.pending());
        } else if (!prefix.equals(continuation.deferredPage.prefix)) {
          // A prior prefix completed before the deadline but its bounded candidate page did not.
          // Resume that state when the loop reaches its owning prefix.
          more = false;
          continue;
        }
        DeleteResult flushed =
            flushDeferredTableCandidates(
                accountId,
                tableId,
                referenced,
                walkedPinRoots,
                walkFailures,
                pageSize,
                referenceCapacity,
                referenceFalsePositiveRate);
        deleted += flushed.deleted();
        rescued += flushed.rescued();
        if (walkFailures[0] > 0) {
          return new DeleteResult(scanned, deleted, rescued, true);
        }
        more = continuation.deferredPage.prefixPending;
        continuation.deferredPage.close();
        continuation.deferredPage = null;
        clearRemarkContinuationState(tableId, prefix);
      } while (more);
    }
    return new DeleteResult(scanned, deleted, rescued, false);
  }

  private DeleteResult flushDeferredTableCandidates(
      String accountId,
      String tableId,
      ReferenceIndex referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures,
      int pageSize,
      long referenceCapacity,
      double referenceFalsePositiveRate) {
    DeferredPageState state = continuation.deferredPage;
    while (true) {
      rootLivePinChains(referenced, walkedPinRoots, walkFailures);
      if (walkFailures[0] > 0) {
        return new DeleteResult(0, 0, 0, true);
      }
      if (state.fresh == null) {
        state.remarkProof = reachabilityGuard.beginProof(accountId, tableId);
        state.fresh =
            newReferenceIndex(
                referenceCapacity,
                referenceFalsePositiveRate,
                ThreadLocalRandom.current().nextLong());
      }
      if (state.generationRefresh != null) {
        var tableResourceId =
            ResourceId.newBuilder()
                .setAccountId(accountId)
                .setId(tableId)
                .setKind(ResourceKind.RK_TABLE)
                .build();
        if (!statsRepository.discoverGenerationKeys(
            tableResourceId, activeDeadlineMs, state.generationRefresh)) {
          checkDeadline();
          throw DeadlineReached.INSTANCE;
        }
        for (Keys.GenerationKey generation : state.generationRefresh.generations()) {
          continuation.addGenerationKey(generation);
        }
        state.generationRefresh = null;
      }
      if (!state.remarkComplete) {
        if (!remarkTable(accountId, tableId, state.fresh, pageSize)) {
          walkFailures[0]++;
          return new DeleteResult(0, 0, 0, true);
        }
        state.remarkComplete = true;
      }
      var guarded =
          reachabilityGuard.deleteIfUnchanged(
              state.remarkProof,
              () -> {
                while (state.deleteIndex < state.candidates.size()) {
                  checkDeadline();
                  DeferredCandidate candidate = state.candidates.get(state.deleteIndex);
                  String normalized = normalizeKey(candidate.key());
                  if (!referenced.mightContain(normalized)
                      && !state.fresh.mightContain(normalized)) {
                    if (keepForLatePin(normalized, referenced, walkedPinRoots, walkFailures)) {
                      if (walkFailures[0] > 0) {
                        return false;
                      }
                    } else if (blobStore.delete(candidate.key(), candidate.versionId())) {
                      state.deleted++;
                      state.deletedKeys.add(normalized);
                    }
                  }
                  state.deleteIndex++;
                }
                return true;
              });
      if (guarded.changed()) {
        resetDeferredRemark(tableId, state);
        checkDeadline();
        continue;
      }
      if (!Boolean.TRUE.equals(guarded.value())) {
        return new DeleteResult(0, state.deleted, 0, true);
      }
      break;
    }
    // The delete ran under the exact publication epoch retained by remarkProof. If that epoch is
    // still unchanged, no publisher could have re-referenced a deleted ownerless blob and a second
    // whole-table mark is unnecessary. Only an overlapping publication pays for the exact,
    // page-bounded verification re-mark.
    boolean publicationAfterDelete =
        !state.deletedKeys.isEmpty()
            && reachabilityGuard.deleteIfUnchanged(state.remarkProof, () -> true).changed();
    if (publicationAfterDelete
        && !state.deletedKeys.isEmpty()
        && !state.verificationRemarkComplete) {
      if (state.verificationProbe == null) {
        state.verificationProbe = new CandidateSetProbeReferenceIndex(state.deletedKeys);
      }
      if (!remarkTable(accountId, tableId, state.verificationProbe, pageSize)) {
        walkFailures[0]++;
        return new DeleteResult(0, state.deleted, 0, true);
      }
      state.verificationRemarkComplete = true;
    }
    while (publicationAfterDelete && state.verifyIndex < state.deletedKeys.size()) {
      checkDeadline();
      String normalized = state.deletedKeys.get(state.verifyIndex);
      if (blobStore.head(normalized).isEmpty()
          && state.verificationProbe.mightContain(normalized)) {
        LOG.errorf(
            "cas gc deleted deferred blob %s of table %s that a concurrent commit"
                + " re-referenced during the flush: the resource is corrupted — repair"
                + " (re-create/re-sync) required",
            normalized, tableId);
      }
      state.verifyIndex++;
    }
    observeReferenceIndex(continuation, state.fresh);
    return new DeleteResult(0, state.deleted, 0, false);
  }

  private void resetDeferredRemark(String tableId, DeferredPageState state) {
    if (state.fresh != null) {
      state.fresh.close();
    }
    if (state.remarkProof != null) {
      state.remarkProof.close();
    }
    state.remarkProof = null;
    state.fresh = null;
    state.remarkComplete = false;
    state.generationRefresh = new StatsRepository.GenerationGcContinuation();
    clearRemarkContinuationState(tableId, state.prefix);
  }

  private void clearRemarkContinuationState(String tableId, String prefix) {
    String remarkScope = "remark:" + tableId + ":" + prefix;
    String verifyScope = "verify:" + tableId + ":" + prefix;
    continuation
        .pointerTokens
        .keySet()
        .removeIf(key -> key.startsWith(remarkScope) || key.startsWith(verifyScope));
    continuation
        .chainPageUris
        .keySet()
        .removeIf(key -> key.startsWith(remarkScope) || key.startsWith(verifyScope));
    continuation
        .chainEntryIndexes
        .keySet()
        .removeIf(key -> key.startsWith(remarkScope) || key.startsWith(verifyScope));
    if (continuation.chainRoot.startsWith(remarkScope)
        || continuation.chainRoot.startsWith(verifyScope)) {
      if (continuation.chainVisited != null) {
        continuation.chainVisited.close();
      }
      continuation.chainVisited = null;
      continuation.chainRoot = "";
    }
  }

  private void clearCompletedTableState(String accountId, String tableId, PassContinuation pass) {
    String tablePrefix = Keys.tableBlobPrefix(accountId, tableId);
    pass.pointerTokens.keySet().removeIf(key -> key.contains(tablePrefix));
    pass.blobTokens.keySet().removeIf(key -> key.contains(tablePrefix));
    pass.chainPageUris
        .keySet()
        .removeIf(key -> key.contains(tablePrefix) || key.contains(normalizeKey(tablePrefix)));
    pass.chainEntryIndexes
        .keySet()
        .removeIf(key -> key.contains(tablePrefix) || key.contains(normalizeKey(tablePrefix)));
    if (pass.deferredPage != null) {
      pass.deferredPage.close();
      pass.deferredPage = null;
    }
    if (pass.chainVisited != null) {
      pass.chainVisited.close();
      pass.chainVisited = null;
      pass.chainRoot = "";
    }
    if (pass.tableReferenced != null) {
      pass.completedTableReferenceInsertions += pass.tableReferenced.insertions();
      observeReferenceIndex(pass, pass.tableReferenced);
      pass.tableReferenced.close();
      pass.tableReferenced = null;
    }
    pass.currentTableId = "";
    pass.generationGcContinuation = null;
    pass.tableWalkedPinRoots.clear();
    pass.tableGenerationKeys.clear();
    pass.tableWalkFailures[0] = 0;
  }

  private String generationCursor(String accountId) {
    return pointerStore
        .get(Keys.casGcGenerationCursorPointer(accountId))
        .map(Pointer::getBlobUri)
        .orElse("");
  }

  private void advanceGenerationCursor(String accountId, String nextTableId) {
    String key = Keys.casGcGenerationCursorPointer(accountId);
    for (int attempt = 0; attempt < BaseResourceRepository.CAS_MAX; attempt++) {
      Pointer current = pointerStore.get(key).orElse(null);
      long expectedVersion = current == null ? 0L : current.getVersion();
      if (pointerStore.compareAndSet(
          key,
          expectedVersion,
          PointerReferences.opaqueMarkerPointer(key, nextTableId, expectedVersion + 1L))) {
        return;
      }
    }
    throw new BaseResourceRepository.AbortRetryableException(
        "exhausted CAS attempts advancing CAS GC generation cursor for account " + accountId);
  }

  /**
   * Root a table-root chain: the root blob itself, every snapshot-manifest page, and each entry's
   * definition/snapshot/stats-generation-manifest/constraints blob URIs. Returns whether the walk
   * COMPLETED — manifest pages and the refs inside them have no pointer-based protection of their
   * own, so an incomplete walk (unreadable root or page, transient storage error) must poison the
   * sweep: deleting against a partially-rooted set could destroy a live chain permanently.
   */
  private boolean rootTableRootChain(String rootBlobUri, ReferenceIndex referenced) {
    referenced.add(normalizeKey(rootBlobUri));
    boolean resumable = isRetainedContinuationIndex(referenced);
    String chainKey = continuationIndexScope(referenced) + ":" + rootBlobUri;
    String savedPage = resumable ? continuation.chainPageUris.get(chainKey) : null;
    if (SCAN_COMPLETE.equals(savedPage)) {
      return true;
    }
    ReferenceIndex walkedPages;
    if (resumable) {
      if (!chainKey.equals(continuation.chainRoot)) {
        if (continuation.chainVisited != null) {
          continuation.chainVisited.close();
        }
        continuation.chainRoot = chainKey;
        continuation.chainVisited =
            newReferenceIndex(
                continuation.referenceCapacity,
                continuation.referenceFalsePositiveRate,
                ThreadLocalRandom.current().nextLong());
      }
      walkedPages = continuation.chainVisited;
    } else {
      var cfg = ConfigProvider.getConfig();
      walkedPages =
          newReferenceIndex(
              Math.max(
                  1L,
                  cfg.getOptionalValue(
                          "floecat.gc.cas.reference-index.expected-capacity", Long.class)
                      .orElse(2_000_000L)),
              cfg.getOptionalValue(
                      "floecat.gc.cas.reference-index.false-positive-rate", Double.class)
                  .orElse(0.0000001d),
              ThreadLocalRandom.current().nextLong());
    }
    try {
      checkDeadline();
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
      var pageRef = root.hasSnapshotManifestRef() ? root.getSnapshotManifestRef() : null;
      boolean resumingPage = resumable && continuation.chainEntryIndexes.containsKey(chainKey);
      if (savedPage != null && !savedPage.isBlank()) {
        pageRef = ai.floedb.floecat.catalog.rpc.BlobRef.newBuilder().setUri(savedPage).build();
      }
      while (pageRef != null && !pageRef.getUri().isBlank()) {
        checkDeadline();
        String normalizedPage = normalizeKey(pageRef.getUri());
        if (!resumingPage && walkedPages.mightContain(normalizedPage)) {
          // Content-addressed pages are acyclic by construction, so a repeated URI means a corrupt
          // or malformed chain. Fail safe (poison the sweep) instead of looping until OOM on the
          // background GC thread, which has no request timeout to rescue it.
          LOG.warnf(
              "cas gc manifest chain of root %s is cyclic at %s; poisoning the sweep",
              rootBlobUri, pageRef.getUri());
          return false;
        }
        if (!resumingPage) {
          walkedPages.add(normalizedPage);
          if (resumable) {
            continuation.chainPageUris.put(chainKey, pageRef.getUri());
            continuation.chainEntryIndexes.put(chainKey, 0);
          }
        }
        referenced.add(normalizedPage);
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
        int entryStart = resumable ? continuation.chainEntryIndexes.getOrDefault(chainKey, 0) : 0;
        for (int entryIndex = entryStart; entryIndex < page.getEntriesCount(); entryIndex++) {
          checkDeadline();
          var entry = page.getEntries(entryIndex);
          if (entry.hasSnapshotRef() && !entry.getSnapshotRef().getUri().isBlank()) {
            referenced.add(normalizeKey(entry.getSnapshotRef().getUri()));
          }
          if (entry.hasStatsGenerationRef() && !entry.getStatsGenerationRef().getUri().isBlank()) {
            String statsRefUri = entry.getStatsGenerationRef().getUri();
            referenced.add(normalizeKey(statsRefUri));
            rememberTableGeneration(referenced, statsRefUri);
          }
          if (entry.hasReuseStatsGenerationRef()
              && !entry.getReuseStatsGenerationRef().getUri().isBlank()) {
            String reuseRefUri = entry.getReuseStatsGenerationRef().getUri();
            referenced.add(normalizeKey(reuseRefUri));
            rememberTableGeneration(referenced, reuseRefUri);
          }
          if (entry.hasConstraintsRef() && !entry.getConstraintsRef().getUri().isBlank()) {
            referenced.add(normalizeKey(entry.getConstraintsRef().getUri()));
          }
          if (resumable) {
            continuation.chainEntryIndexes.put(chainKey, entryIndex + 1);
          }
        }
        pageRef = page.hasPrevPageRef() ? page.getPrevPageRef() : null;
        if (resumable) {
          continuation.chainEntryIndexes.remove(chainKey);
          continuation.chainPageUris.put(
              chainKey,
              pageRef == null || pageRef.getUri().isBlank() ? SCAN_COMPLETE : pageRef.getUri());
        }
        resumingPage = false;
        checkDeadline();
      }
      if (resumable) {
        continuation.chainPageUris.put(chainKey, SCAN_COMPLETE);
        continuation.chainEntryIndexes.remove(chainKey);
        continuation.chainVisited.close();
        continuation.chainVisited = null;
        continuation.chainRoot = "";
      }
      return true;
    } catch (DeadlineReached | ReferenceIndex.CapacityExceededException e) {
      throw e;
    } catch (RuntimeException e) {
      LOG.warnf(e, "cas gc chain walk failed for root %s; sweep will be skipped", rootBlobUri);
      return false;
    } finally {
      if (!resumable) {
        walkedPages.close();
      }
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
      String prefix, ReferenceIndex referenced, List<String> tableIds, int pageSize) {
    return collectPointers(prefix, referenced, tableIds, pageSize, null, null);
  }

  /**
   * Discovers blob-only/orphaned tables through native common-prefix listing. Without this pass a
   * deleted table whose by-id pointer is already gone would never reach its table-scoped sweep.
   */
  private void collectTableBlobIds(String accountId, List<String> tableIds, int pageSize) {
    String prefix = Keys.tableRootPrefix(accountId);
    String normalizedPrefix = normalizeKey(prefix);
    String scanKey = "table-blob-prefixes:" + prefix;
    String token = continuation.pointerTokens.getOrDefault(scanKey, "");
    if (SCAN_COMPLETE.equals(token)) {
      return;
    }
    while (true) {
      checkDeadline();
      BlobStore.Page page = blobStore.listPrefixes(prefix, pageSize, token);
      for (String childPrefix : page.keys()) {
        checkDeadline();
        String normalizedChild = normalizeKey(childPrefix);
        String suffix =
            normalizedChild.startsWith(normalizedPrefix)
                ? normalizedChild.substring(normalizedPrefix.length())
                : "";
        int slash = suffix.indexOf('/');
        if (slash >= 0) {
          suffix = suffix.substring(0, slash);
        }
        if (!suffix.isBlank()) {
          String tableId = URLDecoder.decode(suffix, StandardCharsets.UTF_8);
          continuation.addTableId(tableId);
        }
      }
      token = page.nextToken();
      continuation.pointerTokens.put(
          scanKey, token == null || token.isBlank() ? SCAN_COMPLETE : token);
      if (token == null || token.isBlank()) {
        return;
      }
      checkDeadline();
    }
  }

  private int collectPointers(
      String prefix,
      ReferenceIndex referenced,
      List<String> tableIds,
      int pageSize,
      Predicate<Pointer> filter) {
    return collectPointers(prefix, referenced, tableIds, pageSize, filter, null);
  }

  private int collectPointers(
      String prefix,
      ReferenceIndex referenced,
      List<String> tableIds,
      int pageSize,
      Predicate<Pointer> filter,
      StorageEstimate storageEstimate) {
    return collectPointers(prefix, referenced, tableIds, pageSize, filter, storageEstimate, true);
  }

  private int collectPointers(
      String prefix,
      ReferenceIndex referenced,
      List<String> tableIds,
      int pageSize,
      Predicate<Pointer> filter,
      StorageEstimate storageEstimate,
      boolean markBlobReferences) {
    boolean resumable = isRetainedContinuationIndex(referenced);
    String scanKey = continuationIndexScope(referenced) + ":pointers:" + prefix;
    String token = resumable ? continuation.pointerTokens.getOrDefault(scanKey, "") : "";
    if (SCAN_COMPLETE.equals(token)) {
      return 0;
    }
    int scanned = 0;

    while (true) {
      checkDeadline();
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer p : pointers) {
        checkDeadline();
        if (filter != null && !filter.test(p)) {
          continue;
        }
        scanned++;
        if (storageEstimate != null) {
          storageEstimate.observe(p);
        }
        if (markBlobReferences && p.getBlobUri() != null && !p.getBlobUri().isBlank()) {
          referenced.add(normalizeKey(p.getBlobUri()));
        }
        if (tableIds != null) {
          String id = decodeSuffix(prefix, p.getKey());
          if (id != null && !id.isBlank()) {
            continuation.addTableId(id);
          }
        }
      }

      token = next.toString();
      if (resumable) {
        continuation.pointerTokens.put(scanKey, token.isEmpty() ? SCAN_COMPLETE : token);
      }
      if (token.isEmpty()) {
        break;
      }
      checkDeadline();
    }
    return scanned;
  }

  private int collectSharedIndexArtifactReferences(
      String accountId,
      String tableId,
      ReferenceIndex referenced,
      int pageSize,
      StorageEstimate storageEstimate) {
    int scanned = 0;
    // StatsRepository is the only component that walks the broad target-stats pointer subtree. It
    // exposes the exact, small generation identities discovered by that owner scan; generic GC can
    // then query only each generation's index-artifact sub-prefix instead of paging millions of
    // unrelated target-record pointers. Root walks may add a newly protected generation identity
    // before a settled re-mark reaches this point.
    var tableResourceId =
        ResourceId.newBuilder()
            .setAccountId(accountId)
            .setId(tableId)
            .setKind(ResourceKind.RK_TABLE)
            .build();
    for (Keys.GenerationKey generation : continuation.tableGenerationKeys) {
      checkDeadline();
      // Generation cleanup removes wrapper blobs before its bounded pointer cleanup necessarily
      // reaches the corresponding wrapper pointers. Those pointers no longer represent live
      // shared-sidecar roots, and attempting to dereference them would turn expected cleanup
      // progress into a fatal missing-wrapper restart.
      if (statsRepository.isGenerationDeletionInProgress(
          tableResourceId, generation.snapshotId(), generation.generationId())) {
        continue;
      }
      scanned +=
          collectSharedIndexArtifactReferencesForPrefix(
              Keys.snapshotIndexArtifactGenerationPrefix(
                  accountId, tableId, generation.snapshotId(), generation.generationId()),
              referenced,
              pageSize,
              storageEstimate);
    }
    return scanned;
  }

  private int collectSharedIndexArtifactReferencesForPrefix(
      String prefix, ReferenceIndex referenced, int pageSize, StorageEstimate storageEstimate) {
    boolean resumable = isRetainedContinuationIndex(referenced);
    String scanKey = continuationIndexScope(referenced) + ":shared-index-pointers:" + prefix;
    String token = resumable ? continuation.pointerTokens.getOrDefault(scanKey, "") : "";
    if (SCAN_COMPLETE.equals(token)) {
      return 0;
    }
    int scanned = 0;
    while (true) {
      checkDeadline();
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers = pointerStore.listPointersByPrefix(prefix, pageSize, token, next);
      for (Pointer pointer : pointers) {
        checkDeadline();
        if (pointer.getKey() == null
            || !pointer.getKey().contains(Keys.SEG_INDEX_ARTIFACTS)
            || pointer.getKey().endsWith(Keys.SUFFIX_INDEX_CAPTURE_MANIFEST_POINTER)
            || !PointerReferences.isBlobPointer(pointer)
            || pointer.getBlobUri().isBlank()) {
          continue;
        }
        scanned++;
        if (storageEstimate != null) {
          storageEstimate.observe(pointer);
        }
        try {
          byte[] bytes = blobStore.get(pointer.getBlobUri());
          if (bytes == null) {
            throw new IllegalStateException(
                "missing index artifact wrapper " + pointer.getBlobUri());
          }
          if (pointer.getBlobUri().contains("/reuse-bundles/")) {
            ReusableArtifactBundlePayload bundle = ReusableArtifactBundlePayload.parseFrom(bytes);
            for (IndexArtifactRecord record : bundle.getIndexArtifactsList()) {
              addSharedArtifactReference(referenced, record);
            }
          } else {
            addSharedArtifactReference(referenced, IndexArtifactRecord.parseFrom(bytes));
          }
        } catch (ReferenceIndex.CapacityExceededException e) {
          throw e;
        } catch (Exception e) {
          throw new IllegalStateException(
              "cannot prove shared index-artifact references from " + pointer.getBlobUri(), e);
        }
      }
      token = next.toString();
      if (resumable) {
        continuation.pointerTokens.put(scanKey, token.isBlank() ? SCAN_COMPLETE : token);
      }
      if (token.isBlank()) {
        return scanned;
      }
      checkDeadline();
    }
  }

  private void rememberTableGeneration(ReferenceIndex referenced, String manifestUri) {
    if (continuation == null
        || continuation.currentTableId.isBlank()
        || (referenced != continuation.tableReferenced
            && (continuation.deferredPage == null
                || (referenced != continuation.deferredPage.fresh
                    && referenced != continuation.deferredPage.verificationProbe)))) {
      return;
    }
    Keys.GenerationKey generation = Keys.generationFromManifestBlobUri(manifestUri);
    if (generation != null) {
      continuation.addGenerationKey(generation);
    }
  }

  private static void addSharedArtifactReference(
      ReferenceIndex referenced, IndexArtifactRecord record) {
    if (record != null && !record.getArtifactUri().isBlank()) {
      referenced.add(normalizeKey(record.getArtifactUri()));
    }
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
      ReferenceIndex referenced, Set<String> walkedPinRoots, int[] walkFailures) {
    for (String pinRoot : normalizedPinRoots()) {
      if (continuation != null && referenced == continuation.tableReferenced) {
        String pinTableId =
            Keys.extractResourceIdFromBlobUri(pinRoot.startsWith("/") ? pinRoot : "/" + pinRoot);
        if (!continuation.currentTableId.equals(pinTableId)) {
          continue;
        }
      }
      referenced.add(pinRoot);
      if (continuation != null && referenced == continuation.referenced) {
        // Account-scoped families do not depend on table-root chains. The table-scoped mark below
        // expands the pin only for its owning table, avoiding an account-wide table reference set.
        continue;
      }
      if (pinRoot.contains(Keys.SEG_TABLE_ROOT) && !walkedPinRoots.contains(pinRoot)) {
        if (!rootTableRootChain(pinRoot, referenced)) {
          walkFailures[0]++;
        } else {
          walkedPinRoots.add(pinRoot);
        }
      }
    }
  }

  private record DeleteResult(int scanned, int deleted, int rescued, boolean pending) {}

  /** A deferred no-owner candidate: the key plus the exact version the sweep age-checked. */
  private record DeferredCandidate(String key, String versionId) {}

  private DeleteResult deleteUnreferenced(
      String prefix,
      ReferenceIndex referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures,
      Predicate<String> isCandidate,
      List<DeferredCandidate> deferNoOwnerTo,
      int pageSize,
      long nowMs,
      long minAgeMs) {
    boolean resumable = isRetainedContinuationIndex(referenced);
    String scanKey = continuationIndexScope(referenced) + ":blobs:" + prefix;
    String token = resumable ? continuation.blobTokens.getOrDefault(scanKey, "") : "";
    if (SCAN_COMPLETE.equals(token)) {
      return new DeleteResult(0, 0, 0, false);
    }
    int scanned = 0;
    int deleted = 0;
    int rescued = 0;

    while (true) {
      checkDeadline();
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
        return new DeleteResult(scanned, deleted, rescued, true);
      }
      for (String key : page.keys()) {
        // A garbage key can issue several remote calls (HEAD, owner GET, delete, verification).
        // Check per key so page-size controls throughput without defeating the scheduler budget.
        checkDeadline();
        scanned++;
        String normalized = normalizeKey(key);
        if (!isCandidate.test(normalized)) {
          continue;
        }
        if (!referenced.mightContain(normalized)) {
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
            // No owner pointer derivable from the key (chain-walked manifest pages and shared
            // content-addressed sidecars) — and blob listing is lexicographic, so these often sort
            // BEFORE any blob whose rescue could reveal a stale referenced set. Deleting them
            // inline would destroy a missed table's chain before anything can object. Defer them
            // (with the version this pass age-checked): the flush below re-proves liveness per
            // table against the settled store before deleting (see the flush comment for why the
            // rescue signal alone is not sufficient proof).
            deferNoOwnerTo.add(new DeferredCandidate(key, versionId));
            continue;
          }
          if (ownerPointerKey != null && ownedBy(ownerPointerKey, normalized)) {
            if (normalized.contains(Keys.SEG_INDEX_CAPTURE_MANIFESTS)) {
              // Capture-manifest pointers are intentionally not discovered by a broad
              // snapshotRootPrefix scan: that subtree also contains millions of
              // generation-owned target-stat pointers. Their key shape has an exact derivable
              // owner, so this delete-time read is their normal root check, not a stale-mark
              // rescue signal.
              continue;
            }
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
              return new DeleteResult(scanned, deleted, rescued, true);
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
      if (resumable) {
        continuation.blobTokens.put(
            scanKey, token == null || token.isBlank() ? SCAN_COMPLETE : token);
      }
      if (token == null || token.isBlank()) {
        break;
      }
      checkDeadline();
    }

    return new DeleteResult(scanned, deleted, rescued, false);
  }

  /**
   * Re-snapshots the live query pins immediately before an irreversible delete and reports whether
   * the blob must be kept. A query can publish a pin to a SUPERSEDED root at any instant — the
   * root's manifest pages and chain blobs are then absent from the sweep-time referenced set and
   * from the current-root re-mark, yet must survive. Pin publication is read-only (no re-PUT), so
   * the version fence cannot catch it either. Re-reading the (node-local, cheap) pin set here and
   * expanding any newly-pinned root's chain protects owner-derived candidates that do not use the
   * deferred table proof. Deferred manifest-page and shared-sidecar deletes additionally serialize
   * with resolving-pin publication through {@link TableBlobReachabilityGuard}. A pin-chain walk
   * failure makes reachability unprovable, so it too returns "keep" (and the caller aborts).
   */
  private boolean keepForLatePin(
      String normalizedKey,
      ReferenceIndex referenced,
      Set<String> walkedPinRoots,
      int[] walkFailures) {
    rootLivePinChains(referenced, walkedPinRoots, walkFailures);
    return walkFailures[0] > 0 || referenced.mightContain(normalizedKey);
  }

  /** Whether the given owner pointer currently references exactly this normalized blob key. */
  private boolean ownedBy(String ownerPointerKey, String normalizedKey) {
    var owner = pointerStore.get(ownerPointerKey).orElse(null);
    return owner != null && normalizedKey.equals(normalizeKey(owner.getBlobUri()));
  }

  /**
   * Re-proves one table's chain-referenced liveness against the SETTLED store: re-reads the
   * table-root pointer and re-walks its chain, then re-scans the table's shared-artifact and
   * constraints pointer prefixes, accumulating every referenced URI into {@code fresh}. Returns
   * false when the chain walk could not complete — the caller must then treat the table's
   * candidates as unprovable and keep them.
   *
   * <p>Honesty about the guarantee: only the chain walk carries a completeness signal. The pointer
   * re-scans rely on {@code listPointersByPrefix} either returning the full settled listing or
   * THROWING (which aborts the whole sweep) — a silently partial, non-throwing listing is
   * undetectable at this layer and would leave {@code fresh} incomplete. The flush therefore still
   * requires a candidate to be absent from BOTH the sweep's original referenced set and this
   * re-mark before deleting; that is two independent scans, not a proof.
   */
  private boolean remarkTable(
      String accountId, String tableId, ReferenceIndex fresh, int pageSize) {
    var rootPtr = pointerStore.get(Keys.tableRootByTable(accountId, tableId)).orElse(null);
    if (rootPtr != null && !rootPtr.getBlobUri().isBlank()) {
      if (!rootTableRootChain(rootPtr.getBlobUri(), fresh)) {
        return false;
      }
    }
    collectSharedIndexArtifactReferences(accountId, tableId, fresh, pageSize, null);
    collectPointers(
        Keys.snapshotConstraintsPointerPrefix(accountId, tableId), fresh, null, pageSize);
    return true;
  }

  /** Exact, page-bounded probe used only by the post-delete corruption detector. */
  private static final class CandidateSetProbeReferenceIndex implements ReferenceIndex {
    private final Set<String> candidates;
    private final Set<String> matched = new HashSet<>();

    private CandidateSetProbeReferenceIndex(List<String> candidates) {
      this.candidates = new HashSet<>(candidates);
    }

    @Override
    public void add(String key) {
      if (candidates.contains(key)) {
        matched.add(key);
      }
    }

    @Override
    public boolean mightContain(String key) {
      return matched.contains(key);
    }

    @Override
    public long insertions() {
      return matched.size();
    }

    @Override
    public long capacity() {
      return candidates.size();
    }

    @Override
    public double saturation() {
      return candidates.isEmpty() ? 0.0d : matched.size() / (double) candidates.size();
    }

    @Override
    public double estimatedFalsePositiveProbability() {
      return 0.0d;
    }

    @Override
    public long memoryBytes() {
      return 0L;
    }

    @Override
    public void close() {
      candidates.clear();
      matched.clear();
    }
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
