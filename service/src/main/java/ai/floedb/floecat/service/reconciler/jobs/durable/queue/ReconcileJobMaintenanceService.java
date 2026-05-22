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

package ai.floedb.floecat.service.reconciler.jobs.durable.queue;

import ai.floedb.floecat.common.rpc.Pointer;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore.LeaseRequest;
import ai.floedb.floecat.service.reconciler.jobs.durable.model.StoredReconcileJob;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ReconcileJobMaintenanceService {
  private static final Logger LOG = Logger.getLogger(ReconcileJobMaintenanceService.class);
  private static final String LEASE_EXPIRY_POINTER_PREFIX =
      "/accounts/by-id/reconcile/job-leases/by-expiry/";

  @FunctionalInterface
  public interface RepairHintDrainer {
    void drain(int repairLimit, long budgetMs);
  }

  @FunctionalInterface
  public interface RepairReadyPointer {
    boolean apply(
        String canonicalPointerKey,
        StoredReconcileJob record,
        String repairPhase,
        String repairReason);
  }

  @FunctionalInterface
  public interface ReclaimCanonicalJob {
    void accept(Pointer leaseExpiryPointer, long nowMs);
  }

  @FunctionalInterface
  public interface ReclaimStatePointer {
    void accept(Pointer statePointer, String expectedState, long nowMs);
  }

  private PointerStore pointerStore;
  private RepairHintDrainer drainPendingRepairHintsForMaintenance;
  private Function<String, String> statePointerPrefix;
  private BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
      readCurrentRecordFromStateIndexPointer;
  private BiPredicate<StoredReconcileJob, LeaseRequest> matchesLeaseRequest;
  private BiPredicate<StoredReconcileJob, String> hasValidReadyPointers;
  private RepairReadyPointer repairReadyPointer;
  private Function<String, Long> parseLeaseExpiryMillis;
  private ReclaimCanonicalJob repairAndReclaimCanonicalJob;
  private ReclaimStatePointer repairAndReclaimFromStatePointer;
  private int maxPendingRepairHints;
  private int readyScanLimit;
  private int listScanMaxPages;
  private int activeStateReclaimFallbackPageBudget;
  private long reclaimIntervalMs;
  private long invalidOrderedPointerMs;

  private volatile long lastReclaimAtMs;
  private final ReentrantLock reclaimLock = new ReentrantLock();

  public void bind(
      PointerStore pointerStore,
      RepairHintDrainer drainPendingRepairHintsForMaintenance,
      Function<String, String> statePointerPrefix,
      BiFunction<Pointer, Predicate<StoredReconcileJob>, Optional<StoredReconcileJob>>
          readCurrentRecordFromStateIndexPointer,
      BiPredicate<StoredReconcileJob, LeaseRequest> matchesLeaseRequest,
      BiPredicate<StoredReconcileJob, String> hasValidReadyPointers,
      RepairReadyPointer repairReadyPointer,
      Function<String, Long> parseLeaseExpiryMillis,
      ReclaimCanonicalJob repairAndReclaimCanonicalJob,
      ReclaimStatePointer repairAndReclaimFromStatePointer,
      int maxPendingRepairHints,
      int readyScanLimit,
      int listScanMaxPages,
      int activeStateReclaimFallbackPageBudget,
      long reclaimIntervalMs,
      long invalidOrderedPointerMs) {
    this.pointerStore = pointerStore;
    this.drainPendingRepairHintsForMaintenance = drainPendingRepairHintsForMaintenance;
    this.statePointerPrefix = statePointerPrefix;
    this.readCurrentRecordFromStateIndexPointer = readCurrentRecordFromStateIndexPointer;
    this.matchesLeaseRequest = matchesLeaseRequest;
    this.hasValidReadyPointers = hasValidReadyPointers;
    this.repairReadyPointer = repairReadyPointer;
    this.parseLeaseExpiryMillis = parseLeaseExpiryMillis;
    this.repairAndReclaimCanonicalJob = repairAndReclaimCanonicalJob;
    this.repairAndReclaimFromStatePointer = repairAndReclaimFromStatePointer;
    this.maxPendingRepairHints = maxPendingRepairHints;
    this.readyScanLimit = readyScanLimit;
    this.listScanMaxPages = listScanMaxPages;
    this.activeStateReclaimFallbackPageBudget = activeStateReclaimFallbackPageBudget;
    this.reclaimIntervalMs = reclaimIntervalMs;
    this.invalidOrderedPointerMs = invalidOrderedPointerMs;
  }

  public void runMaintenanceOnce(long maxMillis) {
    long startedAtMs = System.currentTimeMillis();
    long budgetMs = Math.max(0L, maxMillis);
    long stepStartedAtMs = startedAtMs;
    drainPendingRepairHintsForMaintenance.drain(maxPendingRepairHints, budgetMs);
    long drainElapsedMs = System.currentTimeMillis() - stepStartedAtMs;
    if (budgetMs > 0L && System.currentTimeMillis() - startedAtMs >= budgetMs) {
      LOG.debugf(
          "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
          System.currentTimeMillis() - startedAtMs, drainElapsedMs, 0, 0);
      return;
    }

    stepStartedAtMs = System.currentTimeMillis();
    repairQueuedReadyPointersIfNeeded(System.currentTimeMillis(), LeaseRequest.all());
    long readyRepairElapsedMs = System.currentTimeMillis() - stepStartedAtMs;
    if (budgetMs > 0L && System.currentTimeMillis() - startedAtMs >= budgetMs) {
      LOG.debugf(
          "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
          System.currentTimeMillis() - startedAtMs, drainElapsedMs, readyRepairElapsedMs, 0);
      return;
    }

    stepStartedAtMs = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
    LOG.debugf(
        "runMaintenanceOnce total_ms=%d drain_ms=%d ready_repair_ms=%d reclaim_ms=%d",
        System.currentTimeMillis() - startedAtMs,
        drainElapsedMs,
        readyRepairElapsedMs,
        System.currentTimeMillis() - stepStartedAtMs);
  }

  private boolean repairQueuedReadyPointersIfNeeded(long nowMs, LeaseRequest request) {
    boolean repaired = false;
    String token = "";
    int pages = 0;
    while (true) {
      String priorToken = token;
      StringBuilder next = new StringBuilder();
      List<Pointer> pointers =
          pointerStore.listPointersByPrefix(
              statePointerPrefix.apply("JS_QUEUED"), readyScanLimit, token, next);
      if (pointers.isEmpty()) {
        return repaired;
      }
      for (Pointer statePointer : pointers) {
        String canonicalPointerKey = statePointer.getBlobUri();
        if (blank(canonicalPointerKey)) {
          continue;
        }
        var current =
            readCurrentRecordFromStateIndexPointer.apply(
                statePointer,
                record ->
                    record != null
                        && "JS_QUEUED".equals(record.state)
                        && record.nextAttemptAtMs <= nowMs
                        && matchesLeaseRequest.test(record, request));
        if (current.isEmpty()) {
          continue;
        }
        if (hasValidReadyPointers.test(current.get(), canonicalPointerKey)) {
          continue;
        }
        repaired |=
            repairReadyPointer.apply(
                canonicalPointerKey, current.get(), "ready_scan_repair", "ready_pointer_missing");
      }
      token = next.toString();
      if (token.isBlank() || token.equals(priorToken)) {
        return repaired;
      }
      pages++;
      if (pages >= listScanMaxPages) {
        return repaired;
      }
    }
  }

  private void reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    if (!reclaimLock.tryLock()) {
      return;
    }
    try {
      if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
        return;
      }
      lastReclaimAtMs = nowMs;

      scanLeaseExpiryPointersForReclaim(nowMs);
      scanActiveStatePointersForLeaseRepairAndReclaim(
          nowMs, "JS_RUNNING", activeStateReclaimFallbackPageBudget);
      scanActiveStatePointersForLeaseRepairAndReclaim(
          nowMs, "JS_CANCELLING", activeStateReclaimFallbackPageBudget);
    } finally {
      reclaimLock.unlock();
    }
  }

  private void scanLeaseExpiryPointersForReclaim(long nowMs) {
    String token = "";
    int pages = 0;
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> leaseExpiries =
          pointerStore.listPointersByPrefix(
              LEASE_EXPIRY_POINTER_PREFIX, readyScanLimit, token, next);
      if (leaseExpiries.isEmpty()) {
        return;
      }
      for (Pointer leaseExpiry : leaseExpiries) {
        long expiresAtMs = parseLeaseExpiryMillis.apply(leaseExpiry.getKey());
        if (expiresAtMs == invalidOrderedPointerMs) {
          pointerStore.compareAndDelete(leaseExpiry.getKey(), leaseExpiry.getVersion());
          continue;
        }
        if (expiresAtMs > nowMs) {
          return;
        }
        repairAndReclaimCanonicalJob.accept(leaseExpiry, nowMs);
      }

      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return;
      }
      if (nextToken.equals(token)) {
        LOG.warn(
            "Reconcile lease-expiry reclaim pagination token did not advance; aborting scan to"
                + " avoid livelock");
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= 10_000) {
        LOG.warn("Reconcile lease-expiry reclaim pagination hit safety page cap; aborting scan");
        return;
      }
    }
  }

  private void scanActiveStatePointersForLeaseRepairAndReclaim(
      long nowMs, String state, int pageBudget) {
    if (blank(state) || pageBudget <= 0) {
      return;
    }
    String token = "";
    int pages = 0;
    String prefix = statePointerPrefix.apply(state);
    while (true) {
      StringBuilder next = new StringBuilder();
      List<Pointer> statePointers =
          pointerStore.listPointersByPrefix(prefix, readyScanLimit, token, next);
      if (statePointers.isEmpty()) {
        return;
      }
      for (Pointer statePointer : statePointers) {
        repairAndReclaimFromStatePointer.accept(statePointer, state, nowMs);
      }
      String nextToken = next.toString();
      if (nextToken.isBlank()) {
        return;
      }
      if (nextToken.equals(token)) {
        LOG.warnf(
            "Reconcile %s state reclaim pagination token did not advance; aborting scan",
            blankToEmpty(state));
        return;
      }
      token = nextToken;
      pages++;
      if (pages >= pageBudget) {
        return;
      }
    }
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value;
  }
}
