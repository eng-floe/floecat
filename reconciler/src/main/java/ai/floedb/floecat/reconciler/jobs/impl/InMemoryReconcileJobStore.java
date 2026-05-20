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

package ai.floedb.floecat.reconciler.jobs.impl;

import ai.floedb.floecat.reconciler.impl.ReconcilerService.CaptureMode;
import ai.floedb.floecat.reconciler.jobs.ReconcileCapturePolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileFileGroupTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileSnapshotTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import io.quarkus.arc.Arc;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@ApplicationScoped
@IfBuildProperty(
    name = "floecat.reconciler.job-store",
    stringValue = "memory",
    enableIfMissing = true)
public class InMemoryReconcileJobStore implements ReconcileJobStore {

  // ---------------------------------------------------------------------------
  // Job state transition table
  //
  //  QUEUED      → RUNNING     leaseNext() succeeds
  //  RUNNING     → CANCELLING  explicit cancel() call while job is running;
  //                             preemption (phase 4, enabled via feature flag)
  //  RUNNING     → SUCCEEDED   markSucceeded()
  //  RUNNING     → QUEUED      lease expired (reclaimExpiredLeasesIfDue); markFailed() retry
  //  RUNNING     → FAILED      markFailed() after maxAttempts exceeded; markFailedTerminal()
  //  CANCELLING  → RUNNING     leaseNext() re-leases CANCELLING job so executor can observe it
  //  CANCELLING  → QUEUED      markCancelled() with fileResults checkpoint (phase 4 resume path)
  //  CANCELLING  → CANCELLED   markCancelled() completes cancellation
  //  CANCELLING  → FAILED      lease expired while CANCELLING before executor responded
  //  QUEUED      → CANCELLED   cancel() on a job that has never started
  // ---------------------------------------------------------------------------

  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;

  // ---------------------------------------------------------------------------
  // Health band thresholds
  // ---------------------------------------------------------------------------
  private static final long P0_RED_BUDGET_MS = 1_000L;
  private static final long P2_ORANGE_THRESHOLD = 200L;
  private static final long P3_YELLOW_THRESHOLD = 500L;

  // ---------------------------------------------------------------------------
  // Starvation aging constants
  // ---------------------------------------------------------------------------
  static final long P3_AGING_THRESHOLD_MS = 300_000L; // 5 min
  static final long P2_AGING_THRESHOLD_MS = 120_000L; // 2 min
  static final long P1_AGING_THRESHOLD_MS = Long.MAX_VALUE; // never ages (already high priority)
  static final long AGING_COOLDOWN_MS = 60_000L;

  // ---------------------------------------------------------------------------
  // Admission control constants
  // ---------------------------------------------------------------------------
  private static final long DEFER_DELAY_MS = 5_000L;

  // ---------------------------------------------------------------------------
  // WRR constants
  // ---------------------------------------------------------------------------
  private static final int MAX_WRR_SCAN = 8;

  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final Map<String, Long> createdAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> leaseEpochs = new ConcurrentHashMap<>();
  private final Map<String, Long> leaseExpiresAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> pinnedExecutors = new ConcurrentHashMap<>();
  private final Map<String, String> dedupeKeysByJobId = new ConcurrentHashMap<>();
  private final Map<String, String> activeJobIdByDedupeKey = new ConcurrentHashMap<>();
  private final Map<String, String> laneKeysByJobId = new ConcurrentHashMap<>();
  private final Map<String, String> activeJobIdByLaneKey = new ConcurrentHashMap<>();
  private final Map<String, String> activeJobIdBySnapshotLeaseKey = new ConcurrentHashMap<>();
  private final Map<String, Integer> attemptsByJobId = new ConcurrentHashMap<>();
  private final Map<String, Long> nextAttemptAtMs = new ConcurrentHashMap<>();
  private final PriorityReadyQueue readyQueue = new PriorityReadyQueue();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();
  private volatile int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private volatile long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private volatile long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private volatile long leaseMs = DEFAULT_LEASE_MS;
  private volatile long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private volatile long lastReclaimAtMs;

  // ---------------------------------------------------------------------------
  // Health band state
  // ---------------------------------------------------------------------------
  private final AtomicReference<SchedulerHealthBand> currentBand =
      new AtomicReference<>(SchedulerHealthBand.GREEN);

  // ---------------------------------------------------------------------------
  // Starvation aging state
  // ---------------------------------------------------------------------------
  private final Map<String, Long> promotedJobIds =
      new ConcurrentHashMap<>(); // jobId → promotion-expiry-ms
  private final AtomicLong agingPromotionsTotal = new AtomicLong();

  // ---------------------------------------------------------------------------
  // Admission control state
  // ---------------------------------------------------------------------------
  private final Map<StatsPriorityClass, AtomicLong> admissionDeferred =
      new EnumMap<>(StatsPriorityClass.class);

  // ---------------------------------------------------------------------------
  // WRR state
  // ---------------------------------------------------------------------------
  private final Map<String, Long> laneServiceCounts = new ConcurrentHashMap<>();

  public InMemoryReconcileJobStore() {
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      admissionDeferred.put(cls, new AtomicLong());
    }
    reloadConfig();
  }

  @PostConstruct
  void init() {
    reloadConfig();
  }

  private void reloadConfig() {
    maxAttempts =
        Math.max(1, readInt("floecat.reconciler.job-store.max-attempts", DEFAULT_MAX_ATTEMPTS));
    baseBackoffMs =
        Math.max(
            100L,
            readLong("floecat.reconciler.job-store.base-backoff-ms", DEFAULT_BASE_BACKOFF_MS));
    maxBackoffMs =
        Math.max(
            baseBackoffMs,
            readLong("floecat.reconciler.job-store.max-backoff-ms", DEFAULT_MAX_BACKOFF_MS));
    leaseMs = Math.max(1_000L, readLong("floecat.reconciler.job-store.lease-ms", DEFAULT_LEASE_MS));
    reclaimIntervalMs =
        Math.max(
            1_000L,
            readLong(
                "floecat.reconciler.job-store.reclaim-interval-ms", DEFAULT_RECLAIM_INTERVAL_MS));
  }

  private int readInt(String key, int defaultValue) {
    return Math.toIntExact(readLong(key, defaultValue));
  }

  private long readLong(String key, long defaultValue) {
    try {
      var container = Arc.container();
      if (container != null) {
        var config = container.instance(org.eclipse.microprofile.config.Config.class);
        if (config.isAvailable()) {
          return config.get().getOptionalValue(key, Long.class).orElse(defaultValue);
        }
      }
    } catch (RuntimeException ignored) {
      // Fall back to system properties for plain unit construction.
    }
    String raw = System.getProperty(key);
    if (raw == null || raw.isBlank()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException ignored) {
      return defaultValue;
    }
  }

  @Override
  public String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    ReconcileJobKind effectiveJobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
    ReconcileSnapshotTask effectiveSnapshotTask =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    ReconcileFileGroupTask effectiveFileGroupTask =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    requireExplicitSnapshotCoverage(effectiveJobKind, effectiveSnapshotTask, "enqueue");
    ReconcileScope effectiveScope =
        normalizeScopeForJobKind(
            scope == null ? ReconcileScope.empty() : scope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask);
    ReconcileExecutionPolicy effectivePolicy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    String effectiveParentJobId = parentJobId == null ? "" : parentJobId.trim();
    String effectivePinnedExecutorId = pinnedExecutorId == null ? "" : pinnedExecutorId.trim();
    String dedupeKey =
        dedupeKey(
            accountId,
            connectorId,
            fullRescan,
            captureMode,
            effectiveScope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            effectivePolicy,
            effectiveParentJobId,
            effectivePinnedExecutorId);
    String activeJobId = activeJobIdByDedupeKey.get(dedupeKey);
    if (activeJobId != null) {
      ReconcileJob existing = jobs.get(activeJobId);
      if (existing != null && !isTerminalState(existing.state)) {
        return activeJobId;
      }
      activeJobIdByDedupeKey.remove(dedupeKey, activeJobId);
    }

    String id = UUID.randomUUID().toString();
    long now = System.currentTimeMillis();
    createdAtMs.put(id, now);
    attemptsByJobId.put(id, 0);
    nextAttemptAtMs.put(id, now);
    dedupeKeysByJobId.put(id, dedupeKey);
    activeJobIdByDedupeKey.put(dedupeKey, id);
    // Lane key is derived from scope + job-kind, not from executionPolicy.lane().
    // executionPolicy.lane() is stored on the job record and propagated to LeasedJob for future
    // use (Phase 2 WRR fairness), but the actual lane-mutex assignment uses the computed key
    // below.  Both approaches produce per-table serialization, so runtime behaviour is correct;
    // the divergence only matters when a caller wants to override the lane with a custom key.
    // TODO(phase2): honour executionPolicy.lane() when non-blank, falling back to the computed key.
    laneKeysByJobId.put(
        id,
        laneKey(
            connectorId,
            effectiveScope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask));
    var job =
        new ReconcileJob(
            id,
            accountId,
            connectorId,
            "JS_QUEUED",
            fullRescan ? "Queued (full)" : "Queued",
            0L,
            0L,
            0,
            0,
            0,
            0,
            0,
            fullRescan,
            captureMode,
            0,
            0,
            effectiveScope,
            effectivePolicy,
            "",
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask,
            effectiveParentJobId);
    jobs.put(id, job);
    pinnedExecutors.put(id, effectivePinnedExecutorId);
    long deferMs = admissionDeferMs(effectivePolicy.priorityClass(), currentBand.get());
    if (deferMs > 0) {
      nextAttemptAtMs.put(id, now + deferMs);
      admissionDeferred.get(effectivePolicy.priorityClass()).incrementAndGet();
    }
    readyQueue.enqueue(id, effectivePolicy.priorityClass(), effectivePolicy.priorityScore());
    return id;
  }

  @Override
  public Optional<ReconcileJob> get(String accountId, String jobId) {
    var job = jobs.get(jobId);
    if (job == null) {
      return Optional.empty();
    }
    if (accountId != null && !accountId.isBlank() && !accountId.equals(job.accountId)) {
      return Optional.empty();
    }
    return Optional.of(withPinnedExecutor(job));
  }

  @Override
  public ReconcileJobPage list(
      String accountId, int pageSize, String pageToken, String connectorId, Set<String> states) {
    int offset = 0;
    if (pageToken != null && !pageToken.isBlank()) {
      try {
        offset = Math.max(0, Integer.parseInt(pageToken));
      } catch (NumberFormatException ignored) {
        offset = 0;
      }
    }
    int limit = Math.max(1, pageSize);
    var filtered =
        jobs.values().stream()
            .filter(j -> accountId == null || accountId.isBlank() || accountId.equals(j.accountId))
            .filter(
                j ->
                    connectorId == null
                        || connectorId.isBlank()
                        || connectorId.equals(j.connectorId))
            .filter(j -> states == null || states.isEmpty() || states.contains(j.state))
            .sorted(
                (a, b) ->
                    Long.compare(
                        b.startedAtMs == 0L ? b.finishedAtMs : b.startedAtMs,
                        a.startedAtMs == 0L ? a.finishedAtMs : a.startedAtMs))
            .collect(Collectors.toList());
    if (offset >= filtered.size()) {
      return new ReconcileJobPage(List.of(), "");
    }
    int end = Math.min(filtered.size(), offset + limit);
    String next = end < filtered.size() ? Integer.toString(end) : "";
    return new ReconcileJobPage(
        filtered.subList(offset, end).stream().map(this::withPinnedExecutor).toList(), next);
  }

  @Override
  public List<ReconcileJob> childJobs(String accountId, String parentJobId) {
    if (parentJobId == null || parentJobId.isBlank()) {
      return List.of();
    }
    return jobs.values().stream()
        .filter(j -> accountId == null || accountId.isBlank() || accountId.equals(j.accountId))
        .filter(j -> parentJobId.equals(j.parentJobId))
        .sorted(
            (a, b) ->
                Long.compare(
                    b.startedAtMs == 0L ? b.finishedAtMs : b.startedAtMs,
                    a.startedAtMs == 0L ? a.finishedAtMs : a.startedAtMs))
        .map(this::withPinnedExecutor)
        .collect(Collectors.toUnmodifiableList());
  }

  private ReconcileJob withPinnedExecutor(ReconcileJob job) {
    if (job == null) {
      return null;
    }
    return new ReconcileJob(
        job.jobId,
        job.accountId,
        job.connectorId,
        job.state,
        job.message,
        job.startedAtMs,
        job.finishedAtMs,
        job.tablesScanned,
        job.tablesChanged,
        job.viewsScanned,
        job.viewsChanged,
        job.errors,
        job.fullRescan,
        job.captureMode,
        job.snapshotsProcessed,
        job.statsProcessed,
        job.scope,
        job.executionPolicy,
        pinnedExecutors.getOrDefault(job.jobId, ""),
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
  }

  @Override
  public QueueStats queueStats() {
    long queued = 0L;
    long running = 0L;
    long cancelling = 0L;
    long oldestQueued = 0L;
    long oldestP0QueuedCreatedAtMs = 0L;
    long now = System.currentTimeMillis();
    for (ReconcileJob job : jobs.values()) {
      if (job == null || job.state == null) {
        continue;
      }
      switch (job.state) {
        case "JS_QUEUED" -> {
          queued++;
          long created = createdAtMs.getOrDefault(job.jobId, 0L);
          if (created > 0L && (oldestQueued == 0L || created < oldestQueued)) {
            oldestQueued = created;
          }
          if (job.executionPolicy != null
              && job.executionPolicy.priorityClass() == StatsPriorityClass.P0_SYNC) {
            if (created > 0L
                && (oldestP0QueuedCreatedAtMs == 0L || created < oldestP0QueuedCreatedAtMs)) {
              oldestP0QueuedCreatedAtMs = created;
            }
          }
        }
        case "JS_RUNNING" -> running++;
        case "JS_CANCELLING" -> cancelling++;
        default -> {}
      }
    }
    Map<StatsPriorityClass, Long> byClass = readyQueue.sizeByAllClasses();
    long p0Count = byClass.getOrDefault(StatsPriorityClass.P0_SYNC, 0L);
    long p2Count = byClass.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L);
    long p3Count = byClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L);

    SchedulerHealthBand band;
    if (p0Count > 0
        && oldestP0QueuedCreatedAtMs > 0L
        && now - oldestP0QueuedCreatedAtMs > P0_RED_BUDGET_MS) {
      band = SchedulerHealthBand.RED;
    } else if (p2Count > P2_ORANGE_THRESHOLD) {
      band = SchedulerHealthBand.ORANGE;
    } else if (p3Count > P3_YELLOW_THRESHOLD) {
      band = SchedulerHealthBand.YELLOW;
    } else {
      band = SchedulerHealthBand.GREEN;
    }
    currentBand.set(band);

    Map<StatsPriorityClass, Long> deferredSnapshot = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      deferredSnapshot.put(cls, admissionDeferred.get(cls).get());
    }

    return new QueueStats(
        queued,
        running,
        cancelling,
        oldestQueued,
        byClass,
        band,
        agingPromotionsTotal.get(),
        deferredSnapshot);
  }

  @Override
  public void persistSnapshotPlan(String jobId, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    jobs.computeIfPresent(
        jobId,
        (id, existing) -> {
          if (existing.jobKind == ReconcileJobKind.PLAN_SNAPSHOT
              && !effective.fileGroupPlanRecorded()) {
            throw new IllegalArgumentException(
                "persistSnapshotPlan requires explicit snapshot coverage metadata for PLAN_SNAPSHOT jobs");
          }
          return new ReconcileJob(
              existing.jobId,
              existing.accountId,
              existing.connectorId,
              existing.state,
              existing.message,
              existing.startedAtMs,
              existing.finishedAtMs,
              existing.tablesScanned,
              existing.tablesChanged,
              existing.viewsScanned,
              existing.viewsChanged,
              existing.errors,
              existing.fullRescan,
              existing.captureMode,
              existing.snapshotsProcessed,
              existing.statsProcessed,
              existing.scope,
              existing.executionPolicy,
              existing.executorId,
              existing.jobKind,
              existing.tableTask,
              existing.viewTask,
              effective,
              existing.fileGroupTask,
              existing.parentJobId);
        });
  }

  @Override
  public void persistFileGroupResult(String jobId, ReconcileFileGroupTask fileGroupTask) {
    ReconcileFileGroupTask effective =
        fileGroupTask == null ? ReconcileFileGroupTask.empty() : fileGroupTask;
    jobs.computeIfPresent(
        jobId,
        (id, existing) ->
            new ReconcileJob(
                existing.jobId,
                existing.accountId,
                existing.connectorId,
                existing.state,
                existing.message,
                existing.startedAtMs,
                existing.finishedAtMs,
                existing.tablesScanned,
                existing.tablesChanged,
                existing.viewsScanned,
                existing.viewsChanged,
                existing.errors,
                existing.fullRescan,
                existing.captureMode,
                existing.snapshotsProcessed,
                existing.statsProcessed,
                existing.scope,
                existing.executionPolicy,
                existing.executorId,
                existing.jobKind,
                existing.tableTask,
                existing.viewTask,
                existing.snapshotTask,
                effective,
                existing.parentJobId));
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    long now = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(now);
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;

    // Lazily clean expired aging promotions
    if (!promotedJobIds.isEmpty()) {
      promotedJobIds.entrySet().removeIf(e -> e.getValue() < now);
    }

    // Dispatch loop: iterate priority classes from most- to least-urgent.
    //
    // For each class we run a bounded WRR tournament (up to MAX_WRR_SCAN candidates). Jobs that
    // pass all checks compete on the lane's virtual service count; the job from the least-served
    // lane wins. Losers are requeued. The winner is leased and returned.
    //
    // P0_SYNC guard: if any P0 job was requeued (genuinely deferred, not merely discarded as
    // stale/terminal), we return empty rather than falling through to P1/P2/P3. Sync-capture
    // jobs must not wait behind async work.
    //
    // Known limitation: laneServiceCounts can overflow for very long-lived deployments.
    // Phase 3 will introduce periodic reset or virtual-time modulo to address this.
    boolean anyP0Requeued = false;
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      long classSize = readyQueue.sizeByClass(cls);
      if (classSize == 0) {
        continue;
      }

      String bestJobId = null;
      ReconcileJob bestJob = null;
      String bestLaneKey = null;
      String bestWrrLane = null;
      long bestVirtualTime = Long.MAX_VALUE;

      long scanLimit = Math.min(classSize, MAX_WRR_SCAN);
      for (long i = 0; i < scanLimit; i++) {
        String jobId = readyQueue.pollHighest(cls);
        if (jobId == null) {
          break;
        }

        var job = jobs.get(jobId);
        if (job == null) {
          // Job was removed from the store; discard without triggering the P0 guard.
          continue;
        }

        if (!"JS_QUEUED".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
          // Terminal or already-running: discard without triggering the P0 guard.
          continue;
        }

        if (nextAttemptAtMs.getOrDefault(jobId, 0L) > now) {
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
          continue;
        }

        if (!effective.matches(
            job.executionPolicy, pinnedExecutors.getOrDefault(jobId, ""), job.jobKind)) {
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
          continue;
        }

        String laneKey = laneKeysByJobId.getOrDefault(jobId, "");
        if (!laneKey.isBlank()) {
          String laneOwner = activeJobIdByLaneKey.get(laneKey);
          if (laneOwner != null && !laneOwner.equals(jobId) && hasLiveLaneLease(laneOwner, now)) {
            readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
            if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
            continue;
          }
        }

        if (!tryAcquireSnapshotLease(job, jobId, now)) {
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
          continue;
        }

        // WRR: use executionPolicy.lane() when non-blank (honors TODO(phase2)), else computed key
        String wrrLane =
            job.executionPolicy.lane().isBlank() ? laneKey : job.executionPolicy.lane();
        long vt = laneServiceCounts.getOrDefault(wrrLane, 0L);

        if (bestJobId == null || vt < bestVirtualTime) {
          // Displace previous best candidate (if any) back to queue
          if (bestJobId != null) {
            releaseSnapshotLease(bestJobId);
            readyQueue.requeue(
                bestJobId,
                cls,
                jobs.get(bestJobId) != null
                    ? jobs.get(bestJobId).executionPolicy.priorityScore()
                    : 0L);
            if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
          }
          bestJobId = jobId;
          bestJob = job;
          bestLaneKey = laneKey;
          bestWrrLane = wrrLane;
          bestVirtualTime = vt;
        } else {
          // This candidate loses; requeue it
          releaseSnapshotLease(jobId);
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          if (cls == StatsPriorityClass.P0_SYNC) anyP0Requeued = true;
        }
      }

      if (bestJobId == null) {
        // P0 guard: stop after the P0 class scan if any live P0 job was deferred.
        if (cls == StatsPriorityClass.P0_SYNC && anyP0Requeued) {
          return Optional.empty();
        }
        continue;
      }

      // Apply starvation aging (just track promotion; effectiveCls for WRR accounting in Phase 3+)
      long ageMs = now - createdAtMs.getOrDefault(bestJobId, now);
      if (!promotedJobIds.containsKey(bestJobId) && ageMs > agingThresholdMs(cls)) {
        promotedJobIds.put(bestJobId, now + AGING_COOLDOWN_MS);
        agingPromotionsTotal.incrementAndGet();
      }

      final String finalBestJobId = bestJobId;
      final String finalBestLaneKey = bestLaneKey;
      final String finalBestWrrLane = bestWrrLane;

      if (leased.add(finalBestJobId)) {
        String leaseEpoch = UUID.randomUUID().toString();
        leaseEpochs.put(finalBestJobId, leaseEpoch);
        leaseExpiresAtMs.put(finalBestJobId, now + leaseMs);
        if (!finalBestLaneKey.isBlank()) {
          activeJobIdByLaneKey.put(finalBestLaneKey, finalBestJobId);
        }
        laneServiceCounts.merge(finalBestWrrLane, 1L, Long::sum);
        jobs.computeIfPresent(
            finalBestJobId,
            (id, current) ->
                new ReconcileJob(
                    current.jobId,
                    current.accountId,
                    current.connectorId,
                    "JS_CANCELLING".equals(current.state) ? "JS_CANCELLING" : "JS_RUNNING",
                    "JS_CANCELLING".equals(current.state) ? current.message : "Leased",
                    current.startedAtMs > 0L ? current.startedAtMs : now,
                    0L,
                    current.tablesScanned,
                    current.tablesChanged,
                    current.viewsScanned,
                    current.viewsChanged,
                    current.errors,
                    current.fullRescan,
                    current.captureMode,
                    current.snapshotsProcessed,
                    current.statsProcessed,
                    current.scope,
                    current.executionPolicy,
                    current.executorId,
                    current.jobKind,
                    current.tableTask,
                    current.viewTask,
                    current.snapshotTask,
                    current.fileGroupTask,
                    current.parentJobId));
        ReconcileJob leasedJob = jobs.get(finalBestJobId);
        return Optional.of(
            new LeasedJob(
                leasedJob.jobId,
                leasedJob.accountId,
                leasedJob.connectorId,
                leasedJob.fullRescan,
                leasedJob.captureMode,
                leasedJob.scope,
                leasedJob.executionPolicy,
                leaseEpoch,
                pinnedExecutors.getOrDefault(finalBestJobId, ""),
                leasedJob.executorId,
                leasedJob.jobKind,
                leasedJob.tableTask,
                leasedJob.viewTask,
                leasedJob.snapshotTask,
                leasedJob.fileGroupTask,
                leasedJob.parentJobId));
      }
      releaseSnapshotLease(finalBestJobId);

      // P0 guard check after attempting to lease
      if (cls == StatsPriorityClass.P0_SYNC && anyP0Requeued) {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  @Override
  public boolean renewLease(String jobId, String leaseEpoch) {
    long now = System.currentTimeMillis();
    var job = jobs.get(jobId);
    if (job == null) {
      return false;
    }
    if (!"JS_RUNNING".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
      return false;
    }
    if (!hasActiveLease(jobId, leaseEpoch, now)) {
      return false;
    }
    leaseExpiresAtMs.put(jobId, now + leaseMs);
    return true;
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs, String executorId) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          boolean cancelling = "JS_CANCELLING".equals(job.state);
          long effectiveStartedAtMs = job.startedAtMs > 0L ? job.startedAtMs : startedAtMs;
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              cancelling ? "JS_CANCELLING" : "JS_RUNNING",
              cancelling ? job.message : "Running",
              effectiveStartedAtMs,
              0L,
              job.tablesScanned,
              job.tablesChanged,
              job.viewsScanned,
              job.viewsChanged,
              job.errors,
              job.fullRescan,
              job.captureMode,
              job.snapshotsProcessed,
              job.statsProcessed,
              job.scope,
              job.executionPolicy,
              executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public void markProgress(
      String jobId,
      String leaseEpoch,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed,
      String message) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          if ("JS_CANCELLED".equals(job.state)
              || "JS_SUCCEEDED".equals(job.state)
              || "JS_FAILED".equals(job.state)) {
            return job;
          }
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              job.state,
              message == null ? (job.message == null ? "" : job.message) : message,
              job.startedAtMs,
              job.finishedAtMs,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              job.executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long snapshotsProcessed,
      long statsProcessed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          if ("JS_CANCELLED".equals(job.state) || "JS_CANCELLING".equals(job.state)) {
            return job;
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          pinnedExecutors.remove(id);
          clearDedupe(id);
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_SUCCEEDED",
              "Succeeded",
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              job.errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              job.executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          if ("JS_CANCELLED".equals(job.state) || "JS_CANCELLING".equals(job.state)) {
            return job;
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          int attempts = attemptsByJobId.merge(id, 1, Integer::sum);
          if (attempts >= maxAttempts) {
            pinnedExecutors.remove(id);
            clearDedupe(id);
            return new ReconcileJob(
                job.jobId,
                job.accountId,
                job.connectorId,
                "JS_FAILED",
                message == null ? "Failed" : message,
                job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
                finishedAtMs,
                tablesScanned,
                tablesChanged,
                viewsScanned,
                viewsChanged,
                errors,
                job.fullRescan,
                job.captureMode,
                snapshotsProcessed,
                statsProcessed,
                job.scope,
                job.executionPolicy,
                job.executorId,
                job.jobKind,
                job.tableTask,
                job.viewTask,
                job.snapshotTask,
                job.fileGroupTask,
                job.parentJobId);
          }

          long now = System.currentTimeMillis();
          nextAttemptAtMs.put(id, now + backoffMs(attempts));
          readyQueue.enqueue(
              id, job.executionPolicy.priorityClass(), job.executionPolicy.priorityScore());
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_QUEUED",
              message == null ? "Retrying" : message,
              job.startedAtMs,
              0L,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              "",
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public void markWaiting(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          if ("JS_CANCELLED".equals(job.state) || "JS_CANCELLING".equals(job.state)) {
            return job;
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          long now = System.currentTimeMillis();
          nextAttemptAtMs.put(id, now + baseBackoffMs);
          readyQueue.enqueue(
              id, job.executionPolicy.priorityClass(), job.executionPolicy.priorityScore());
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_QUEUED",
              message == null ? "Waiting on dependency" : message,
              job.startedAtMs,
              0L,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              "",
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public void markFailedTerminal(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          if ("JS_CANCELLED".equals(job.state) || "JS_CANCELLING".equals(job.state)) {
            return job;
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          attemptsByJobId.merge(id, 1, Integer::sum);
          pinnedExecutors.remove(id);
          clearDedupe(id);
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_FAILED",
              message == null ? "Failed" : message,
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              job.executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  @Override
  public Optional<ReconcileJob> cancel(String accountId, String jobId, String reason) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (accountId != null && !accountId.isBlank() && !accountId.equals(job.accountId)) {
            return job;
          }
          if ("JS_SUCCEEDED".equals(job.state)
              || "JS_FAILED".equals(job.state)
              || "JS_CANCELLED".equals(job.state)
              || "JS_CANCELLING".equals(job.state)) {
            return job;
          }
          if ("JS_RUNNING".equals(job.state)) {
            long now = System.currentTimeMillis();
            long cancelPokeExpiry = now + CANCEL_POKE_MAX_DELAY_MS;
            leaseExpiresAtMs.compute(
                id,
                (ignored, expiry) ->
                    expiry == null || expiry <= 0L
                        ? cancelPokeExpiry
                        : Math.min(expiry, cancelPokeExpiry));
            nextAttemptAtMs.put(id, now);
            readyQueue.enqueue(
                id, job.executionPolicy.priorityClass(), job.executionPolicy.priorityScore());
            return new ReconcileJob(
                job.jobId,
                job.accountId,
                job.connectorId,
                "JS_CANCELLING",
                (reason == null || reason.isBlank()) ? "Cancelling" : reason,
                job.startedAtMs,
                0L,
                job.tablesScanned,
                job.tablesChanged,
                job.viewsScanned,
                job.viewsChanged,
                job.errors,
                job.fullRescan,
                job.captureMode,
                job.snapshotsProcessed,
                job.statsProcessed,
                job.scope,
                job.executionPolicy,
                job.executorId,
                job.jobKind,
                job.tableTask,
                job.viewTask,
                job.snapshotTask,
                job.fileGroupTask,
                job.parentJobId);
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          pinnedExecutors.remove(id);
          nextAttemptAtMs.remove(id);
          clearDedupe(id);
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_CANCELLED",
              (reason == null || reason.isBlank()) ? "Cancelled" : reason,
              job.startedAtMs,
              System.currentTimeMillis(),
              job.tablesScanned,
              job.tablesChanged,
              job.viewsScanned,
              job.viewsChanged,
              job.errors,
              job.fullRescan,
              job.captureMode,
              job.snapshotsProcessed,
              job.statsProcessed,
              job.scope,
              job.executionPolicy,
              job.executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
    var current = jobs.get(jobId);
    if (current == null) {
      return Optional.empty();
    }
    if (accountId != null && !accountId.isBlank() && !accountId.equals(current.accountId)) {
      return Optional.empty();
    }
    if (!"JS_CANCELLED".equals(current.state) && !"JS_CANCELLING".equals(current.state)) {
      return Optional.empty();
    }
    // Note: when a QUEUED job is cancelled, it may still be in readyQueue. The next leaseNext()
    // call will poll it and discard it (state is no longer JS_QUEUED or JS_CANCELLING).
    return Optional.of(current);
  }

  @Override
  public boolean isCancellationRequested(String jobId) {
    var job = jobs.get(jobId);
    return job != null && ("JS_CANCELLING".equals(job.state) || "JS_CANCELLED".equals(job.state));
  }

  @Override
  public void markCancelled(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long tablesScanned,
      long tablesChanged,
      long viewsScanned,
      long viewsChanged,
      long errors,
      long snapshotsProcessed,
      long statsProcessed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          releaseLane(id);
          releaseSnapshotLease(id);
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          // readyQueue has no remove-by-value: any residual entry will be discarded on
          // the next leaseNext() poll because the job state is no longer JS_QUEUED/JS_CANCELLING.
          pinnedExecutors.remove(id);
          nextAttemptAtMs.remove(id);
          clearDedupe(id);
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_CANCELLED",
              message == null || message.isBlank() ? "Cancelled" : message,
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              tablesScanned,
              tablesChanged,
              viewsScanned,
              viewsChanged,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope,
              job.executionPolicy,
              job.executorId,
              job.jobKind,
              job.tableTask,
              job.viewTask,
              job.snapshotTask,
              job.fileGroupTask,
              job.parentJobId);
        });
  }

  private boolean hasActiveLease(String jobId, String leaseEpoch, long nowMs) {
    String expected = leaseEpochs.get(jobId);
    Long expiry = leaseExpiresAtMs.get(jobId);
    return expected != null
        && !expected.isBlank()
        && expected.equals(leaseEpoch)
        && expiry != null
        && expiry > nowMs;
  }

  private boolean hasActiveLease(String jobId, String leaseEpoch) {
    return hasActiveLease(jobId, leaseEpoch, System.currentTimeMillis());
  }

  private synchronized void reclaimExpiredLeasesIfDue(long nowMs) {
    if (nowMs - lastReclaimAtMs < reclaimIntervalMs) {
      return;
    }
    lastReclaimAtMs = nowMs;
    for (String jobId : new HashSet<>(leased)) {
      Long expiry = leaseExpiresAtMs.get(jobId);
      if (expiry == null || expiry <= 0L || expiry > nowMs) {
        continue;
      }
      jobs.computeIfPresent(
          jobId,
          (id, job) -> {
            if (!leased.remove(id)) {
              return job;
            }
            releaseLane(id);
            releaseSnapshotLease(id);
            leaseEpochs.remove(id);
            leaseExpiresAtMs.remove(id);
            if ("JS_RUNNING".equals(job.state)) {
              nextAttemptAtMs.put(id, nowMs);
              readyQueue.enqueue(
                  id, job.executionPolicy.priorityClass(), job.executionPolicy.priorityScore());
              return new ReconcileJob(
                  job.jobId,
                  job.accountId,
                  job.connectorId,
                  "JS_QUEUED",
                  "Lease expired; requeued",
                  job.startedAtMs,
                  0L,
                  job.tablesScanned,
                  job.tablesChanged,
                  job.viewsScanned,
                  job.viewsChanged,
                  job.errors,
                  job.fullRescan,
                  job.captureMode,
                  job.snapshotsProcessed,
                  job.statsProcessed,
                  job.scope,
                  job.executionPolicy,
                  "",
                  job.jobKind,
                  job.tableTask,
                  job.viewTask,
                  job.snapshotTask,
                  job.fileGroupTask,
                  job.parentJobId);
            }
            if ("JS_CANCELLING".equals(job.state)) {
              nextAttemptAtMs.put(id, nowMs);
              readyQueue.enqueue(
                  id, job.executionPolicy.priorityClass(), job.executionPolicy.priorityScore());
              return new ReconcileJob(
                  job.jobId,
                  job.accountId,
                  job.connectorId,
                  "JS_CANCELLING",
                  "Lease expired while cancelling",
                  job.startedAtMs,
                  0L,
                  job.tablesScanned,
                  job.tablesChanged,
                  job.viewsScanned,
                  job.viewsChanged,
                  job.errors,
                  job.fullRescan,
                  job.captureMode,
                  job.snapshotsProcessed,
                  job.statsProcessed,
                  job.scope,
                  job.executionPolicy,
                  job.executorId,
                  job.jobKind,
                  job.tableTask,
                  job.viewTask,
                  job.snapshotTask,
                  job.fileGroupTask,
                  job.parentJobId);
            }
            return job;
          });
    }
  }

  private boolean hasLiveLaneLease(String jobId, long nowMs) {
    ReconcileJob job = jobs.get(jobId);
    if (job == null) {
      return false;
    }
    if (!"JS_RUNNING".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
      return false;
    }
    return leaseExpiresAtMs.getOrDefault(jobId, 0L) > nowMs;
  }

  private void releaseLane(String jobId) {
    String laneKey = laneKeysByJobId.get(jobId);
    if (laneKey != null && !laneKey.isBlank()) {
      activeJobIdByLaneKey.remove(laneKey, jobId);
    }
  }

  private boolean tryAcquireSnapshotLease(ReconcileJob job, String jobId, long nowMs) {
    String snapshotLeaseKey = snapshotLeaseKey(job);
    if (snapshotLeaseKey.isBlank()) {
      return true;
    }
    while (true) {
      String ownerJobId = activeJobIdBySnapshotLeaseKey.get(snapshotLeaseKey);
      if (ownerJobId == null) {
        if (activeJobIdBySnapshotLeaseKey.putIfAbsent(snapshotLeaseKey, jobId) == null) {
          return true;
        }
        continue;
      }
      if (ownerJobId.equals(jobId)) {
        return true;
      }
      if (hasLiveSnapshotLease(ownerJobId, nowMs)) {
        return false;
      }
      activeJobIdBySnapshotLeaseKey.remove(snapshotLeaseKey, ownerJobId);
    }
  }

  private boolean hasLiveSnapshotLease(String jobId, long nowMs) {
    ReconcileJob job = jobs.get(jobId);
    if (job == null || job.jobKind != ReconcileJobKind.PLAN_SNAPSHOT) {
      return false;
    }
    if (!"JS_RUNNING".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
      return false;
    }
    return leaseExpiresAtMs.getOrDefault(jobId, 0L) > nowMs;
  }

  private void releaseSnapshotLease(String jobId) {
    ReconcileJob job = jobs.get(jobId);
    String snapshotLeaseKey = snapshotLeaseKey(job);
    if (!snapshotLeaseKey.isBlank()) {
      activeJobIdBySnapshotLeaseKey.remove(snapshotLeaseKey, jobId);
    }
  }

  private void clearDedupe(String jobId) {
    String dedupeKey = dedupeKeysByJobId.get(jobId);
    if (dedupeKey != null && !dedupeKey.isBlank()) {
      activeJobIdByDedupeKey.remove(dedupeKey, jobId);
    }
  }

  /** Package-private for testing: force the current health band. */
  void setCurrentBandForTest(SchedulerHealthBand band) {
    currentBand.set(band);
  }

  private static long agingThresholdMs(StatsPriorityClass cls) {
    return switch (cls) {
      case P3_BACKGROUND -> P3_AGING_THRESHOLD_MS;
      case P2_REPAIR -> P2_AGING_THRESHOLD_MS;
      default -> Long.MAX_VALUE; // P0/P1 never age
    };
  }

  private static long admissionDeferMs(StatsPriorityClass cls, SchedulerHealthBand band) {
    return switch (cls) {
      case P0_SYNC, P1_FRESHNESS -> 0L; // always admit
      case P2_REPAIR -> band == SchedulerHealthBand.RED ? DEFER_DELAY_MS : 0L;
      case P3_BACKGROUND ->
          switch (band) {
            case GREEN -> 0L;
            case YELLOW -> ThreadLocalRandom.current().nextBoolean() ? 0L : DEFER_DELAY_MS;
            case ORANGE, RED -> DEFER_DELAY_MS;
          };
    };
  }

  private long backoffMs(int attempts) {
    long base = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(maxBackoffMs, base);
  }

  private static String snapshotLeaseKey(ReconcileJob job) {
    if (job == null
        || job.jobKind != ReconcileJobKind.PLAN_SNAPSHOT
        || job.snapshotTask == null
        || blank(job.snapshotTask.tableId())
        || job.snapshotTask.snapshotId() < 0L) {
      return "";
    }
    return job.snapshotTask.tableId() + "|" + job.snapshotTask.snapshotId();
  }

  private static boolean isTerminalState(String state) {
    return "JS_SUCCEEDED".equals(state)
        || "JS_FAILED".equals(state)
        || "JS_CANCELLED".equals(state);
  }

  private static ReconcileScope normalizeScopeForJobKind(
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask) {
    ReconcileScope effectiveScope = scope == null ? ReconcileScope.empty() : scope;
    if (jobKind == ReconcileJobKind.PLAN_TABLE
        && tableTask != null
        && tableTask.strict()
        && !blank(tableTask.destinationTableId())) {
      if (effectiveScope.hasTableFilter()
          && !tableTask.destinationTableId().equals(effectiveScope.destinationTableId())) {
        throw new IllegalArgumentException(
            "table task destinationTableId does not match scope destinationTableId");
      }
      if (effectiveScope.hasViewFilter() || effectiveScope.hasNamespaceFilter()) {
        throw new IllegalArgumentException(
            "table task destinationTableId cannot be combined with namespace or view scope");
      }
      return effectiveScope.hasTableFilter()
          ? effectiveScope
          : ReconcileScope.of(
              List.of(),
              tableTask.destinationTableId(),
              effectiveScope.destinationCaptureRequests(),
              effectiveScope.capturePolicy());
    }
    if (jobKind == ReconcileJobKind.PLAN_VIEW
        && viewTask != null
        && viewTask.strict()
        && !blank(viewTask.destinationViewId())) {
      if (effectiveScope.hasViewFilter()
          && !viewTask.destinationViewId().equals(effectiveScope.destinationViewId())) {
        throw new IllegalArgumentException(
            "view task destinationViewId does not match scope destinationViewId");
      }
      if (effectiveScope.hasNamespaceFilter()
          && !effectiveScope
              .destinationNamespaceIds()
              .contains(viewTask.destinationNamespaceId())) {
        throw new IllegalArgumentException(
            "view task destinationNamespaceId does not match scope destinationNamespaceIds");
      }
      if (effectiveScope.hasTableFilter() || effectiveScope.hasCaptureRequestFilter()) {
        throw new IllegalArgumentException(
            "view task destinationViewId cannot be combined with table or capture scope");
      }
      return effectiveScope.hasViewFilter()
          ? effectiveScope
          : ReconcileScope.ofView(List.of(), viewTask.destinationViewId());
    }
    return effectiveScope;
  }

  private static String laneKey(
      String connectorId,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    if (jobKind == ReconcileJobKind.PLAN_TABLE && tableTask != null) {
      return scope.destinationTableId() == null || scope.destinationTableId().isBlank()
          ? "tables|" + namespaces
          : "table|" + scope.destinationTableId();
    }
    if (jobKind == ReconcileJobKind.PLAN_VIEW && viewTask != null) {
      return scope.destinationViewId() == null || scope.destinationViewId().isBlank()
          ? "views|" + namespaces
          : "view|" + scope.destinationViewId();
    }
    if (jobKind == ReconcileJobKind.PLAN_SNAPSHOT
        && snapshotTask != null
        && !blank(snapshotTask.tableId())) {
      return "snapshot-plan|" + snapshotTask.tableId();
    }
    if (jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
        && snapshotTask != null
        && !blank(snapshotTask.tableId())) {
      String snapshotPart =
          snapshotTask.snapshotId() >= 0L ? Long.toString(snapshotTask.snapshotId()) : "*";
      return "snapshot-finalize|" + snapshotTask.tableId() + "|" + snapshotPart;
    }
    if (jobKind == ReconcileJobKind.EXEC_FILE_GROUP
        && fileGroupTask != null
        && !blank(fileGroupTask.tableId())) {
      String snapshotPart =
          fileGroupTask.snapshotId() >= 0L ? Long.toString(fileGroupTask.snapshotId()) : "*";
      String groupPart = blank(fileGroupTask.groupId()) ? "*" : fileGroupTask.groupId();
      return "file-group|" + fileGroupTask.tableId() + "|" + snapshotPart + "|" + groupPart;
    }
    String resource =
        scope.destinationTableId() != null
            ? scope.destinationTableId()
            : (scope.destinationViewId() == null ? "*" : scope.destinationViewId());
    return namespaces + "|" + resource;
  }

  private static String dedupeKey(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope,
      ReconcileJobKind jobKind,
      ReconcileTableTask tableTask,
      ReconcileViewTask viewTask,
      ReconcileSnapshotTask snapshotTask,
      ReconcileFileGroupTask fileGroupTask,
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    String table = scope.destinationTableId() == null ? "*" : scope.destinationTableId();
    String captureRequests =
        scope.destinationCaptureRequests().stream()
            .map(InMemoryReconcileJobStore::canonicalCaptureRequest)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String capturePolicy = canonicalCapturePolicy(scope.capturePolicy());
    String canonicalTableDisplayName =
        tableTask != null && tableTask.strict() && !blank(tableTask.destinationTableId())
            ? ""
            : (tableTask == null ? "" : blankToEmpty(tableTask.destinationTableDisplayName()));
    String canonicalViewDisplayName =
        viewTask != null && viewTask.strict() && !blank(viewTask.destinationViewId())
            ? ""
            : (viewTask == null ? "" : blankToEmpty(viewTask.destinationViewDisplayName()));
    ReconcileExecutionPolicy policy =
        executionPolicy == null ? ReconcileExecutionPolicy.defaults() : executionPolicy;
    String payload =
        String.join(
            "\n",
            "account_id=" + blankToEmpty(accountId),
            "connector_id=" + blankToEmpty(connectorId),
            "job_kind="
                + (jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR.name() : jobKind.name()),
            "full_rescan=" + fullRescan,
            "capture_mode="
                + (captureMode == null
                    ? CaptureMode.METADATA_AND_CAPTURE.name()
                    : captureMode.name()),
            "table_task.source_namespace="
                + (tableTask == null ? "" : blankToEmpty(tableTask.sourceNamespace())),
            "table_task.source_table="
                + (tableTask == null ? "" : blankToEmpty(tableTask.sourceTable())),
            "table_task.destination_table_id="
                + (tableTask == null ? "" : blankToEmpty(tableTask.destinationTableId())),
            "table_task.destination_namespace_id="
                + (tableTask == null ? "" : blankToEmpty(tableTask.destinationNamespaceId())),
            "table_task.destination_table_display_name=" + canonicalTableDisplayName,
            "table_task.mode=" + (tableTask == null ? "" : tableTask.mode().name()),
            "view_task.source_namespace="
                + (viewTask == null ? "" : blankToEmpty(viewTask.sourceNamespace())),
            "view_task.source_view="
                + (viewTask == null ? "" : blankToEmpty(viewTask.sourceView())),
            "view_task.destination_namespace_id="
                + (viewTask == null ? "" : blankToEmpty(viewTask.destinationNamespaceId())),
            "view_task.destination_view_id="
                + (viewTask == null ? "" : blankToEmpty(viewTask.destinationViewId())),
            "view_task.destination_view_display_name=" + canonicalViewDisplayName,
            "view_task.mode=" + (viewTask == null ? "" : viewTask.mode().name()),
            "snapshot_task.table_id="
                + (snapshotTask == null ? "" : blankToEmpty(snapshotTask.tableId())),
            "snapshot_task.snapshot_id=" + (snapshotTask == null ? 0L : snapshotTask.snapshotId()),
            "snapshot_task.source_namespace="
                + (snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceNamespace())),
            "snapshot_task.source_table="
                + (snapshotTask == null ? "" : blankToEmpty(snapshotTask.sourceTable())),
            "snapshot_task.file_group_plan_recorded="
                + (snapshotTask != null && snapshotTask.fileGroupPlanRecorded()),
            "snapshot_task.file_groups="
                + String.join(
                    ",",
                    canonicalSnapshotFileGroups(
                        snapshotTask == null ? List.of() : snapshotTask.fileGroups())),
            "file_group_task.plan_id="
                + (fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.planId())),
            "file_group_task.group_id="
                + (fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.groupId())),
            "file_group_task.table_id="
                + (fileGroupTask == null ? "" : blankToEmpty(fileGroupTask.tableId())),
            "file_group_task.snapshot_id="
                + (fileGroupTask == null ? 0L : fileGroupTask.snapshotId()),
            "file_group_task.file_paths="
                + (fileGroupTask == null ? "" : String.join(",", fileGroupTask.filePaths())),
            "scope.namespaces=" + namespaces,
            "scope.table=" + table,
            "scope.view=" + blankToEmpty(scope.destinationViewId()),
            "scope.capture_requests=" + captureRequests,
            "scope.capture_policy=" + capturePolicy,
            "policy.execution_class=" + policy.executionClass().name(),
            "policy.lane=" + policy.lane(),
            "policy.attributes=" + canonicalAttributes(policy.attributes()),
            "parent_job_id=" + blankToEmpty(parentJobId),
            "pinned_executor_id=" + blankToEmpty(pinnedExecutorId));
    return hashValue(payload);
  }

  private static String canonicalCaptureRequest(ReconcileScope.ScopedCaptureRequest request) {
    return request.tableId()
        + "|"
        + request.snapshotId()
        + "|"
        + request.targetSpec()
        + "|"
        + String.join(",", request.columnSelectors());
  }

  private static String canonicalCapturePolicy(ReconcileCapturePolicy policy) {
    if (policy == null || policy.isEmpty()) {
      return "";
    }
    String columns =
        policy.columns().stream()
            .map(
                column ->
                    column.selector() + ":" + column.captureStats() + ":" + column.captureIndex())
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
    String outputs =
        policy.outputs().stream().map(Enum::name).sorted().reduce((a, b) -> a + "," + b).orElse("");
    return columns + "|" + outputs;
  }

  private static List<String> canonicalSnapshotFileGroups(List<ReconcileFileGroupTask> fileGroups) {
    if (fileGroups == null || fileGroups.isEmpty()) {
      return List.of();
    }
    return fileGroups.stream()
        .filter(group -> group != null && !group.isEmpty())
        .map(
            group ->
                blankToEmpty(group.planId())
                    + "|"
                    + blankToEmpty(group.groupId())
                    + "|"
                    + blankToEmpty(group.tableId())
                    + "|"
                    + group.snapshotId()
                    + "|"
                    + String.join(",", group.filePaths()))
        .sorted()
        .toList();
  }

  private static String canonicalAttributes(Map<String, String> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return "";
    }
    return attributes.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> blankToEmpty(entry.getKey()) + "=" + blankToEmpty(entry.getValue()))
        .reduce((a, b) -> a + "," + b)
        .orElse("");
  }

  private static void requireExplicitSnapshotCoverage(
      ReconcileJobKind jobKind, ReconcileSnapshotTask snapshotTask, String operation) {
    if (jobKind == null || snapshotTask == null) {
      return;
    }
    if (jobKind == ReconcileJobKind.FINALIZE_SNAPSHOT_CAPTURE
        && !snapshotTask.fileGroupPlanRecorded()) {
      throw new IllegalArgumentException(
          "FINALIZE_SNAPSHOT_CAPTURE requires explicit snapshot coverage metadata");
    }
  }

  private static String hashValue(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      return java.util.Base64.getUrlEncoder()
          .withoutPadding()
          .encodeToString(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      return value;
    }
  }

  private static String blankToEmpty(String value) {
    return value == null ? "" : value.trim();
  }

  private static boolean blank(String value) {
    return value == null || value.isBlank();
  }
}
