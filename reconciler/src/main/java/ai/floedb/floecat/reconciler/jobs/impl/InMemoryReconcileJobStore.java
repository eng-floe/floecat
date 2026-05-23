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
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionClass;
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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
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
  //                             preemption (not yet implemented; requires feature flag)
  //  RUNNING     → SUCCEEDED   markSucceeded()
  //  RUNNING     → QUEUED      lease expired (reclaimExpiredLeasesIfDue); markFailed() retry
  //  RUNNING     → FAILED      markFailed() after maxAttempts exceeded; markFailedTerminal()
  //  CANCELLING  → RUNNING     leaseNext() re-leases CANCELLING job so executor can observe it
  //  CANCELLING  → QUEUED      (not yet implemented; would require fileResults checkpoint support)
  //  CANCELLING  → CANCELLED   markCancelled() — current behavior
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
  // Scheduler constants — live in SchedulerStoreHelpers and SchedulerBandState; kept here as
  // package-private aliases so existing tests that import them directly continue to compile.
  // TODO: migrate test imports to SchedulerStoreHelpers directly and remove these aliases.
  // ---------------------------------------------------------------------------
  static final long P3_AGING_THRESHOLD_MS = SchedulerStoreHelpers.P3_AGING_THRESHOLD_MS;
  static final long P2_AGING_THRESHOLD_MS = SchedulerStoreHelpers.P2_AGING_THRESHOLD_MS;
  static final long P1_AGING_THRESHOLD_MS = SchedulerStoreHelpers.P1_AGING_THRESHOLD_MS;
  static final long AGING_COOLDOWN_MS = SchedulerStoreHelpers.AGING_COOLDOWN_MS;
  static final long DEFER_DELAY_MS = SchedulerStoreHelpers.DEFER_DELAY_MS;

  // ---------------------------------------------------------------------------
  // Scheduler constants
  // ---------------------------------------------------------------------------
  /**
   * Hard cap on total {@link PriorityReadyQueue#pollHighest} calls per priority class per {@code
   * leaseNext()} invocation. Bounds the worst case when many jobs are ineligible (e.g. all lanes
   * blocked during a burst). Must be larger than {@link #MAX_WRR_CANDIDATES}.
   */
  private static final int MAX_TOTAL_POLL = 64;

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
  // Accessed only inside synchronized reclaimExpiredLeasesIfDue(); volatile is not needed.
  private long lastReclaimAtMs;

  // ---------------------------------------------------------------------------
  // Health band state
  // ---------------------------------------------------------------------------
  private final SchedulerBandState bandState = new SchedulerBandState();

  // ---------------------------------------------------------------------------
  // Starvation aging state
  // ---------------------------------------------------------------------------
  private final AgingPromotionTracker agingTracker = new AgingPromotionTracker();

  // ---------------------------------------------------------------------------
  // Admission control state
  // ---------------------------------------------------------------------------
  private final Map<StatsPriorityClass, AtomicLong> admissionDeferred =
      new EnumMap<>(StatsPriorityClass.class);

  // ---------------------------------------------------------------------------
  // WRR + P0 guard state (shared with DurableReconcileJobStore via SchedulerDispatcher)
  // ---------------------------------------------------------------------------
  private final SchedulerDispatcher dispatcher = new SchedulerDispatcher();

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
        maybePromoteDedupedQueuedJob(activeJobId, existing, effectivePolicy);
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
    // Canonical lane key: prefer the caller-supplied policy lane when non-blank (allows the
    // orchestrator to override fairness grouping, e.g. "accountId:tableId"), otherwise fall back
    // to the computed scope+job-kind key.  The same key drives both the lane-mutex
    // (activeJobIdByLaneKey) and the WRR virtual-time counter (laneServiceCounts), so the two
    // sub-systems are always in agreement.
    String computedLane =
        laneKey(
            connectorId,
            effectiveScope,
            effectiveJobKind,
            effectiveTableTask,
            effectiveViewTask,
            effectiveSnapshotTask,
            effectiveFileGroupTask);
    String policyLane = effectivePolicy.lane();
    laneKeysByJobId.put(id, policyLane.isBlank() ? computedLane : policyLane);
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
    // Escalate health band based on current queue depths + P0 starvation.
    Map<StatsPriorityClass, Long> depths = readyQueue.sizeByAllClasses();
    long p0Depth = depths.getOrDefault(StatsPriorityClass.P0_SYNC, 0L);
    long oldestP0AgeMs = 0L;
    if (p0Depth > 0) {
      long oldestP0CreatedAt =
          jobs.values().stream()
              .filter(
                  j ->
                      "JS_QUEUED".equals(j.state)
                          && j.executionPolicy != null
                          && j.executionPolicy.priorityClass() == StatsPriorityClass.P0_SYNC)
              .mapToLong(j -> createdAtMs.getOrDefault(j.jobId, now))
              .min()
              .orElse(now);
      oldestP0AgeMs = now - oldestP0CreatedAt;
    }
    bandState.maybeEscalate(now, depths, oldestP0AgeMs);
    boolean policyDeferred =
        "true"
            .equals(
                effectivePolicy
                    .attributes()
                    .getOrDefault(SchedulerStoreHelpers.ATTR_POLICY_DEFERRED, ""));
    long deferMs =
        SchedulerStoreHelpers.admissionDeferMs(
            effectivePolicy.priorityClass(), bandState.current(), policyDeferred);
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

  /**
   * Promotes the scheduling priority of an existing deduped queued job when a stronger enqueue
   * request arrives for the same logical work.
   *
   * <p>Promotion rules:
   *
   * <ul>
   *   <li>Only applies to {@code JS_QUEUED} jobs.
   *   <li>Priority class can only move to a higher urgency class (e.g. P3 → P1), never downgrade.
   *   <li>Within the same class, score is raised to the max(current, requested).
   *   <li>Deferred jobs are pulled forward to {@code now} so urgent follow-up requests are not
   *       blocked behind old defer timers.
   * </ul>
   */
  private void maybePromoteDedupedQueuedJob(
      String jobId, ReconcileJob existing, ReconcileExecutionPolicy requestedPolicy) {
    if (existing == null
        || existing.executionPolicy == null
        || requestedPolicy == null
        || !"JS_QUEUED".equals(existing.state)) {
      return;
    }
    ReconcileExecutionPolicy currentPolicy = existing.executionPolicy;
    ReconcileExecutionPolicy promoted =
        promotedPriorityPolicy(
            currentPolicy.priorityClass(),
            currentPolicy.priorityScore(),
            requestedPolicy.priorityClass(),
            requestedPolicy.priorityScore(),
            currentPolicy.executionClass(),
            currentPolicy.lane(),
            currentPolicy.attributes());
    if (promoted.priorityClass() == currentPolicy.priorityClass()
        && promoted.priorityScore() == currentPolicy.priorityScore()) {
      return;
    }

    long now = System.currentTimeMillis();
    long nextDue = Math.min(nextAttemptAtMs.getOrDefault(jobId, now), now);
    nextAttemptAtMs.put(jobId, nextDue);

    readyQueue.removeJob(jobId);
    readyQueue.enqueue(jobId, promoted.priorityClass(), promoted.priorityScore());

    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!"JS_QUEUED".equals(job.state)) {
            return job;
          }
          return withExecutionPolicy(job, promoted);
        });
  }

  private static ReconcileExecutionPolicy promotedPriorityPolicy(
      StatsPriorityClass currentClass,
      long currentScore,
      StatsPriorityClass requestedClass,
      long requestedScore,
      ReconcileExecutionClass executionClass,
      String lane,
      Map<String, String> attributes) {
    StatsPriorityClass effectiveCurrent =
        currentClass == null ? StatsPriorityClass.P3_BACKGROUND : currentClass;
    StatsPriorityClass effectiveRequested =
        requestedClass == null ? StatsPriorityClass.P3_BACKGROUND : requestedClass;
    StatsPriorityClass promotedClass =
        effectiveRequested.ordinal() < effectiveCurrent.ordinal()
            ? effectiveRequested
            : effectiveCurrent;
    long promotedScore =
        (promotedClass == effectiveCurrent && promotedClass == effectiveRequested)
            ? Math.max(currentScore, requestedScore)
            : (promotedClass == effectiveRequested
                ? Math.max(0L, requestedScore)
                : Math.max(0L, currentScore));
    return new ReconcileExecutionPolicy(
        executionClass, lane, attributes, promotedClass, promotedScore);
  }

  private static ReconcileJob withExecutionPolicy(
      ReconcileJob job, ReconcileExecutionPolicy executionPolicy) {
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
        executionPolicy,
        job.executorId,
        job.jobKind,
        job.tableTask,
        job.viewTask,
        job.snapshotTask,
        job.fileGroupTask,
        job.parentJobId);
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
    // Track oldest enqueue time per lane for lane-wait metric (top-10 by wait time).
    Map<String, Long> laneOldestCreatedAtMs = new java.util.HashMap<>();
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
          // Track per-lane oldest enqueue time for lane wait metrics.
          String laneKey = laneKeysByJobId.getOrDefault(job.jobId, "");
          if (!laneKey.isBlank() && created > 0L) {
            laneOldestCreatedAtMs.merge(laneKey, created, Math::min);
          }
        }
        case "JS_RUNNING" -> running++;
        case "JS_CANCELLING" -> cancelling++;
        default -> {}
      }
    }
    // Note: `queued` (above) counts JS_QUEUED entries in the jobs map; `byClass` counts entries in
    // the skip-list buckets.  These are different views of the same logical set: the jobs map is
    // the authoritative record, the skip-list is the dispatch index.  They can momentarily diverge
    // under concurrent enqueue/lease, so callers should not expect exact equality.
    Map<StatsPriorityClass, Long> byClass = readyQueue.sizeByAllClasses();
    long p0Count = byClass.getOrDefault(StatsPriorityClass.P0_SYNC, 0L);
    long p2Count = byClass.getOrDefault(StatsPriorityClass.P2_REPAIR, 0L);
    long p3Count = byClass.getOrDefault(StatsPriorityClass.P3_BACKGROUND, 0L);

    // Note on P0 RED condition: p0Count comes from the skip-list (approximate AtomicLong) while
    // oldestP0QueuedCreatedAtMs comes from the jobs-map scan above (authoritative).  A P0 job that
    // has just been polled from the skip-list but not yet marked JS_RUNNING can briefly make
    // p0Count == 0 while still appearing as JS_QUEUED in the map.  In that window the condition
    // below is false and queueStats() will compute GREEN/YELLOW/ORANGE instead of RED.  The next
    // bandState.maybeEscalate() call (within 1 s) will re-escalate to RED if the job is still
    // waiting.
    // This one-cycle inconsistency is an accepted trade-off for avoiding a second O(n) map scan.
    long oldestP0AgeMs =
        (p0Count > 0 && oldestP0QueuedCreatedAtMs > 0L) ? now - oldestP0QueuedCreatedAtMs : 0L;
    SchedulerHealthBand band = bandState.computeAndSet(byClass, oldestP0AgeMs);

    Map<StatsPriorityClass, Long> deferredSnapshot = new EnumMap<>(StatsPriorityClass.class);
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      deferredSnapshot.put(cls, admissionDeferred.get(cls).get());
    }

    // Compute top-10 lanes by oldest queued job wait time (most-starved first).
    // Use LinkedHashMap to preserve the sort order in the returned map.
    Map<String, Long> topLaneWaitMs =
        laneOldestCreatedAtMs.entrySet().stream()
            .sorted(Map.Entry.comparingByValue()) // oldest created-at first = most-starved first
            .limit(10)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> now - e.getValue(),
                    (a, b) -> a,
                    java.util.LinkedHashMap::new));

    return new QueueStats(
        queued,
        running,
        cancelling,
        oldestQueued,
        byClass,
        band,
        agingTracker.totalPromotions(),
        deferredSnapshot,
        topLaneWaitMs);
  }

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
  public String persistSnapshotPlanManifest(
      String accountId, String jobId, ReconcileSnapshotTask snapshotTask) {
    ReconcileSnapshotTask effective =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    persistSnapshotPlan(jobId, effective);
    return effective.fileGroupPlanBlobUri();
  }

  @Override
  public boolean adoptSnapshotPlanManifest(
      String jobId,
      String leaseEpoch,
      ReconcileSnapshotTask snapshotTask,
      String manifestUri,
      boolean allowExpiredWithinGrace) {
    if (jobId == null || jobId.isBlank() || leaseEpoch == null || leaseEpoch.isBlank()) {
      return false;
    }
    ReconcileSnapshotTask base =
        snapshotTask == null ? ReconcileSnapshotTask.empty() : snapshotTask;
    String effectiveManifestUri =
        manifestUri != null && !manifestUri.isBlank()
            ? manifestUri.trim()
            : base.fileGroupPlanBlobUri();
    ReconcileSnapshotTask effective =
        ReconcileSnapshotTask.of(
            base.tableId(),
            base.snapshotId(),
            base.sourceNamespace(),
            base.sourceTable(),
            base.fileGroups(),
            base.fileGroupPlanRecorded(),
            base.completionMode(),
            effectiveManifestUri,
            base.fileGroupCount(),
            base.directStatsBlobUri(),
            base.directStatsRecordCount());

    long now = System.currentTimeMillis();
    String expectedEpoch = leaseEpochs.get(jobId);
    Long leaseExpiryMs = leaseExpiresAtMs.get(jobId);
    if (expectedEpoch == null
        || !expectedEpoch.equals(leaseEpoch)
        || leaseExpiryMs == null
        || leaseExpiryMs <= 0L) {
      return false;
    }
    if (leaseExpiryMs <= now) {
      if (!allowExpiredWithinGrace) {
        return false;
      }
      if (now - leaseExpiryMs > leaseMs) {
        return false;
      }
    }
    persistSnapshotPlan(jobId, effective);
    return true;
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

    // Lazily clean expired aging promotions.
    agingTracker.cleanupExpired(now);

    // Dispatch loop: scan priority classes P0→P3 in order.
    //
    // For each class we collect up to MAX_WRR_CANDIDATES eligible candidates (each having passed
    // backoff, filter, lane-mutex, and snapshot-lease checks), then run a WRR tournament via
    // SchedulerDispatcher.selectWinner(). The lane with the lowest virtual-time counter wins;
    // ties broken by score. Non-winners have their snapshot leases released and are requeued.
    //
    // P0_SYNC guard: if any P0-compatible candidate was blocked (backoff, lane, snapshot, or WRR
    // loser), we return empty rather than falling through to P1/P2/P3. Filter-rejected candidates
    // (wrong execution class / pinned executor) do NOT trigger the guard — this executor cannot
    // help them and should remain productive on lower-priority work it can actually run.
    //
    // Known limitation: laneServiceCounts values can overflow for very long-lived deployments
    // (long wraps at ~9.2e18 increments). SchedulerDispatcher holds these counters in-memory;
    // a periodic reset or virtual-time modulo would address the overflow; current deployment
    // lifetimes make it unrealistic.

    // blockedCount tracks P0 candidates that THIS EXECUTOR COULD RUN but cannot right now
    // (lane-held, backoff-deferred, snapshot-blocked, WRR loser). Filter-rejected P0 candidates
    // (wrong execution class / pinned executor) do NOT count — this executor cannot help them.
    int p0BlockedCount = 0;
    for (StatsPriorityClass cls : StatsPriorityClass.values()) {
      if (readyQueue.sizeByClass(cls) == 0) {
        continue;
      }

      // Collect up to MAX_WRR_CANDIDATES eligible candidates for WRR tournament.
      // Each candidate has its snapshot lease acquired; losers are released after the tournament.
      int scanLimit = (int) Math.min(readyQueue.sizeByClass(cls), MAX_TOTAL_POLL);
      int candidatesFound = 0;
      int blockedThisClass = 0;

      // Use a small fixed-size list; capacity = MAX_WRR_CANDIDATES
      List<SchedulerDispatcher.WrrCandidate> candidates =
          new ArrayList<>(SchedulerDispatcher.MAX_WRR_CANDIDATES);
      // Parallel list: snapshot-lease bookkeeping per candidate index
      List<String> candidateJobIds = new ArrayList<>(SchedulerDispatcher.MAX_WRR_CANDIDATES);

      for (int i = 0;
          i < scanLimit && candidatesFound < SchedulerDispatcher.MAX_WRR_CANDIDATES;
          i++) {
        String jobId = readyQueue.pollHighest(cls);
        if (jobId == null) {
          break;
        }

        var job = jobs.get(jobId);
        if (job == null) {
          // Removed from store — discard, no P0 effect.
          continue;
        }

        if (!"JS_QUEUED".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
          // Terminal or already-running — discard, no P0 effect.
          continue;
        }

        StatsPriorityClass authoritativeClass =
            job.executionPolicy == null
                ? StatsPriorityClass.P3_BACKGROUND
                : job.executionPolicy.priorityClass();
        if (authoritativeClass != cls) {
          // Stale ready-queue entry (e.g. dedupe priority upgrade): move it to the
          // authoritative class before continuing. Do not count this as "blocked" for P0 guard.
          readyQueue.requeue(
              jobId,
              authoritativeClass,
              job.executionPolicy == null ? 0L : job.executionPolicy.priorityScore());
          continue;
        }

        if (nextAttemptAtMs.getOrDefault(jobId, 0L) > now) {
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          blockedThisClass++;
          continue;
        }

        if (!effective.matches(
            job.executionPolicy, pinnedExecutors.getOrDefault(jobId, ""), job.jobKind)) {
          // Filter-rejected: this executor cannot run this job regardless.
          // Requeue but do NOT count as blocked — it must not trigger the P0 guard.
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          continue;
        }

        String laneKey = laneKeysByJobId.getOrDefault(jobId, "");
        if (!laneKey.isBlank()) {
          String laneOwner = activeJobIdByLaneKey.get(laneKey);
          if (laneOwner != null && !laneOwner.equals(jobId) && hasLiveLaneLease(laneOwner, now)) {
            readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
            blockedThisClass++;
            continue;
          }
        }

        if (!tryAcquireSnapshotLease(job, jobId, now)) {
          readyQueue.requeue(jobId, cls, job.executionPolicy.priorityScore());
          blockedThisClass++;
          continue;
        }

        // Eligible: add to WRR tournament pool.
        candidates.add(
            new SchedulerDispatcher.WrrCandidate(
                jobId, laneKey, job.executionPolicy.priorityScore()));
        candidateJobIds.add(jobId);
        candidatesFound++;
      }

      if (cls == StatsPriorityClass.P0_SYNC) {
        p0BlockedCount += blockedThisClass;
      }

      // Run WRR tournament
      Optional<SchedulerDispatcher.WrrCandidate> winnerOpt = dispatcher.selectWinner(candidates);

      if (winnerOpt.isEmpty()) {
        // No eligible candidate in this class.
        if (SchedulerDispatcher.shouldBlockLowerClasses(cls, p0BlockedCount)) {
          // Release all held snapshot leases (candidates is empty here, but be safe)
          for (String id : candidateJobIds) {
            releaseSnapshotLease(id);
          }
          return Optional.empty();
        }
        continue;
      }

      SchedulerDispatcher.WrrCandidate winner = winnerOpt.get();

      // Release snapshot leases for all non-winners; requeue them.
      for (int idx = 0; idx < candidateJobIds.size(); idx++) {
        String id = candidateJobIds.get(idx);
        if (!id.equals(winner.jobId())) {
          releaseSnapshotLease(id);
          ReconcileJob loserJob = jobs.get(id);
          readyQueue.requeue(
              id, cls, loserJob != null ? loserJob.executionPolicy.priorityScore() : 0L);
          if (cls == StatsPriorityClass.P0_SYNC) {
            p0BlockedCount++;
          }
        }
      }

      // Starvation aging
      long ageMs = now - createdAtMs.getOrDefault(winner.jobId(), now);
      agingTracker.recordIfEligible(winner.jobId(), ageMs, cls, now);

      final String finalBestJobId = winner.jobId();
      final String finalBestLaneKey = winner.laneKey();

      if (leased.add(finalBestJobId)) {
        String leaseEpoch = UUID.randomUUID().toString();
        leaseEpochs.put(finalBestJobId, leaseEpoch);
        leaseExpiresAtMs.put(finalBestJobId, now + leaseMs);
        if (!finalBestLaneKey.isBlank()) {
          activeJobIdByLaneKey.put(finalBestLaneKey, finalBestJobId);
        }
        dispatcher.recordDispatch(finalBestLaneKey);
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
        if (leasedJob == null) {
          // Job was removed between leased.add() and jobs.get() — treat as a failed lease.
          leased.remove(finalBestJobId);
          leaseEpochs.remove(finalBestJobId);
          leaseExpiresAtMs.remove(finalBestJobId);
          if (!finalBestLaneKey.isBlank()) {
            activeJobIdByLaneKey.remove(finalBestLaneKey, finalBestJobId);
          }
          releaseSnapshotLease(finalBestJobId);
          readyQueue.requeue(finalBestJobId, cls, 0L);
          if (cls == StatsPriorityClass.P0_SYNC) {
            p0BlockedCount++;
          }
          // Fall through to P0 guard check below.
        } else {
          bandState.maybeClearRedOnP0Drain(readyQueue.sizeByAllClasses());
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
        } // end else (leasedJob != null)
      } else {
        // leased.add() returned false: another executor holds the original lease.
        releaseSnapshotLease(finalBestJobId);
        readyQueue.requeue(
            finalBestJobId,
            cls,
            jobs.get(finalBestJobId) != null
                ? jobs.get(finalBestJobId).executionPolicy.priorityScore()
                : 0L);
        if (cls == StatsPriorityClass.P0_SYNC) {
          p0BlockedCount++;
        }
      }

      // P0 guard check after attempting to lease the winner
      if (SchedulerDispatcher.shouldBlockLowerClasses(cls, p0BlockedCount)) {
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

  // synchronized here prevents concurrent sweeps from doing redundant O(n) work; it is NOT the
  // correctness guard.  Correctness comes from the CAS on leased.remove(id) inside the loop:
  // even without this modifier, each jobId would be re-enqueued at most once because
  // leased.remove() is atomic and returns false for a second concurrent caller.
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
    bandState.setForTest(band);
  }

  /** Package-private for testing: backdate a job's creation timestamp to simulate aging. */
  void backdateCreatedAtForTest(String jobId, long createdAtMs) {
    this.createdAtMs.put(jobId, createdAtMs);
  }

  /** Package-private for testing: reset the band-refresh TTL so the next enqueue triggers it. */
  void resetBandRefreshForTest() {
    bandState.resetEscalateCooldownForTest();
  }

  /**
   * Package-private for testing: immediately expire all active leases so that the next {@link
   * #leaseNext()} call will trigger {@link #reclaimExpiredLeasesIfDue} and re-queue them. Simulates
   * the passage of the full lease duration without sleeping.
   */
  void forceExpireAllLeasesForTest() {
    leaseExpiresAtMs.replaceAll((id, expiry) -> 1L); // epoch 1 ms is always in the past
    lastReclaimAtMs = 0L; // ensure the reclaim runs on the next leaseNext() call
  }

  /**
   * Package-private for testing: return the current WRR virtual-time counter for the given lane
   * key, or 0 if the lane has never been dispatched.
   */
  long laneServiceCountForTest(String laneKey) {
    return dispatcher.virtualTimeFor(laneKey);
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
            // policy.lane is included in the dedupe key so that two callers requesting the same
            // logical work but routing it to different fairness lanes produce distinct job entries.
            // Without this, a background P3 enqueue could silently absorb a caller-supplied P0
            // lane override and route the dispatch through the wrong WRR bucket.
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
    if (policy == null) {
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
    return columns + "|" + outputs + "|" + policy.maxCost().name();
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
