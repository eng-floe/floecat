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
import ai.floedb.floecat.reconciler.jobs.ReconcileExecutionPolicy;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import ai.floedb.floecat.reconciler.jobs.ReconcileTableTask;
import ai.floedb.floecat.reconciler.jobs.ReconcileViewTask;
import io.quarkus.arc.Arc;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

@ApplicationScoped
@IfBuildProperty(
    name = "floecat.reconciler.job-store",
    stringValue = "memory",
    enableIfMissing = true)
public class InMemoryReconcileJobStore implements ReconcileJobStore {
  private static final int DEFAULT_MAX_ATTEMPTS = 8;
  private static final long DEFAULT_BASE_BACKOFF_MS = 500L;
  private static final long DEFAULT_MAX_BACKOFF_MS = 30_000L;
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;

  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final Map<String, Long> createdAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> leaseEpochs = new ConcurrentHashMap<>();
  private final Map<String, Long> leaseExpiresAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> pinnedExecutors = new ConcurrentHashMap<>();
  private final Map<String, String> dedupeKeysByJobId = new ConcurrentHashMap<>();
  private final Map<String, String> activeJobIdByDedupeKey = new ConcurrentHashMap<>();
  private final Map<String, String> laneKeysByJobId = new ConcurrentHashMap<>();
  private final Map<String, String> activeJobIdByLaneKey = new ConcurrentHashMap<>();
  private final Map<String, Integer> attemptsByJobId = new ConcurrentHashMap<>();
  private final Map<String, Long> nextAttemptAtMs = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> ready = new ConcurrentLinkedQueue<>();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();
  private volatile int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private volatile long baseBackoffMs = DEFAULT_BASE_BACKOFF_MS;
  private volatile long maxBackoffMs = DEFAULT_MAX_BACKOFF_MS;
  private volatile long leaseMs = DEFAULT_LEASE_MS;
  private volatile long reclaimIntervalMs = DEFAULT_RECLAIM_INTERVAL_MS;
  private volatile long lastReclaimAtMs;

  public InMemoryReconcileJobStore() {
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
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    ReconcileJobKind effectiveJobKind = jobKind == null ? ReconcileJobKind.PLAN_CONNECTOR : jobKind;
    ReconcileTableTask effectiveTableTask =
        tableTask == null ? ReconcileTableTask.empty() : tableTask;
    ReconcileViewTask effectiveViewTask = viewTask == null ? ReconcileViewTask.empty() : viewTask;
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
    laneKeysByJobId.put(
        id,
        laneKey(
            connectorId, effectiveScope, effectiveJobKind, effectiveTableTask, effectiveViewTask));
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
            effectiveParentJobId);
    jobs.put(id, job);
    pinnedExecutors.put(id, effectivePinnedExecutorId);
    ready.add(id);
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
    return Optional.of(job);
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
    return new ReconcileJobPage(filtered.subList(offset, end), next);
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
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public QueueStats queueStats() {
    long queued = 0L;
    long running = 0L;
    long cancelling = 0L;
    long oldestQueued = 0L;
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
        }
        case "JS_RUNNING" -> running++;
        case "JS_CANCELLING" -> cancelling++;
        default -> {}
      }
    }
    return new QueueStats(queued, running, cancelling, oldestQueued);
  }

  @Override
  public Optional<LeasedJob> leaseNext(LeaseRequest request) {
    long now = System.currentTimeMillis();
    reclaimExpiredLeasesIfDue(now);
    LeaseRequest effective = request == null ? LeaseRequest.all() : request;
    int attempts = Math.max(1, ready.size());
    for (int i = 0; i < attempts; i++) {
      String jobId = ready.poll();
      if (jobId == null) {
        return Optional.empty();
      }

      var job = jobs.get(jobId);

      if (job == null) {
        continue;
      }

      if (!"JS_QUEUED".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
        continue;
      }

      if (nextAttemptAtMs.getOrDefault(jobId, 0L) > now) {
        ready.add(jobId);
        continue;
      }

      if (!effective.matches(
          job.executionPolicy, pinnedExecutors.getOrDefault(jobId, ""), job.jobKind)) {
        ready.add(jobId);
        continue;
      }

      String laneKey = laneKeysByJobId.getOrDefault(jobId, "");
      if (!laneKey.isBlank()) {
        String laneOwner = activeJobIdByLaneKey.get(laneKey);
        if (laneOwner != null && !laneOwner.equals(jobId) && hasLiveLaneLease(laneOwner, now)) {
          ready.add(jobId);
          continue;
        }
      }

      if (leased.add(jobId)) {
        String leaseEpoch = UUID.randomUUID().toString();
        leaseEpochs.put(jobId, leaseEpoch);
        leaseExpiresAtMs.put(jobId, now + leaseMs);
        if (!laneKey.isBlank()) {
          activeJobIdByLaneKey.put(laneKey, jobId);
        }
        jobs.computeIfPresent(
            jobId,
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
                    current.parentJobId));
        ReconcileJob leasedJob = jobs.get(jobId);
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
                pinnedExecutors.getOrDefault(jobId, ""),
                leasedJob.executorId,
                leasedJob.jobKind,
                leasedJob.tableTask,
                leasedJob.viewTask,
                leasedJob.parentJobId));
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
                job.parentJobId);
          }

          long now = System.currentTimeMillis();
          nextAttemptAtMs.put(id, now + backoffMs(attempts));
          ready.add(id);
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
            ready.add(id);
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
                job.parentJobId);
          }
          releaseLane(id);
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
    if ("JS_CANCELLED".equals(current.state)) {
      ready.remove(jobId);
    }
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
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          ready.remove(id);
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
            leaseEpochs.remove(id);
            leaseExpiresAtMs.remove(id);
            if ("JS_RUNNING".equals(job.state)) {
              nextAttemptAtMs.put(id, nowMs);
              ready.add(id);
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
                  job.parentJobId);
            }
            if ("JS_CANCELLING".equals(job.state)) {
              nextAttemptAtMs.put(id, nowMs);
              ready.add(id);
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

  private void clearDedupe(String jobId) {
    String dedupeKey = dedupeKeysByJobId.get(jobId);
    if (dedupeKey != null && !dedupeKey.isBlank()) {
      activeJobIdByDedupeKey.remove(dedupeKey, jobId);
    }
  }

  private long backoffMs(int attempts) {
    long base = baseBackoffMs * (1L << Math.min(8, Math.max(0, attempts - 1)));
    return Math.min(maxBackoffMs, base);
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
    if (jobKind == ReconcileJobKind.EXEC_TABLE
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
              List.of(), tableTask.destinationTableId(), effectiveScope.destinationStatsRequests());
    }
    if (jobKind == ReconcileJobKind.EXEC_VIEW
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
      if (effectiveScope.hasTableFilter() || effectiveScope.hasStatsRequestFilter()) {
        throw new IllegalArgumentException(
            "view task destinationViewId cannot be combined with table or stats scope");
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
      ReconcileViewTask viewTask) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    if (jobKind == ReconcileJobKind.EXEC_TABLE && tableTask != null) {
      return scope.destinationTableId() == null || scope.destinationTableId().isBlank()
          ? "tables|" + namespaces
          : "table|" + scope.destinationTableId();
    }
    if (jobKind == ReconcileJobKind.EXEC_VIEW && viewTask != null) {
      return scope.destinationViewId() == null || scope.destinationViewId().isBlank()
          ? "views|" + namespaces
          : "view|" + scope.destinationViewId();
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
      ReconcileExecutionPolicy executionPolicy,
      String parentJobId,
      String pinnedExecutorId) {
    String namespaces =
        scope.destinationNamespaceIds().stream().sorted().reduce((a, b) -> a + "," + b).orElse("*");
    String table = scope.destinationTableId() == null ? "*" : scope.destinationTableId();
    String statsRequests =
        scope.destinationStatsRequests().stream()
            .map(InMemoryReconcileJobStore::canonicalStatsRequest)
            .sorted()
            .reduce((a, b) -> a + "," + b)
            .orElse("");
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
                    ? CaptureMode.METADATA_AND_STATS.name()
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
            "scope.namespaces=" + namespaces,
            "scope.table=" + table,
            "scope.view=" + blankToEmpty(scope.destinationViewId()),
            "scope.stats_requests=" + statsRequests,
            "policy.execution_class=" + policy.executionClass().name(),
            "policy.lane=" + policy.lane(),
            "policy.attributes=" + canonicalAttributes(policy.attributes()),
            "parent_job_id=" + blankToEmpty(parentJobId),
            "pinned_executor_id=" + blankToEmpty(pinnedExecutorId));
    return hashValue(payload);
  }

  private static String canonicalStatsRequest(ReconcileScope.ScopedStatsRequest request) {
    return request.tableId()
        + "|"
        + request.snapshotId()
        + "|"
        + request.targetSpec()
        + "|"
        + String.join(",", request.columnSelectors());
  }

  private static String canonicalAttributes(Map<String, String> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return "";
    }
    return attributes.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> entry.getKey() + "=" + blankToEmpty(entry.getValue()))
        .reduce((a, b) -> a + "," + b)
        .orElse("");
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
