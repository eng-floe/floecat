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
  private static final long DEFAULT_LEASE_MS = 30_000L;
  private static final long DEFAULT_RECLAIM_INTERVAL_MS = 5_000L;
  private static final long CANCEL_POKE_MAX_DELAY_MS = 1_000L;

  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final Map<String, Long> createdAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> leaseEpochs = new ConcurrentHashMap<>();
  private final Map<String, Long> leaseExpiresAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> pinnedExecutors = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> ready = new ConcurrentLinkedQueue<>();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();
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
    leaseMs = Math.max(1_000L, readLong("floecat.reconciler.job-store.lease-ms", DEFAULT_LEASE_MS));
    reclaimIntervalMs =
        Math.max(
            1_000L,
            readLong(
                "floecat.reconciler.job-store.reclaim-interval-ms", DEFAULT_RECLAIM_INTERVAL_MS));
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
    String id = UUID.randomUUID().toString();
    createdAtMs.put(id, System.currentTimeMillis());
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
            scope,
            executionPolicy,
            "",
            jobKind,
            tableTask,
            viewTask,
            parentJobId);
    jobs.put(id, job);
    pinnedExecutors.put(id, pinnedExecutorId == null ? "" : pinnedExecutorId.trim());
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
    reclaimExpiredLeasesIfDue(System.currentTimeMillis());
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

      if (!effective.matches(
          job.executionPolicy, pinnedExecutors.getOrDefault(jobId, ""), job.jobKind)) {
        ready.add(jobId);
        continue;
      }

      if (leased.add(jobId)) {
        long now = System.currentTimeMillis();
        String leaseEpoch = UUID.randomUUID().toString();
        leaseEpochs.put(jobId, leaseEpoch);
        leaseExpiresAtMs.put(jobId, now + leaseMs);
        return Optional.of(
            new LeasedJob(
                job.jobId,
                job.accountId,
                job.connectorId,
                job.fullRescan,
                job.captureMode,
                job.scope,
                job.executionPolicy,
                leaseEpoch,
                pinnedExecutors.getOrDefault(jobId, ""),
                job.executorId,
                job.jobKind,
                job.tableTask,
                job.viewTask,
                job.parentJobId));
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
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              cancelling ? "JS_CANCELLING" : "JS_RUNNING",
              cancelling ? job.message : "Running",
              startedAtMs,
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
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          pinnedExecutors.remove(id);
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
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          pinnedExecutors.remove(id);
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
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          pinnedExecutors.remove(id);
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
          leased.remove(id);
          leaseEpochs.remove(id);
          leaseExpiresAtMs.remove(id);
          ready.remove(id);
          pinnedExecutors.remove(id);
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
            leaseEpochs.remove(id);
            leaseExpiresAtMs.remove(id);
            if ("JS_RUNNING".equals(job.state)) {
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
}
