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
import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import io.quarkus.arc.properties.IfBuildProperty;
import jakarta.enterprise.context.ApplicationScoped;
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
  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final Map<String, Long> createdAtMs = new ConcurrentHashMap<>();
  private final Map<String, String> leaseEpochs = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> ready = new ConcurrentLinkedQueue<>();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();

  @Override
  public String enqueue(
      String accountId,
      String connectorId,
      boolean fullRescan,
      CaptureMode captureMode,
      ReconcileScope scope) {
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
            fullRescan,
            captureMode,
            0,
            0,
            scope);
    jobs.put(id, job);
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
  public Optional<LeasedJob> leaseNext() {
    for (; ; ) {
      String jobId = ready.poll();
      if (jobId == null) {
        return Optional.empty();
      }

      var job = jobs.get(jobId);

      if (job == null) {
        continue;
      }

      if (!"JS_QUEUED".equals(job.state)) {
        continue;
      }

      if (leased.add(jobId)) {
        String leaseEpoch = UUID.randomUUID().toString();
        leaseEpochs.put(jobId, leaseEpoch);
        return Optional.of(
            new LeasedJob(
                job.jobId,
                job.accountId,
                job.connectorId,
                job.fullRescan,
                job.captureMode,
                job.scope,
                leaseEpoch));
      }
    }
  }

  @Override
  public boolean renewLease(String jobId, String leaseEpoch) {
    var job = jobs.get(jobId);
    if (job == null) {
      return false;
    }
    if (!"JS_RUNNING".equals(job.state) && !"JS_CANCELLING".equals(job.state)) {
      return false;
    }
    return hasActiveLease(jobId, leaseEpoch);
  }

  @Override
  public void markRunning(String jobId, String leaseEpoch, long startedAtMs) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          if (!hasActiveLease(id, leaseEpoch)) {
            return job;
          }
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_RUNNING",
              "Running",
              startedAtMs,
              0L,
              job.tablesScanned,
              job.tablesChanged,
              job.errors,
              job.fullRescan,
              job.captureMode,
              job.snapshotsProcessed,
              job.statsProcessed,
              job.scope);
        });
  }

  @Override
  public void markProgress(
      String jobId,
      String leaseEpoch,
      long scanned,
      long changed,
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
              scanned,
              changed,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope);
        });
  }

  @Override
  public void markSucceeded(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      long scanned,
      long changed,
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
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_SUCCEEDED",
              "Succeeded",
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              scanned,
              changed,
              job.errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope);
        });
  }

  @Override
  public void markFailed(
      String jobId,
      String leaseEpoch,
      long finishedAtMs,
      String message,
      long scanned,
      long changed,
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
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_FAILED",
              message == null ? "Failed" : message,
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              scanned,
              changed,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope);
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
              || "JS_CANCELLED".equals(job.state)) {
            return job;
          }
          if ("JS_RUNNING".equals(job.state)) {
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
                job.errors,
                job.fullRescan,
                job.captureMode,
                job.snapshotsProcessed,
                job.statsProcessed,
                job.scope);
          }
          leased.remove(id);
          leaseEpochs.remove(id);
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
              job.errors,
              job.fullRescan,
              job.captureMode,
              job.snapshotsProcessed,
              job.statsProcessed,
              job.scope);
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
      long scanned,
      long changed,
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
          ready.remove(id);
          return new ReconcileJob(
              job.jobId,
              job.accountId,
              job.connectorId,
              "JS_CANCELLED",
              message == null || message.isBlank() ? "Cancelled" : message,
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              scanned,
              changed,
              errors,
              job.fullRescan,
              job.captureMode,
              snapshotsProcessed,
              statsProcessed,
              job.scope);
        });
  }

  private boolean hasActiveLease(String jobId, String leaseEpoch) {
    String expected = leaseEpochs.get(jobId);
    return expected != null && !expected.isBlank() && expected.equals(leaseEpoch);
  }
}
