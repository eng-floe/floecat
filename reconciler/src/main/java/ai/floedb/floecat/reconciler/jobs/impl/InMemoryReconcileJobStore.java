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

import ai.floedb.floecat.reconciler.jobs.ReconcileJobStore;
import ai.floedb.floecat.reconciler.jobs.ReconcileScope;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@ApplicationScoped
public class InMemoryReconcileJobStore implements ReconcileJobStore {
  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> ready = new ConcurrentLinkedQueue<>();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();

  @Override
  public String enqueue(
      String accountId, String connectorId, boolean fullRescan, ReconcileScope scope) {
    String id = UUID.randomUUID().toString();
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
            scope);
    jobs.put(id, job);
    ready.add(id);
    return id;
  }

  @Override
  public Optional<ReconcileJob> get(String jobId) {
    return Optional.ofNullable(jobs.get(jobId));
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
        return Optional.of(
            new LeasedJob(job.jobId, job.accountId, job.connectorId, job.fullRescan, job.scope));
      }
    }
  }

  @Override
  public void markRunning(String jobId, long startedAtMs) {
    jobs.computeIfPresent(
        jobId,
        (id, job) ->
            new ReconcileJob(
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
                job.scope));
  }

  @Override
  public void markProgress(String jobId, long scanned, long changed, long errors, String message) {
    jobs.computeIfPresent(
        jobId,
        (id, job) ->
            new ReconcileJob(
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
                job.scope));
  }

  @Override
  public void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          leased.remove(id);
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
              job.scope);
        });
  }

  @Override
  public void markFailed(
      String jobId, long finishedAtMs, String message, long scanned, long changed, long errors) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          leased.remove(id);
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
              job.scope);
        });
  }
}
