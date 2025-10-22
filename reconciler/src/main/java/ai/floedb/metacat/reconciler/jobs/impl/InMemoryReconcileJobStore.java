package ai.floedb.metacat.reconciler.jobs.impl;

import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;
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
  public String enqueue(String tenantId, String connectorId, boolean fullRescan) {
    String id = UUID.randomUUID().toString();
    var job =
        new ReconcileJob(
            id,
            tenantId,
            connectorId,
            "JS_QUEUED",
            fullRescan ? "Queued (full)" : "Queued",
            0L,
            0L,
            0,
            0,
            0,
            fullRescan);
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
        return Optional.of(new LeasedJob(job.jobId, job.tenantId, job.connectorId, job.fullRescan));
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
                job.tenantId,
                job.connectorId,
                "JS_RUNNING",
                "Running",
                startedAtMs,
                0L,
                job.tablesScanned,
                job.tablesChanged,
                job.errors,
                job.fullRescan));
  }

  @Override
  public void markProgress(String jobId, long scanned, long changed, long errors, String message) {
    jobs.computeIfPresent(
        jobId,
        (id, job) ->
            new ReconcileJob(
                job.jobId,
                job.tenantId,
                job.connectorId,
                job.state,
                message == null ? (job.message == null ? "" : job.message) : message,
                job.startedAtMs,
                job.finishedAtMs,
                scanned,
                changed,
                errors,
                job.fullRescan));
  }

  @Override
  public void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed) {
    jobs.computeIfPresent(
        jobId,
        (id, job) -> {
          leased.remove(id);
          return new ReconcileJob(
              job.jobId,
              job.tenantId,
              job.connectorId,
              "JS_SUCCEEDED",
              "Succeeded",
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              scanned,
              changed,
              job.errors,
              job.fullRescan);
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
              job.tenantId,
              job.connectorId,
              "JS_FAILED",
              message == null ? "Failed" : message,
              job.startedAtMs == 0 ? finishedAtMs : job.startedAtMs,
              finishedAtMs,
              scanned,
              changed,
              errors,
              job.fullRescan);
        });
  }
}
