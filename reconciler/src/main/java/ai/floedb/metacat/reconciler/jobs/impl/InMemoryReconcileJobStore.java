package ai.floedb.metacat.reconciler.jobs.impl;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import jakarta.enterprise.context.ApplicationScoped;

import ai.floedb.metacat.reconciler.jobs.ReconcileJobStore;

@ApplicationScoped
public class InMemoryReconcileJobStore implements ReconcileJobStore {

  private final Map<String, ReconcileJob> jobs = new ConcurrentHashMap<>();
  private final ConcurrentLinkedQueue<String> ready = new ConcurrentLinkedQueue<>();
  private final Set<String> leased = ConcurrentHashMap.newKeySet();

  @Override
  public String enqueue(String tenantId, String connectorId, boolean fullRescan) {
    String id = UUID.randomUUID().toString();
    long now = System.currentTimeMillis();
    var job = new ReconcileJob(
        id, tenantId, connectorId,
        "JS_QUEUED",
        fullRescan ? "Queued (full)" : "Queued",
        0L, 0L,
        0, 0, 0,
        fullRescan
    );
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
    for (;;) {
      String id = ready.poll();
      if (id == null) return Optional.empty();

      var j = jobs.get(id);
      if (j == null) continue;
      if (!"JS_QUEUED".equals(j.state)) continue;

      if (leased.add(id)) {
        return Optional.of(new LeasedJob(j.jobId, j.tenantId, j.connectorId, j.fullRescan));
      }
    }
  }

  @Override
  public void markRunning(String jobId, long startedAtMs) {
    jobs.computeIfPresent(jobId, (id, j) ->
        new ReconcileJob(
            j.jobId, j.tenantId, j.connectorId,
            "JS_RUNNING",
            "Running",
            startedAtMs, 0L,
            j.tablesScanned, j.tablesChanged, j.errors,
            j.fullRescan
        ));
  }

  @Override
  public void markProgress(String jobId, long scanned, long changed, long errors, String message) {
    jobs.computeIfPresent(jobId, (id, j) ->
        new ReconcileJob(
            j.jobId, j.tenantId, j.connectorId,
            j.state, message == null ? (j.message == null ? "" : j.message) : message,
            j.startedAtMs, j.finishedAtMs,
            scanned, changed, errors,
            j.fullRescan
        ));
  }

  @Override
  public void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed) {
    jobs.computeIfPresent(jobId, (id, j) -> {
      leased.remove(id);
      return new ReconcileJob(
          j.jobId, j.tenantId, j.connectorId,
          "JS_SUCCEEDED",
          "Succeeded",
          j.startedAtMs == 0 ? finishedAtMs : j.startedAtMs,
          finishedAtMs,
          scanned, changed, j.errors,
          j.fullRescan
      );
    });
  }

  @Override
  public void markFailed(String jobId, long finishedAtMs, String message,
                         long scanned, long changed, long errors) {
    jobs.computeIfPresent(jobId, (id, j) -> {
      leased.remove(id);
      return new ReconcileJob(
          j.jobId, j.tenantId, j.connectorId,
          "JS_FAILED",
          message == null ? "Failed" : message,
          j.startedAtMs == 0 ? finishedAtMs : j.startedAtMs,
          finishedAtMs,
          scanned, changed, errors,
          j.fullRescan
      );
    });
  }
}
