package ai.floedb.metacat.reconciler.jobs;

import java.util.Optional;

public interface ReconcileJobStore {
  String enqueue(String tenantId, String connectorId, boolean fullRescan);
  Optional<ReconcileJob> get(String jobId);

  Optional<LeasedJob> leaseNext();
  void markRunning(String jobId, long startedAtMs);
  void markProgress(String jobId, long scanned, long changed, long errors, String message);
  void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed);
  void markFailed(String jobId, long finishedAtMs, String message, long scanned, long changed, long errors);

  final class ReconcileJob {
    public final String jobId;
    public final String tenantId;
    public final String connectorId;
    public final String state;
    public final String message;
    public final long startedAtMs;
    public final long finishedAtMs;
    public final long tablesScanned;
    public final long tablesChanged;
    public final long errors;
    public final boolean fullRescan;

    public ReconcileJob(
        String jobId, String tenantId,
        String connectorId, String state, String message,
        long startedAtMs, long finishedAtMs,
        long tablesScanned, long tablesChanged, long errors,
        boolean fullRescan) {
      this.jobId = jobId;
      this.tenantId = tenantId;
      this.connectorId = connectorId;
      this.state = state;
      this.message = message;
      this.startedAtMs = startedAtMs;
      this.finishedAtMs = finishedAtMs;
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.errors = errors;
      this.fullRescan = fullRescan;
    }
  }

  final class LeasedJob {
    public final String jobId;
    public final String tenantId;
    public final String connectorId;
    public final boolean fullRescan;

    public LeasedJob(String jobId, String tenantId, String connectorId, boolean fullRescan) {
      this.jobId = jobId;
      this.tenantId = tenantId;
      this.connectorId = connectorId;
      this.fullRescan = fullRescan;
    }
  }
}
