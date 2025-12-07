package ai.floedb.floecat.reconciler.jobs;

import java.util.Optional;

public interface ReconcileJobStore {
  String enqueue(String accountId, String connectorId, boolean fullRescan, ReconcileScope scope);

  Optional<ReconcileJob> get(String jobId);

  Optional<LeasedJob> leaseNext();

  void markRunning(String jobId, long startedAtMs);

  void markProgress(String jobId, long scanned, long changed, long errors, String message);

  void markSucceeded(String jobId, long finishedAtMs, long scanned, long changed);

  void markFailed(
      String jobId, long finishedAtMs, String message, long scanned, long changed, long errors);

  final class ReconcileJob {
    public final String jobId;
    public final String accountId;
    public final String connectorId;
    public final String state;
    public final String message;
    public final long startedAtMs;
    public final long finishedAtMs;
    public final long tablesScanned;
    public final long tablesChanged;
    public final long errors;
    public final boolean fullRescan;
    public final ReconcileScope scope;

    public ReconcileJob(
        String jobId,
        String accountId,
        String connectorId,
        String state,
        String message,
        long startedAtMs,
        long finishedAtMs,
        long tablesScanned,
        long tablesChanged,
        long errors,
        boolean fullRescan,
        ReconcileScope scope) {
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.state = state;
      this.message = message;
      this.startedAtMs = startedAtMs;
      this.finishedAtMs = finishedAtMs;
      this.tablesScanned = tablesScanned;
      this.tablesChanged = tablesChanged;
      this.errors = errors;
      this.fullRescan = fullRescan;
      this.scope = scope == null ? ReconcileScope.empty() : scope;
    }
  }

  final class LeasedJob {
    public final String jobId;
    public final String accountId;
    public final String connectorId;
    public final boolean fullRescan;
    public final ReconcileScope scope;

    public LeasedJob(
        String jobId,
        String accountId,
        String connectorId,
        boolean fullRescan,
        ReconcileScope scope) {
      this.jobId = jobId;
      this.accountId = accountId;
      this.connectorId = connectorId;
      this.fullRescan = fullRescan;
      this.scope = scope == null ? ReconcileScope.empty() : scope;
    }
  }
}
