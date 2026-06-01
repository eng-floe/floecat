package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.ReconcileJobKind;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;

public interface SchedulerPriorityPolicy {
  PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context);

  default PriorityAssignment assignForReconcileJob(
      ReconcileJobKind kind,
      String tableId,
      long snapshotId,
      boolean isNewSnapshot,
      SchedulerContext context) {
    StatsPriorityClass cls =
        isNewSnapshot ? StatsPriorityClass.P1_FRESHNESS : StatsPriorityClass.P3_BACKGROUND;
    return new PriorityAssignment(cls, 0L, tableId);
  }

  record PriorityAssignment(StatsPriorityClass priorityClass, long score, String laneKey) {
    public PriorityAssignment {
      if (priorityClass == null) priorityClass = StatsPriorityClass.P3_BACKGROUND;
      score = Math.max(0L, score);
      laneKey = laneKey == null ? "" : laneKey.trim();
    }
  }
}
