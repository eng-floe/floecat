package ai.floedb.floecat.service.statistics.scheduler;

import ai.floedb.floecat.reconciler.jobs.CoverageLevel;
import ai.floedb.floecat.reconciler.jobs.SchedulerHealthBand;
import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import java.util.Map;
import java.util.OptionalLong;

public interface SchedulerContext {
  SchedulerHealthBand currentBand();

  Map<StatsPriorityClass, Long> queueDepthByClass();

  OptionalLong lastSuccessfulCaptureMs(String tableId);

  CoverageLevel coverageLevel(String tableId, long snapshotId);

  OptionalLong snapshotDeltaRows(String tableId, long snapshotId);

  default long recentPlannerRequestCount(String tableId) {
    return 0L;
  }

  default long recentColumnRequestCount(String tableId, String normalizedColumnSelector) {
    return 0L;
  }
}
