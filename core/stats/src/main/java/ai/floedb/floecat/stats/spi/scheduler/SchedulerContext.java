package ai.floedb.floecat.stats.spi.scheduler;

import ai.floedb.floecat.stats.spi.CoverageLevel;
import ai.floedb.floecat.stats.spi.SchedulerHealthBand;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
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
