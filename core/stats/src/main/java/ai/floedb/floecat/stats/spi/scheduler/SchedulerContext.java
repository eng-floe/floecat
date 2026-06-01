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
