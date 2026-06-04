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

package ai.floedb.floecat.stats.spi.testing;

import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsPriorityClass;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerContext;
import ai.floedb.floecat.stats.spi.scheduler.SchedulerPriorityPolicy;

/** Deterministic test helper priority policy for scheduler unit/integration tests. */
public final class TestSchedulerPriorityPolicy implements SchedulerPriorityPolicy {
  private final StatsPriorityClass fixedClass;
  private final long fixedScore;

  private TestSchedulerPriorityPolicy(StatsPriorityClass fixedClass, long fixedScore) {
    this.fixedClass = fixedClass == null ? StatsPriorityClass.P3_BACKGROUND : fixedClass;
    this.fixedScore = Math.max(0L, fixedScore);
  }

  public static TestSchedulerPriorityPolicy alwaysP3() {
    return new TestSchedulerPriorityPolicy(StatsPriorityClass.P3_BACKGROUND, 0L);
  }

  public static TestSchedulerPriorityPolicy alwaysP1() {
    return new TestSchedulerPriorityPolicy(StatsPriorityClass.P1_FRESHNESS, 0L);
  }

  public static TestSchedulerPriorityPolicy fixed(StatsPriorityClass fixedClass, long fixedScore) {
    return new TestSchedulerPriorityPolicy(fixedClass, fixedScore);
  }

  @Override
  public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
    String laneKey = request.tableId().getAccountId() + ":" + request.tableId().getId();
    return new PriorityAssignment(fixedClass, fixedScore, laneKey);
  }
}
