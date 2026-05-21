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

package ai.floedb.floecat.service.statistics.scheduler.testing;

import ai.floedb.floecat.reconciler.jobs.StatsPriorityClass;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerContext;
import ai.floedb.floecat.service.statistics.scheduler.SchedulerPriorityPolicy;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import java.util.Objects;

/**
 * Deterministic test helper for scheduler priority policy tests.
 *
 * <p>Returns a fixed {@link StatsPriorityClass} and score for every assignment, regardless of the
 * request or scheduler context. Useful for unit-testing components that consume {@link
 * SchedulerPriorityPolicy} without needing a full CDI container or a real scoring implementation.
 *
 * <p>This helper is intentionally simple. It is useful for OSS contributors who want to test
 * priority-aware enqueue paths without implementing a full policy bean.
 *
 * <pre>{@code
 * // Use a fixed P3 policy in a unit test:
 * SchedulerPriorityPolicy policy = TestSchedulerPriorityPolicy.alwaysP3();
 * PriorityAssignment assignment = policy.assign(request, context);
 * assertThat(assignment.priorityClass()).isEqualTo(StatsPriorityClass.P3_BACKGROUND);
 * }</pre>
 */
public final class TestSchedulerPriorityPolicy implements SchedulerPriorityPolicy {

  private final StatsPriorityClass fixedClass;
  private final long fixedScore;

  /**
   * Creates a policy that always assigns the given class and score.
   *
   * @param fixedClass the priority class to return for every assignment; must not be {@code null}
   *     and must not be {@link StatsPriorityClass#P0_SYNC} (reserved for the sync-capture path)
   * @param fixedScore the score to return for every assignment; clamped to ≥ 0
   */
  public TestSchedulerPriorityPolicy(StatsPriorityClass fixedClass, long fixedScore) {
    this.fixedClass = Objects.requireNonNull(fixedClass, "fixedClass");
    this.fixedScore = Math.max(0L, fixedScore);
  }

  /**
   * Returns a policy that always assigns {@link StatsPriorityClass#P3_BACKGROUND} with score 0.
   * Suitable as a safe no-op default in tests that do not care about priority ordering.
   */
  public static TestSchedulerPriorityPolicy alwaysP3() {
    return new TestSchedulerPriorityPolicy(StatsPriorityClass.P3_BACKGROUND, 0L);
  }

  /**
   * Returns a policy that always assigns {@link StatsPriorityClass#P1_FRESHNESS} with score 0.
   * Suitable for tests that verify new-snapshot or freshness-priority code paths.
   */
  public static TestSchedulerPriorityPolicy alwaysP1() {
    return new TestSchedulerPriorityPolicy(StatsPriorityClass.P1_FRESHNESS, 0L);
  }

  /**
   * Returns a policy that always assigns {@link StatsPriorityClass#P2_REPAIR} with score 0.
   * Suitable for tests that exercise follow-up repair enqueue paths.
   */
  public static TestSchedulerPriorityPolicy alwaysP2() {
    return new TestSchedulerPriorityPolicy(StatsPriorityClass.P2_REPAIR, 0L);
  }

  /**
   * Returns a policy that always assigns the given class with a fixed score. Convenience factory
   * for tests that need a specific (class, score) pair.
   */
  public static TestSchedulerPriorityPolicy of(StatsPriorityClass cls, long score) {
    return new TestSchedulerPriorityPolicy(cls, score);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Ignores {@code request} and {@code context}; always returns the fixed class and score. The
   * lane key is derived from the request's table ID as {@code "accountId:tableId"}.
   */
  @Override
  public PriorityAssignment assign(StatsCaptureRequest request, SchedulerContext context) {
    String laneKey = request.tableId().getAccountId() + ":" + request.tableId().getId();
    return new PriorityAssignment(fixedClass, fixedScore, laneKey);
  }

  /** Returns the fixed priority class this policy always assigns. */
  public StatsPriorityClass fixedClass() {
    return fixedClass;
  }

  /** Returns the fixed score this policy always assigns. */
  public long fixedScore() {
    return fixedScore;
  }
}
