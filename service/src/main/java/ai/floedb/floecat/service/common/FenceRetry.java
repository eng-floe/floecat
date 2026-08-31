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
package ai.floedb.floecat.service.common;

import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * The one retry policy for a write that lost a fence.
 *
 * <p>Lives apart from {@link BaseServiceImpl} because the writers that take these fences are not
 * all services: bootstrap seeding is a concurrent writer against a user's request on any restart,
 * and it has no service to inherit a policy from. A second copy of the numbers is how the two drift
 * apart.
 *
 * <p>A lost fence is not an error. It means another writer changed the shape this write asserted,
 * so the answer is to re-sample and try again -- bounded, because a caller that never wins must
 * eventually surface the contention rather than block forever.
 */
public final class FenceRetry {

  static final Duration BACKOFF_MIN = Duration.ofMillis(5);
  static final Duration BACKOFF_MAX = Duration.ofMillis(200);
  static final double JITTER = 0.5;
  static final int RETRIES = 8;

  private FenceRetry() {}

  /**
   * Runs {@code fencedWrite} until it wins its fence, or the budget is spent.
   *
   * @param what named in the abort when the budget is spent
   * @param fencedWrite re-samples its own conditions per attempt and returns whether it committed
   */
  public static void retryWhileFenceLost(String what, BooleanSupplier fencedWrite) {
    retryWhileFenceLost(
        what, () -> fencedWrite.getAsBoolean() ? Optional.of(Boolean.TRUE) : Optional.empty());
  }

  /**
   * Runs a value-producing fenced write until it commits and returns its exact result.
   *
   * @param what named in the abort when the budget is spent
   * @param fencedWrite re-samples its own conditions and returns the committed result, or empty
   *     when it lost the fence
   */
  public static <T> T retryWhileFenceLost(String what, Supplier<Optional<T>> fencedWrite) {
    for (int attempt = 1; ; attempt++) {
      Optional<T> committed;
      try {
        committed = fencedWrite.get();
      } catch (BaseResourceRepository.AbortRetryableException lost) {
        if (attempt > RETRIES) {
          throw lost;
        }
        sleepBackoff(attempt);
        continue;
      }
      if (committed.isPresent()) {
        return committed.get();
      }
      if (attempt > RETRIES) {
        throw BaseResourceRepository.AbortRetryableException.lostFence(
            what + " after " + attempt + " attempts");
      }
      sleepBackoff(attempt);
    }
  }

  /** Exponential backoff with jitter, capped at {@link #BACKOFF_MAX}. */
  public static void sleepBackoff(int attempts) {
    long baseMs = BACKOFF_MIN.toMillis();
    long maxMs = BACKOFF_MAX.toMillis();
    long delayMs = Math.max(1L, baseMs);
    int steps = Math.min(attempts, 10);
    for (int i = 0; i < steps && delayMs < maxMs; i++) {
      delayMs = Math.min(maxMs, delayMs * 2L);
    }
    double jitter = 1.0 + ((ThreadLocalRandom.current().nextDouble() * 2.0 - 1.0) * JITTER);
    long sleepMs = Math.max(1L, (long) (delayMs * jitter));
    try {
      Thread.sleep(sleepMs);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("idempotent retry backoff interrupted", ie);
    }
  }
}
