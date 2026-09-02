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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.repo.util.BaseResourceRepository;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * What a fenced write's retry loop promises its callers.
 *
 * <p>It exists so a lost fence is answered where it happened. An idempotent create keeps its
 * PENDING reservation on a retryable failure and {@code runOnce} cannot reclaim one, so letting
 * contention unwind through that layer wedges the idempotency key until its TTL.
 *
 * <p>Which makes the loop's own contract load-bearing in a way a passing service test would not
 * show: the body has to run again on every kind of loss, and it has to give up rather than spin.
 */
class FenceRetryLoopTest {

  @Test
  void aWriteThatWinsFirstTimeRunsExactlyOnce() {
    var attempts = new AtomicInteger();
    FenceRetry.retryWhileFenceLost(
        "write",
        () -> {
          attempts.incrementAndGet();
          return true;
        });
    assertEquals(1, attempts.get());
  }

  @Test
  void aLostFenceRunsTheWholeBodyAgain() {
    // Not just the write: the body re-resolves the fence AND re-asserts what the fence protects,
    // because whoever won it may have deleted the namespace this write is joining.
    var attempts = new AtomicInteger();
    FenceRetry.retryWhileFenceLost("write", () -> attempts.incrementAndGet() >= 3);
    assertEquals(3, attempts.get());
  }

  @Test
  void aFenceLostByThrowingIsRetriedLikeOneLostByReturningFalse() {
    // Resolving a fence can find the parent it was going to fence on already deleted, which throws
    // rather than returning false. Same condition, so it must not escape the loop that exists to
    // keep it from unwinding through the idempotency layer.
    var attempts = new AtomicInteger();
    FenceRetry.retryWhileFenceLost(
        "write",
        () -> {
          if (attempts.incrementAndGet() < 3) {
            throw new BaseResourceRepository.AbortRetryableException("parent vanished");
          }
          return true;
        });
    assertEquals(3, attempts.get());
  }

  @Test
  void aWriteThatKeepsLosingGivesUpRetryablyRatherThanSpinning() {
    var attempts = new AtomicInteger();
    var thrown =
        assertThrows(
            BaseResourceRepository.AbortRetryableException.class,
            () ->
                FenceRetry.retryWhileFenceLost(
                    "create table",
                    () -> {
                      attempts.incrementAndGet();
                      return false;
                    }));
    assertTrue(thrown.getMessage().contains("create table"), "names the operation that gave up");
    assertTrue(attempts.get() > 1, "retried before giving up");
    assertTrue(attempts.get() <= BaseServiceImpl.RETRIES + 1, "bounded by the shared retry budget");
  }

  @Test
  void anErrorThatIsNotAFenceLossPropagatesImmediately() {
    var attempts = new AtomicInteger();
    assertThrows(
        IllegalStateException.class,
        () ->
            FenceRetry.retryWhileFenceLost(
                "write",
                () -> {
                  attempts.incrementAndGet();
                  throw new IllegalStateException("not a fence problem");
                }));
    assertEquals(1, attempts.get(), "only a lost fence is retried here");
  }
}
