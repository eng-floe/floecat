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

package ai.floedb.floecat.service.testsupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;

/**
 * The detailed test observer plus the protocol for taking a stable measurement through it.
 *
 * <p>A bean rather than something each suite constructs because it must share the observer selected
 * under the deployed store decorators. Seeing fixture reads is the proof that those decorators and
 * this recorder are on the service's real store path.
 */
@Alternative
@Singleton
public final class StoreCostMeter {

  private final RecordingStoreReadObserver reads;

  @Inject
  StoreCostMeter(RecordingStoreReadObserver reads) {
    this.reads = reads;
  }

  /** How long to wait for a request's trailing store calls before giving up on quiescence. */
  private static final Duration SETTLE_DEADLINE = Duration.ofSeconds(10);

  /** How often to resample the counters while waiting. */
  private static final Duration SETTLE_POLL = Duration.ofMillis(100);

  /** Consecutive equal samples that count as settled; one gap can fall between a reader's ticks. */
  private static final int SETTLE_AGREEMENTS = 3;

  /**
   * Fails unless the fixture read both stores through the shared deployed decorators.
   *
   * <p>Called BEFORE the measured request because that request is meant to reach zero reads as
   * cache work lands; a liveness check on it would turn red exactly when the design succeeds.
   */
  public void assertWiredAndLive() {
    assertTrue(
        reads.pointerRoundTrips() > 0,
        "building the fixture must have read the pointer store through its decorator");
    assertTrue(
        reads.blobRoundTrips() > 0,
        "building the fixture must have read the blob store through its decorator");
  }

  /**
   * Discards whatever the previous test left on the counters.
   *
   * <p>Called before the fixture is built, so the liveness half of {@link #assertWiredAndLive} is
   * checking that THIS test's fixture read the stores. The counters are otherwise reset only inside
   * {@link #measure}, which would leave that half satisfied from the second test onwards by the
   * previous test's measured window -- and a guard against reading nothing must not be answered by
   * someone else's reads.
   */
  public void resetBetweenTests() {
    reads.resetCounts();
  }

  /**
   * Runs {@code body} with the counters covering it and nothing else.
   *
   * <p>Four steps, and the order of the first two is the whole point. Settling BEFORE the reset
   * lets the previous request's trailing store calls land and be discarded: a request fans calls
   * out to a pool and some of them outrun the response, so resetting the moment it returns bills
   * its tail to the measurement that follows. Settling after is what makes the measured request's
   * own tail count.
   */
  public void measure(Runnable body) {
    settle();
    reads.resetCounts();

    body.run();
    settle();
  }

  /**
   * Waits until both stores stop being read.
   *
   * <p>Each store's own count has to hold: summing them would let one rise while the other falls
   * and read as quiescence, and they are not the same unit to begin with. Several consecutive
   * samples, not two -- one quiet gap can fall between the ticks of a periodic reader. Returns
   * early once the counts hold, or when the wait budget runs out, so a reader that never stops
   * shows up as a moving number rather than a hang.
   */
  private void settle() {
    long deadline = System.nanoTime() + SETTLE_DEADLINE.toNanos();
    int previousKv = -1;
    int previousS3 = -1;
    int agreements = 0;
    while (System.nanoTime() - deadline < 0) {
      // Round trips on both sides: this is a change detector, and adding a listing to an object
      // count -- the thing this harness forbids everywhere else -- would be doing it in the file
      // that states the rule.
      int kv = reads.pointerRoundTrips();
      int s3 = reads.blobRoundTrips();
      agreements = (kv == previousKv && s3 == previousS3) ? agreements + 1 : 0;
      if (agreements >= SETTLE_AGREEMENTS) {
        return;
      }
      previousKv = kv;
      previousS3 = s3;
      try {
        Thread.sleep(SETTLE_POLL.toMillis());
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new AssertionError(
            "settling was interrupted before the store-read window became stable", interrupted);
      }
    }
    throw new AssertionError(
        "the stores never went quiet within "
            + SETTLE_DEADLINE
            + ": something is still reading, and any cost measured now is that reader's, not the"
            + " request's");
  }

  /**
   * What the stores counted, as a block a failing run carries with it.
   *
   * <p>The observer renders both sections so the protocol does not reach into its raw counters.
   */
  public String report(String header) {
    StringBuilder out = new StringBuilder();
    out.append("=== ").append(header).append(" ===\n");
    reads.appendTo(out);
    return out.toString();
  }
}
