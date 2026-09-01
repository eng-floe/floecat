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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.storage.spi.BlobStore;
import ai.floedb.floecat.storage.spi.PointerStore;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.time.Duration;

/**
 * The two counting stores as one thing, with the protocol for taking a measurement through them.
 *
 * <p>They are a pair everywhere they are used -- reset together, settled together, reported
 * together -- so passing both halves to three static methods was the shape asking to become an
 * object.
 *
 * <p>A bean rather than something each suite constructs, because {@link #assertWiredAndLive} needs
 * the stores the CONTAINER resolved -- which a hand-built object cannot ask for, and which is the
 * whole substance of that check. An alternative, selected by the same profile as the stores it
 * injects, so no other test deploys it.
 */
@Alternative
@Singleton
public final class StoreCostMeter {

  private final CountingPointerStore pointers;
  private final CountingBlobStore blobs;
  private final PointerStore wiredPointers;
  private final BlobStore wiredBlobs;

  @Inject
  StoreCostMeter(
      CountingPointerStore pointers,
      CountingBlobStore blobs,
      PointerStore wiredPointers,
      BlobStore wiredBlobs) {
    this.pointers = pointers;
    this.blobs = blobs;
    this.wiredPointers = wiredPointers;
    this.wiredBlobs = wiredBlobs;
  }

  /** How long to wait for a request's trailing store calls before giving up on quiescence. */
  private static final Duration SETTLE_DEADLINE = Duration.ofSeconds(10);

  /** How often to resample the counters while waiting. */
  private static final Duration SETTLE_POLL = Duration.ofMillis(100);

  /** Consecutive equal samples that count as settled; one gap can fall between a reader's ticks. */
  private static final int SETTLE_AGREEMENTS = 3;

  /**
   * Fails unless the service resolved these very instances, and unless the fixture read through
   * them.
   *
   * <p>Both halves are needed and neither is sufficient. A counting store the container did not
   * wire measures a path nothing takes; a wired store nothing ever called reports zero and looks
   * like a triumph. Called BEFORE the measured request, because the measured request is meant to
   * reach zero store reads as the cache work lands -- a liveness check on it would turn red exactly
   * when the design succeeds.
   */
  public void assertWiredAndLive() {
    assertSame(
        pointers,
        wiredPointers,
        "the counting pointer store must be the instance the service uses");
    assertSame(blobs, wiredBlobs, "the counting blob store must be too, or every blob number is 0");
    assertTrue(
        pointers.roundTrips() > 0,
        "building the fixture must have read the store, or nothing here is being measured");
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
    pointers.resetCounts();
    blobs.resetCounts();
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
    pointers.resetCounts();
    blobs.resetCounts();

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
      int kv = pointers.roundTrips();
      int s3 = blobs.roundTrips();
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
        return;
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
   * <p>Each store renders its own section: the counts it keeps are its business, and having this
   * reach in for them is what made a dozen accessors public that no test ever asserts on.
   */
  public String report(String header) {
    StringBuilder out = new StringBuilder();
    out.append("=== ").append(header).append(" ===\n");
    pointers.appendTo(out);
    blobs.appendTo(out);
    return out.toString();
  }
}
