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

package ai.floedb.floecat.service.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * Pins the pool's construction contract. Every claim here was previously carried by the class
 * javadoc alone: daemon threads, a bounded queue, abort-on-full rather than silent discard, and
 * core threads that time out so a burst does not pin its peak worker count forever.
 */
class MetadataIoExecutorsTest {

  @Test
  void workersAreDaemonsSoTheyCannotHoldUpJvmExit() throws Exception {
    ThreadPoolExecutor pool = MetadataIoExecutors.newBoundedDaemonPool(1, 1, "test-md-io-");
    try {
      var observed = new AtomicReference<Thread>();
      var ran = new CountDownLatch(1);
      pool.execute(
          () -> {
            observed.set(Thread.currentThread());
            ran.countDown();
          });
      assertTrue(ran.await(5, TimeUnit.SECONDS));
      assertTrue(observed.get().isDaemon(), "a non-daemon worker would block JVM exit");
      assertTrue(
          observed.get().getName().startsWith("test-md-io-"),
          "the prefix is what identifies these threads in a dump: " + observed.get().getName());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void aFullQueueRejectsRatherThanSilentlyDiscarding() throws Exception {
    // The scheduler's only exits are a completion notification, cancellation, or an interrupt, so a
    // silently dropped task would park its caller forever. Rejection is the contract.
    ThreadPoolExecutor pool = MetadataIoExecutors.newBoundedDaemonPool(1, 1, "test-md-io-");
    var release = new CountDownLatch(1);
    var started = new CountDownLatch(1);
    try {
      pool.execute(
          () -> {
            started.countDown();
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      pool.execute(() -> {}); // fills the single queue slot
      assertThrows(RejectedExecutionException.class, () -> pool.execute(() -> {}));
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }

  @Test
  void idleWorkersAreReclaimedRatherThanPinnedAtPeak() {
    // Core threads are exempt from the keep-alive unless allowCoreThreadTimeOut is set, so without
    // it one burst to the ceiling holds that many platform threads for the life of the process.
    ThreadPoolExecutor pool = MetadataIoExecutors.newBoundedDaemonPool(4, 4, "test-md-io-");
    try {
      assertTrue(pool.allowsCoreThreadTimeOut(), "peak worker count would become steady state");
      assertTrue(
          pool.getKeepAliveTime(TimeUnit.SECONDS) > 0,
          "allowCoreThreadTimeOut requires a non-zero keep-alive");
      assertEquals(4, pool.getMaximumPoolSize());
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void nonPositiveSizesAreRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> MetadataIoExecutors.newBoundedDaemonPool(0, 1, "test-md-io-"));
    assertThrows(
        IllegalArgumentException.class,
        () -> MetadataIoExecutors.newBoundedDaemonPool(1, 0, "test-md-io-"));
  }
}
