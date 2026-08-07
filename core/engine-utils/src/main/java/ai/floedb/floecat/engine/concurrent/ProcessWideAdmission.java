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
package ai.floedb.floecat.engine.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns admission permits that must survive a Quarkus runtime-classloader restart.
 *
 * <p>This is deliberately a normal runtime dependency, rather than application code. Quarkus loads
 * such dependencies from its persistent base runtime classloader, so this state continues to
 * account for a previous lifecycle's store calls while a replacement application classloader starts
 * accepting work.
 */
public final class ProcessWideAdmission {

  /** The fixed-capacity semaphore that every overlapping lifecycle must share. */
  public record State(int capacity, Semaphore permits) {}

  private static final AtomicReference<State> CURRENT = new AtomicReference<>();

  private ProcessWideAdmission() {}

  /**
   * Return the shared semaphore for {@code capacity}.
   *
   * <p>The first runtime in a JVM establishes the ceiling. Later runtime generations deliberately
   * retain it even if their configuration differs: replacing the semaphore could let a caller that
   * already resolved the old gate admit work outside the new gate. A full JVM restart is required
   * to apply a changed ceiling.
   */
  public static State resolve(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("process-wide metadata-I/O capacity must be positive");
    }
    while (true) {
      State current = CURRENT.get();
      if (current != null) {
        return current;
      }
      State fresh = new State(capacity, new Semaphore(capacity));
      if (CURRENT.compareAndSet(current, fresh)) {
        return fresh;
      }
    }
  }

  /**
   * Forget a gate after its owning runtime has stopped completely.
   *
   * <p>The owner calls this only after it has prevented every new admission and its executor has
   * terminated. Checking identity keeps a stale runtime from clearing a gate installed by a later
   * lifecycle.
   */
  public static void clearIfIdle(Semaphore permits) {
    State current = CURRENT.get();
    if (current != null
        && current.permits() == permits
        && current.permits().availablePermits() == current.capacity()) {
      CURRENT.compareAndSet(current, null);
    }
  }
}
