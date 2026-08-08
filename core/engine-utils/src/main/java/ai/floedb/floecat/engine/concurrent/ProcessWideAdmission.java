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

/**
 * Owns admission permits that must survive a Quarkus runtime-classloader restart.
 *
 * <p>The gate is stored in the JVM's system-properties table, not a class static. Quarkus can load
 * any application dependency in a reloadable runtime classloader, so a static here would silently
 * mint a second semaphore on dev-mode reload. The stored values are JDK types only, which makes
 * them safely readable from successive application classloaders without retaining one of them.
 */
public final class ProcessWideAdmission {

  /** The fixed-capacity semaphore that every overlapping lifecycle must share. */
  public record State(int capacity, Semaphore permits) {}

  private static final String CAPACITY_KEY =
      "ai.floedb.floecat.metadata-io.process-wide-admission.capacity";
  private static final String PERMITS_KEY =
      "ai.floedb.floecat.metadata-io.process-wide-admission.permits";

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
    synchronized (System.getProperties()) {
      Object storedCapacity = System.getProperties().get(CAPACITY_KEY);
      Object storedPermits = System.getProperties().get(PERMITS_KEY);
      if (storedCapacity == null && storedPermits == null) {
        Semaphore permits = new Semaphore(capacity);
        System.getProperties().put(CAPACITY_KEY, capacity);
        System.getProperties().put(PERMITS_KEY, permits);
        return new State(capacity, permits);
      }
      if (!(storedCapacity instanceof Integer currentCapacity)
          || !(storedPermits instanceof Semaphore currentPermits)) {
        throw new IllegalStateException("process-wide metadata-I/O admission state is invalid");
      }
      return new State(currentCapacity, currentPermits);
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
    synchronized (System.getProperties()) {
      Object storedCapacity = System.getProperties().get(CAPACITY_KEY);
      Object storedPermits = System.getProperties().get(PERMITS_KEY);
      if (storedCapacity instanceof Integer capacity
          && storedPermits == permits
          && permits.availablePermits() == capacity) {
        System.getProperties().remove(CAPACITY_KEY);
        System.getProperties().remove(PERMITS_KEY);
      }
    }
  }
}
