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
 * Owns the metadata-I/O admission semaphore shared by every application lifecycle in this JVM.
 *
 * <p>This class belongs to the JDK-only {@code floecat-core-engine-utils} artifact. Floecat's
 * Quarkus configuration marks that artifact parent-first, so the holder is loaded once by the
 * persistent parent runtime classloader rather than once per reloadable application generation. Its
 * static state can therefore safely hold ordinary objects without using a configuration API as an
 * object registry.
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
      State candidate = new State(capacity, new Semaphore(capacity));
      if (CURRENT.compareAndSet(null, candidate)) {
        return candidate;
      }
    }
  }

  /** Reset this holder only from same-package tests that have stopped every caller. */
  static void resetForTests() {
    State current = CURRENT.get();
    if (current != null && current.permits().availablePermits() == current.capacity()) {
      CURRENT.set(null);
    }
  }
}
