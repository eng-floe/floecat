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
package ai.floedb.floecat.runtime.concurrent;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/** Owns the one metadata-I/O admission gate shared by every application generation in this JVM. */
public final class ProcessWideAdmission {

  /** Immutable identity of the process gate; the semaphore itself carries its live usage. */
  public record State(int capacity, Semaphore permits) {
    public State {
      if (capacity < 1) {
        throw new IllegalArgumentException("process-wide metadata-I/O capacity must be positive");
      }
      if (permits == null) {
        throw new NullPointerException("permits");
      }
    }
  }

  private static final AtomicReference<State> CURRENT = new AtomicReference<>();

  private ProcessWideAdmission() {}

  /**
   * Return the process gate, creating it with {@code capacity} on first use.
   *
   * <p>The first application generation fixes the ceiling until JVM exit. Replacing a live gate
   * during reload would let old and new generations admit independently.
   */
  public static State resolve(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("process-wide metadata-I/O capacity must be positive");
    }
    State current = CURRENT.get();
    if (current != null) {
      return current;
    }
    State candidate = new State(capacity, new Semaphore(capacity));
    return CURRENT.compareAndSet(null, candidate) ? candidate : CURRENT.get();
  }
}
