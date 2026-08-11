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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

/** Owns named admission gates shared by every application generation in this JVM. */
public final class ProcessWideAdmission {

  /** Fixed admission domains keep the JVM-lifetime registry bounded across reloads. */
  public enum Domain {
    METADATA_IO,
    STATS_WARM
  }

  /** Immutable identity of the process gate; the semaphore itself carries its live usage. */
  public record State(int capacity, Semaphore permits) {
    /** Require a positive capacity and a live semaphore for the process gate. */
    public State {
      if (capacity < 1) {
        throw new IllegalArgumentException("process-wide admission capacity must be positive");
      }
      if (permits == null) {
        throw new NullPointerException("permits");
      }
    }
  }

  private static final ConcurrentMap<Domain, State> GATES = new ConcurrentHashMap<>();

  private ProcessWideAdmission() {}

  /**
   * Return the process gate, creating it with {@code capacity} on first use.
   *
   * <p>The first application generation fixes the ceiling until JVM exit. Replacing a live gate
   * during reload would let old and new generations admit independently.
   */
  public static State resolve(int capacity) {
    return resolve(Domain.METADATA_IO, capacity);
  }

  /**
   * Return the domain's process gate, creating it with {@code capacity} on first use.
   *
   * <p>Domains isolate independent ceilings. The first application generation to resolve a domain
   * fixes that gate's capacity until JVM exit.
   */
  public static State resolve(Domain domain, int capacity) {
    if (domain == null) {
      throw new NullPointerException("domain");
    }
    if (capacity < 1) {
      throw new IllegalArgumentException("process-wide admission capacity must be positive");
    }
    return GATES.computeIfAbsent(domain, ignored -> new State(capacity, new Semaphore(capacity)));
  }
}
