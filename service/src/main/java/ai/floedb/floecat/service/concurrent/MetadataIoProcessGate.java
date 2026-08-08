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

import java.util.concurrent.Semaphore;

/**
 * The one typed owner of metadata-I/O admission state for this runtime.
 *
 * <p>The holder is the bootstrap-owned system-properties table, which is shared by every Quarkus
 * application classloader. Only JDK values cross that classloader seam: an {@link Integer} capacity
 * and a {@link Semaphore}. The first resolved capacity remains fixed while work exists, so
 * replacing an executor during a controlled lifecycle transition cannot create a second admission
 * ceiling.
 */
final class MetadataIoProcessGate {

  record State(int capacity, Semaphore permits) {}

  private static final String CAPACITY_KEY = "ai.floedb.floecat.metadata-io.process-gate.capacity";
  private static final String PERMITS_KEY = "ai.floedb.floecat.metadata-io.process-gate.permits";

  private MetadataIoProcessGate() {}

  static State resolve(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("process-wide metadata-I/O capacity must be positive");
    }
    synchronized (System.getProperties()) {
      Object currentCapacity = System.getProperties().get(CAPACITY_KEY);
      Object currentPermits = System.getProperties().get(PERMITS_KEY);
      if (currentCapacity == null && currentPermits == null) {
        Semaphore permits = new Semaphore(capacity);
        System.getProperties().put(CAPACITY_KEY, capacity);
        System.getProperties().put(PERMITS_KEY, permits);
        return new State(capacity, permits);
      }
      if (!(currentCapacity instanceof Integer storedCapacity)
          || !(currentPermits instanceof Semaphore storedPermits)) {
        throw new IllegalStateException("metadata-I/O process gate state is invalid");
      }
      return new State(storedCapacity, storedPermits);
    }
  }

  static void clearIfIdle(Semaphore permits) {
    synchronized (System.getProperties()) {
      Object currentCapacity = System.getProperties().get(CAPACITY_KEY);
      Object currentPermits = System.getProperties().get(PERMITS_KEY);
      if (currentCapacity instanceof Integer capacity
          && currentPermits == permits
          && permits.availablePermits() == capacity) {
        System.getProperties().remove(CAPACITY_KEY);
        System.getProperties().remove(PERMITS_KEY);
      }
    }
  }
}
