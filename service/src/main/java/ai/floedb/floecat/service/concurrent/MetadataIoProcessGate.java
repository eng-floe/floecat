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
import java.util.concurrent.atomic.AtomicReference;

/**
 * The one typed owner of metadata-I/O admission state for this runtime.
 *
 * <p>Configuration enters as a number; the resulting semaphore never crosses the system-properties
 * string boundary. The first resolved capacity remains fixed while work exists, so replacing an
 * executor during a controlled lifecycle transition cannot create a second admission ceiling.
 */
final class MetadataIoProcessGate {

  record State(int capacity, Semaphore permits) {}

  private static final AtomicReference<State> CURRENT = new AtomicReference<>();

  private MetadataIoProcessGate() {}

  static State resolve(int capacity) {
    if (capacity < 1) {
      throw new IllegalArgumentException("process-wide metadata-I/O capacity must be positive");
    }
    while (true) {
      State current = CURRENT.get();
      if (current != null) {
        return current;
      }
      State fresh = new State(capacity, new Semaphore(capacity));
      if (CURRENT.compareAndSet(null, fresh)) {
        return fresh;
      }
    }
  }

  static void clearIfIdle(Semaphore permits) {
    State current = CURRENT.get();
    if (current != null
        && current.permits() == permits
        && permits.availablePermits() == current.capacity()) {
      CURRENT.compareAndSet(current, null);
    }
  }
}
