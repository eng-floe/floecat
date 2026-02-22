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

package ai.floedb.floecat.flight;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.arrow.memory.BufferAllocator;

/** Holder for the Flight server's parent allocator so transports can create bounded children. */
@ApplicationScoped
public final class FlightAllocatorHolder {

  private volatile BufferAllocator allocator;

  public synchronized void setAllocator(BufferAllocator allocator) {
    if (allocator == null) {
      throw new IllegalArgumentException("Flight allocator cannot be null");
    }
    // Some services initialize a generic Flight allocator first and then replace it with
    // a dedicated server allocator during startup. Keep the latest allocator.
    this.allocator = allocator;
  }

  public BufferAllocator allocator() {
    BufferAllocator alloc = this.allocator;
    if (alloc == null) {
      throw new IllegalStateException("Flight allocator has not been initialized");
    }
    return alloc;
  }

  public synchronized void clear() {
    this.allocator = null;
  }
}
