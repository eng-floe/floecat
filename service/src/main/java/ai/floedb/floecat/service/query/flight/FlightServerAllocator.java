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

package ai.floedb.floecat.service.query.flight;

import ai.floedb.floecat.flight.FlightAllocatorProvider;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/** Owns the Flight server parent allocator used by all Flight streams in this process. */
@ApplicationScoped
public final class FlightServerAllocator implements FlightAllocatorProvider {

  private static final Logger LOG = Logger.getLogger(FlightServerAllocator.class);

  @ConfigProperty(name = "floecat.flight.memory.max-bytes", defaultValue = "0")
  long flightMemoryMaxBytes;

  private BufferAllocator allocator;

  @PostConstruct
  void init() {
    long parentCap = flightMemoryMaxBytes > 0 ? flightMemoryMaxBytes : Long.MAX_VALUE;
    allocator = new RootAllocator(parentCap);
  }

  @Override
  public BufferAllocator allocator() {
    if (allocator == null) {
      throw new IllegalStateException("Flight allocator has not been initialized");
    }
    return allocator;
  }

  @PreDestroy
  void close() {
    if (allocator == null) {
      return;
    }
    try {
      allocator.close();
    } catch (Exception e) {
      LOG.warn("Error closing Flight server allocator", e);
    } finally {
      allocator = null;
    }
  }
}
