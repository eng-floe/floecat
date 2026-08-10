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
package ai.floedb.floecat.service.query.catalog;

import ai.floedb.floecat.runtime.concurrent.ProcessWideAdmission;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.eclipse.microprofile.config.ConfigProvider;

/** Application-generation facade over the JVM-lifetime gate for query-driven stats warms. */
@ApplicationScoped
final class StatsWarmAdmission {

  static final String MAX_PARALLEL_STATS_WARMS = "floecat.catalog.bundle.max_parallel_stats_warms";
  private static final int DEFAULT_CAPACITY = 16;
  private static final long POLL_MILLIS = 10;

  private final int capacity;
  private final Semaphore permits;

  @Inject
  StatsWarmAdmission() {
    this(
        ProcessWideAdmission.resolve(ProcessWideAdmission.Domain.STATS_WARM, configuredCapacity()));
  }

  /** Build an isolated gate for focused same-package tests. */
  StatsWarmAdmission(int capacity) {
    this.capacity = Math.max(1, capacity);
    this.permits = new Semaphore(this.capacity, true);
  }

  private StatsWarmAdmission(ProcessWideAdmission.State admission) {
    this.capacity = admission.capacity();
    this.permits = admission.permits();
  }

  int capacity() {
    return capacity;
  }

  /** Run one warm while retaining the shared permit until its backing work returns. */
  <T> T call(BooleanSupplier cancelled, Supplier<T> operation) {
    Objects.requireNonNull(cancelled, "cancelled");
    Objects.requireNonNull(operation, "operation");
    boolean acquired = false;
    try {
      while (!acquired) {
        throwIfCancelled(cancelled);
        try {
          acquired = permits.tryAcquire(POLL_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          CancellationException cancellation =
              new CancellationException("interrupted while awaiting stats-warm admission");
          cancellation.initCause(e);
          throw cancellation;
        }
      }
      throwIfCancelled(cancelled);
      return operation.get();
    } finally {
      if (acquired) {
        permits.release();
      }
    }
  }

  private static int configuredCapacity() {
    return Math.max(
        1,
        ConfigProvider.getConfig()
            .getOptionalValue(MAX_PARALLEL_STATS_WARMS, Integer.class)
            .orElse(DEFAULT_CAPACITY));
  }

  private static void throwIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean()) {
      throw new CancellationException("stats warm cancelled");
    }
  }
}
