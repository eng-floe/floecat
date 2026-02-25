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

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.eclipse.microprofile.context.ManagedExecutor;

/** Shared executor for processing Flight streams off the calling thread. */
@ApplicationScoped
public final class FlightExecutor {

  private static final AtomicInteger COUNTER = new AtomicInteger(1);

  // Default fallback when no managed executor is available (e.g. plain unit tests).
  private ExecutorService executor =
      Executors.newCachedThreadPool(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread thread = new Thread(r, "flight-stream-" + COUNTER.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            }
          });
  private boolean ownsExecutor = true;

  @Inject
  void init(Instance<ManagedExecutor> managedExecutors) {
    if (managedExecutors == null) {
      return;
    }
    managedExecutors.stream().findFirst().ifPresent(this::adoptManagedExecutor);
  }

  public ExecutorService executor() {
    return executor;
  }

  synchronized void adoptManagedExecutor(ManagedExecutor managedExecutor) {
    if (managedExecutor == null || !ownsExecutor) {
      return;
    }
    ExecutorService previous = executor;
    executor = managedExecutor;
    ownsExecutor = false;
    previous.shutdown();
  }

  @PreDestroy
  void shutdown() {
    if (!ownsExecutor) {
      return;
    }
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
