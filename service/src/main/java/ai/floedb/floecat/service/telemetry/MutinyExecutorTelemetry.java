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

package ai.floedb.floecat.service.telemetry;

import ai.floedb.floecat.telemetry.Observability;
import ai.floedb.floecat.telemetry.helpers.ExecutorMetrics;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

@ApplicationScoped
public class MutinyExecutorTelemetry {
  private static final String COMPONENT = "service";
  private static final String OPERATION = "worker.pool";
  private static final String DEFAULT_POOL = "mutiny-default";

  private final Observability observability;

  private volatile Executor originalExecutor;
  private volatile Executor instrumentedExecutor;

  @Inject
  public MutinyExecutorTelemetry(Observability observability) {
    this.observability = observability;
  }

  @PostConstruct
  void onStartup() {
    Executor current = Infrastructure.getDefaultExecutor();
    if (current instanceof InstrumentedExecutor) {
      instrumentedExecutor = current;
      originalExecutor = ((InstrumentedExecutor) current).getDelegate();
      return;
    }

    originalExecutor = current;
    ExecutorMetrics metrics =
        new ExecutorMetrics(observability, COMPONENT, OPERATION, DEFAULT_POOL);
    instrumentedExecutor = new InstrumentedExecutor(current, metrics);
    Infrastructure.setDefaultExecutor(instrumentedExecutor);
  }

  @PreDestroy
  void onShutdown() {
    Executor current = Infrastructure.getDefaultExecutor();
    if (instrumentedExecutor != null
        && current == instrumentedExecutor
        && originalExecutor != null) {
      Infrastructure.setDefaultExecutor(originalExecutor);
    }
  }

  private static final class InstrumentedExecutor implements Executor {
    private final Executor delegate;
    private final ExecutorMetrics metrics;

    InstrumentedExecutor(Executor delegate, ExecutorMetrics metrics) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.metrics = Objects.requireNonNull(metrics, "metrics");
    }

    Executor getDelegate() {
      return delegate;
    }

    @Override
    public void execute(Runnable command) {
      Objects.requireNonNull(command, "command");
      long queuedAt = System.nanoTime();
      Runnable wrapped =
          () -> {
            long start = System.nanoTime();
            metrics.timerTaskWait(nanosToDuration(start - queuedAt));
            long runStart = start;
            try {
              command.run();
            } finally {
              metrics.timerTaskRun(nanosToDuration(System.nanoTime() - runStart));
            }
          };
      try {
        delegate.execute(wrapped);
      } catch (RejectedExecutionException e) {
        metrics.counterRejected(1);
        if (shouldSwallowRejected(delegate)) {
          return;
        }
        throw e;
      }
    }

    private static Duration nanosToDuration(long nanos) {
      return Duration.ofNanos(Math.max(0, nanos));
    }

    private static boolean shouldSwallowRejected(Executor delegate) {
      if (delegate instanceof ExecutorService service) {
        return service.isShutdown();
      }
      return false;
    }
  }
}
