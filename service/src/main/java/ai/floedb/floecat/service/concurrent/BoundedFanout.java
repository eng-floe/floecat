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

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.function.Function;

/** Runs independent, mostly-blocking tasks on a shared executor with a concurrency bound. */
public final class BoundedFanout {

  private BoundedFanout() {}

  /**
   * Apply {@code task} to each item on {@code executor}, at most {@code permits} running at once,
   * and return the results in input order. Each task runs under the caller's OpenTelemetry context.
   * A task failure surfaces unwrapped — its original {@link RuntimeException} or {@link Error},
   * never a {@link CompletionException} wrapper — and the first such failure propagates to the
   * caller once its future is joined.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> items, int permits, Executor executor, Function<I, O> task) {
    Semaphore gate = new Semaphore(permits);
    Context otelContext = Context.current();
    List<CompletableFuture<O>> futures =
        items.stream()
            .map(
                item ->
                    CompletableFuture.supplyAsync(
                        () -> {
                          gate.acquireUninterruptibly();
                          try (Scope ignored = otelContext.makeCurrent()) {
                            return task.apply(item);
                          } finally {
                            gate.release();
                          }
                        },
                        executor))
            .toList();

    List<O> results = new ArrayList<>(items.size());
    for (CompletableFuture<O> future : futures) {
      results.add(join(future));
    }
    return results;
  }

  private static <O> O join(CompletableFuture<O> future) {
    try {
      return future.join();
    } catch (CompletionException ce) {
      Throwable cause = ce.getCause();
      if (cause instanceof RuntimeException re) {
        throw re;
      }
      if (cause instanceof Error e) {
        throw e;
      }
      throw new IllegalStateException("unexpected checked exception from parallel task", cause);
    }
  }
}
