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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.eclipse.microprofile.context.ThreadContext;
import org.junit.jupiter.api.Test;

class FlightExecutorTest {

  @Test
  void defaultExecutorUsesFallbackThreadPool() throws InterruptedException {
    FlightExecutor executor = new FlightExecutor();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<String> threadName = new AtomicReference<>();
    try {
      executor
          .executor()
          .execute(
              () -> {
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
              });

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(threadName.get().startsWith("flight-stream-"));
    } finally {
      executor.shutdown();
    }
  }

  @Test
  void managedExecutorIsAdoptedButNotShutDownByFlightExecutor() throws InterruptedException {
    FlightExecutor executor = new FlightExecutor();
    TestManagedExecutor managed = new TestManagedExecutor();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<String> threadName = new AtomicReference<>();
    try {
      executor.adoptManagedExecutor(managed);
      executor
          .executor()
          .execute(
              () -> {
                threadName.set(Thread.currentThread().getName());
                latch.countDown();
              });

      assertTrue(latch.await(5, TimeUnit.SECONDS));
      assertTrue(threadName.get().startsWith("managed-flight-"));

      executor.shutdown();
      assertFalse(managed.shutdownCalled());
    } finally {
      managed.shutdownNow();
    }
  }

  private static final class TestManagedExecutor extends AbstractExecutorService
      implements ManagedExecutor {

    private final ExecutorService delegate =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "managed-flight-1");
              thread.setDaemon(true);
              return thread;
            });
    private final AtomicBoolean shutdownCalled = new AtomicBoolean(false);

    @Override
    public void shutdown() {
      shutdownCalled.set(true);
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdownCalled.set(true);
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
      delegate.execute(command);
    }

    @Override
    public <U> CompletableFuture<U> completedFuture(U value) {
      return CompletableFuture.completedFuture(value);
    }

    @Override
    public <U> CompletionStage<U> completedStage(U value) {
      return CompletableFuture.completedFuture(value);
    }

    @Override
    public <U> CompletableFuture<U> failedFuture(Throwable ex) {
      CompletableFuture<U> future = new CompletableFuture<>();
      future.completeExceptionally(ex);
      return future;
    }

    @Override
    public <U> CompletionStage<U> failedStage(Throwable ex) {
      return failedFuture(ex);
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
      return new CompletableFuture<>();
    }

    @Override
    public CompletableFuture<Void> runAsync(Runnable runnable) {
      return CompletableFuture.runAsync(runnable, delegate);
    }

    @Override
    public <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier) {
      return CompletableFuture.supplyAsync(supplier, delegate);
    }

    @Override
    public <T> CompletableFuture<T> copy(CompletableFuture<T> stage) {
      CompletableFuture<T> copy = new CompletableFuture<>();
      stage.whenComplete(
          (value, error) -> {
            if (error != null) {
              copy.completeExceptionally(error);
            } else {
              copy.complete(value);
            }
          });
      return copy;
    }

    @Override
    public <T> CompletionStage<T> copy(CompletionStage<T> stage) {
      CompletableFuture<T> copy = new CompletableFuture<>();
      stage.whenComplete(
          (value, error) -> {
            if (error != null) {
              copy.completeExceptionally(error);
            } else {
              copy.complete(value);
            }
          });
      return copy;
    }

    @Override
    public ThreadContext getThreadContext() {
      return null;
    }

    boolean shutdownCalled() {
      return shutdownCalled.get();
    }
  }
}
