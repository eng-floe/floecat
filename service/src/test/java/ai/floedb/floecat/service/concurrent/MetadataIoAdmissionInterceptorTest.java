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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ai.floedb.floecat.service.context.PropagatedContext;
import jakarta.interceptor.InvocationContext;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The repository interceptor runs each annotated read under admission and preserves its outcome.
 */
class MetadataIoAdmissionInterceptorTest {

  @Test
  void interceptedReadRunsUnderAdmission() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    try {
      var interceptor = new MetadataIoAdmissionInterceptor(runner);
      AtomicBoolean admittedDuringCall = new AtomicBoolean();
      // Before/after the intercepted call the thread is not admitted; during it, it is — proving
      // the
      // permit is held exactly around the store round-trip.
      assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
      Object result =
          interceptor.admit(
              ctx(
                  () -> {
                    admittedDuringCall.set(MetadataIoRunner.isRunningAdmittedOperation());
                    return "value";
                  }));
      assertEquals("value", result);
      assertTrue(admittedDuringCall.get(), "the read must run inside an admitted operation");
      assertFalse(MetadataIoRunner.isRunningAdmittedOperation());
    } finally {
      runner.close();
    }
  }

  @Test
  void interceptedReadPropagatesTheRepositoryCheckedException() {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    try {
      var interceptor = new MetadataIoAdmissionInterceptor(runner);
      IOException thrown =
          assertThrows(
              IOException.class,
              () ->
                  interceptor.admit(
                      ctx(
                          () -> {
                            throw new IOException("store unreachable");
                          })));
      assertEquals("store unreachable", thrown.getMessage());
    } finally {
      runner.close();
    }
  }

  @Test
  void nestedInterceptedReadsReuseTheOnePermit() throws Exception {
    // A read reaching admission again on the same thread (an intercepted repo method calling
    // another)
    // reuses the held permit. With capacity 1 a second acquire would block forever, so returning
    // proves re-entrant reuse.
    MetadataIoRunner runner = new MetadataIoRunner(1);
    try {
      var interceptor = new MetadataIoAdmissionInterceptor(runner);
      // Bounded: if re-entrant reuse regresses, the inner admit blocks on the capacity-1 semaphore
      // the outer one holds. Without a timeout that is an untimed hang rather than a test failure.
      Object result =
          assertTimeoutPreemptively(
              java.time.Duration.ofSeconds(5),
              () -> interceptor.admit(ctx(() -> interceptor.admit(ctx(() -> "inner")))),
              "nested admission deadlocked instead of reusing the held permit");
      assertEquals("inner", result);
    } finally {
      runner.close();
    }
  }

  @Test
  void interceptedReadHonorsThePropagatedCancellationSignal() {
    // A cancellable request (e.g. a GetUserObjects stream) binds its signal; the interceptor reads
    // it
    // from PropagatedContext. Already-cancelled here, so admission throws before the read even runs
    // —
    // proving the interceptor uses the propagated signal rather than the non-cancellable path.
    MetadataIoRunner runner = new MetadataIoRunner(1);
    AtomicBoolean readRan = new AtomicBoolean(false);
    try (PropagatedContext.CancellationScope ignored =
        PropagatedContext.bindCancellation(() -> true)) {
      assertThrows(
          CancellationException.class,
          () ->
              interceptor(runner)
                  .admit(
                      ctx(
                          () -> {
                            readRan.set(true);
                            return "unreachable";
                          })));
      assertFalse(readRan.get(), "an already-cancelled request must not run the store read");
    } finally {
      runner.close();
    }
  }

  @Test
  void aBlockedCancellableReadIsAbandonedWhenTheRequestCancels() throws Exception {
    // A read stuck mid-flight (ignoring interrupts) must not pin the caller: with the request token
    // bound, admission runs the read off-thread and returns CancellationException on cancel while
    // the
    // worker unwinds. This is the streaming-cancel guarantee, now enforced at the store boundary.
    MetadataIoRunner runner = new MetadataIoRunner(2);
    var cancelled = new AtomicBoolean(false);
    var started = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    try {
      Future<Throwable> call =
          caller.submit(
              () -> {
                try (PropagatedContext.CancellationScope scope =
                    PropagatedContext.bindCancellation(cancelled::get)) {
                  interceptor(runner)
                      .admit(
                          ctx(
                              () -> {
                                started.countDown();
                                awaitUninterruptibly(release);
                                return "unreachable";
                              }));
                  return null;
                } catch (Throwable failure) {
                  return failure;
                }
              });
      assertTrue(started.await(1, TimeUnit.SECONDS));
      cancelled.set(true);
      assertInstanceOf(CancellationException.class, call.get(1, TimeUnit.SECONDS));
    } finally {
      release.countDown();
      caller.shutdownNow();
      runner.close();
    }
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  private static MetadataIoAdmissionInterceptor interceptor(MetadataIoRunner runner) {
    return new MetadataIoAdmissionInterceptor(runner);
  }

  /** A minimal {@link InvocationContext} whose {@code proceed()} runs {@code body}. */
  private static InvocationContext ctx(Callable<Object> body) {
    return new InvocationContext() {
      @Override
      public Object proceed() throws Exception {
        return body.call();
      }

      @Override
      public Object getTarget() {
        return null;
      }

      @Override
      public Object getTimer() {
        return null;
      }

      @Override
      public Method getMethod() {
        return null;
      }

      @Override
      public Constructor<?> getConstructor() {
        return null;
      }

      @Override
      public Object[] getParameters() {
        return new Object[0];
      }

      @Override
      public void setParameters(Object[] params) {}

      @Override
      public Map<String, Object> getContextData() {
        return Map.of();
      }
    };
  }
}
