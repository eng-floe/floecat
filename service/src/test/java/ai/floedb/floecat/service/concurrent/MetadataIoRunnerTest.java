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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.eclipse.microprofile.config.ConfigValue;
import org.junit.jupiter.api.Test;

/** Verifies the observable contract of process-wide metadata admission. */
class MetadataIoRunnerTest {

  private static final CancellableCallRunner.FailureMessages FAILURES =
      new CancellableCallRunner.FailureMessages(
          "metadata read cancelled", "metadata read interrupted");

  @Test
  void limitsConcurrentCallsAndReportsGateState() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(2);
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);

    CompletableFuture<String> first = blockingCall(runner, entered, release, "first");
    CompletableFuture<String> second = blockingCall(runner, entered, release, "second");
    assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(runner.capacity()).isEqualTo(2);
    assertThat(runner.permitsInUse()).isEqualTo(2);

    AtomicBoolean thirdEntered = new AtomicBoolean();
    CompletableFuture<String> third =
        CompletableFuture.supplyAsync(
            () ->
                runner.callWithoutCancellation(
                    () -> {
                      thirdEntered.set(true);
                      return "third";
                    },
                    FAILURES));
    await(() -> runner.admissionWaiters() == 1);
    assertThat(thirdEntered).isFalse();

    release.countDown();
    assertThat(first.get(2, TimeUnit.SECONDS)).isEqualTo("first");
    assertThat(second.get(2, TimeUnit.SECONDS)).isEqualTo("second");
    assertThat(third.get(2, TimeUnit.SECONDS)).isEqualTo("third");
    assertThat(runner.permitsInUse()).isZero();
  }

  @Test
  void cancellationAbandonsAnAdmissionWaitWithoutStartingTheOperation() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    UninterruptibleBlocker blocker = new UninterruptibleBlocker();
    CompletableFuture<String> holder =
        CompletableFuture.supplyAsync(
            () -> runner.callWithoutCancellation(() -> block(blocker, "holder"), FAILURES));
    assertThat(blocker.started.await(2, TimeUnit.SECONDS)).isTrue();

    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean operationStarted = new AtomicBoolean();
    CompletableFuture<String> waiting =
        CompletableFuture.supplyAsync(
            () ->
                runner.call(
                    cancelled::get,
                    () -> {
                      operationStarted.set(true);
                      return "waiting";
                    },
                    FAILURES));
    await(() -> runner.admissionWaiters() == 1);
    cancelled.set(true);

    assertThatThrownBy(() -> waiting.get(2, TimeUnit.SECONDS))
        .hasCauseInstanceOf(CancellationException.class);
    assertThat(operationStarted).isFalse();
    assertThat(runner.permitsInUse()).isEqualTo(1);

    blocker.release.countDown();
    assertThat(holder.get(2, TimeUnit.SECONDS)).isEqualTo("holder");
  }

  @Test
  void cancelledRunningCallRetainsAdmissionUntilTheStoreReturns() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    UninterruptibleBlocker blocker = new UninterruptibleBlocker();
    AtomicBoolean cancelled = new AtomicBoolean();
    CompletableFuture<String> abandoned =
        CompletableFuture.supplyAsync(
            () -> runner.call(cancelled::get, () -> block(blocker, "abandoned"), FAILURES));
    assertThat(blocker.started.await(2, TimeUnit.SECONDS)).isTrue();

    cancelled.set(true);
    assertThatThrownBy(() -> abandoned.get(2, TimeUnit.SECONDS))
        .hasCauseInstanceOf(CancellationException.class);
    assertThat(blocker.interrupted.await(2, TimeUnit.SECONDS)).isTrue();
    assertThat(runner.permitsInUse()).isEqualTo(1);

    AtomicBoolean contenderEntered = new AtomicBoolean();
    CompletableFuture<String> contender =
        CompletableFuture.supplyAsync(
            () ->
                runner.callWithoutCancellation(
                    () -> {
                      contenderEntered.set(true);
                      return "contender";
                    },
                    FAILURES));
    await(() -> runner.admissionWaiters() == 1);
    assertThat(contenderEntered).isFalse();

    blocker.release.countDown();
    assertThat(contender.get(2, TimeUnit.SECONDS)).isEqualTo("contender");
    assertThat(runner.permitsInUse()).isZero();
  }

  @Test
  void interruptedUncancellableCallerStillLeavesThePermitWithTheStoreCall() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    UninterruptibleBlocker blocker = new UninterruptibleBlocker();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    AtomicBoolean interruptedStatus = new AtomicBoolean();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    runner.callWithoutCancellation(() -> block(blocker, "ignored"), FAILURES);
                  } catch (Throwable thrown) {
                    failure.set(thrown);
                    interruptedStatus.set(Thread.currentThread().isInterrupted());
                  }
                });
    assertThat(blocker.started.await(2, TimeUnit.SECONDS)).isTrue();

    caller.interrupt();
    caller.join(Duration.ofSeconds(2));
    assertThat(caller.isAlive()).isFalse();
    assertThat(failure.get()).isInstanceOf(CancellationException.class);
    assertThat(interruptedStatus).isTrue();
    assertThat(runner.permitsInUse()).isEqualTo(1);

    blocker.release.countDown();
    await(() -> runner.permitsInUse() == 0);
  }

  @Test
  void workersCarryExplicitContextButNotArbitraryInheritedRequestState() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    InheritableThreadLocal<String> requestLocal = new InheritableThreadLocal<>();
    requestLocal.set("must-not-leak");
    URLClassLoader callerLoader = new URLClassLoader(new URL[0], getClass().getClassLoader());
    Thread caller = Thread.currentThread();
    ClassLoader prior = caller.getContextClassLoader();
    try {
      caller.setContextClassLoader(callerLoader);
      AtomicReference<ClassLoader> observedLoader = new AtomicReference<>();
      AtomicReference<String> observedLocal = new AtomicReference<>();
      AtomicReference<Boolean> virtual = new AtomicReference<>();
      runner.callWithoutCancellation(
          () -> {
            observedLoader.set(Thread.currentThread().getContextClassLoader());
            observedLocal.set(requestLocal.get());
            virtual.set(Thread.currentThread().isVirtual());
            return null;
          },
          FAILURES);
      assertThat(observedLoader.get()).isSameAs(callerLoader);
      assertThat(observedLocal.get()).isNull();
      assertThat(virtual.get()).isTrue();
    } finally {
      caller.setContextClassLoader(prior);
      requestLocal.remove();
      callerLoader.close();
    }
  }

  @Test
  void operationFailuresPropagateUnwrappedAndReleaseAdmission() {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    IllegalStateException expected = new IllegalStateException("store failed");

    assertThatThrownBy(
            () ->
                runner.callWithoutCancellation(
                    () -> {
                      throw expected;
                    },
                    FAILURES))
        .isSameAs(expected);
    assertThat(runner.permitsInUse()).isZero();
  }

  @Test
  void saturationIsRecordedOncePerWaitingCall() throws Exception {
    AtomicInteger saturated = new AtomicInteger();
    MetadataIoRunner runner = new MetadataIoRunner(1, saturated::incrementAndGet);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CompletableFuture<String> holder = blockingCall(runner, entered, release, "holder");
    assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();

    CompletableFuture<String> waiting =
        CompletableFuture.supplyAsync(
            () -> runner.callWithoutCancellation(() -> "waiting", FAILURES));
    await(() -> runner.admissionWaiters() == 1);
    assertThat(saturated).hasValue(1);

    release.countDown();
    assertThat(holder.get(2, TimeUnit.SECONDS)).isEqualTo("holder");
    assertThat(waiting.get(2, TimeUnit.SECONDS)).isEqualTo("waiting");
    assertThat(saturated).hasValue(1);
  }

  @Test
  void capacityConfigurationRejectsInvalidValues() {
    assertThat(MetadataIoRunner.parseConfiguredCapacity(config(null, null))).isEqualTo(64);
    assertThatThrownBy(() -> MetadataIoRunner.parseConfiguredCapacity(config("", "")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("blank");
    assertThatThrownBy(() -> MetadataIoRunner.parseConfiguredCapacity(config("0", "0")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("between 1 and 256");
    assertThatThrownBy(() -> MetadataIoRunner.parseConfiguredCapacity(config("many", "many")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("must be an integer");
  }

  /** Start an uncancellable admitted call that holds its permit until {@code release} opens. */
  private static CompletableFuture<String> blockingCall(
      MetadataIoRunner runner, CountDownLatch entered, CountDownLatch release, String result) {
    return CompletableFuture.supplyAsync(
        () ->
            runner.callWithoutCancellation(
                () -> {
                  entered.countDown();
                  awaitUninterruptibly(release);
                  return result;
                },
                FAILURES));
  }

  private static String block(UninterruptibleBlocker blocker, String result) {
    blocker.await();
    return result;
  }

  /** Poll a concurrent condition for at most two seconds and fail if it never becomes true. */
  private static void await(BooleanSupplier condition) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertThat(condition.getAsBoolean()).isTrue();
  }

  /** Await a latch through interrupts and restore the worker's interrupted status on return. */
  private static void awaitUninterruptibly(CountDownLatch latch) {
    boolean interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
  }

  /** Build the minimal resolved/raw configuration view used by capacity parser tests. */
  private static ConfigValue config(String raw, String value) {
    return new ConfigValue() {
      @Override
      public String getName() {
        return MetadataIoRunner.MAX_CONCURRENCY_PROPERTY;
      }

      @Override
      public String getValue() {
        return value;
      }

      @Override
      public String getRawValue() {
        return raw;
      }

      @Override
      public String getSourceName() {
        return "test";
      }

      @Override
      public int getSourceOrdinal() {
        return 1;
      }
    };
  }
}
