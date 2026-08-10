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

import ai.floedb.floecat.service.testsupport.ConcurrentTestSupport;
import io.smallrye.common.vertx.VertxContext;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Verifies the configured public fan-out interface. */
class MetadataFanoutTest {

  @Test
  void concurrentModeBoundsWorkAndReturnsInputOrder() {
    AtomicInteger active = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();

    List<Integer> results =
        MetadataFanout.concurrent(3)
            .mapOrdered(
                IntStream.range(0, 20).boxed().toList(),
                value -> {
                  int now = active.incrementAndGet();
                  peak.accumulateAndGet(now, Math::max);
                  try {
                    Thread.sleep((20 - value) % 4L);
                    return value * 10;
                  } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new CancellationException("interrupted");
                  } finally {
                    active.decrementAndGet();
                  }
                });

    assertThat(results)
        .containsExactlyElementsOf(IntStream.range(0, 20).map(i -> i * 10).boxed().toList());
    assertThat(peak).hasValueBetween(2, 3);
  }

  @Test
  void serialModeUsesTheCallerThreadAndPreservesInputOrder() {
    Thread caller = Thread.currentThread();
    AtomicReference<Thread> observed = new AtomicReference<>();

    List<Integer> results =
        MetadataFanout.serial()
            .mapOrdered(
                List.of(3, 1, 2),
                value -> {
                  observed.compareAndSet(null, Thread.currentThread());
                  assertThat(Thread.currentThread()).isSameAs(caller);
                  return value * 10;
                });

    assertThat(results).containsExactly(30, 10, 20);
    assertThat(observed.get()).isSameAs(caller);
  }

  @Test
  void serialModeRunsInlineOnADuplicatedVertxContext() throws Exception {
    Vertx vertx = Vertx.vertx();
    try {
      List<Integer> results =
          onDuplicatedContext(
              vertx,
              () -> MetadataFanout.serial().mapOrdered(List.of(1, 2, 3), value -> value * 2));

      assertThat(results).containsExactly(2, 4, 6);
    } finally {
      vertx.close();
    }
  }

  @Test
  void concurrentCancellationAbandonsActiveSiblings() throws Exception {
    AtomicBoolean cancelled = new AtomicBoolean();
    CountDownLatch started = new CountDownLatch(2);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    MetadataFanout.concurrent(2)
                        .mapOrdered(
                            List.of(1, 2, 3),
                            ignored -> {
                              started.countDown();
                              try {
                                new CountDownLatch(1).await();
                                return ignored;
                              } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt();
                                throw new CancellationException("interrupted");
                              }
                            },
                            cancelled::get);
                  } catch (Throwable thrown) {
                    failure.set(thrown);
                  }
                });
    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();

    cancelled.set(true);
    caller.join(TimeUnit.SECONDS.toMillis(2));
    assertThat(caller.isAlive()).isFalse();
    assertThat(failure.get()).isInstanceOf(CancellationException.class);
  }

  @Test
  void serialCancellationStopsBeforeTheNextUnit() {
    AtomicInteger calls = new AtomicInteger();
    AtomicBoolean cancelled = new AtomicBoolean();

    assertThatThrownBy(
            () ->
                MetadataFanout.serial()
                    .mapOrdered(
                        List.of(1, 2, 3),
                        value -> {
                          calls.incrementAndGet();
                          cancelled.set(true);
                          return value;
                        },
                        cancelled::get))
        .isInstanceOf(CancellationException.class);
    assertThat(calls).hasValue(1);
  }

  @Test
  void serialCancellationAbandonsAUnitWaitingForMetadataAdmission() throws Exception {
    MetadataIoRunner runner = new MetadataIoRunner(1);
    MetadataResourceReader reader = new MetadataResourceReader(runner);
    CountDownLatch holderEntered = new CountDownLatch(1);
    CountDownLatch releaseHolder = new CountDownLatch(1);
    var failures =
        new CancellableCallRunner.FailureMessages("cancelled", "interrupted while waiting");
    CompletableFuture<String> holder =
        CompletableFuture.supplyAsync(
            () ->
                runner.callWithoutCancellation(
                    () -> {
                      holderEntered.countDown();
                      ConcurrentTestSupport.awaitUninterruptibly(releaseHolder);
                      return "holder";
                    },
                    failures));
    assertThat(holderEntered.await(2, TimeUnit.SECONDS)).isTrue();

    AtomicBoolean cancelled = new AtomicBoolean();
    AtomicBoolean backendStarted = new AtomicBoolean();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread caller =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    MetadataFanout.serial()
                        .mapOrdered(
                            List.of(1),
                            ignored ->
                                reader.read(
                                    () -> {
                                      backendStarted.set(true);
                                      return ignored;
                                    }),
                            cancelled::get);
                  } catch (Throwable thrown) {
                    failure.set(thrown);
                  }
                });
    try {
      ConcurrentTestSupport.await(() -> runner.admissionWaiters() == 1, Duration.ofSeconds(2));
      cancelled.set(true);
      caller.join(TimeUnit.SECONDS.toMillis(2));

      assertThat(caller.isAlive()).isFalse();
      assertThat(failure.get()).isInstanceOf(CancellationException.class);
      assertThat(backendStarted).isFalse();
    } finally {
      cancelled.set(true);
      releaseHolder.countDown();
      caller.join(TimeUnit.SECONDS.toMillis(2));
      assertThat(holder.get(2, TimeUnit.SECONDS)).isEqualTo("holder");
      ConcurrentTestSupport.await(() -> runner.permitsInUse() == 0, Duration.ofSeconds(2));
    }
  }

  @Test
  void concurrentModeRejectsAnInvalidBoundAtConfigurationTime() {
    assertThatThrownBy(() -> MetadataFanout.concurrent(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }

  private static <T> T onDuplicatedContext(Vertx vertx, Supplier<T> body) throws Exception {
    CompletableFuture<T> result = new CompletableFuture<>();
    io.vertx.core.Context duplicated =
        VertxContext.createNewDuplicatedContext(vertx.getOrCreateContext());
    duplicated.runOnContext(
        ignored -> {
          try {
            result.complete(body.get());
          } catch (Throwable failure) {
            result.completeExceptionally(failure);
          }
        });
    return result.get(10, TimeUnit.SECONDS);
  }
}
