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

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
  void serialModeUsesTheCallerThreadAndTheSameOrderedScheduler() {
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
  void concurrentModeRejectsAnInvalidBoundAtConfigurationTime() {
    assertThatThrownBy(() -> MetadataFanout.concurrent(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("positive");
  }
}
