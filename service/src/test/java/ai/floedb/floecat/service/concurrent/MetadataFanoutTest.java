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

import static ai.floedb.floecat.service.testsupport.ConcurrentTestSupport.awaitUninterruptibly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * The parallel stage delivers in input order and cancels while repository reads apply admission at
 * the store boundary. These tests cover ordering, serial mode, cancellation, and the guard against
 * starting a fan-out from within an admitted store operation.
 */
class MetadataFanoutTest {

  private static final CancellableCallRunner.FailureMessages MSGS =
      new CancellableCallRunner.FailureMessages("op cancelled", "op interrupted");

  @Test
  void concurrentModeReturnsResultsInInputOrder() {
    List<Integer> inputs = IntStream.range(0, 40).boxed().toList();
    // Reverse completion order (later items sleep less) to prove ordering is by input.
    List<Integer> out =
        MetadataFanout.mapOrdered(
            inputs,
            4,
            true,
            item -> {
              try {
                Thread.sleep((40 - item) % 5);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return item * 10;
            });
    assertThat(out).isEqualTo(inputs.stream().map(i -> i * 10).toList());
  }

  @Test
  void serialModeRunsUnitsInOrderOnTheCallerThread() {
    Thread caller = Thread.currentThread();
    AtomicBoolean ranOffThread = new AtomicBoolean(false);
    List<Integer> out =
        MetadataFanout.mapOrdered(
            List.of(1, 2, 3),
            0,
            false,
            item -> {
              if (Thread.currentThread() != caller) {
                ranOffThread.set(true);
              }
              return item * 10;
            });
    assertThat(out).containsExactly(10, 20, 30);
    assertThat(ranOffThread).isFalse();
  }

  @Test
  void uncancellableForEachOverloadDeliversResultsInOrder() {
    List<Integer> published = new ArrayList<>();
    MetadataFanout.forEachOrdered(List.of(3, 1, 2), 2, true, item -> item * 10, published::add);
    assertThat(published).containsExactly(30, 10, 20);
  }

  @Test
  void concurrentModeRejectsPermitsBelowOne() {
    assertThatThrownBy(() -> MetadataFanout.mapOrdered(List.of(1, 2), 0, true, item -> item))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void cancellationBeforeAUnitStopsTheSerialStage() {
    AtomicBoolean cancelled = new AtomicBoolean(false);
    assertThatThrownBy(
            () ->
                MetadataFanout.forEachOrdered(
                    List.of(1, 2, 3),
                    0,
                    false,
                    item -> {
                      cancelled.set(true); // cancel after the first unit starts
                      return item;
                    },
                    result -> {},
                    cancelled::get))
        .isInstanceOf(CancellationException.class);
  }

  @Test
  void serialResultIsNotPublishedWhenCancellationFlipsAfterTheUnit() {
    AtomicBoolean cancelled = new AtomicBoolean();
    List<Integer> published = new ArrayList<>();
    assertThatThrownBy(
            () ->
                MetadataFanout.forEachOrdered(
                    List.of(1),
                    0,
                    false,
                    item -> {
                      int resolved = item * 10;
                      cancelled.set(true); // flips after the unit, before publish
                      return resolved;
                    },
                    published::add,
                    cancelled::get))
        .isInstanceOf(CancellationException.class);
    assertThat(published).isEmpty();
  }

  @Test
  void concurrentResultIsNotPublishedWhenCancellationFlipsAfterTheUnit() {
    AtomicBoolean cancelled = new AtomicBoolean();
    List<Integer> published = new ArrayList<>();
    assertThatThrownBy(
            () ->
                MetadataFanout.forEachOrdered(
                    List.of(1),
                    4,
                    true,
                    item -> {
                      int resolved = item * 10;
                      cancelled.set(true); // flips after the unit, before publish
                      return resolved;
                    },
                    published::add,
                    cancelled::get))
        .isInstanceOf(CancellationException.class);
    assertThat(published).isEmpty();
  }

  @Test
  void unitFailureSurfacesUnwrapped() {
    assertThatThrownBy(
            () ->
                MetadataFanout.mapOrdered(
                    List.of(1, 2, 3),
                    4,
                    true,
                    item -> {
                      if (item == 1) {
                        throw new IllegalStateException("boom on input 1");
                      }
                      return item;
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom on input 1");
  }

  @Test
  void serialUnitIsNotPreemptedMidFlightThenCancelsOnceItReturns() throws Exception {
    // A thread-confined unit runs on the request thread and cannot be preempted mid-flight;
    // cancelling while it is stalled must not return early. Only after it returns is cancellation
    // observed (the post-unit check throws).
    CountDownLatch unitStarted = new CountDownLatch(1);
    CountDownLatch releaseUnit = new CountDownLatch(1);
    AtomicBoolean cancelled = new AtomicBoolean(false);
    AtomicBoolean unitReturned = new AtomicBoolean(false);
    ExecutorService driver = Executors.newSingleThreadExecutor();
    try {
      Future<?> running =
          driver.submit(
              () ->
                  MetadataFanout.forEachOrdered(
                      List.of(1),
                      0,
                      false,
                      item -> {
                        unitStarted.countDown();
                        awaitUninterruptibly(releaseUnit);
                        unitReturned.set(true);
                        return item;
                      },
                      result -> {},
                      cancelled::get));

      assertThat(unitStarted.await(1, TimeUnit.SECONDS)).isTrue();
      cancelled.set(true); // cancel while the unit is stalled
      assertThatThrownBy(() -> running.get(200, TimeUnit.MILLISECONDS))
          .isInstanceOf(TimeoutException.class);
      assertThat(unitReturned).isFalse();

      releaseUnit.countDown();
      assertThatThrownBy(running::get).hasCauseInstanceOf(CancellationException.class);
      assertThat(unitReturned).isTrue();
    } finally {
      releaseUnit.countDown();
      driver.shutdownNow();
    }
  }

  @Test
  void aConcurrentFanOutStartedFromWithinAnAdmittedOperationIsRejected() {
    // Admitted store operations hold a permit on their thread. Dispatching units off-thread from
    // within one holds that permit while each unit waits for its own — a saturation deadlock.
    // BoundedFanout owns the dispatch-boundary check, so the invariant has one failure message.
    assertThatThrownBy(() -> admitted(() -> MetadataFanout.mapOrdered(List.of(1), 4, true, i -> i)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ran inside an admitted metadata-I/O operation");
  }

  @Test
  void aSerialFanOutIsAllowedFromWithinAnAdmittedOperation() {
    // Serial units run inline on the admitted thread, so they reuse its permit instead of taking a
    // second one — the re-entrant case admission supports without risking pool saturation.
    assertThat(admitted(() -> MetadataFanout.mapOrdered(List.of(1, 2), 4, false, i -> i * 10)))
        .containsExactly(10, 20);
  }

  /** Run one fan-out body inside isolated metadata admission and always close its worker pool. */
  private static <T> T admitted(Supplier<T> body) {
    MetadataIoRunner admission = new MetadataIoRunner(4);
    try {
      return admission.call(() -> false, body, MSGS);
    } finally {
      admission.close();
    }
  }
}
