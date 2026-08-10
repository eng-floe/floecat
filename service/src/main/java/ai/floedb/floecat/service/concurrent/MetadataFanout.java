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

import ai.floedb.floecat.service.context.PropagatedContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Ordered metadata fan-out configured as either caller-thread serial work or bounded concurrency.
 *
 * <p>Serial mode runs inline without capturing or reinstalling context; concurrent mode propagates
 * caller context to isolated workers. Both preserve input order and check cancellation before and
 * after every unit. Storage adapters own metadata admission for each backend read invoked by a
 * unit.
 */
public final class MetadataFanout {

  private static final Executor DIRECT = Runnable::run;
  private static final Executor VIRTUAL_THREADS =
      command ->
          Thread.ofVirtual()
              .inheritInheritableThreadLocals(false)
              .name("floecat-metadata-fanout")
              .start(command);

  private final int permits;
  private final Executor executor;
  private final boolean callerThread;

  private MetadataFanout(int permits, Executor executor, boolean callerThread) {
    this.permits = permits;
    this.executor = executor;
    this.callerThread = callerThread;
  }

  /** Run every unit synchronously on the caller thread. */
  public static MetadataFanout serial() {
    return new MetadataFanout(1, DIRECT, true);
  }

  /** Run at most {@code permits} units concurrently on isolated virtual threads. */
  public static MetadataFanout concurrent(int permits) {
    if (permits < 1) {
      throw new IllegalArgumentException("metadata fan-out permits must be positive");
    }
    return new MetadataFanout(permits, VIRTUAL_THREADS, false);
  }

  /**
   * Apply uncancellable units and deliver results in input order. The first reachable unit or
   * consumer failure propagates immediately; caller interruption abandons active siblings.
   */
  public <I, O> void forEachOrdered(
      List<I> units, Function<? super I, ? extends O> unit, Consumer<? super O> consumer) {
    forEachOrdered(units, unit, consumer, BoundedFanout.NEVER_CANCELLED);
  }

  /**
   * Apply cancellable units and deliver their results in input order.
   *
   * <p>The first unit or consumer failure reachable in input order propagates immediately. Observed
   * cancellation throws {@link CancellationException} and abandons active siblings.
   */
  public <I, O> void forEachOrdered(
      List<I> units,
      Function<? super I, ? extends O> unit,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled) {
    if (callerThread) {
      try (var cancellationScope = PropagatedContext.bindCancellation(cancelled)) {
        forEachInline(units, unit, consumer, cancelled);
      }
      return;
    }
    BoundedFanout.forEachOrdered(
        units, permits, executor, item -> unit.apply(item), consumer, cancelled);
  }

  /**
   * Run uncancellable units and collect results in input order. The first failure reachable in
   * input order propagates immediately; caller interruption abandons active siblings.
   */
  public <I, O> List<O> mapOrdered(List<I> units, Function<? super I, ? extends O> unit) {
    return mapOrdered(units, unit, BoundedFanout.NEVER_CANCELLED);
  }

  /**
   * Run cancellable units and collect results in input order. The first reachable unit failure
   * propagates immediately; observed cancellation throws {@link CancellationException} and abandons
   * active siblings.
   */
  public <I, O> List<O> mapOrdered(
      List<I> units, Function<? super I, ? extends O> unit, BooleanSupplier cancelled) {
    if (callerThread) {
      ArrayList<O> results = new ArrayList<>(units.size());
      try (var cancellationScope = PropagatedContext.bindCancellation(cancelled)) {
        forEachInline(units, unit, results::add, cancelled);
      }
      return results;
    }
    return BoundedFanout.mapOrdered(units, permits, executor, item -> unit.apply(item), cancelled);
  }

  /** Run serial units directly without mutating context owned by the caller's Vert.x dispatcher. */
  private static <I, O> void forEachInline(
      List<I> units,
      Function<? super I, ? extends O> unit,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled) {
    for (I item : units) {
      throwIfCancelled(cancelled);
      O result = unit.apply(item);
      throwIfCancelled(cancelled);
      consumer.accept(result);
    }
    throwIfCancelled(cancelled);
  }

  private static void throwIfCancelled(BooleanSupplier cancelled) {
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      throw new CancellationException("fan-out cancelled");
    }
  }
}
