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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The canonical parallel metadata stage: fan a request's independent units out under a per-request
 * concurrency bound, deliver results in input order with first-failure precedence and prompt
 * cancellation, and re-establish the caller's request context on each worker (via {@link
 * BoundedFanout}). One place owns that, so no resolution stage re-wires it.
 *
 * <p>This stage does <em>not</em> apply admission — repository families composed with {@link
 * MetadataResourceReader} enforce it at their explicit read boundary. A unit simply calls the
 * repository; an admitted read acquires a permit when (and only when) it makes a round-trip, and
 * the orchestrating thread holds none. That is why a fan-out must never be started from within an
 * admitted store operation: that thread would hold a permit while its off-thread units wait for
 * more, deadlocking under saturation. The stage rejects that case.
 *
 * <p><b>Admission covers explicitly composed repositories only</b> — the catalog, namespace, table
 * and view families. A unit that reads through a direct repository (the snapshot, stats,
 * current-snapshot-pointer and table-root families) is <em>not</em> bounded by the process-wide
 * ceiling, so such a fan-out's store concurrency is bounded by its own {@code permits} alone and
 * scales with concurrent requests. Composing a family with the admitted read policy brings it under
 * the ceiling; do not assume this stage does it.
 *
 * <p>When the metadata source cannot resolve concurrently — a thread-confined overlay — pass {@code
 * concurrent=false} and units run serially on the caller thread with the same ordering and
 * first-failure precedence. Cancellation is honored between units either way; a lookup already in
 * flight on the caller thread completes before cancellation is observed. Callers do not otherwise
 * branch on the mode: they define a unit and call {@link #forEachOrdered}/{@link #mapOrdered}.
 */
public final class MetadataFanout {

  // Process-wide virtual-thread executor: a unit parked on store admission consumes a virtual
  // thread, not a platform worker, and it carries no Vert.x context, so PropagatedContext's MDC
  // isolation holds off-thread. Virtual-thread carriers are daemons, so there is no lifecycle to
  // manage.
  private static final ExecutorService EXECUTOR = Executors.newVirtualThreadPerTaskExecutor();

  private MetadataFanout() {}

  /**
   * Apply an uncancellable {@code unit} to each element with the ordering and failure semantics of
   * {@link #forEachOrdered(List, int, boolean, Function, Consumer, BooleanSupplier)}. Thread
   * interruption still aborts the stage.
   */
  public static <I, O> void forEachOrdered(
      List<I> units,
      int permits,
      boolean concurrent,
      Function<? super I, ? extends O> unit,
      Consumer<? super O> consumer) {
    forEachOrdered(units, permits, concurrent, unit, consumer, BoundedFanout.NEVER_CANCELLED);
  }

  /**
   * Apply {@code unit} to each element in input order, delivering each result to {@code consumer}
   * as its input-order prefix completes. The first failure reachable in input order — from a unit
   * or from {@code consumer} — propagates immediately; concurrent siblings are cancelled rather
   * than drained. {@code permits} bounds how many of this request's units run at once (concurrent
   * mode only); {@code cancelled} must be non-blocking and thread-safe, and observed cancellation
   * throws {@link CancellationException}.
   */
  public static <I, O> void forEachOrdered(
      List<I> units,
      int permits,
      boolean concurrent,
      Function<? super I, ? extends O> unit,
      Consumer<? super O> consumer,
      BooleanSupplier cancelled) {
    if (concurrent) {
      // Only the concurrent branch can deadlock, and BoundedFanout already rejects it — checking
      // here too would give one rule two messages. Serial units run inline on this thread, which is
      // the re-entrant case admission supports: MetadataIoRunner reuses the held permit rather than
      // taking a second one, so rejecting it would refuse a nesting that cannot wedge the pool.
      BoundedFanout.forEachOrdered(
          units, permits, EXECUTOR, item -> unit.apply(item), consumer, cancelled);
      return;
    }
    // The source owns thread-confined state, so units cannot run off-thread. Preserve the same
    // ordered delivery and first-failure precedence: a throwing unit or consumer stops the stage at
    // once. Re-check cancellation after each unit's post-I/O work so a cancellation that became
    // true between the store call and here does not publish a result.
    for (I item : units) {
      throwIfCancelled(cancelled);
      O result = unit.apply(item);
      throwIfCancelled(cancelled);
      consumer.accept(result);
    }
  }

  /**
   * Run uncancellable units and collect their results into an input-ordered list. Thread
   * interruption still aborts the stage.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> units, int permits, boolean concurrent, Function<? super I, ? extends O> unit) {
    return mapOrdered(units, permits, concurrent, unit, BoundedFanout.NEVER_CANCELLED);
  }

  /**
   * Run cancellable units and collect their results into an input-ordered list. {@code cancelled}
   * follows the contract of {@link #forEachOrdered(List, int, boolean, Function, Consumer,
   * BooleanSupplier)}.
   */
  public static <I, O> List<O> mapOrdered(
      List<I> units,
      int permits,
      boolean concurrent,
      Function<? super I, ? extends O> unit,
      BooleanSupplier cancelled) {
    List<O> results = new ArrayList<>(units.size());
    forEachOrdered(units, permits, concurrent, unit, results::add, cancelled);
    return results;
  }

  /** Throw when either cooperative cancellation or thread interruption stops the serial stage. */
  private static void throwIfCancelled(BooleanSupplier cancelled) {
    // Honor a scheduler interrupt as well as the cooperative signal, matching the concurrent path
    // (BoundedFanout.checkCancelled/abortIfCancelled). Otherwise a serial batch on an interrupted
    // request thread — a container unwinding a deadline — would run every remaining unit to
    // completion, each doing a store round-trip, and the interrupt would never take effect.
    if (cancelled.getAsBoolean() || Thread.currentThread().isInterrupted()) {
      throw new CancellationException("metadata fan-out cancelled");
    }
  }
}
