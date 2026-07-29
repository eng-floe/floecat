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
package ai.floedb.floecat.telemetry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * A thread-safe {@link PhaseDiagnostics} that sums counts and durations per key, for reporting
 * diagnostics from work spread across threads. Concurrent tasks share one instance; the request
 * thread then calls {@link #flushInto} once to emit the totals to the real (not necessarily
 * thread-safe) diagnostics.
 *
 * <p>Only counters and durations aggregate meaningfully, so those are the only calls kept: {@code
 * count}/{@code add} sum, {@code timer}/{@code nanos} sum the elapsed time, and each summed key is
 * flushed once as a single {@code add} / {@code nanos}. The per-key values (e.g. total snapshot
 * lookups and the total time spent in them) are the per-request aggregate; per-item ordering and
 * one-shot fields ({@code put}, {@code emit}) are not representable and are omitted — a caller that
 * needs them should record on the request thread.
 */
public final class AggregatingPhaseDiagnostics implements PhaseDiagnostics {

  private final Map<String, LongAdder> counts = new ConcurrentHashMap<>();
  private final Map<String, LongAdder> nanosByKey = new ConcurrentHashMap<>();

  @Override
  public Timer timer(String key) {
    long startNanos = System.nanoTime();
    return () -> nanos(key, System.nanoTime() - startNanos);
  }

  @Override
  public void nanos(String key, long nanos) {
    nanosByKey.computeIfAbsent(key, k -> new LongAdder()).add(nanos);
  }

  @Override
  public void count(String key) {
    add(key, 1L);
  }

  @Override
  public void add(String key, long amount) {
    counts.computeIfAbsent(key, k -> new LongAdder()).add(amount);
  }

  // One-shot values do not aggregate across items. Omitting telemetry must never turn into a
  // request failure when a concurrent path reaches a newly added diagnostic.
  @Override
  public void put(String key, String value) {}

  @Override
  public void put(String key, long value) {}

  @Override
  public void put(String key, double value) {}

  @Override
  public void put(String key, boolean value) {}

  @Override
  public void emit(String eventName) {}

  /**
   * Emit the accumulated totals to {@code target}: one {@code add} per counter, one summed {@code
   * nanos} per timed key.
   */
  public void flushInto(PhaseDiagnostics target) {
    counts.forEach((key, adder) -> target.add(key, adder.sum()));
    nanosByKey.forEach((key, adder) -> target.nanos(key, adder.sum()));
  }
}
