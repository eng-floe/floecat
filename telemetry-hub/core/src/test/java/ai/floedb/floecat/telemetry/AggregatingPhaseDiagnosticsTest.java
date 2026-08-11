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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class AggregatingPhaseDiagnosticsTest {

  /** Records the totals flushed to it, so aggregation can be asserted. */
  private static final class CapturingDiagnostics implements PhaseDiagnostics {
    final Map<String, Long> added = new LinkedHashMap<>();
    final Map<String, Long> nanos = new LinkedHashMap<>();

    @Override
    public Timer timer(String key) {
      return Timer.NOOP;
    }

    @Override
    public void nanos(String key, long value) {
      nanos.merge(key, value, Long::sum);
    }

    @Override
    public void count(String key) {
      add(key, 1L);
    }

    @Override
    public void add(String key, long amount) {
      added.merge(key, amount, Long::sum);
    }

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
  }

  @Test
  void flushSumsCountsAndDurationsPerKey() {
    AggregatingPhaseDiagnostics agg = new AggregatingPhaseDiagnostics();
    agg.count("pin.snapshot_calls");
    agg.count("pin.snapshot_calls");
    agg.add("pin.current_snapshot_cache_hits", 3);
    agg.nanos("pin.snapshot_lookup", 10);
    agg.nanos("pin.snapshot_lookup", 25);

    CapturingDiagnostics target = new CapturingDiagnostics();
    agg.flushInto(target);

    assertThat(target.added).containsEntry("pin.snapshot_calls", 2L);
    assertThat(target.added).containsEntry("pin.current_snapshot_cache_hits", 3L);
    assertThat(target.nanos).containsEntry("pin.snapshot_lookup", 35L);
  }

  @Test
  void oneShotAndEmitAreOmitted() {
    AggregatingPhaseDiagnostics agg = new AggregatingPhaseDiagnostics();
    agg.put("k", "v");
    agg.put("n", 1L);
    agg.put("ratio", 1.0d);
    agg.put("enabled", true);
    agg.emit("event");

    CapturingDiagnostics target = new CapturingDiagnostics();
    agg.flushInto(target);

    assertThat(target.added).isEmpty();
    assertThat(target.nanos).isEmpty();
  }

  @Test
  void concurrentWritersAllAggregate() {
    AggregatingPhaseDiagnostics agg = new AggregatingPhaseDiagnostics();
    int writers = 64;
    CompletableFuture<?>[] futures =
        IntStream.range(0, writers)
            .mapToObj(
                i ->
                    CompletableFuture.runAsync(
                        () -> {
                          agg.count("pin.snapshot_calls");
                          agg.nanos("pin.snapshot_lookup", 5);
                        }))
            .toArray(CompletableFuture[]::new);
    CompletableFuture.allOf(futures).join();

    CapturingDiagnostics target = new CapturingDiagnostics();
    agg.flushInto(target);

    assertThat(target.added).containsEntry("pin.snapshot_calls", (long) writers);
    assertThat(target.nanos).containsEntry("pin.snapshot_lookup", (long) writers * 5);
  }

  @Test
  void timerSumsElapsedIntoItsKey() {
    AggregatingPhaseDiagnostics agg = new AggregatingPhaseDiagnostics();
    try (PhaseDiagnostics.Timer ignored = agg.timer("pin.snapshot_lookup")) {
      // close records the elapsed
    }

    CapturingDiagnostics target = new CapturingDiagnostics();
    agg.flushInto(target);

    assertThat(target.nanos).containsKey("pin.snapshot_lookup");
    assertThat(target.nanos.get("pin.snapshot_lookup")).isGreaterThanOrEqualTo(0L);
  }
}
