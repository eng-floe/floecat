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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BoundedFanoutTest {

  @Test
  void resultsAreReturnedInInputOrder() {
    List<Integer> inputs = IntStream.range(0, 50).boxed().toList();
    // Reverse the completion order (later items sleep less) to prove ordering is by input, not
    // completion.
    List<Integer> out =
        BoundedFanout.mapOrdered(
            inputs,
            8,
            ForkJoinPool.commonPool(),
            i -> {
              try {
                Thread.sleep((50 - i) % 5);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return i * 10;
            });
    assertThat(out).isEqualTo(inputs.stream().map(i -> i * 10).toList());
  }

  @Test
  void neverRunsMoreThanPermitsAtOnce() {
    int permits = 3;
    AtomicInteger inFlight = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    BoundedFanout.mapOrdered(
        IntStream.range(0, 40).boxed().toList(),
        permits,
        ForkJoinPool.commonPool(),
        i -> {
          int now = inFlight.incrementAndGet();
          peak.accumulateAndGet(now, Math::max);
          try {
            Thread.sleep(2);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          } finally {
            inFlight.decrementAndGet();
          }
          return i;
        });
    assertThat(peak.get()).isLessThanOrEqualTo(permits);
  }

  @Test
  void taskFailureSurfacesUnwrapped() {
    assertThatThrownBy(
            () ->
                BoundedFanout.mapOrdered(
                    List.of(1, 2, 3),
                    4,
                    ForkJoinPool.commonPool(),
                    i -> {
                      if (i == 2) {
                        throw new IllegalStateException("boom");
                      }
                      return i;
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("boom");
  }
}
