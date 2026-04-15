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

package ai.floedb.floecat.stats.spi.testing;

import ai.floedb.floecat.stats.spi.StatsCapabilities;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Minimal test helper for stats engine routing tests and examples.
 *
 * <p>This helper is intentionally simple and deterministic. It is useful for OSS contributors who
 * want to test custom engine routing without implementing a full provider.
 */
public final class TestStatsCaptureEngine implements StatsCaptureEngine {
  private final String id;
  private final int priority;
  private final StatsCapabilities capabilities;
  private final Function<StatsCaptureRequest, Optional<StatsCaptureResult>> captureFn;

  private TestStatsCaptureEngine(
      String id,
      int priority,
      StatsCapabilities capabilities,
      Function<StatsCaptureRequest, Optional<StatsCaptureResult>> captureFn) {
    this.id = Objects.requireNonNull(id, "id");
    this.priority = priority;
    this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
    this.captureFn = Objects.requireNonNull(captureFn, "captureFn");
  }

  public static Builder builder(String id) {
    return new Builder(id);
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public int priority() {
    return priority;
  }

  @Override
  public StatsCapabilities capabilities() {
    return capabilities;
  }

  @Override
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    return captureFn.apply(request);
  }

  @Override
  public StatsCaptureBatchResult captureBatch(StatsCaptureBatchRequest batchRequest) {
    return StatsCaptureBatchResult.of(
        batchRequest.requests().stream()
            .map(
                request -> {
                  if (!supports(request)) {
                    return StatsCaptureBatchItemResult.uncapturable(
                        request, "unsupported by engine");
                  }
                  try {
                    return captureFn
                        .apply(request)
                        .map(result -> StatsCaptureBatchItemResult.captured(request, result))
                        .orElseGet(
                            () ->
                                StatsCaptureBatchItemResult.uncapturable(
                                    request, "no capture result"));
                  } catch (RuntimeException e) {
                    return StatsCaptureBatchItemResult.degraded(
                        request, "capture failed: " + e.getClass().getSimpleName());
                  }
                })
            .collect(Collectors.toList()));
  }

  public static final class Builder {
    private final String id;
    private int priority = 100;
    private StatsCapabilities capabilities = StatsCapabilities.matchAll();
    private Function<StatsCaptureRequest, Optional<StatsCaptureResult>> captureFn =
        req -> Optional.empty();

    private Builder(String id) {
      this.id = id;
    }

    public Builder priority(int priority) {
      this.priority = priority;
      return this;
    }

    public Builder capabilities(StatsCapabilities capabilities) {
      this.capabilities = Objects.requireNonNull(capabilities, "capabilities");
      return this;
    }

    public Builder fixed(Optional<StatsCaptureResult> out) {
      Optional<StatsCaptureResult> copy = out == null ? Optional.empty() : out;
      this.captureFn = req -> copy;
      return this;
    }

    public Builder captureFn(
        Function<StatsCaptureRequest, Optional<StatsCaptureResult>> captureFn) {
      this.captureFn = Objects.requireNonNull(captureFn, "captureFn");
      return this;
    }

    public TestStatsCaptureEngine build() {
      return new TestStatsCaptureEngine(id, priority, capabilities, captureFn);
    }
  }
}
