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

package ai.floedb.floecat.service.statistics.engine;

import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/** Registry and routing layer for available stats capture engines. */
@ApplicationScoped
public class StatsEngineRegistry {

  private static final Logger LOG = Logger.getLogger(StatsEngineRegistry.class);

  private final List<StatsCaptureEngine> engines;

  @Inject
  public StatsEngineRegistry(Instance<StatsCaptureEngine> engines) {
    this(engines == null ? List.of() : engines.stream().toList());
  }

  public StatsEngineRegistry(List<StatsCaptureEngine> engines) {
    this.engines =
        (engines == null ? List.<StatsCaptureEngine>of() : engines)
            .stream()
                .sorted(
                    Comparator.comparingInt(StatsCaptureEngine::priority)
                        .thenComparing(StatsCaptureEngine::id))
                .toList();
  }

  public List<StatsCaptureEngine> engines() {
    return engines;
  }

  public List<StatsCaptureEngine> candidates(StatsCaptureRequest request) {
    return engines.stream().filter(e -> e.supports(request)).toList();
  }

  /**
   * Routes the request by capability and priority. The first engine returning a result wins.
   * Engines may return empty when data is unavailable for the request.
   */
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    for (StatsCaptureEngine engine : candidates(request)) {
      Optional<StatsCaptureResult> out = engine.capture(request);
      if (out.isPresent()) {
        return out;
      }
      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "Stats engine %s returned empty for request target=%s", engine.id(), request.target());
      }
    }
    return Optional.empty();
  }
}
