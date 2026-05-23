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

package ai.floedb.floecat.reconciler.spi.capture;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/** Registry and routing layer for unified capture engines. */
@ApplicationScoped
public class CaptureEngineRegistry {
  private static final Logger LOG = Logger.getLogger(CaptureEngineRegistry.class);
  private final List<CaptureEngine> captureEngines;

  @Inject
  public CaptureEngineRegistry(Instance<CaptureEngine> captureEngines) {
    this(captureEngines == null ? List.of() : captureEngines.stream().toList());
  }

  public CaptureEngineRegistry(List<CaptureEngine> captureEngines) {
    this.captureEngines =
        (captureEngines == null ? List.<CaptureEngine>of() : captureEngines)
            .stream()
                .sorted(
                    Comparator.comparingInt(CaptureEngine::priority)
                        .thenComparing(CaptureEngine::id))
                .toList();
  }

  public List<CaptureEngine> engines() {
    return captureEngines;
  }

  public List<CaptureEngine> candidates(CaptureEngineRequest request) {
    return captureEngines.stream().filter(engine -> engine.supports(request)).toList();
  }

  public Optional<CaptureEngine> select(CaptureEngineRequest request) {
    return candidates(request).stream().findFirst();
  }

  /**
   * Dispatches the request to the first eligible engine that fits within the cost budget.
   *
   * <p>Before calling {@code engine.capture()}, this method checks {@code
   * engine.estimatedCost(request).fitsIn(request.maxCost())}. Engines whose estimated cost exceeds
   * the budget are skipped silently; the caller receives an empty result rather than a failure.
   * This enforces the {@link ai.floedb.floecat.stats.spi.JobCostHint} contract set by the
   * originating {@code ReconcileCapturePolicy.maxCost()} (e.g. {@code MEDIUM} on sync-capture paths
   * to prevent expensive full-column scans from blocking query planning).
   */
  public CaptureEngineResult capture(CaptureEngineRequest request) {
    List<CaptureEngine> eligible = candidates(request);
    if (eligible.isEmpty()) {
      LOG.warnf(
          "capture engine registry has no eligible engine for source=%s.%s snapshotId=%d requestedKinds=%s pageIndex=%s maxCost=%s enginesRegistered=%d",
          request == null ? "" : request.sourceNamespace(),
          request == null ? "" : request.sourceTable(),
          request == null ? -1L : request.snapshotId(),
          request == null ? List.of() : request.requestedStatsTargetKinds(),
          request != null && request.capturePageIndex(),
          request == null ? null : request.maxCost(),
          captureEngines.size());
    }
    for (CaptureEngine engine : eligible) {
      if (!engine.estimatedCost(request).fitsIn(request.maxCost())) {
        continue;
      }
      Optional<CaptureEngineResult> result = engine.capture(request);
      if (result.isPresent()) {
        return result.get();
      }
    }
    return CaptureEngineResult.empty();
  }
}
