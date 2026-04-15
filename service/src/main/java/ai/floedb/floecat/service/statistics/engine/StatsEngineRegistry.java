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

import ai.floedb.floecat.stats.spi.StatsCaptureBatchItemResult;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureBatchResult;
import ai.floedb.floecat.stats.spi.StatsCaptureEngine;
import ai.floedb.floecat.stats.spi.StatsCaptureRequest;
import ai.floedb.floecat.stats.spi.StatsCaptureResult;
import ai.floedb.floecat.stats.spi.StatsTargetType;
import ai.floedb.floecat.stats.spi.StatsUnsupportedTargetException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;

/** Registry and routing layer for available stats capture engines. */
@ApplicationScoped
public class StatsEngineRegistry {

  private static final Logger LOG = Logger.getLogger(StatsEngineRegistry.class);

  private final List<StatsCaptureEngine> captureEngines;

  /** CDI constructor used in production; discovered engines are sorted by priority then id. */
  @Inject
  public StatsEngineRegistry(Instance<StatsCaptureEngine> captureEngines) {
    this(captureEngines == null ? List.of() : captureEngines.stream().toList());
  }

  /** Testing/override constructor; engines are sorted by priority then id. */
  public StatsEngineRegistry(List<StatsCaptureEngine> captureEngines) {
    this.captureEngines =
        (captureEngines == null ? List.<StatsCaptureEngine>of() : captureEngines)
            .stream()
                .sorted(
                    Comparator.comparingInt(StatsCaptureEngine::priority)
                        .thenComparing(StatsCaptureEngine::id))
                .toList();
  }

  /** Returns all registered engines sorted by priority then id. */
  public List<StatsCaptureEngine> engines() {
    return captureEngines;
  }

  /** Returns request-compatible engines after applying {@link StatsCaptureEngine#supports}. */
  public List<StatsCaptureEngine> candidates(StatsCaptureRequest request) {
    return captureEngines.stream().filter(e -> e.supports(request)).toList();
  }

  /**
   * Routes the request by capability and priority. The first engine returning a result wins.
   * Engines may return empty when data is unavailable for the request.
   */
  public Optional<StatsCaptureResult> capture(StatsCaptureRequest request) {
    List<StatsCaptureEngine> candidates = candidates(request);
    if (candidates.isEmpty()) {
      throw new StatsUnsupportedTargetException(StatsTargetType.from(request.target()), request);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debugf(
          "Stats capture routed corr=%s target=%s candidates=%s",
          request.correlationId(),
          StatsTargetType.from(request.target()),
          candidates.stream().map(StatsCaptureEngine::id).toList());
    }
    for (StatsCaptureEngine engine : candidates) {
      Optional<StatsCaptureResult> out = engine.capture(request);
      if (out.isPresent()) {
        return out;
      }
      if (LOG.isTraceEnabled()) {
        LOG.tracef(
            "Stats engine %s returned empty corr=%s target=%s",
            engine.id(), request.correlationId(), request.target());
      }
    }
    return Optional.empty();
  }

  /**
   * Routes each request in the batch independently using the same capability + priority policy as
   * {@link #capture(StatsCaptureRequest)}.
   */
  public StatsCaptureBatchResult captureBatch(StatsCaptureBatchRequest batchRequest) {
    List<StatsCaptureRequest> requests = batchRequest.requests();
    List<StatsCaptureBatchItemResult> finalResults = new ArrayList<>(requests.size());
    for (int i = 0; i < requests.size(); i++) {
      finalResults.add(null);
    }

    List<List<StatsCaptureEngine>> candidateLists = new ArrayList<>(requests.size());
    Map<Integer, Integer> pending = new LinkedHashMap<>();
    for (int i = 0; i < requests.size(); i++) {
      StatsCaptureRequest request = requests.get(i);
      List<StatsCaptureEngine> candidates = candidates(request);
      candidateLists.add(candidates);
      if (candidates.isEmpty()) {
        finalResults.set(
            i, StatsCaptureBatchItemResult.uncapturable(request, "target unsupported"));
      } else {
        pending.put(i, 0);
      }
    }

    while (!pending.isEmpty()) {
      Map<StatsCaptureEngine, List<Integer>> stageGroups = new LinkedHashMap<>();
      List<Integer> exhausted = new ArrayList<>();
      for (Map.Entry<Integer, Integer> entry : pending.entrySet()) {
        int requestIndex = entry.getKey();
        int stage = entry.getValue();
        List<StatsCaptureEngine> candidates = candidateLists.get(requestIndex);
        if (stage >= candidates.size()) {
          exhausted.add(requestIndex);
          continue;
        }
        stageGroups
            .computeIfAbsent(candidates.get(stage), ignored -> new ArrayList<>())
            .add(requestIndex);
      }

      for (Integer requestIndex : exhausted) {
        StatsCaptureRequest request = requests.get(requestIndex);
        finalResults.set(
            requestIndex, StatsCaptureBatchItemResult.uncapturable(request, "no capture result"));
        pending.remove(requestIndex);
      }

      for (Map.Entry<StatsCaptureEngine, List<Integer>> group : stageGroups.entrySet()) {
        StatsCaptureEngine engine = group.getKey();
        List<Integer> indexes = group.getValue();
        StatsCaptureBatchRequest engineBatch =
            StatsCaptureBatchRequest.of(indexes.stream().map(requests::get).toList());
        StatsCaptureBatchResult engineResult;
        try {
          engineResult = engine.captureBatch(engineBatch);
        } catch (RuntimeException e) {
          for (Integer requestIndex : indexes) {
            StatsCaptureRequest request = requests.get(requestIndex);
            finalResults.set(
                requestIndex,
                StatsCaptureBatchItemResult.degraded(
                    request, "capture failed: " + e.getClass().getSimpleName()));
            pending.remove(requestIndex);
          }
          continue;
        }

        List<StatsCaptureBatchItemResult> items = engineResult.results();
        if (items.size() != indexes.size()) {
          for (Integer requestIndex : indexes) {
            StatsCaptureRequest request = requests.get(requestIndex);
            finalResults.set(
                requestIndex, StatsCaptureBatchItemResult.degraded(request, "batch size mismatch"));
            pending.remove(requestIndex);
          }
          continue;
        }

        for (int i = 0; i < indexes.size(); i++) {
          int requestIndex = indexes.get(i);
          StatsCaptureBatchItemResult item = items.get(i);
          switch (item.outcome()) {
            case CAPTURED, QUEUED, DEGRADED -> {
              finalResults.set(requestIndex, item);
              pending.remove(requestIndex);
            }
            case UNCAPTURABLE -> {
              pending.computeIfPresent(requestIndex, (ignored, stage) -> stage + 1);
            }
          }
        }
      }
    }

    return StatsCaptureBatchResult.of(finalResults);
  }
}
