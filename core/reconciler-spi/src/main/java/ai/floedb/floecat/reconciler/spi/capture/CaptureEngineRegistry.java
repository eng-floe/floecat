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

/** Registry and routing layer for unified capture engines. */
@ApplicationScoped
public class CaptureEngineRegistry {
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

  public CaptureEngineResult capture(CaptureEngineRequest request) {
    for (CaptureEngine engine : candidates(request)) {
      Optional<CaptureEngineResult> result = engine.capture(request);
      if (result.isPresent()) {
        return result.get();
      }
    }
    return CaptureEngineResult.empty();
  }
}
