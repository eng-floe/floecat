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

package ai.floedb.floecat.scanner.utils;

import java.util.Objects;

/** The selected environment and engine for one catalog operation. */
public record CatalogContext(EnvironmentContext environment, EngineContext engine) {

  public CatalogContext {
    environment = Objects.requireNonNull(environment, "environment");
    engine = Objects.requireNonNull(engine, "engine");
  }

  /**
   * Selects an environment and engine while preserving the current single-engine default: when no
   * environment is supplied, the environment follows the engine.
   */
  public static CatalogContext of(EnvironmentContext environment, EngineContext engine) {
    EngineContext selectedEngine = engine == null ? EngineContext.empty() : engine;
    EnvironmentContext selectedEnvironment = environment;
    if (selectedEnvironment == null || !selectedEnvironment.hasEnvironmentKind()) {
      selectedEnvironment =
          EnvironmentContext.of(selectedEngine.engineKind(), selectedEngine.engineVersion());
    }
    return new CatalogContext(selectedEnvironment, selectedEngine);
  }
}
