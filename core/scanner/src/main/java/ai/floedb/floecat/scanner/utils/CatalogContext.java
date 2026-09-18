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

  /** Returns the empty catalog context. */
  public static CatalogContext empty() {
    return new CatalogContext(EnvironmentContext.empty(), EngineContext.empty());
  }

  /** Returns the explicit internal catalog selection used by legacy request adapters. */
  public static CatalogContext floecatInternal() {
    return new CatalogContext(
        EnvironmentContext.empty(),
        EngineContext.of(EngineCatalogNames.FLOECAT_DEFAULT_CATALOG, ""));
  }

  /** Adapts an engine-only request boundary to an explicit catalog selection. */
  public static CatalogContext forEngine(EngineContext engine) {
    Objects.requireNonNull(engine, "engine");
    return engine.hasEngineKind() ? of(EnvironmentContext.empty(), engine) : floecatInternal();
  }

  /** Selects the environment and engine for a catalog operation. */
  public static CatalogContext of(EnvironmentContext environment, EngineContext engine) {
    return new CatalogContext(
        Objects.requireNonNull(environment, "environment"),
        Objects.requireNonNull(engine, "engine"));
  }
}
