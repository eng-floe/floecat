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

package ai.floedb.floecat.systemcatalog.registry;

import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides cached access to builtin catalogs for each engine kind. Delegates loading to a
 * SystemCatalogProvider.
 */
public final class SystemDefinitionRegistry {

  private final SystemCatalogProvider provider;
  private final ConcurrentMap<String, SystemEngineCatalog> cache = new ConcurrentHashMap<>();

  public SystemDefinitionRegistry(SystemCatalogProvider provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  public SystemEngineCatalog catalog(EngineContext ctx) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;
    String requestedKey = canonical.effectiveEngineKind();
    SystemEngineCatalog existing = cache.get(requestedKey);
    if (existing != null) {
      return existing;
    }
    SystemEngineCatalog loaded = provider.load(canonical);
    String resolvedKey = loaded.engineKind();
    SystemEngineCatalog winner = cache.computeIfAbsent(resolvedKey, ignored -> loaded);
    if (!resolvedKey.equals(requestedKey)) {
      cache.putIfAbsent(requestedKey, winner);
    }
    return winner;
  }

  /** Test-only: clears catalog cache. */
  public void clear() {
    cache.clear();
  }

  public List<String> engineKinds() {
    return provider.engineKinds();
  }
}
