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

package ai.floedb.floecat.service.testsupport;

import ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry;
import ai.floedb.floecat.systemcatalog.provider.SystemCatalogProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemDefinitionRegistry;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.systemcatalog.util.EngineCatalogNames;
import ai.floedb.floecat.systemcatalog.util.EngineContextNormalizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test-only SystemNodeRegistry backed by an in-memory SystemCatalogProvider.
 *
 * <p>Uses the real SystemNodeRegistry + SystemDefinitionRegistry logic.
 */
public final class FakeSystemNodeRegistry extends SystemNodeRegistry {

  private final FakeSystemCatalogProvider provider;

  public FakeSystemNodeRegistry() {
    this(new FakeSystemCatalogProvider());
  }

  private FakeSystemNodeRegistry(FakeSystemCatalogProvider provider) {
    super(new SystemDefinitionRegistry(provider));
    this.provider = provider;
  }

  /**
   * Registers a synthetic system catalog for an engine kind.
   *
   * <p>Engine versions are still filtered later by SystemNodeRegistry.
   */
  public FakeSystemNodeRegistry register(String engineKind, SystemCatalogData catalogData) {

    provider.register(engineKind, catalogData);
    return this;
  }

  /* ----------------------------------------------------------------------
   * Fake provider
   * ---------------------------------------------------------------------- */

  private static final class FakeSystemCatalogProvider implements SystemCatalogProvider {

    private final Map<String, SystemEngineCatalog> catalogs = new HashMap<>();

    void register(String engineKind, SystemCatalogData data) {
      String key = EngineContextNormalizer.normalizeEngineKind(engineKind);
      catalogs.put(key, SystemEngineCatalog.from(key, data));
    }

    @Override
    public SystemEngineCatalog load(String engineKind) {
      String key = EngineContextNormalizer.normalizeEngineKind(engineKind);
      if (key.isBlank()) {
        key = EngineCatalogNames.FLOECAT_DEFAULT_CATALOG;
      }
      SystemEngineCatalog catalog = catalogs.get(key);
      if (catalog == null) {
        throw new IllegalStateException(
            "No fake system catalog registered for engine: " + engineKind);
      }
      return catalog;
    }

    @Override
    public List<String> engineKinds() {
      return List.copyOf(catalogs.keySet());
    }
  }
}
