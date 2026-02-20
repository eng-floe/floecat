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

package ai.floedb.floecat.systemcatalog.provider;

import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.scanner.utils.EngineContextNormalizer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Test-only provider for supplying fixed builtin catalogs. */
public final class StaticSystemCatalogProvider implements SystemCatalogProvider {

  private final Map<String, SystemCatalogData> catalogs = new HashMap<>();

  public StaticSystemCatalogProvider(Map<String, SystemCatalogData> input) {
    input.forEach(
        (kind, data) -> catalogs.put(EngineContextNormalizer.normalizeEngineKind(kind), data));
  }

  @Override
  public List<String> engineKinds() {
    return catalogs.keySet().stream().sorted().toList();
  }

  @Override
  public SystemEngineCatalog load(EngineContext ctx) {
    EngineContext canonical = ctx == null ? EngineContext.empty() : ctx;
    String normalized = canonical.effectiveEngineKind();
    SystemCatalogData data = catalogs.get(normalized);
    return SystemEngineCatalog.from(normalized, data != null ? data : SystemCatalogData.empty());
  }
}
