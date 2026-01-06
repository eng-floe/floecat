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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Test-only provider for supplying fixed builtin catalogs. */
public final class StaticSystemCatalogProvider implements SystemCatalogProvider {

  private final Map<String, SystemCatalogData> catalogs = new HashMap<>();

  public StaticSystemCatalogProvider(Map<String, SystemCatalogData> input) {
    input.forEach((kind, data) -> catalogs.put(kind.toLowerCase(Locale.ROOT), data));
  }

  @Override
  public List<String> engineKinds() {
    return catalogs.keySet().stream().sorted().toList();
  }

  @Override
  public SystemEngineCatalog load(String engineKind) {
    SystemCatalogData data = catalogs.get(engineKind.toLowerCase(Locale.ROOT));
    if (data != null) {
      return SystemEngineCatalog.from(engineKind, data);
    }
    return SystemEngineCatalog.from(engineKind, SystemCatalogData.empty());
  }
}
