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

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.def.SystemTypeDef;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.spi.EngineSystemCatalogExtension;
import java.util.List;
import java.util.Optional;

/** Test-only extension that intentionally returns invalid catalog data. */
public final class InvalidCatalogExtension implements EngineSystemCatalogExtension {

  static final String ENGINE_KIND = "invalid_engine";

  @Override
  public String engineKind() {
    return ENGINE_KIND;
  }

  @Override
  public SystemCatalogData loadSystemCatalog() {
    // Invalid by design: namespace-scoped type name is unqualified.
    return new SystemCatalogData(
        List.of(),
        List.of(),
        List.of(
            new SystemTypeDef(
                NameRef.newBuilder().setName("unqualified_type").build(),
                "S",
                false,
                null,
                List.of())),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of());
  }

  @Override
  public List<SystemObjectDef> definitions() {
    return List.of();
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return ENGINE_KIND.equals(engineKind);
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    return supportsEngine(engineKind);
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return Optional.empty();
  }
}
