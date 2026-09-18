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

package ai.floedb.floecat.systemcatalog.spi;

import ai.floedb.floecat.engine.util.EngineIdentityNormalizer;
import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import java.util.List;

/**
 * Live system-catalog contribution from an engine.
 *
 * <p>This SPI is deliberately separate from {@link EngineSystemCatalogExtension}. The older
 * extension loads a materialised {@code SystemCatalogData}, which is useful for Floecat-owned
 * static definitions. An engine integration such as DuckDB should not have to copy its changing
 * builtin types, functions, or system relations into PBtxt files.
 *
 * <p>Implementations are expected to obtain engine-owned metadata through their runtime bridge. The
 * inherited version-aware {@code definitions(...)} and {@code provide(...)} methods are called
 * while building the requested catalog, so the provider remains the source of truth for the
 * engine's current metadata. Floecat does not snapshot or persist the result of this SPI.
 *
 * <p>The selected catalog environment is intentionally not part of this interface. The provider
 * describes the executor identified by {@link #engineKind()}; environment-owned catalog shape is
 * selected by the catalog composition layer.
 */
public interface EngineCatalogProvider extends SystemObjectScannerProvider {

  /** Globally unique executor/engine identifier, for example {@code duckdb}. */
  String engineKind();

  /**
   * Returns the mapper for the selected engine version.
   *
   * <p>The default keeps providers that only contribute system relations small. A provider that
   * maps Floecat logical types into engine-specific type nodes should override this method.
   */
  default EngineTypeMapper typeMapper(EngineContext engine) {
    return EngineTypeMapper.EMPTY;
  }

  /**
   * Dynamic providers normally have no checked-in object definitions.
   *
   * <p>They can override the inherited version-aware method when the engine exposes relation
   * definitions dynamically. Returning an empty list is the intended default for a provider whose
   * relation shape is supplied by the environment layer.
   */
  @Override
  default List<SystemObjectDef> definitions() {
    return List.of();
  }

  /** Returns whether this provider handles the supplied engine identifier. */
  @Override
  default boolean supportsEngine(String engineKind) {
    return EngineIdentityNormalizer.normalizeEngineKind(engineKind)
        .equals(EngineIdentityNormalizer.normalizeEngineKind(engineKind()));
  }
}
