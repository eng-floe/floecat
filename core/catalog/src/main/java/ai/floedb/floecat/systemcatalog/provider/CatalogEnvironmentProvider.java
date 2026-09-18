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
import ai.floedb.floecat.engine.util.EngineIdentityNormalizer;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.scanner.utils.EnvironmentContext;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import java.util.List;
import java.util.Optional;

/**
 * Supplies system relations owned by a catalog environment.
 *
 * <p>The selected engine remains available through {@link CatalogContext}; the environment owns
 * relation shape and scanner behavior. Engine capabilities belong to {@link
 * ai.floedb.floecat.systemcatalog.spi.EngineCatalogProvider}.
 */
public interface CatalogEnvironmentProvider extends SystemObjectScannerProvider {

  /** Stable identifier of the environment supplied by this provider. */
  String environmentKind();

  /** Returns whether this provider is selected for the supplied environment. */
  default boolean supportsEnvironment(EnvironmentContext environment) {
    return environment != null
        && environment
            .normalizedKind()
            .equals(EngineIdentityNormalizer.normalizeEngineKind(environmentKind()));
  }

  /** Returns definitions for the complete catalog context. */
  default List<SystemObjectDef> definitions(CatalogContext context) {
    CatalogContext selected = context == null ? CatalogContext.of(null, null) : context;
    return definitions(
        selected.engine().effectiveEngineKind(), selected.engine().normalizedVersion());
  }

  /** Returns whether this provider owns the named object in the complete catalog context. */
  default boolean supports(NameRef name, CatalogContext context) {
    CatalogContext selected = context == null ? CatalogContext.of(null, null) : context;
    return supports(
        name, selected.engine().effectiveEngineKind(), selected.engine().normalizedVersion());
  }

  /** Resolves a scanner for the complete catalog context. */
  default Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
    CatalogContext selected = context == null ? CatalogContext.of(null, null) : context;
    return provide(
        scannerId, selected.engine().effectiveEngineKind(), selected.engine().normalizedVersion());
  }
}
