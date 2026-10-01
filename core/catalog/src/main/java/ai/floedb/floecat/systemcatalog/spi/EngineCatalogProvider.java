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
import ai.floedb.floecat.systemcatalog.hint.HintClearContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.spi.types.EngineTypeMapper;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;
import java.util.Optional;

/**
 * Engine-owned system-catalog contribution.
 *
 * <p>An implementation may provide materialised {@code SystemCatalogData}, live relation
 * definitions/scanners, or both. Live providers do not copy changing engine types, functions, or
 * system relations into PBtxt files.
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

  /** Globally unique executor/engine identifier. */
  String engineKind();

  /**
   * Returns static builtin catalog data for this engine.
   *
   * <p>Dynamic engine integrations should keep the default empty catalog and implement the live
   * scanner methods inherited from {@link SystemObjectScannerProvider} instead.
   */
  default SystemCatalogData loadSystemCatalog() {
    return SystemCatalogData.empty();
  }

  /** Optional extension-specific validation for static catalog data. */
  default List<ValidationIssue> validate(SystemCatalogData catalog) {
    return List.of();
  }

  /** Optional decorator for engine metadata sinks. */
  default Optional<EngineMetadataDecorator> decorator() {
    return Optional.empty();
  }

  /** Optional policy for clearing engine-specific hints. */
  default HintClearDecision decideHintClear(EngineContext ctx, HintClearContext context) {
    return HintClearDecision.dropAll();
  }

  /** Optional error hook for extension loading diagnostics. */
  default void onLoadError(Exception e) {}

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
