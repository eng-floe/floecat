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

import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearContext;
import ai.floedb.floecat.systemcatalog.hint.HintClearDecision;
import ai.floedb.floecat.systemcatalog.provider.SystemObjectScannerProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.spi.decorator.EngineMetadataDecorator;
import ai.floedb.floecat.systemcatalog.validation.ValidationIssue;
import java.util.List;
import java.util.Optional;

/**
 * SPI for engine-specific builtin catalogs.
 *
 * <p>Implemented by plugin JARs discovered via Java ServiceLoader.
 */
public interface EngineSystemCatalogExtension extends SystemObjectScannerProvider {

  /** Globally unique engine identifier (e.g. "floe", "postgres", "trino"). */
  String engineKind();

  /** Returns builtin catalog data for current engine kind. */
  SystemCatalogData loadSystemCatalog();

  /**
   * Optional hook: plugin can validate itself or emit diagnostics. Floecat will ignore exceptions
   * by default.
   */
  default void onLoadError(Exception e) {
    // default: no-op
  }

  /** Optional extension-specific validation that runs as soon as the catalog is loaded. */
  default List<ValidationIssue> validate(SystemCatalogData catalog) {
    return List.of();
  }

  /** Optional decorator for engine metadata sinks. */
  default Optional<EngineMetadataDecorator> decorator() {
    return Optional.empty();
  }

  /**
   * Optional hook that lets the engine decide how to clear persisted hints for a particular update.
   */
  default HintClearDecision decideHintClear(EngineContext ctx, HintClearContext context) {
    return HintClearDecision.dropAll();
  }
}
