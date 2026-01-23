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
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.engine.EngineSpecificRule;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;

/**
 * SPI for built-in and plugin providers of system objects scanner.
 *
 * <p>All definitions returned by this SPI are merged into {@link
 * ai.floedb.floecat.systemcatalog.graph.SystemNodeRegistry#mergeCatalogData}, which seeds every
 * request with the shared {@link ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider}
 * (the `floecat_internal` base). Plugin and overlay providers should expect their canonical names
 * to override the base definitions when an engine header is supplied.
 *
 * <p>If your definitions vary by version (via {@link #definitions(String, String)}), the
 * corresponding {@link #provide(String, String, String)} call must resolve the `scannerId` to a
 * scanner that produces rows matching the schema returned for the same `(engineKind,
 * engineVersion)` tuple.
 */
public interface SystemObjectScannerProvider {

  /** All definitions provided by this provider (no filtering). */
  List<SystemObjectDef> definitions();

  /** Checks if this provider supports the engine kind. */
  boolean supportsEngine(String engineKind);

  /** Checks if this provider supports a given object based on a NameRef lookup. */
  boolean supports(NameRef name, String engineKind);

  /**
   * Version-aware definition set; defaults to {@link #definitions()} when a provider doesn't care
   * about versions.
   */
  default List<SystemObjectDef> definitions(String engineKind, String engineVersion) {
    return definitions();
  }

  /**
   * Version-aware support check.
   *
   * <p>Defaults to {@link #supports(NameRef, String)} when versions are irrelevant.
   */
  default boolean supports(NameRef name, String engineKind, String engineVersion) {
    return supports(name, engineKind);
  }

  /** Resolves scanner by scannerId (for SystemObjectNode lookups). */
  Optional<SystemObjectScanner> provide(String scannerId, String engineKind, String engineVersion);

  /**
   * Registry-level engine hints provided per engine/version.
   *
   * <p>Defaults to empty for providers that don't publish any registry payloads.
   */
  default List<EngineSpecificRule> registryEngineSpecific(String engineKind, String engineVersion) {
    return List.of();
  }
}
