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
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import java.util.List;
import java.util.Optional;

/**
 * SPI for internal or engine-owned system object definitions and scanners.
 *
 * <p>Definitions returned by this SPI are merged for the supplied catalog context. Providers may
 * build them from static resources or from live engine metadata; the catalog model does not
 * distinguish those implementations. The internal provider is selected separately; engine providers
 * do not inherit or override its definitions. Environment-owned definitions and scanners use {@link
 * CatalogEnvironmentProvider} instead.
 */
public interface SystemObjectScannerProvider {

  /** Definitions provided by this provider for the supplied catalog context. */
  List<SystemObjectDef> definitions(CatalogContext context);

  /** Checks if this provider is selected for the supplied catalog context. */
  default boolean supports(CatalogContext context) {
    return context != null;
  }

  /** Checks if this provider owns a named object in the supplied catalog context. */
  default boolean supports(NameRef name, CatalogContext context) {
    return supports(context);
  }

  /** Resolves a scanner by scanner id for the supplied catalog context. */
  Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context);
}
