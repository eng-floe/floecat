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

package ai.floedb.floecat.service.it;

import ai.floedb.floecat.common.rpc.NameRef;
import ai.floedb.floecat.scanner.spi.SystemObjectScanner;
import ai.floedb.floecat.scanner.utils.CatalogContext;
import ai.floedb.floecat.systemcatalog.def.SystemObjectDef;
import ai.floedb.floecat.systemcatalog.provider.CatalogEnvironmentProvider;
import ai.floedb.floecat.systemcatalog.provider.FloecatInternalProvider;
import ai.floedb.floecat.systemcatalog.registry.SystemCatalogData;
import ai.floedb.floecat.systemcatalog.util.NameRefUtil;
import java.util.List;
import java.util.Optional;

/** Test-only environment that owns the shared {@code information_schema} relation definitions. */
public final class TestCatalogEnvironmentProvider implements CatalogEnvironmentProvider {

  public static final String ENVIRONMENT_KIND = "test-environment";

  private static final List<SystemObjectDef> DEFINITIONS = informationSchemaDefinitions();

  @Override
  public String environmentKind() {
    return ENVIRONMENT_KIND;
  }

  @Override
  public List<SystemObjectDef> definitions(CatalogContext context) {
    return DEFINITIONS;
  }

  @Override
  public boolean supports(NameRef name, CatalogContext context) {
    return NameRefUtil.canonical(name).equals("information_schema")
        || NameRefUtil.namespaceCanonical(name).equals("information_schema");
  }

  @Override
  public Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
    // SystemScannerResolver supplies the shared information_schema scanners when the selected
    // environment does not override them. This fixture intentionally exercises that reuse path.
    return Optional.empty();
  }

  private static List<SystemObjectDef> informationSchemaDefinitions() {
    SystemCatalogData catalog = FloecatInternalProvider.catalogData();
    return java.util.stream.Stream.concat(
            catalog.namespaces().stream()
                .filter(def -> NameRefUtil.canonical(def.name()).equals("information_schema")),
            catalog.tables().stream()
                .filter(
                    def -> NameRefUtil.namespaceCanonical(def.name()).equals("information_schema")))
        .map(def -> (SystemObjectDef) def)
        .toList();
  }
}
