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

/** Test-only environment provider used to verify service-loader discovery. */
public final class DynamicTestEnvironmentProvider implements CatalogEnvironmentProvider {

  @Override
  public String environmentKind() {
    return "test-env";
  }

  @Override
  public List<SystemObjectDef> definitions(CatalogContext context) {
    return List.of();
  }

  @Override
  public boolean supports(NameRef name, CatalogContext context) {
    return true;
  }

  @Override
  public Optional<SystemObjectScanner> provide(String scannerId, CatalogContext context) {
    return Optional.empty();
  }
}
