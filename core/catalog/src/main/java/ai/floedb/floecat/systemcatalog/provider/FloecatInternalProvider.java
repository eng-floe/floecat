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
import ai.floedb.floecat.systemcatalog.informationschema.InformationSchemaProvider;
import ai.floedb.floecat.systemcatalog.spi.scanner.SystemObjectScanner;
import java.util.List;
import java.util.Optional;

/**
 * Provider for the floecat_internal catalog layer.
 *
 * <p>All resolved catalogs are seeded from this layer so the shared information_schema objects are
 * always present, even when the client targets another engine kind. The provider advertises support
 * for every engine kind and delegates to InformationSchemaProvider so consumers can resolve the
 * same scanners regardless of the requested engine.
 */
public final class FloecatInternalProvider implements SystemObjectScannerProvider {

  private final InformationSchemaProvider informationSchema = new InformationSchemaProvider();

  @Override
  public List<SystemObjectDef> definitions() {
    return informationSchema.definitions();
  }

  @Override
  public boolean supportsEngine(String engineKind) {
    return true;
  }

  @Override
  public boolean supports(NameRef name, String engineKind) {
    return informationSchema.supports(name, engineKind);
  }

  @Override
  public Optional<SystemObjectScanner> provide(
      String scannerId, String engineKind, String engineVersion) {
    return informationSchema.provide(scannerId, engineKind, engineVersion);
  }
}
