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

import ai.floedb.floecat.scanner.utils.EngineContext;
import ai.floedb.floecat.systemcatalog.registry.SystemEngineCatalog;
import java.util.List;

/**
 * Provides system objects metadata catalogs per engine.
 *
 * <p>Used by SystemDefinitionRegistry. Production uses the ServiceLoader-based implementation,
 * while tests can supply their own static catalogs.
 */
public interface SystemCatalogProvider {

  /** Loads the catalog for the given engine context. */
  SystemEngineCatalog load(EngineContext ctx);

  List<String> engineKinds();
}
